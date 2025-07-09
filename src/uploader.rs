use crate::{models::Envelope, Error, HttpClient};
use backon::{Backoff, BackoffBuilder, Retryable};
use bytes::Bytes;
use flate2::{write::GzEncoder, Compression};
use http::{Request, Response, Uri};
use serde::Deserialize;
use std::io::Write;

const STATUS_OK: u16 = 200;
const STATUS_PARTIAL_CONTENT: u16 = 206;
const STATUS_REQUEST_TIMEOUT: u16 = 408;
const STATUS_TOO_MANY_REQUESTS: u16 = 429;
const STATUS_APPLICATION_INACTIVE: u16 = 439; // Quota
const STATUS_INTERNAL_SERVER_ERROR: u16 = 500;
const STATUS_SERVICE_UNAVAILABLE: u16 = 503;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Transmission {
    items_received: usize,
    items_accepted: usize,
    errors: Vec<TransmissionItem>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransmissionItem {
    status_code: u16,
}

/// Sends a telemetry items to the server.
pub(crate) async fn send<B>(
    client: &dyn HttpClient,
    endpoint: &Uri,
    items: Vec<Envelope>,
    backoff: Option<B>,
) -> Result<(), Error>
where
    B: BackoffBuilder + Clone + Send + Sync + 'static,
    B::Backoff: Backoff + Send + 'static,
{
    let payload = Bytes::from(serialize_envelopes(items)?);

    let operation = async || {
        let request = Request::post(endpoint)
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(http::header::CONTENT_ENCODING, "gzip")
            .body(payload.clone())
            .expect("request should be valid");

        let response = client
            .send_bytes(request)
            .await
            .map_err(Error::UploadConnection)?;

        handle_response(response)
    };

    if let Some(backoff) = backoff {
        operation.retry(backoff).when(can_retry_operation).await
    } else {
        operation().await
    }
}

fn serialize_envelopes(items: Vec<Envelope>) -> Result<Vec<u8>, Error> {
    // Weirdly gzip_encoder.write_all(serde_json::to_vec()) seems to be faster than
    // serde_json::to_writer(gzip_encoder). In a local test operating on items that result in
    // ~13MiB of JSON, this is what I've seen:
    // gzip_encoder.write_all(serde_json::to_vec()): 159ms
    // serde_json::to_writer(gzip_encoder):          247ms
    let serialized = serde_json::to_vec(&items).map_err(Error::UploadSerializeRequest)?;
    serialize_request_body(serialized)
}

pub(crate) fn serialize_request_body(data: Vec<u8>) -> Result<Vec<u8>, Error> {
    let mut gzip_encoder = GzEncoder::new(Vec::new(), Compression::default());
    gzip_encoder
        .write_all(&data)
        .map_err(Error::UploadCompressRequest)?;
    gzip_encoder.finish().map_err(Error::UploadCompressRequest)
}

fn handle_response(response: Response<Bytes>) -> Result<(), Error> {
    match response.status().as_u16() {
        STATUS_OK => Ok(()),
        status @ STATUS_PARTIAL_CONTENT => {
            let content: Transmission = serde_json::from_slice(response.body())
                .map_err(Error::UploadDeserializeResponse)?;
            if content.items_received == content.items_accepted {
                Ok(())
            } else if content.errors.iter().any(can_retry_item) {
                Err(Error::Upload {
                    status,
                    can_retry: true,
                })
            } else {
                Err(Error::Upload {
                    status,
                    can_retry: false,
                })
            }
        }
        status @ STATUS_REQUEST_TIMEOUT
        | status @ STATUS_TOO_MANY_REQUESTS
        | status @ STATUS_APPLICATION_INACTIVE
        | status @ STATUS_SERVICE_UNAVAILABLE => Err(Error::Upload {
            status,
            can_retry: true,
        }),
        status @ STATUS_INTERNAL_SERVER_ERROR => {
            let content: Transmission = serde_json::from_slice(response.body())
                .map_err(Error::UploadDeserializeResponse)?;

            if content.errors.iter().any(can_retry_item) {
                Err(Error::Upload {
                    status,
                    can_retry: true,
                })
            } else {
                Err(Error::Upload {
                    status,
                    can_retry: false,
                })
            }
        }
        status => Err(Error::Upload {
            status,
            can_retry: false,
        }),
    }
}

/// Determines that a telemetry item can be re-send corresponding to this submission status
/// descriptor.
fn can_retry_item(item: &TransmissionItem) -> bool {
    item.status_code == STATUS_PARTIAL_CONTENT
        || item.status_code == STATUS_REQUEST_TIMEOUT
        || item.status_code == STATUS_TOO_MANY_REQUESTS
        || item.status_code == STATUS_APPLICATION_INACTIVE
        || item.status_code == STATUS_INTERNAL_SERVER_ERROR
        || item.status_code == STATUS_SERVICE_UNAVAILABLE
}

fn can_retry_operation(error: &Error) -> bool {
    matches!(
        error,
        Error::UploadConnection(_)
            | Error::Upload {
                can_retry: true,
                ..
            }
    )
}
