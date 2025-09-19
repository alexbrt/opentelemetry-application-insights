use crate::{internal_error, internal_info, internal_warn, models::Envelope, Error, HttpClient};
use backon::{ExponentialBuilder, FuturesTimerSleeper, RetryableWithContext};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use flate2::{write::GzEncoder, Compression};
use http::{Request, Response, Uri};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    io::Write,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime},
};
use uuid::Uuid;

const X_MS_CLIENT_REQUEST_ID_HEADER: &str = "x-ms-client-request-id";

// We need these constants because HTTP 439 is not part of the official HTTP
// status code registry.
const STATUS_OK: u16 = 200;
const STATUS_PARTIAL_CONTENT: u16 = 206;
const STATUS_REQUEST_TIMEOUT: u16 = 408;
const STATUS_TOO_MANY_REQUESTS: u16 = 429;
const STATUS_APPLICATION_INACTIVE: u16 = 439; // Quota
const STATUS_INTERNAL_SERVER_ERROR: u16 = 500;
const STATUS_SERVICE_UNAVAILABLE: u16 = 503;

const RETRY_MIN_DELAY: Duration = Duration::from_millis(500);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const RETRY_TOTAL_DELAY: Duration = Duration::from_secs(35);

/// Response containing the status of each telemetry item.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrackResponse {
    /// The number of items received.
    items_received: usize,
    /// The number of items accepted.
    items_accepted: usize,
    /// An array of error detail objects.
    errors: Vec<ErrorDetails>,
}

/// The error details.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ErrorDetails {
    /// The index in the original payload of the item.
    index: usize,
    /// The item specific HTTP Response status code.
    status_code: u16,
    /// Server-provided diagnostic
    #[serde(default)]
    message: Option<String>,
}

/// Sends a telemetry items to the server.
pub(crate) async fn send(
    client: &dyn HttpClient,
    endpoint: &Uri,
    items: Vec<Envelope>,
    retry_notify: Option<Arc<Mutex<dyn FnMut(&Error, Duration) + Send + 'static>>>,
) -> Result<(), Error> {
    let start_batch = Instant::now();
    let mut attempt_number: u32 = 0;

    let batch_id = new_batch_id();
    let item_count = items.len();
    internal_info!(
        message: "AaiExport.StartBatch",
        batch_id = batch_id,
        item_count = item_count,
        endpoint = endpoint.to_string()
    );

    let attempt = |mut items: Vec<Envelope>| {
        // bump before constructing the future
        attempt_number += 1;
        let this_attempt_number = attempt_number;

        async move {
            let attempt_id = new_attempt_id();
            let start = Instant::now();
            internal_info!(
                message: "AaiExport.AttemptBegin",
                batch_id = batch_id,
                attempt_id = attempt_id,
                attempt_number = this_attempt_number,
                remaining_items = items.len()
            );

            match send_internal(client, endpoint, &items, batch_id, attempt_id).await {
                Ok(()) => {
                    internal_info!(
                        message: "AaiExport.AttemptSuccess",
                        batch_id = batch_id,
                        attempt_id = attempt_id,
                        attempt_number = this_attempt_number,
                        elapsed_ms = start.elapsed().as_millis()
                    );
                    (Vec::new(), Ok(()))
                }
                Err(UploadError::RetryAll(err)) => {
                    internal_warn!(
                        message: "AaiExport.RetryAll",
                        batch_id = batch_id,
                        attempt_id = attempt_id,
                        attempt_number = this_attempt_number,
                        elapsed_ms = start.elapsed().as_millis(),
                        error = err.to_string()
                    );
                    (items, Err(UploadError::RetryAll(err)))
                }
                Err(UploadError::RetrySome { err, to_retry }) => {
                    let to_retry_count = to_retry.len();
                    internal_warn!(
                        message: "AaiExport.RetrySome",
                        batch_id = batch_id,
                        attempt_id = attempt_id,
                        attempt_number = this_attempt_number,
                        elapsed_ms = start.elapsed().as_millis(),
                        error = err.to_string(),
                        to_retry_count = to_retry_count
                    );

                    let mut index: usize = 0;
                    items.retain(|_| {
                        let retry = to_retry.contains(&index);
                        index += 1;
                        retry
                    });

                    if items.is_empty() {
                        internal_info!(
                            message: "AaiExport.PartialRetryNoItems",
                            batch_id = batch_id,
                            attempt_id = attempt_id,
                            attempt_number = this_attempt_number
                        );
                        return (items, Ok(()));
                    }

                    (items, Err(UploadError::RetrySome { err, to_retry }))
                }
                Err(err) => {
                    internal_error!(
                        message: "AaiExport.Fatal",
                        batch_id = batch_id,
                        attempt_id = attempt_id,
                        attempt_number = this_attempt_number,
                        elapsed_ms = start.elapsed().as_millis(),
                        error = err.error().to_string()
                    );
                    (Vec::new(), Err(err))
                }
            }
        }
    };

    let (_, result) = attempt
        .retry(
            ExponentialBuilder::new()
                .with_min_delay(RETRY_MIN_DELAY)
                .with_max_delay(RETRY_MAX_DELAY)
                .with_jitter()
                // No max delay or max times should needed, because the batch span processor already
                // enforces a `max_export_timeout`. However, as of `opentelemetry_sdk` v0.30.0:
                // - the option is only respected for ::span_processor_with_async_runtime::BatchSpanProcessor
                // - the option doesn't exist for metric or log exports or the SimpleSpanProcessor
                // Therefore, add a total delay here, which is slightly larger than the default
                // `max_export_timeout`.
                .without_max_times()
                .with_total_delay(Some(RETRY_TOTAL_DELAY)),
        )
        .sleep(FuturesTimerSleeper)
        .context(items)
        .when(|err| {
            matches!(
                err,
                UploadError::RetryAll(_) | UploadError::RetrySome { .. }
            )
        })
        .notify(|error, duration| {
            internal_warn!(
                message: "AaiExport.Backoff",
                batch_id = batch_id,
                backoff_ms = duration.as_millis(),
                error = error.error().to_string()
            );
            if let Some(ref notify) = retry_notify {
                let mut notify = notify.lock().unwrap();
                notify(error.error(), duration);
            }
        })
        .await;

    if let Err(error) = &result {
        internal_error!(
            message: "AaiExport.BatchFailed",
            batch_id = batch_id,
            elapsed_ms = start_batch.elapsed().as_millis(),
            error = error.to_string()
        );
    } else {
        internal_info!(
            message: "AaiExport.BatchSucceeded",
            batch_id = batch_id,
            attempts = attempt_number,
            elapsed_ms = start_batch.elapsed().as_millis()
        );
    }

    result.map_err(|err| err.into_error())
}

async fn send_internal(
    client: &dyn HttpClient,
    endpoint: &Uri,
    items: &[Envelope],
    batch_id: u64,
    attempt_id: u64,
) -> Result<(), UploadError> {
    let payload = Bytes::from(serialize_envelopes(items).map_err(UploadError::Fatal)?);
    let client_request_id = Uuid::new_v4().to_string();

    let request = Request::post(endpoint)
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::CONTENT_ENCODING, "gzip")
        .header(X_MS_CLIENT_REQUEST_ID_HEADER, client_request_id.clone())
        .body(payload)
        .expect("request should be valid");

    internal_info!(
        message: "AaiExport.Send",
        batch_id = batch_id,
        attempt_id = attempt_id,
        endpoint = endpoint.to_string(),
        client_request_id = &client_request_id
    );
    let response = client.send_bytes(request).await.map_err(|err| {
        internal_error!(
            message: "AaiExport.ConnectionError",
            batch_id = batch_id,
            attempt_id = attempt_id,
            endpoint = endpoint.to_string(),
            client_request_id = &client_request_id,
            error = err.to_string()
        );
        UploadError::RetryAll(Error::UploadConnection(err))
    })?;

    let status = response.status().as_u16();
    let retry_after_header = header_str(&response, http::header::RETRY_AFTER).unwrap_or_default();
    let location_header = header_str(&response, http::header::LOCATION).unwrap_or_default();
    let retry_after_parsed = match parse_retry_after(retry_after_header) {
        Some(duration) => Some(duration.as_millis()),
        None => {
            if !retry_after_header.is_empty() {
                internal_warn!(message: "AaiExport.RetryAfterUnparsed", value = retry_after_header);
            }
            None
        }
    };

    internal_info!(
        message: "AaiExport.ResponseReceived",
        batch_id = batch_id,
        attempt_id = attempt_id,
        status = status,
        client_request_id = client_request_id,
        retry_after = retry_after_header,
        retry_after_ms = retry_after_parsed.unwrap_or_default(),
        location = location_header
    );

    if is_redirect(status) {
        internal_warn!(
            message: "AaiExport.RedirectNotFollowed",
            batch_id = batch_id,
            attempt_id = attempt_id,
            status = status,
            location = location_header
        );
    }

    handle_upload_response(response, batch_id, attempt_id)
}

fn serialize_envelopes(items: &[Envelope]) -> Result<Vec<u8>, Error> {
    let serialized = serde_json::to_vec(items).map_err(Error::UploadSerializeRequest)?;
    // log uncompressed size & envelope count before gzip
    internal_info!(
        message: "AaiExport.Serialize",
        uncompressed_bytes = serialized.len(),
        item_count = items.len()
    );
    serialize_request_body(&serialized)
}

pub(crate) fn serialize_request_body(data: &[u8]) -> Result<Vec<u8>, Error> {
    // Weirdly gzip_encoder.write_all(serde_json::to_vec()) seems to be faster than
    // serde_json::to_writer(gzip_encoder). In a local test operating on items that result in
    // ~13MiB of JSON, this is what I've seen:
    // gzip_encoder.write_all(serde_json::to_vec()): 159ms
    // serde_json::to_writer(gzip_encoder):          247ms
    let mut gzip_encoder = GzEncoder::new(Vec::new(), Compression::default());
    gzip_encoder
        .write_all(&data)
        .map_err(Error::UploadCompressRequest)?;
    gzip_encoder.finish().map_err(Error::UploadCompressRequest)
}

#[derive(Debug, thiserror::Error)]
enum UploadError {
    #[error("upload failed with {0}")]
    RetryAll(Error),
    #[error("upload partially failed with {err}, retrying {to_retry:?}")]
    RetrySome {
        err: Error,
        to_retry: HashSet<usize>,
    },
    #[error("upload failed fatally with {0}")]
    Fatal(Error),
}

impl UploadError {
    fn error(&self) -> &Error {
        match self {
            Self::RetryAll(err) => err,
            Self::RetrySome { err, .. } => err,
            Self::Fatal(err) => err,
        }
    }

    fn into_error(self) -> Error {
        match self {
            Self::RetryAll(err) => err,
            Self::RetrySome { err, .. } => err,
            Self::Fatal(err) => err,
        }
    }
}

static NEXT_BATCH_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);

fn new_batch_id() -> u64 {
    NEXT_BATCH_ID.fetch_add(1, Ordering::Relaxed)
}

fn new_attempt_id() -> u64 {
    NEXT_ATTEMPT_ID.fetch_add(1, Ordering::Relaxed)
}

fn header_str(response: &Response<Bytes>, name: http::header::HeaderName) -> Option<&str> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

fn handle_upload_response(
    response: Response<Bytes>,
    batch_id: u64,
    attempt_id: u64,
) -> Result<(), UploadError> {
    match response.status().as_u16() {
        STATUS_OK => {
            internal_info!(
                message: "AaiExport.Success",
                batch_id = batch_id,
                attempt_id = attempt_id
            );
            Ok(())
        }
        status_code @ STATUS_PARTIAL_CONTENT => {
            let content: TrackResponse = match serde_json::from_slice(response.body()) {
                Ok(content) => content,
                Err(err) => {
                    internal_error!(
                        message: "AaiExport.ResponseError",
                        batch_id = batch_id,
                        attempt_id = attempt_id,
                        status = status_code,
                        error = err.to_string()
                    );
                    return Err(UploadError::Fatal(Error::UploadDeserializeResponse(err)));
                }
            };

            let dropped = content
                .items_received
                .saturating_sub(content.items_accepted);

            let sample = content
                .errors
                .iter()
                .take(DIAGNOSTICS_DROPPED_SPANS_SAMPLE_COUNT)
                .collect::<Vec<_>>();

            internal_info!(
                message: "AaiExport.ResponseReceived",
                batch_id = batch_id,
                attempt_id = attempt_id,
                status = status_code,
                received = content.items_received,
                accepted = content.items_accepted,
                dropped = dropped,
                sample = format!("{sample:?}"),
            );

            if content.items_received == content.items_accepted {
                return Ok(());
            }

            let to_retry = content
                .errors
                .iter()
                .filter(|error| can_retry_status_code(error.status_code))
                .map(|error| error.index)
                .collect::<HashSet<_>>();
            let non_retryable = content.errors.len().saturating_sub(to_retry.len());

            let sample_non_retryable: Vec<(usize, u16)> = content
                .errors
                .iter()
                .filter(|error_details| !can_retry_status_code(error_details.status_code))
                .take(DIAGNOSTICS_DROPPED_SPANS_SAMPLE_COUNT)
                .map(|error_details| (error_details.index, error_details.status_code))
                .collect();

            internal_info!(
                message: "AaiExport.PartialSample",
                batch_id = batch_id,
                attempt_id = attempt_id,
                retryable_sample = format!("{sample:?}"),
                non_retryable_sample = format!("{sample_non_retryable:?}"),
            );

            if to_retry.is_empty() {
                internal_error!(
                    message: "AaiExport.ResponseError",
                    batch_id = batch_id,
                    attempt_id = attempt_id,
                    status = status_code,
                    received = content.items_received,
                    accepted = content.items_accepted,
                    non_retryable = non_retryable,
                );
                Err(UploadError::Fatal(Error::Upload(format!(
                    "{status_code}: Accepted {}/{} items; none were retryable.",
                    content.items_accepted, content.items_received
                ))))
            } else {
                internal_warn!(
                    message: "AaiExport.ResponseWarning",
                    batch_id = batch_id,
                    attempt_id = attempt_id,
                    status = status_code,
                    retry_count = to_retry.len(),
                    non_retryable = non_retryable,
                    retry_after = header_str(&response, http::header::RETRY_AFTER).unwrap_or_default(),
                );
                Err(UploadError::RetrySome {
                    err: status_code_error(status_code),
                    to_retry,
                })
            }
        }
        status_code @ (STATUS_REQUEST_TIMEOUT
        | STATUS_TOO_MANY_REQUESTS
        | STATUS_APPLICATION_INACTIVE
        | STATUS_SERVICE_UNAVAILABLE) => {
            let raw_retry_after =
                header_str(&response, http::header::RETRY_AFTER).unwrap_or_default();
            internal_warn!(
                message: "AaiExport.ResponseWarning",
                batch_id = batch_id,
                attempt_id = attempt_id,
                status = status_code,
                retry_after = raw_retry_after,
                retry_after_ms = parse_retry_after(raw_retry_after).map(|d| d.as_millis()).unwrap_or(0),
            );
            Err(UploadError::RetryAll(status_code_error(status_code)))
        }
        status_code @ STATUS_INTERNAL_SERVER_ERROR => {
            let parsed = serde_json::from_slice::<TrackResponse>(response.body());
            match parsed {
                Ok(content) => {
                    let to_retry = content
                        .errors
                        .iter()
                        .filter(|error| can_retry_status_code(error.status_code))
                        .map(|error| error.index)
                        .collect::<HashSet<_>>();
                    let non_retryable = content.errors.len().saturating_sub(to_retry.len());

                    internal_warn!(
                        message: "AaiExport.ResponseWarning",
                        batch_id = batch_id,
                        attempt_id = attempt_id,
                        status = status_code,
                        received = content.items_received,
                        accepted = content.items_accepted,
                        retry_count = to_retry.len(),
                        non_retryable = non_retryable,
                    );

                    if to_retry.is_empty() {
                        Err(UploadError::Fatal(Error::Upload(format!(
                            "{status_code}: Accepted {}/{} items; none were retryable.",
                            content.items_accepted, content.items_received
                        ))))
                    } else {
                        Err(UploadError::RetrySome {
                            err: status_code_error(status_code),
                            to_retry,
                        })
                    }
                }
                Err(_) => {
                    internal_warn!(
                        message: "AaiExport.ResponseWarning",
                        batch_id = batch_id,
                        attempt_id = attempt_id,
                        status = status_code
                    );
                    Err(UploadError::RetryAll(status_code_error(status_code)))
                }
            }
        }
        status_code => {
            let body_preview = preview_body(response.body(), DIAGNOSTICS_BODY_PREVIEW_BYTES_LIMIT);
            internal_error!(
                message: "AaiExport.ResponseError",
                batch_id = batch_id,
                attempt_id = attempt_id,
                status = status_code,
                body_preview = &body_preview,
            );
            Err(UploadError::Fatal(status_code_error(status_code)))
        }
    }
}

const DIAGNOSTICS_DROPPED_SPANS_SAMPLE_COUNT: usize = 20;
const DIAGNOSTICS_BODY_PREVIEW_BYTES_LIMIT: usize = 512;

fn can_retry_status_code(code: u16) -> bool {
    code == STATUS_PARTIAL_CONTENT
        || code == STATUS_REQUEST_TIMEOUT
        || code == STATUS_TOO_MANY_REQUESTS
        || code == STATUS_APPLICATION_INACTIVE
        || code == STATUS_INTERNAL_SERVER_ERROR
        || code == STATUS_SERVICE_UNAVAILABLE
}

fn status_code_error(status_code: u16) -> Error {
    Error::Upload(format!("{status_code}"))
}

fn is_redirect(status: u16) -> bool {
    (300..=399).contains(&status)
}

/// Parse Retry-After as either HTTP-date (RFC1123) or delta-seconds.
/// Returns a Duration until retry, or None if it can't be parsed.
fn parse_retry_after(string: &str) -> Option<Duration> {
    if string.is_empty() {
        return None;
    }

    // Case 1: simple integer (delta-seconds)
    if let Ok(delta) = string.trim().parse::<u64>() {
        return Some(Duration::from_secs(delta));
    }

    // Case 2: HTTP-date (RFC1123, e.g. "Fri, 27 Sep 2024 15:00:00 GMT")
    if let Ok(datetime) = DateTime::parse_from_rfc2822(string.trim()) {
        // Normalize to UTC
        let datetime_utc = datetime.with_timezone(&Utc);
        // Convert into std::time::SystemTime
        let target_time: SystemTime = datetime_utc.into();
        match target_time.duration_since(SystemTime::now()) {
            Ok(duration) => Some(duration),
            Err(_) => Some(Duration::from_secs(0)), // past date should retry immediately
        }
    } else {
        None
    }
}

/// Return safe, trimmed body preview for logs (to help 400/404/5xx debugging)
fn preview_body(bytes: &[u8], limit: usize) -> String {
    if bytes.is_empty() {
        return "".to_owned();
    }
    // Try UTF-8, fall back to hex of first N bytes
    if let Ok(string) = std::str::from_utf8(bytes) {
        let string = string.trim();
        if string.len() <= limit {
            string.to_owned()
        } else {
            format!("{}…", &string[..limit])
        }
    } else {
        let max = bytes.len().min(limit.min(256));
        format!(
            "0x{}",
            &bytes[..max]
                .iter()
                .map(|bytes| format!("{bytes:02x}"))
                .collect::<String>()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use flate2::read::GzDecoder;
    use http::{Request, Response};
    use opentelemetry_http::{HttpClient, HttpError};
    use std::{collections::VecDeque, sync::Mutex};

    #[derive(Default, Debug)]
    struct TestClient {
        requests: Mutex<Vec<Request<Bytes>>>,
        responses: Mutex<VecDeque<Result<Response<Bytes>, HttpError>>>,
    }

    impl TestClient {
        fn with_response(self, response: Result<Response<Bytes>, HttpError>) -> Self {
            self.responses.lock().unwrap().push_back(response);
            self
        }

        fn with_200(self) -> Self {
            self.with_response(Ok(Response::builder()
                .status(200)
                .body(Bytes::from("{}"))
                .expect("")))
        }

        fn with_206(self, track_response: TrackResponse) -> Self {
            self.with_response(Ok(Response::builder()
                .status(206)
                .body(Bytes::from(serde_json::to_vec(&track_response).unwrap()))
                .expect("")))
        }

        fn with_400(self) -> Self {
            self.with_response(Ok(Response::builder()
                .status(400)
                .body(Bytes::from("{}"))
                .expect("")))
        }

        fn with_connection_error(self) -> Self {
            self.with_response(Err("connection error".into()))
        }
    }

    #[async_trait]
    impl HttpClient for TestClient {
        async fn send_bytes(&self, req: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
            self.requests.lock().unwrap().push(req);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("not enough responses are set up")
        }
    }

    fn endpoint() -> Uri {
        Uri::from_static("https://example.com/track")
    }

    fn envelopes(n: usize) -> Vec<Envelope> {
        let mut items = Vec::with_capacity(n);
        for index in 0..n {
            items.push(Envelope {
                name: "Test",
                time: index.to_string().into(),
                sample_rate: None,
                i_key: None,
                tags: None,
                data: None,
            });
        }
        items
    }

    fn envelopes_ids_from_request_body(body: &[u8]) -> Vec<usize> {
        let gzip_decoder = GzDecoder::new(body);
        let mut envelopes: Vec<serde_json::Value> =
            serde_json::from_reader(gzip_decoder).expect("body is json array");
        envelopes
            .drain(..)
            .map(|envelope| {
                envelope
                    .as_object()
                    .unwrap()
                    .get("time")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn success() {
        let client = TestClient::default().with_200();
        let result = send(&client, &endpoint(), envelopes(1), None).await;
        assert!(result.is_ok());
        assert_eq!(client.requests.lock().unwrap().len(), 1, "request count");
    }

    #[tokio::test]
    async fn success_partial_with_all_items() {
        let client = TestClient::default().with_206(TrackResponse {
            items_received: 2,
            items_accepted: 2,
            errors: Vec::new(),
        });
        let result = send(&client, &endpoint(), envelopes(2), None).await;
        assert!(result.is_ok());
        assert_eq!(client.requests.lock().unwrap().len(), 1, "request count");
    }

    #[tokio::test]
    async fn fatal() {
        let client = TestClient::default().with_400();
        let result = send(&client, &endpoint(), envelopes(1), None).await;
        assert!(result.is_err());
        assert_eq!(client.requests.lock().unwrap().len(), 1, "request count");
        assert_eq!(result.unwrap_err().to_string(), "upload failed with 400");
    }

    #[tokio::test]
    async fn retry_connection_error() {
        let client = TestClient::default().with_connection_error().with_200();
        let result = send(&client, &endpoint(), envelopes(1), None).await;
        assert!(result.is_ok());
        assert_eq!(client.requests.lock().unwrap().len(), 2, "request count");
    }

    #[tokio::test]
    async fn retry_partial_content() {
        let client = TestClient::default()
            .with_206(TrackResponse {
                items_received: 10,
                items_accepted: 6,
                errors: vec![
                    ErrorDetails {
                        index: 1,
                        status_code: 400,
                        message: None,
                    },
                    ErrorDetails {
                        index: 7,
                        status_code: STATUS_REQUEST_TIMEOUT,
                        message: None,
                    },
                    ErrorDetails {
                        index: 8,
                        status_code: STATUS_REQUEST_TIMEOUT,
                        message: None,
                    },
                    ErrorDetails {
                        index: 9,
                        status_code: STATUS_REQUEST_TIMEOUT,
                        message: None,
                    },
                ],
            })
            .with_206(TrackResponse {
                items_received: 3,
                items_accepted: 2,
                errors: vec![ErrorDetails {
                    index: 2,
                    status_code: STATUS_TOO_MANY_REQUESTS,
                    message: None,
                }],
            })
            .with_200();
        let result = send(&client, &endpoint(), envelopes(10), None).await;
        assert!(result.is_ok());
        let requests = client.requests.lock().unwrap();
        assert_eq!(requests.len(), 3, "request count");
        let items0 = envelopes_ids_from_request_body(requests[0].body());
        assert_eq!(items0, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let items1 = envelopes_ids_from_request_body(requests[1].body());
        assert_eq!(items1, vec![7, 8, 9]);
        let items2 = envelopes_ids_from_request_body(requests[2].body());
        assert_eq!(items2, vec![9]);
    }
}
