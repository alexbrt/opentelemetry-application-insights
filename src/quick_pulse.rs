use crate::{
    models::{context_tag_keys, QuickPulseEnvelope, QuickPulseMetric},
    tags::get_tags_for_resource,
    trace::{get_duration, is_remote_dependency_success, is_request_success, EVENT_NAME_EXCEPTION},
    uploader_quick_pulse::{self, PostOrPing},
    Error, Exporter,
};
use backon::{Backoff, BackoffBuilder};
use futures_util::{pin_mut, select_biased, FutureExt as _, StreamExt as _};
use opentelemetry::{trace::SpanKind, Context, Key};
use opentelemetry_http::HttpClient;
use opentelemetry_sdk::{
    error::OTelSdkResult,
    runtime::{RuntimeChannel, TrySend},
    trace::{IdGenerator as _, RandomIdGenerator, Span, SpanData, SpanProcessor},
    Resource,
};
use opentelemetry_semantic_conventions as semcov;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime},
};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

const MAX_POST_WAIT_TIME: Duration = Duration::from_secs(20);
const MAX_PING_WAIT_TIME: Duration = Duration::from_secs(60);
const FALLBACK_INTERVAL: Duration = Duration::from_secs(60);
const PING_INTERVAL: Duration = Duration::from_secs(5);
const POST_INTERVAL: Duration = Duration::from_secs(1);

const METRIC_PROCESSOR_TIME: &str = "\\Processor(_Total)\\% Processor Time";
const METRIC_COMMITTED_BYTES: &str = "\\Memory\\Committed Bytes";
const METRIC_REQUEST_RATE: &str = "\\ApplicationInsights\\Requests/Sec";
const METRIC_REQUEST_FAILURE_RATE: &str = "\\ApplicationInsights\\Requests Failed/Sec";
const METRIC_REQUEST_DURATION: &str = "\\ApplicationInsights\\Request Duration";
const METRIC_DEPENDENCY_RATE: &str = "\\ApplicationInsights\\Dependency Calls/Sec";
const METRIC_DEPENDENCY_FAILURE_RATE: &str = "\\ApplicationInsights\\Dependency Calls Failed/Sec";
const METRIC_DEPENDENCY_DURATION: &str = "\\ApplicationInsights\\Dependency Call Duration";
const METRIC_EXCEPTION_RATE: &str = "\\ApplicationInsights\\Exceptions/Sec";

/// Application Insights live metrics span processor
///
/// Enables live metrics collection: <https://learn.microsoft.com/en-us/azure/azure-monitor/app/live-stream>.
///
/// ```no_run
/// #[tokio::main]
/// async fn main() {
///     let exporter = opentelemetry_application_insights::Exporter::new_from_connection_string(
///         "connection_string",
///         reqwest::Client::new(),
///     )
///     .expect("valid connection string");
///     let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
///        .with_span_processor(opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor::builder(exporter.clone(), opentelemetry_sdk::runtime::Tokio).build())
///        .with_span_processor(opentelemetry_application_insights::LiveMetricsSpanProcessor::new(exporter, opentelemetry_sdk::runtime::Tokio))
///        .build();
///     opentelemetry::global::set_tracer_provider(tracer_provider.clone());
///
///     // ... send traces ...
///
///     tracer_provider.shutdown().unwrap();
/// }
/// ```
pub struct LiveMetricsSpanProcessor<R: RuntimeChannel> {
    is_collecting: Arc<AtomicBool>,
    shared: Arc<Mutex<Shared>>,
    message_sender: R::Sender<Message>,
}

impl<R: RuntimeChannel> std::fmt::Debug for LiveMetricsSpanProcessor<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveMetricsSpanProcessor").finish()
    }
}

#[derive(Debug)]
enum Message {
    Send,
    Stop,
}

impl<R: RuntimeChannel> LiveMetricsSpanProcessor<R> {
    /// Create new live metrics span processor.
    pub fn new<C, B>(exporter: Exporter<C, B>, runtime: R) -> LiveMetricsSpanProcessor<R>
    where
        C: HttpClient + 'static,
        B: BackoffBuilder + Clone + Send + Sync + 'static,
        B::Backoff: Backoff + Send + 'static,
    {
        let (message_sender, message_receiver) = runtime.batch_message_channel(1);
        let delay_runtime = runtime.clone();
        let is_collecting_outer = Arc::new(AtomicBool::new(false));
        let is_collecting = is_collecting_outer.clone();
        let shared_outer = Arc::new(Mutex::new(Shared {
            metrics_collector: MetricsCollector::new(),
            resource_data: (&exporter.resource).into(),
        }));
        let shared = shared_outer.clone();
        runtime.spawn(Box::pin(async move {
            let mut sender = Sender::new(
                exporter.client,
                exporter.live_post_endpoint,
                exporter.live_ping_endpoint,
            );

            let message_receiver = message_receiver.fuse();
            pin_mut!(message_receiver);
            let mut send_delay = Box::pin(delay_runtime.delay(PING_INTERVAL).fuse());

            loop {
                let msg = select_biased! {
                    msg = message_receiver.next() => msg.unwrap_or(Message::Stop),
                    _ = send_delay => Message::Send
                };
                match msg {
                    Message::Send => {
                        let curr_is_collecting = is_collecting.load(Ordering::SeqCst);
                        let (resource_data, metrics) = {
                            let mut shared = shared.lock().unwrap();
                            let resource_data = shared.resource_data.clone();
                            let metrics = curr_is_collecting
                                .then(|| shared.metrics_collector.collect_and_reset())
                                .unwrap_or_default();
                            (resource_data, metrics)
                        };
                        let (next_is_collecting, next_timeout) = sender
                            .send(curr_is_collecting, resource_data, metrics)
                            .await;
                        if curr_is_collecting != next_is_collecting {
                            is_collecting.store(next_is_collecting, Ordering::SeqCst);
                            if next_is_collecting {
                                // Reset last collection time to get accurate metrics on next collection.
                                shared.lock().unwrap().metrics_collector.reset();
                            }
                        }
                        send_delay = Box::pin(delay_runtime.delay(next_timeout).fuse());
                    }
                    Message::Stop => break,
                }
            }
        }));

        LiveMetricsSpanProcessor {
            is_collecting: is_collecting_outer,
            shared: shared_outer,
            message_sender,
        }
    }
}

impl<R: RuntimeChannel> SpanProcessor for LiveMetricsSpanProcessor<R> {
    fn on_start(&self, _span: &mut Span, _cx: &Context) {}

    fn on_end(&self, span: SpanData) {
        if self.is_collecting.load(Ordering::SeqCst) {
            self.shared
                .lock()
                .unwrap()
                .metrics_collector
                .count_span(span);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.message_sender
            .try_send(Message::Stop)
            .map_err(Error::QuickPulseShutdown)
            .map_err(Into::into)
    }

    fn set_resource(&mut self, resource: &Resource) {
        let mut shared = self.shared.lock().unwrap();
        shared.resource_data = resource.into();
    }
}

impl<R: RuntimeChannel> Drop for LiveMetricsSpanProcessor<R> {
    fn drop(&mut self) {
        if let Err(err) = self.shutdown() {
            let err: &dyn std::error::Error = &err;
            opentelemetry::otel_warn!(name: "ApplicationInsights.LiveMetrics.ShutdownFailed", error = err);
        }
    }
}

struct Shared {
    resource_data: ResourceData,
    metrics_collector: MetricsCollector,
}

#[derive(Clone)]
struct ResourceData {
    version: Option<String>,
    machine_name: String,
    instance: String,
    role_name: Option<String>,
}

impl From<&Resource> for ResourceData {
    fn from(resource: &Resource) -> Self {
        let mut tags = get_tags_for_resource(resource);
        let machine_name = resource
            .get(&Key::from_static_str(semcov::resource::HOST_NAME))
            .map(|v| v.as_str().into_owned())
            .unwrap_or_else(|| "Unknown".into());
        Self {
            version: tags.remove(context_tag_keys::INTERNAL_SDK_VERSION),
            role_name: tags.remove(context_tag_keys::CLOUD_ROLE),
            instance: tags
                .remove(context_tag_keys::CLOUD_ROLE_INSTANCE)
                .unwrap_or_else(|| machine_name.clone()),
            machine_name,
        }
    }
}

struct Sender<C: HttpClient + 'static> {
    client: Arc<C>,
    live_post_endpoint: http::Uri,
    live_ping_endpoint: http::Uri,
    last_success_time: SystemTime,
    polling_interval_hint: Option<Duration>,
    stream_id: String,
}

impl<C: HttpClient + 'static> Sender<C> {
    fn new(client: Arc<C>, live_post_endpoint: http::Uri, live_ping_endpoint: http::Uri) -> Self {
        Self {
            client,
            live_post_endpoint,
            live_ping_endpoint,
            last_success_time: SystemTime::now(),
            polling_interval_hint: None,
            stream_id: format!("{:032x}", RandomIdGenerator::default().new_trace_id()),
        }
    }

    async fn send(
        &mut self,
        is_collecting: bool,
        resource_data: ResourceData,
        metrics: Vec<QuickPulseMetric>,
    ) -> (bool, Duration) {
        let now = SystemTime::now();
        let now_ms = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let envelope = QuickPulseEnvelope {
            metrics,
            invariant_version: 1,
            timestamp: format!("/Date({})/", now_ms),
            version: resource_data.version,
            stream_id: self.stream_id.clone(),
            machine_name: resource_data.machine_name,
            instance: resource_data.instance,
            role_name: resource_data.role_name,
        };

        let res = uploader_quick_pulse::send(
            self.client.as_ref(),
            if is_collecting {
                &self.live_post_endpoint
            } else {
                &self.live_ping_endpoint
            },
            if is_collecting {
                PostOrPing::Post
            } else {
                PostOrPing::Ping
            },
            envelope,
        )
        .await;
        let (last_send_succeeded, mut next_is_collecting) = if let Ok(res) = res {
            self.last_success_time = now;
            if let Some(redirected_host) = res.redirected_host {
                self.live_post_endpoint =
                    replace_host(self.live_post_endpoint.clone(), redirected_host.clone());
                self.live_ping_endpoint =
                    replace_host(self.live_ping_endpoint.clone(), redirected_host);
            }
            if res.polling_interval_hint.is_some() {
                self.polling_interval_hint = res.polling_interval_hint;
            }
            (true, res.should_post)
        } else {
            (false, is_collecting)
        };

        let mut next_timeout = if next_is_collecting {
            POST_INTERVAL
        } else {
            self.polling_interval_hint.unwrap_or(PING_INTERVAL)
        };
        if !last_send_succeeded {
            let time_since_last_success = now
                .duration_since(self.last_success_time)
                .unwrap_or(Duration::MAX);
            if next_is_collecting && time_since_last_success >= MAX_POST_WAIT_TIME {
                // Haven't posted successfully in 20 seconds, so wait 60 seconds and ping
                next_is_collecting = false;
                next_timeout = FALLBACK_INTERVAL;
            } else if !next_is_collecting && time_since_last_success >= MAX_PING_WAIT_TIME {
                // Haven't pinged successfully in 60 seconds, so wait another 60 seconds
                next_timeout = FALLBACK_INTERVAL;
            }
        }

        (next_is_collecting, next_timeout)
    }
}

struct MetricsCollector {
    system: System,
    system_refresh_kind: RefreshKind,
    request_count: usize,
    request_failed_count: usize,
    request_duration: Duration,
    dependency_count: usize,
    dependency_failed_count: usize,
    dependency_duration: Duration,
    exception_count: usize,
    last_collection_time: SystemTime,
}

impl MetricsCollector {
    fn new() -> Self {
        Self {
            system: System::new(),
            system_refresh_kind: RefreshKind::nothing()
                .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
                .with_memory(MemoryRefreshKind::nothing().with_ram()),
            request_count: 0,
            request_failed_count: 0,
            request_duration: Duration::default(),
            dependency_count: 0,
            dependency_failed_count: 0,
            dependency_duration: Duration::default(),
            exception_count: 0,
            last_collection_time: SystemTime::now(),
        }
    }

    fn reset(&mut self) {
        self.request_count = 0;
        self.request_failed_count = 0;
        self.request_duration = Duration::default();
        self.dependency_count = 0;
        self.dependency_failed_count = 0;
        self.dependency_duration = Duration::default();
        self.exception_count = 0;
        self.last_collection_time = SystemTime::now();
    }

    fn count_span(&mut self, span: SpanData) {
        // https://github.com/microsoft/ApplicationInsights-node.js/blob/aaafbfd8ffbc454d4a5c30cda4492891410b9f66/TelemetryProcessors/PerformanceMetricsTelemetryProcessor.ts#L6
        match span.span_kind {
            SpanKind::Server | SpanKind::Consumer => {
                self.request_count += 1;
                if !is_request_success(&span) {
                    self.request_failed_count += 1;
                }
                self.request_duration += get_duration(&span);
            }
            SpanKind::Client | SpanKind::Producer | SpanKind::Internal => {
                self.dependency_count += 1;
                if let Some(false) = is_remote_dependency_success(&span) {
                    self.dependency_failed_count += 1;
                }
                self.dependency_duration += get_duration(&span);
            }
        }

        for event in span.events.iter() {
            if event.name == EVENT_NAME_EXCEPTION {
                self.exception_count += 1;
            }
        }
    }

    fn collect_and_reset(&mut self) -> Vec<QuickPulseMetric> {
        let mut metrics = Vec::new();
        self.system.refresh_specifics(self.system_refresh_kind);
        self.collect_cpu_usage(&mut metrics);
        self.collect_memory_usage(&mut metrics);
        self.collect_requests_dependencies_exceptions(&mut metrics);
        self.reset();
        metrics
    }

    fn collect_cpu_usage(&mut self, metrics: &mut Vec<QuickPulseMetric>) {
        let mut cpu_usage = 0.;
        for cpu in self.system.cpus() {
            cpu_usage += f64::from(cpu.cpu_usage());
        }
        metrics.push(QuickPulseMetric {
            name: METRIC_PROCESSOR_TIME,
            value: cpu_usage,
            weight: 1,
        });
    }

    fn collect_memory_usage(&mut self, metrics: &mut Vec<QuickPulseMetric>) {
        metrics.push(QuickPulseMetric {
            name: METRIC_COMMITTED_BYTES,
            value: self.system.used_memory() as f64,
            weight: 1,
        });
    }

    fn collect_requests_dependencies_exceptions(&mut self, metrics: &mut Vec<QuickPulseMetric>) {
        let elapsed_seconds = SystemTime::now()
            .duration_since(self.last_collection_time)
            .unwrap_or_default()
            .as_secs();
        if elapsed_seconds == 0 {
            return;
        }

        metrics.push(QuickPulseMetric {
            name: METRIC_REQUEST_RATE,
            value: self.request_count as f64 / elapsed_seconds as f64,
            weight: 1,
        });
        metrics.push(QuickPulseMetric {
            name: METRIC_REQUEST_FAILURE_RATE,
            value: self.request_failed_count as f64 / elapsed_seconds as f64,
            weight: 1,
        });
        if self.request_count > 0 {
            metrics.push(QuickPulseMetric {
                name: METRIC_REQUEST_DURATION,
                value: self.request_duration.as_millis() as f64 / self.request_count as f64,
                weight: 1,
            });
        }

        metrics.push(QuickPulseMetric {
            name: METRIC_DEPENDENCY_RATE,
            value: self.dependency_count as f64 / elapsed_seconds as f64,
            weight: 1,
        });
        metrics.push(QuickPulseMetric {
            name: METRIC_DEPENDENCY_FAILURE_RATE,
            value: self.dependency_failed_count as f64 / elapsed_seconds as f64,
            weight: 1,
        });
        if self.dependency_count > 0 {
            metrics.push(QuickPulseMetric {
                name: METRIC_DEPENDENCY_DURATION,
                value: self.dependency_duration.as_millis() as f64 / self.dependency_count as f64,
                weight: 1,
            });
        }

        metrics.push(QuickPulseMetric {
            name: METRIC_EXCEPTION_RATE,
            value: self.exception_count as f64 / elapsed_seconds as f64,
            weight: 1,
        });
    }
}

fn replace_host(uri: http::Uri, new_host: http::Uri) -> http::Uri {
    let mut parts = uri.into_parts();
    let new_parts = new_host.into_parts();
    parts.scheme = new_parts.scheme;
    parts.authority = new_parts.authority;
    http::Uri::from_parts(parts).expect("valid uri + valid uri = valid uri")
}
