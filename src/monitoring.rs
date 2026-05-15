use prometheus::{IntCounter, IntGauge, Histogram, register_int_counter, register_int_gauge, register_histogram, TextEncoder, gather};
use std::sync::LazyLock;

static PIPELINE_RUNS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("pipeline_runs_total", "Total number of pipeline runs").unwrap()
});

static PIPELINE_ERRORS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("pipeline_errors_total", "Total number of pipeline errors").unwrap()
});

static VACANCIES_SAVED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("vacancies_saved_total", "Total number of new vacancies saved").unwrap()
});

static VACANCIES_UPDATED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("vacancies_updated_total", "Total number of vacancies updated").unwrap()
});

static KEYWORDS_FOUND_GAUGE: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("keywords_found", "Number of keywords found in last run").unwrap()
});

static PIPELINE_DURATION_HISTOGRAM: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "pipeline_duration_seconds",
        "Pipeline execution duration in seconds",
        vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
    ).unwrap()
});

static ZERO_UPDATES_STREAK: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("zero_updates_streak", "Consecutive runs with zero updates").unwrap()
});

static MISSED_TICKS_GAUGE: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("missed_ticks", "Number of missed scheduler ticks").unwrap()
});

pub struct Metrics;

impl Metrics {
    pub fn pipeline_runs_total() -> &'static IntCounter { &PIPELINE_RUNS_TOTAL }
    pub fn pipeline_errors_total() -> &'static IntCounter { &PIPELINE_ERRORS_TOTAL }
    pub fn vacancies_saved_total() -> &'static IntCounter { &VACANCIES_SAVED_TOTAL }
    pub fn vacancies_updated_total() -> &'static IntCounter { &VACANCIES_UPDATED_TOTAL }
    pub fn keywords_found_gauge() -> &'static IntGauge { &KEYWORDS_FOUND_GAUGE }
    pub fn pipeline_duration_histogram() -> &'static Histogram { &PIPELINE_DURATION_HISTOGRAM }
    pub fn zero_updates_streak() -> &'static IntGauge { &ZERO_UPDATES_STREAK }
    pub fn missed_ticks_gauge() -> &'static IntGauge { &MISSED_TICKS_GAUGE }
}

pub fn start_monitoring_server() -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(|| {
        let server = tiny_http::Server::http("0.0.0.0:3000").expect("Failed to start monitoring server");
        
        for request in server.incoming_requests() {
            let response = match request.url() {
                "/health" => {
                    tiny_http::Response::from_string(
                        serde_json::json!({"status": "ok", "service": "hh-scheduler"}).to_string()
                    ).with_header("Content-Type: application/json".parse::<tiny_http::Header>().unwrap())
                }
                "/metrics" => {
                    let encoder = TextEncoder::new();
                    let metric_families = gather();
                    let body = encoder.encode_to_string(&metric_families).unwrap_or_else(|e| e.to_string());
                    tiny_http::Response::from_string(body)
                        .with_header("Content-Type: text/plain".parse::<tiny_http::Header>().unwrap())
                }
                _ => {
                    tiny_http::Response::from_string("Not Found")
                        .with_status_code(404)
                }
            };
            let _ = request.respond(response);
        }
    })
}
