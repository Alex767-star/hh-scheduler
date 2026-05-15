mod storage;
mod api;
mod analyzer;
mod monitoring;

use std::sync::Arc;
use std::time::Instant;
use storage::{SqliteStorage, Storage, PipelineMetrics};
use tokio::signal;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{info, error, warn};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "hh-scheduler")]
#[command(about = "HH.ru vacancy analyzer with cron scheduling")]
struct Cli {
    #[arg(long, default_value = "0 0 1/6 * * *")]
    cron: String,

    #[arg(long, default_value = "Rust+developer")]
    query: String,

    #[arg(long, default_value = "113")]
    area: String,

    #[arg(long, default_value = "90")]
    retention_days: i64,

    #[arg(long, default_value = "300")]
    pipeline_timeout_secs: u64,
}

async fn run_pipeline(storage: Arc<dyn Storage>, query: &str, area: &str, retention_days: i64) -> PipelineMetrics {
    let start = Instant::now();
    let mut errors = Vec::new();
    let mut saved_count = 0u64;
    let mut updated_count = 0u64;
    let mut keywords_found = 0usize;
    
    monitoring::Metrics::pipeline_runs_total().inc();
    info!("Pipeline: starting data collection cycle (query={}, area={})", query, area);
    
    match api::fetch_rust_vacancies(query, area).await {
        Ok(vacancies) => {
            let total = vacancies.len();
            match storage.save_vacancies(&vacancies).await {
                Ok((saved, updated)) => {
                    saved_count = saved;
                    updated_count = updated;
                    monitoring::Metrics::vacancies_saved_total().inc_by(saved);
                    monitoring::Metrics::vacancies_updated_total().inc_by(updated);
                    
                    if updated == 0 && saved == 0 {
                        monitoring::Metrics::zero_updates_streak().inc();
                        warn!("Zero updates streak: {}", monitoring::Metrics::zero_updates_streak().get());
                    } else {
                        monitoring::Metrics::zero_updates_streak().set(0);
                    }
                    
                    info!("Pipeline: {} total, {} new, {} updated", total, saved, updated);
                }
                Err(e) => {
                    let msg = format!("Failed to save vacancies: {}", e);
                    error!("{}", msg);
                    errors.push(msg);
                    monitoring::Metrics::pipeline_errors_total().inc();
                }
            }
        }
        Err(e) => {
            let msg = format!("Failed to fetch vacancies: {}", e);
            error!("{}", msg);
            errors.push(msg);
            monitoring::Metrics::pipeline_errors_total().inc();
        }
    }
    
    match storage.get_recent_descriptions().await {
        Ok(descriptions) => {
            let top_skills = analyzer::count_keywords(&descriptions);
            keywords_found = top_skills.len();
            monitoring::Metrics::keywords_found_gauge().set(keywords_found as i64);
            
            match storage.save_skill_stats(&top_skills).await {
                Ok(_) => info!("Pipeline: skill stats updated ({} keywords)", keywords_found),
                Err(e) => {
                    let msg = format!("Failed to save skill stats: {}", e);
                    error!("{}", msg);
                    errors.push(msg);
                    monitoring::Metrics::pipeline_errors_total().inc();
                }
            }
        }
        Err(e) => {
            let msg = format!("Failed to get descriptions: {}", e);
            error!("{}", msg);
            errors.push(msg);
            monitoring::Metrics::pipeline_errors_total().inc();
        }
    }
    
    match storage.cleanup_old_vacancies(retention_days).await {
        Ok(deleted) => {
            if deleted > 0 {
                info!("Pipeline: cleaned up {} old vacancies (>{} days)", deleted, retention_days);
            }
        }
        Err(e) => warn!("Failed to cleanup old vacancies: {}", e),
    }
    
    if let Err(e) = storage.update_last_run_time(chrono::Utc::now()).await {
        warn!("Failed to update last run time: {}", e);
    }
    
    let duration_ms = start.elapsed().as_millis() as u64;
    monitoring::Metrics::pipeline_duration_histogram().observe(duration_ms as f64 / 1000.0);
    info!("Pipeline: cycle completed in {}ms", duration_ms);
    
    PipelineMetrics {
        saved_count,
        updated_count,
        keywords_found,
        duration_ms,
        errors,
    }
}

async fn start_scheduler(
    storage: Arc<dyn Storage>,
    cron_expr: &str,
    query: &str,
    area: &str,
    retention_days: i64,
) -> Result<JobScheduler, anyhow::Error> {
    let scheduler = JobScheduler::new().await?;
    
    let storage_clone = Arc::clone(&storage);
    let query = query.to_string();
    let area = area.to_string();
    
    let job = Job::new_async(cron_expr, move |uuid, mut lock| {
        let storage = Arc::clone(&storage_clone);
        let query = query.clone();
        let area = area.clone();
        
        Box::pin(async move {
            info!("Scheduler: tick started [job: {}]", uuid);
            let metrics = run_pipeline(storage, &query, &area, retention_days).await;
            
            if !metrics.errors.is_empty() {
                warn!(
                    "Scheduler: tick completed with {} errors ({}ms, {} saved, {} updated, {} keywords)",
                    metrics.errors.len(), metrics.duration_ms, metrics.saved_count, metrics.updated_count, metrics.keywords_found
                );
            } else {
                info!(
                    "Scheduler: tick finished ({}ms, {} saved, {} updated, {} keywords) [job: {}]",
                    metrics.duration_ms, metrics.saved_count, metrics.updated_count, metrics.keywords_found, uuid
                );
            }
            
            let next_tick = lock.next_tick_for_job(uuid).await;
            if let Ok(Some(next)) = next_tick {
                info!("Scheduler: next tick at {}", next);
            }
        })
    })?;
    
    scheduler.add(job).await?;
    info!("Scheduler: added job with cron '{}'", cron_expr);
    
    Ok(scheduler)
}

fn calculate_missed_ticks(last_run: Option<chrono::DateTime<chrono::Utc>>) -> i64 {
    match last_run {
        Some(last) => {
            let now = chrono::Utc::now();
            let elapsed_hours = (now - last).num_hours();
            let missed = elapsed_hours / 6;
            if missed > 0 {
                monitoring::Metrics::missed_ticks_gauge().set(missed);
                warn!("Missed {} scheduler ticks since last run at {}", missed, last);
            }
            missed
        }
        None => 0,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .with_target(false)
        .init();
    
    let cli = Cli::parse();
    
    info!("Initializing storage...");
    let storage = Arc::new(
        SqliteStorage::new("sqlite:hh_analytics.db?mode=rwc").await?
    ) as Arc<dyn Storage>;
    
    let last_run = storage.get_last_run_time().await?;
    if let Some(last) = last_run {
        info!("Last pipeline run: {}", last);
        let _missed = calculate_missed_ticks(Some(last));
    }
    
    let monitor_handle = monitoring::start_monitoring_server();
    info!("Monitoring server started on http://0.0.0.0:3000");
    
    info!("Running initial pipeline...");
    let initial_metrics = run_pipeline(
        Arc::clone(&storage),
        &cli.query,
        &cli.area,
        cli.retention_days,
    ).await;
    info!(
        "Initial run: {}ms, {} saved, {} updated, {} keywords, {} errors",
        initial_metrics.duration_ms,
        initial_metrics.saved_count,
        initial_metrics.updated_count,
        initial_metrics.keywords_found,
        initial_metrics.errors.len()
    );
    
    let mut scheduler = start_scheduler(
        Arc::clone(&storage),
        &cli.cron,
        &cli.query,
        &cli.area,
        cli.retention_days,
    ).await?;
    scheduler.start().await?;
    
    info!("System ready, waiting for Ctrl+C...");
    signal::ctrl_c().await?;
    
    info!("Shutdown signal received, draining tasks...");
    
    let drain_result = tokio::time::timeout(
        std::time::Duration::from_secs(cli.pipeline_timeout_secs),
        async {
            info!("Waiting for running pipeline to complete...");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        },
    ).await;
    
    match drain_result {
        Ok(_) => info!("Pipeline drain completed"),
        Err(_) => warn!("Pipeline drain timed out after {}s, forcing shutdown", cli.pipeline_timeout_secs),
    }
    
    scheduler.shutdown().await?;
    monitor_handle.abort();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    info!("Graceful shutdown complete");
    
    Ok(())
}
