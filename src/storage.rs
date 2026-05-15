use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::SqlitePool;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vacancy {
    pub id: String,
    pub name: String,
    pub employer: String,
    pub description: String,
    pub published_at: DateTime<Utc>,
    pub url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

pub struct PipelineMetrics {
    pub saved_count: u64,
    pub updated_count: u64,
    pub keywords_found: usize,
    pub duration_ms: u64,
    pub errors: Vec<String>,
}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn save_vacancies(&self, vacancies: &[Vacancy]) -> Result<(u64, u64), StorageError>;
    async fn get_recent_descriptions(&self) -> Result<Vec<String>, StorageError>;
    async fn get_descriptions_since(&self, since: DateTime<Utc>) -> Result<Vec<String>, StorageError>;
    async fn save_skill_stats(&self, stats: &[(String, i32)]) -> Result<(), StorageError>;
    async fn get_last_run_time(&self) -> Result<Option<DateTime<Utc>>, StorageError>;
    async fn update_last_run_time(&self, time: DateTime<Utc>) -> Result<(), StorageError>;
    async fn cleanup_old_vacancies(&self, retention_days: i64) -> Result<u64, StorageError>;
}

pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    pub async fn new(database_url: &str) -> Result<Self, StorageError> {
        let pool = SqlitePool::connect(database_url).await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS vacancies (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                employer TEXT NOT NULL,
                description TEXT NOT NULL,
                published_at TIMESTAMP NOT NULL,
                url TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS skill_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                keyword TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                UNIQUE(date, keyword)
            );
            CREATE TABLE IF NOT EXISTS pipeline_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_run_at TIMESTAMP NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_vacancies_published_at ON vacancies(published_at);
            CREATE INDEX IF NOT EXISTS idx_skill_stats_date ON skill_stats(date);
            "#
        )
        .execute(&pool)
        .await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn save_vacancies(&self, vacancies: &[Vacancy]) -> Result<(u64, u64), StorageError> {
        let mut saved = 0u64;
        let mut updated = 0u64;
        
        for v in vacancies {
            let existing: Option<String> = sqlx::query_scalar(
                "SELECT id FROM vacancies WHERE id = ?"
            )
            .bind(&v.id)
            .fetch_optional(&self.pool)
            .await?;
            
            let is_new = existing.is_none();
            
            sqlx::query(
                r#"
                INSERT INTO vacancies (id, name, employer, description, published_at, url)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    employer = excluded.employer,
                    description = excluded.description,
                    published_at = excluded.published_at,
                    url = excluded.url
                "#
            )
            .bind(&v.id)
            .bind(&v.name)
            .bind(&v.employer)
            .bind(&v.description)
            .bind(v.published_at)
            .bind(&v.url)
            .execute(&self.pool)
            .await?;
            
            if is_new { saved += 1; } else { updated += 1; }
        }
        Ok((saved, updated))
    }

    async fn get_recent_descriptions(&self) -> Result<Vec<String>, StorageError> {
        self.get_descriptions_since(Utc::now() - chrono::Duration::hours(24)).await
    }

    async fn get_descriptions_since(&self, since: DateTime<Utc>) -> Result<Vec<String>, StorageError> {
        let descriptions = sqlx::query_scalar::<_, String>(
            "SELECT description FROM vacancies WHERE published_at >= ?"
        )
        .bind(since)
        .fetch_all(&self.pool)
        .await?;
        Ok(descriptions)
    }

    async fn save_skill_stats(&self, stats: &[(String, i32)]) -> Result<(), StorageError> {
        let today = Utc::now().format("%Y-%m-%d").to_string();
        for (keyword, count) in stats {
            sqlx::query(
                r#"
                INSERT INTO skill_stats (date, keyword, count)
                VALUES (?, ?, ?)
                ON CONFLICT(date, keyword) DO UPDATE SET
                    count = excluded.count
                "#
            )
            .bind(&today)
            .bind(keyword)
            .bind(count)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn get_last_run_time(&self) -> Result<Option<DateTime<Utc>>, StorageError> {
        let result: Option<String> = sqlx::query_scalar(
            "SELECT last_run_at FROM pipeline_state WHERE id = 1"
        )
        .fetch_optional(&self.pool)
        .await?;
        
        Ok(result.and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(&s)
                .ok()
                .map(|d| d.with_timezone(&Utc))
        }))
    }

    async fn update_last_run_time(&self, time: DateTime<Utc>) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO pipeline_state (id, last_run_at) VALUES (1, ?)
            ON CONFLICT(id) DO UPDATE SET last_run_at = excluded.last_run_at
            "#
        )
        .bind(time.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn cleanup_old_vacancies(&self, retention_days: i64) -> Result<u64, StorageError> {
        let cutoff = Utc::now() - chrono::Duration::days(retention_days);
        let result = sqlx::query(
            "DELETE FROM vacancies WHERE published_at < ?"
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}
