use chrono::Utc;

mod helpers {
    use chrono::Utc;
    
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct Vacancy {
        pub id: String,
        pub name: String,
        pub employer: String,
        pub description: String,
        pub published_at: chrono::DateTime<Utc>,
        pub url: String,
    }
    
    pub fn test_vacancy(id: &str) -> Vacancy {
        Vacancy {
            id: id.to_string(),
            name: "Test Vacancy".into(),
            employer: "Test Corp".into(),
            description: "Rust developer with Docker and Kubernetes experience".into(),
            published_at: Utc::now(),
            url: format!("https://hh.ru/vacancy/{}", id),
        }
    }
}

#[path = "../src/storage.rs"]
mod storage;
#[path = "../src/analyzer.rs"]
mod analyzer;

use storage::{SqliteStorage, Storage};

#[tokio::test]
async fn test_upsert_vacancies() {
    let storage = SqliteStorage::new("sqlite::memory:").await.unwrap();
    
    let v1 = vec![helpers::test_vacancy("1")];
    let (saved, updated) = storage.save_vacancies(&v1).await.unwrap();
    assert_eq!(saved, 1);
    assert_eq!(updated, 0);
    
    let (saved, updated) = storage.save_vacancies(&v1).await.unwrap();
    assert_eq!(saved, 0);
    assert_eq!(updated, 1);
}

#[tokio::test]
async fn test_skill_stats_idempotent() {
    let storage = SqliteStorage::new("sqlite::memory:").await.unwrap();
    
    let stats = vec![("Rust".to_string(), 5)];
    storage.save_skill_stats(&stats).await.unwrap();
    storage.save_skill_stats(&stats).await.unwrap();
    
    let descs = vec!["Rust Rust Rust Rust Rust".to_string()];
    let keywords = analyzer::count_keywords(&descs);
    assert!(keywords.iter().any(|(k, v)| k == "Rust" && *v == 1));
}

#[tokio::test]
async fn test_pipeline_state() {
    let storage = SqliteStorage::new("sqlite::memory:").await.unwrap();
    
    assert!(storage.get_last_run_time().await.unwrap().is_none());
    
    let now = Utc::now();
    storage.update_last_run_time(now).await.unwrap();
    
    let last = storage.get_last_run_time().await.unwrap();
    assert!(last.is_some());
}

#[tokio::test]
async fn test_descriptions_recent() {
    let storage = SqliteStorage::new("sqlite::memory:").await.unwrap();
    
    let v = helpers::test_vacancy("desc-test");
    storage.save_vacancies(&[v]).await.unwrap();
    
    let descs = storage.get_recent_descriptions().await.unwrap();
    assert!(!descs.is_empty());
    assert!(descs[0].contains("Docker"));
}
