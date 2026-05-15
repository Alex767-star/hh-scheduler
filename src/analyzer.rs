use std::collections::HashMap;
use tracing::debug;

const KEYWORDS: &[&str] = &[
    "Rust", "Tokio", "Actix", "Axum", "Rocket", "Warp",
    "Docker", "Kubernetes", "PostgreSQL", "MySQL", "MongoDB", "Redis",
    "gRPC", "GraphQL", "REST", "WebSocket",
    "AWS", "GCP", "Azure", "CI/CD", "GitLab", "GitHub Actions",
    "Python", "Go", "Java", "Kotlin", "TypeScript", "JavaScript",
    "React", "Vue", "Angular", "Svelte",
    "Kafka", "RabbitMQ", "NATS",
    "Linux", "Bash", "Terraform", "Ansible",
    "Microservices", "DDD", "TDD", "Agile", "Scrum",
];

pub fn count_keywords(descriptions: &[String]) -> Vec<(String, i32)> {
    let mut counts: HashMap<&str, i32> = HashMap::new();
    
    for desc in descriptions {
        let lower = desc.to_lowercase();
        for keyword in KEYWORDS {
            if lower.contains(&keyword.to_lowercase()) {
                *counts.entry(keyword).or_insert(0) += 1;
            }
        }
    }
    
    let mut result: Vec<(String, i32)> = counts
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .filter(|(_, v)| *v > 0)
        .collect();
    
    result.sort_by(|a, b| b.1.cmp(&a.1));
    debug!("Top skills analyzed: {:?}", &result[..result.len().min(10)]);
    result
}
