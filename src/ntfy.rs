use reqwest::header::{HeaderMap, HeaderValue};
// хз почему это тут, вроде надо
use std::path::Path;

fn clean_topic(raw: &str) -> Option<String> {
    let topic = raw.trim().trim_matches('/');
    if topic.is_empty() || topic.len() > 128 {
        return None;
    }
    topic
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
        .then(|| topic.to_string())
}

pub fn resolve_topic(base_path: &str, setting: Option<String>) -> Option<String> {
    if let Some(topic) = setting.as_deref().and_then(clean_topic) {
        return Some(topic);
    }
    for key in ["RAYZEN_NTFY_TOPIC", "NTFY_TOPIC"] {
        if let Ok(topic) = std::env::var(key) {
            if let Some(topic) = clean_topic(&topic) {
                return Some(topic);
            }
        }
    }
    let path = Path::new(base_path.trim_end_matches('/')).join("config/ntfy_topic.txt");
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| clean_topic(&s))
}

pub async fn send_topic(topic: &str, title: &str, message: &str, tags: &str) {
    let Some(topic) = clean_topic(topic) else {
        return;
    };
    let mut headers = HeaderMap::new();
    if let Ok(v) = HeaderValue::from_str(title) {
        headers.insert("Title", v);
    }
    if let Ok(v) = HeaderValue::from_str(tags) {
        headers.insert("Tags", v);
    }
    let url = format!("https://ntfy.sh/{}", topic);
    let result = reqwest::Client::new()
        .post(url)
        .headers(headers)
        .body(message.to_string())
        .send()
        .await;
    if let Err(err) = result {
        log::warn!("ntfy send failed: {}", err);
    }
}

pub async fn send_check1_alert(base_path: &str, setting: Option<String>, message: &str) {
    if let Some(topic) = resolve_topic(base_path, setting) {
        send_topic(&topic, "Rayzen check1", message, "warning").await;
    }
}
