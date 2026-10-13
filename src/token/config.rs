use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
pub struct Config {
    pub teloxide_token: String,
    pub owner_id: u64,
    #[serde(default)]
    pub admin_ids: Vec<u64>,
    #[serde(default)]
    pub bot_username: String,
    pub base_path: String,
    #[serde(default)]
    pub mistral_api_key: String,
}
