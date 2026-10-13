use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
pub struct Config {
    pub owner_id: u64,
    pub base_path: String,
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(".env")
            .map_err(|_| "не вышло прочитать файл .env. Убедитесь, что он существует.")?;

        let mut owner_id = None;
        let mut base_path = None;

        for line in content.lines() {
            let line = line.trim();
            if line.starts_with("OWNER_ID=") {
                owner_id = Some(
                    line.replace("OWNER_ID=", "")
                        .replace("\"", "")
                        .trim()
                        .parse::<u64>()?,
                );
            } else if line.starts_with("BASE_PATH=") {
                base_path = Some(
                    line.replace("BASE_PATH=", "")
                        .replace("\"", "")
                        .trim()
                        .to_string(),
                );
            }
        }

        let owner_id = owner_id.ok_or("OWNER_ID не найден в файле .env")?;
        let base_path = base_path.ok_or("BASE_PATH не найден в файле .env")?;

        Ok(Self {
            owner_id,
            base_path,
        })
    }
}
