use dashmap::DashMap;
use serde::{Deserialize, Serialize};
// TODO: переписать это говно
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};
use teloxide::types::{ChatId, MessageId};
use tokio::task::JoinHandle;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AutoReplyConfig {
    pub file_name: String,
    pub delay: u64,
    pub shapka: String,
    pub media_url: Option<String>,
    #[serde(default)]
    pub chat_id: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LastAutoReplyTarget {
    pub chat_id: i64,
    pub user_id: u64,
    pub message_id: i32,
    #[serde(default)]
    pub has_text: bool,
    #[serde(default)]
    pub has_media: bool,
    #[serde(default)]
    pub seen_unix: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AfkConfig {
    pub reason: String,
    #[serde(skip, default = "Instant::now")]
    pub start_time: Instant,
    pub media_url: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PersistentTask {
    pub chat_id: i64,
    pub delay: u64,
    pub file_name: String,
    pub shapka: String,
    pub user_id: Option<u64>,
    pub media: Option<(String, bool)>,
    pub task_type: String,
    #[serde(default)]
    pub origin_chat_id: Option<i64>,
    #[serde(default)]
    pub kal_command_echo: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Check1Task {
    pub chat_id: i64,
    pub user_id: u64,
    pub delay: u64,
    #[serde(default)]
    pub last_seen_unix: i64,
    #[serde(default)]
    pub last_message_id: Option<i32>,
    #[serde(default)]
    pub chat_username: Option<String>,
    #[serde(default)]
    pub tme_internal: Option<i64>,
    #[serde(default)]
    pub silence_notified: bool,
}

pub struct BotState {
    pub bot_username: String,
    pub base_path: String,
    pub start_time: Instant,
    pub online_enabled: AtomicBool,
    pub settings: DashMap<String, String>,
    pub admins: DashMap<u64, bool>,
    pub muted_users: DashMap<u64, bool>,
    pub afk_state: DashMap<u64, AfkConfig>,
    pub auto_replies_text: DashMap<u64, AutoReplyConfig>,
    pub auto_replies_media: DashMap<u64, AutoReplyConfig>,
    pub global_auto_replies_text: DashMap<u64, AutoReplyConfig>,
    pub global_auto_replies_media: DashMap<u64, AutoReplyConfig>,
    pub last_reply_at: DashMap<String, Instant>, // Anti-flood for auto-replies
    pub last_reply_at_media: DashMap<String, Instant>,
    pub last_auto_reply_message_id: DashMap<String, MessageId>,
    pub last_auto_reply_seen_at: DashMap<String, Instant>,
    pub last_auto_reply_targets: DashMap<String, LastAutoReplyTarget>,
    pub last_seen_message_id_by_chat: DashMap<i64, MessageId>,
    pub last_seen_chat_username: DashMap<i64, String>,
    pub last_seen_user_username: DashMap<u64, String>,
    pub text_auto_reply_running: DashMap<String, ()>,
    pub media_auto_reply_running: DashMap<String, ()>,
    pub last_drop_at: DashMap<u64, Instant>,
    pub ai_history: DashMap<u64, Vec<serde_json::Value>>,
    pub missions: DashMap<String, PersistentTask>,
    pub check1_tasks: DashMap<String, Check1Task>,
    pub spam_tasks_ar: DashMap<ChatId, JoinHandle<()>>,
    pub spam_tasks_arp: DashMap<ChatId, JoinHandle<()>>,
    pub spam_tasks_arv: DashMap<ChatId, JoinHandle<()>>,
    pub spam_tasks_tag: DashMap<ChatId, JoinHandle<()>>,
    pub spam_tasks_tagp: DashMap<ChatId, JoinHandle<()>>,
    pub spam_tasks_tagv: DashMap<ChatId, JoinHandle<()>>,
    pub calendar_tasks: DashMap<ChatId, JoinHandle<()>>,
    pub calendar_p_tasks: DashMap<ChatId, JoinHandle<()>>,
    pub calendar_v_tasks: DashMap<ChatId, JoinHandle<()>>,
    pub spam_tasks_sp: DashMap<ChatId, JoinHandle<()>>,
    persistence_lock: Mutex<()>,
}

impl BotState {
    pub fn new(bot_username: String, base_path: String) -> Self {
        Self {
            bot_username,
            base_path,
            start_time: Instant::now(),
            online_enabled: AtomicBool::new(true),
            settings: DashMap::new(),
            admins: DashMap::new(),
            muted_users: DashMap::new(),
            afk_state: DashMap::new(),
            auto_replies_text: DashMap::new(),
            auto_replies_media: DashMap::new(),
            global_auto_replies_text: DashMap::new(),
            global_auto_replies_media: DashMap::new(),
            last_reply_at: DashMap::new(),
            last_reply_at_media: DashMap::new(),
            last_auto_reply_message_id: DashMap::new(),
            last_auto_reply_seen_at: DashMap::new(),
            last_auto_reply_targets: DashMap::new(),
            last_seen_message_id_by_chat: DashMap::new(),
            last_seen_chat_username: DashMap::new(),
            last_seen_user_username: DashMap::new(),
            text_auto_reply_running: DashMap::new(),
            media_auto_reply_running: DashMap::new(),
            last_drop_at: DashMap::new(),
            ai_history: DashMap::new(),
            missions: DashMap::new(),
            check1_tasks: DashMap::new(),
            spam_tasks_ar: DashMap::new(),
            spam_tasks_arp: DashMap::new(),
            spam_tasks_arv: DashMap::new(),
            spam_tasks_tag: DashMap::new(),
            spam_tasks_tagp: DashMap::new(),
            spam_tasks_tagv: DashMap::new(),
            calendar_tasks: DashMap::new(),
            calendar_p_tasks: DashMap::new(),
            calendar_v_tasks: DashMap::new(),
            spam_tasks_sp: DashMap::new(),
            persistence_lock: Mutex::new(()),
        }
    }
    pub fn get_setting(&self, key: &str) -> String {
        self.settings
            .get(key)
            .map(|v| v.value().clone())
            .unwrap_or_default()
    }
    pub fn load_persistence(&mut self) -> usize {
        let path = format!(
            "{}/config/settings.json",
            self.base_path.trim_end_matches('/')
        );
        if let Ok(c) = std::fs::read_to_string(path) {
            if let Ok(j) = serde_json::from_str::<serde_json::Value>(&c) {
                if let Some(s) = j["settings"].as_object() {
                    for (k, v) in s {
                        if let Some(vs) = v.as_str() {
                            self.settings.insert(k.clone(), vs.to_string());
                        }
                    }
                }
                if let Some(a) = j["admins"].as_array() {
                    for v in a {
                        if let Some(id) = v.as_u64() {
                            self.admins.insert(id, true);
                        }
                    }
                }
                if let Some(m) = j["muted_users"].as_array() {
                    for v in m {
                        if let Some(id) = v.as_u64() {
                            self.muted_users.insert(id, true);
                        }
                    }
                }
                if let Some(a) = j["auto_replies_text"].as_object() {
                    for (k, v) in a {
                        if let Ok(uid) = k.parse::<u64>() {
                            if let Ok(cfg) = serde_json::from_value(v.clone()) {
                                self.auto_replies_text.insert(uid, cfg);
                            }
                        }
                    }
                }
                if let Some(a) = j["auto_replies_media"].as_object() {
                    for (k, v) in a {
                        if let Ok(uid) = k.parse::<u64>() {
                            if let Ok(cfg) = serde_json::from_value(v.clone()) {
                                self.auto_replies_media.insert(uid, cfg);
                            }
                        }
                    }
                }
                if let Some(a) = j["global_auto_replies_text"].as_object() {
                    for (k, v) in a {
                        if let Ok(uid) = k.parse::<u64>() {
                            if let Ok(mut cfg) =
                                serde_json::from_value::<AutoReplyConfig>(v.clone())
                            {
                                cfg.chat_id = None;
                                self.global_auto_replies_text.insert(uid, cfg);
                            }
                        }
                    }
                }
                if let Some(a) = j["global_auto_replies_media"].as_object() {
                    for (k, v) in a {
                        if let Ok(uid) = k.parse::<u64>() {
                            if let Ok(mut cfg) =
                                serde_json::from_value::<AutoReplyConfig>(v.clone())
                            {
                                cfg.chat_id = None;
                                self.global_auto_replies_media.insert(uid, cfg);
                            }
                        }
                    }
                }
                if let Some(a) = j["last_auto_reply_targets"].as_object() {
                    for (k, v) in a {
                        if let Ok(target) = serde_json::from_value::<LastAutoReplyTarget>(v.clone())
                        {
                            if target.chat_id != 0 && target.user_id != 0 && target.message_id > 0 {
                                self.last_auto_reply_targets.insert(k.clone(), target);
                            }
                        }
                    }
                }
                if let Some(ms) = j["missions"].as_object() {
                    for (k, v) in ms {
                        if let Ok(task) = serde_json::from_value::<PersistentTask>(v.clone()) {
                            self.missions.insert(k.clone(), task);
                        }
                    }
                }
                if let Some(chk) = j["check1_tasks"].as_object() {
                    for (k, v) in chk {
                        if let Ok(task) = serde_json::from_value::<Check1Task>(v.clone()) {
                            self.check1_tasks.insert(k.clone(), task);
                        }
                    }
                }
            }
        }
        self.missions.len()
            + self.check1_tasks.len()
            + self.auto_replies_text.len()
            + self.auto_replies_media.len()
            + self.global_auto_replies_text.len()
            + self.global_auto_replies_media.len()
    }

    pub fn save_persistence(&self) {
        let _guard = match self.persistence_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let path_dir = format!("{}/config", self.base_path.trim_end_matches('/'));
        let path_file = format!("{}/settings.json", path_dir);
        let mut j = serde_json::json!({"settings": {}, "admins": [], "muted_users": [], "auto_replies_text": {}, "auto_replies_media": {}, "global_auto_replies_text": {}, "global_auto_replies_media": {}, "last_auto_reply_targets": {}, "missions": {}, "check1_tasks": {}});
        for e in self.settings.iter() {
            j["settings"][e.key()] = serde_json::Value::String(e.value().clone());
        }
        for e in self.admins.iter() {
            j["admins"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!(e.key()));
        }
        for e in self.muted_users.iter() {
            j["muted_users"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!(e.key()));
        }
        for e in self.auto_replies_text.iter() {
            j["auto_replies_text"][e.key().to_string()] = serde_json::to_value(e.value()).unwrap();
        }
        for e in self.auto_replies_media.iter() {
            j["auto_replies_media"][e.key().to_string()] = serde_json::to_value(e.value()).unwrap();
        }
        for e in self.global_auto_replies_text.iter() {
            j["global_auto_replies_text"][e.key().to_string()] =
                serde_json::to_value(e.value()).unwrap();
        }
        for e in self.global_auto_replies_media.iter() {
            j["global_auto_replies_media"][e.key().to_string()] =
                serde_json::to_value(e.value()).unwrap();
        }
        for e in self.last_auto_reply_targets.iter() {
            j["last_auto_reply_targets"][e.key()] = serde_json::to_value(e.value()).unwrap();
        }
        for e in self.missions.iter() {
            j["missions"][e.key()] = serde_json::to_value(e.value()).unwrap();
        }
        for e in self.check1_tasks.iter() {
            j["check1_tasks"][e.key()] = serde_json::to_value(e.value()).unwrap();
        }

        if let Ok(json_str) = serde_json::to_string_pretty(&j) {
            if let Err(e) = backup_settings_before_write(&path_dir, &path_file) {
                log::warn!("token settings backup failed: {}", e);
            }
            if let Err(e) = write_settings_atomic(&path_dir, &path_file, &json_str) {
                log::warn!("token save_persistence failed: {}", e);
            }
        }
    }
}

fn backup_settings_before_write(path_dir: &str, path_file: &str) -> std::io::Result<()> {
    if !Path::new(path_file).is_file() {
        return Ok(());
    }
    let backup_dir = format!("{}/backups", path_dir);
    std::fs::create_dir_all(&backup_dir)?;
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let backup_file = format!("{}/settings.json.{}.bak", backup_dir, ts);
    std::fs::copy(path_file, backup_file)?;
    prune_settings_backups(&backup_dir, 50)?;
    Ok(())
}

fn prune_settings_backups(backup_dir: &str, keep: usize) -> std::io::Result<()> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(backup_dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with("settings.json.") && name.ends_with(".bak") {
            let modified = entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(UNIX_EPOCH);
            files.push((modified, path));
        }
    }
    if files.len() <= keep {
        return Ok(());
    }
    files.sort_by_key(|(modified, _)| *modified);
    let remove_count = files.len().saturating_sub(keep);
    for (_, path) in files.into_iter().take(remove_count) {
        let _ = std::fs::remove_file(path);
    }
    Ok(())
}

fn write_settings_atomic(path_dir: &str, path_file: &str, json_str: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(path_dir)?;
    let tmp_file = format!("{}.tmp.{}", path_file, std::process::id());
    std::fs::write(&tmp_file, json_str)?;
    match std::fs::rename(&tmp_file, path_file) {
        Ok(()) => Ok(()),
        Err(rename_err) => {
            if cfg!(windows) {
                std::fs::copy(&tmp_file, path_file)?;
                let _ = std::fs::remove_file(&tmp_file);
                Ok(())
            } else {
                let _ = std::fs::remove_file(&tmp_file);
                Err(rename_err)
            }
        }
    }
}
