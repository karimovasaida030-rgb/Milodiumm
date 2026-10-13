use dashmap::DashMap;
use serde::{Deserialize, Serialize};
// хз почему это тут, вроде надо
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;
use std::time::Instant;
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
pub struct AfkConfig {
    pub reason: String,
    pub start_time: u64,
    pub media_url: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PersistentTask {
    pub chat_id: i64,
    pub packed_ty: u8,
    pub access_hash: Option<i64>,
    pub delay: u64,
    pub file_name: String,
    pub shapka: String,
    pub user_id: Option<u64>,
    pub media_url: Option<String>,
    pub is_video: bool,
    pub task_type: String,
    #[serde(default)]
    pub mirror_chat_id: Option<i64>,
    #[serde(default)]
    pub kal_command_echo: Option<String>,
    #[serde(default)]
    pub scheduled_target_ids: Vec<i32>,
    #[serde(default)]
    pub scheduled_origin_ids: Vec<i32>,
    #[serde(default)]
    pub calendar_restart_due_at: Option<u64>,
    #[serde(default)]
    pub calendar_cycle_started_at: Option<u64>,
    #[serde(default)]
    pub calendar_watchdog_generation: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Check1Task {
    pub chat_id: i64,
    pub user_id: u64,
    pub delay: u64,
    pub last_time: Option<u64>,
    #[serde(default)]
    pub last_alert: Option<u64>,
    #[serde(default)]
    pub last_message_id: Option<i32>,
    #[serde(default)]
    pub silence_notified: bool,
    #[serde(default)]
    pub link_chat_id: Option<i64>,
    #[serde(default)]
    pub chat_username: Option<String>,
    #[serde(default)]
    pub tme_internal: Option<i64>,
}

pub struct BotState {
    pub bot_username: String,
    pub bot_token: String,
    pub base_path: String,
    pub start_time: Instant,
    pub online_enabled: AtomicBool,
    pub settings: DashMap<String, String>,
    pub admins: DashMap<u64, bool>,
    pub muted_users: DashMap<u64, bool>,
    pub known_usernames: DashMap<u64, String>,
    pub known_chat_usernames: DashMap<i64, String>,
    pub last_outgoing_message_id_by_chat: DashMap<i64, i32>,
    pub afk_state: DashMap<u64, AfkConfig>,
    pub afk_notified: DashMap<u64, ()>,
    pub auto_replies_text: DashMap<u64, AutoReplyConfig>,
    pub auto_replies_media: DashMap<u64, AutoReplyConfig>,
    pub global_auto_replies_text: DashMap<u64, AutoReplyConfig>,
    pub global_auto_replies_media: DashMap<u64, AutoReplyConfig>,
    pub last_reply_at: DashMap<String, Instant>,
    pub last_reply_at_media: DashMap<String, Instant>,
    pub last_auto_reply_message_id: DashMap<String, i32>,
    pub last_auto_reply_seen_at: DashMap<String, Instant>,
    pub handled_auto_reply_message_id: DashMap<String, i32>,
    pub text_auto_reply_running: DashMap<String, ()>,
    pub media_auto_reply_running: DashMap<String, ()>,
    pub last_auto_reply_per_chat: DashMap<i64, Instant>,
    pub last_drop_at: DashMap<u64, Instant>,
    pub processed_commands: DashMap<String, Instant>,
    #[allow(dead_code)]
    pub ai_history: DashMap<u64, Vec<serde_json::Value>>,
    pub missions: DashMap<String, PersistentTask>,
    pub check1_tasks: DashMap<String, Check1Task>,
    pub spam_tasks_ar: DashMap<i64, JoinHandle<()>>,
    pub spam_tasks_arp: DashMap<i64, JoinHandle<()>>,
    pub spam_tasks_arv: DashMap<i64, JoinHandle<()>>,
    pub spam_tasks_tag: DashMap<i64, JoinHandle<()>>,
    pub spam_tasks_tagp: DashMap<i64, JoinHandle<()>>,
    pub spam_tasks_tagv: DashMap<i64, JoinHandle<()>>,
    pub calendar_tasks: DashMap<i64, JoinHandle<()>>,
    pub calendar_p_tasks: DashMap<i64, JoinHandle<()>>,
    pub calendar_v_tasks: DashMap<i64, JoinHandle<()>>,
    pub spam_tasks_sp: DashMap<i64, JoinHandle<()>>,
    persistence_lock: Mutex<()>,
}

impl BotState {
    pub fn new(bot_username: String, base_path: String) -> Self {
        Self {
            bot_username,
            bot_token: String::new(),
            base_path,
            start_time: Instant::now(),
            online_enabled: AtomicBool::new(true),
            settings: DashMap::new(),
            admins: DashMap::new(),
            muted_users: DashMap::new(),
            known_usernames: DashMap::new(),
            known_chat_usernames: DashMap::new(),
            last_outgoing_message_id_by_chat: DashMap::new(),
            afk_state: DashMap::new(),
            afk_notified: DashMap::new(),
            auto_replies_text: DashMap::new(),
            auto_replies_media: DashMap::new(),
            global_auto_replies_text: DashMap::new(),
            global_auto_replies_media: DashMap::new(),
            last_reply_at: DashMap::new(),
            last_reply_at_media: DashMap::new(),
            last_auto_reply_message_id: DashMap::new(),
            last_auto_reply_seen_at: DashMap::new(),
            handled_auto_reply_message_id: DashMap::new(),
            text_auto_reply_running: DashMap::new(),
            media_auto_reply_running: DashMap::new(),
            last_auto_reply_per_chat: DashMap::new(),
            last_drop_at: DashMap::new(),
            processed_commands: DashMap::new(),
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
                if let Some(ms) = j["missions"].as_object() {
                    for (k, v) in ms {
                        if let Ok(task) = serde_json::from_value::<PersistentTask>(v.clone()) {
                            let key = canonical_mission_key(k, &task);
                            self.missions.insert(key, task);
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
        let mut j = serde_json::json!({"settings": {}, "admins": [], "muted_users": [], "auto_replies_text": {}, "auto_replies_media": {}, "global_auto_replies_text": {}, "global_auto_replies_media": {}, "missions": {}, "check1_tasks": {}});
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
        for e in self.missions.iter() {
            j["missions"][e.key()] = serde_json::to_value(e.value()).unwrap();
        }
        for e in self.check1_tasks.iter() {
            j["check1_tasks"][e.key()] = serde_json::to_value(e.value()).unwrap();
        }
        if let Ok(json_str) = serde_json::to_string_pretty(&j) {
            if let Err(e) = write_settings_atomic(&path_dir, &path_file, &json_str) {
                log::warn!("account save_persistence failed: {}", e);
            }
        }
    }
}

fn canonical_mission_key(saved_key: &str, task: &PersistentTask) -> String {
    let expected = format!("{}_{}", task.chat_id, task.task_type);
    if saved_key == task.chat_id.to_string() {
        return expected;
    }
    saved_key.to_string()
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
