mod cli_admin;
mod host_uptime;
mod logger_prompt;
mod ntfy;

mod token;

use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use tokio::time::{sleep, Duration};

fn is_process_alive(pid: u32) -> bool {
    Path::new("/proc").join(pid.to_string()).exists()
}

fn acquire_single_instance_lock(base_path: &str) -> bool {
    let bp = base_path.trim_end_matches('/');
    let lock_path = format!("{}/rayzen-bot.lock", bp);

    match std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&lock_path)
    {
        Ok(mut f) => {
            let _ = writeln!(f, "{}", std::process::id());
            return true;
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if let Ok(s) = std::fs::read_to_string(&lock_path) {
                if let Ok(pid) = s.trim().parse::<u32>() {
                    if pid != 0 && is_process_alive(pid) {
                        log::error!(
                            "Второй запуск запрещён: уже есть rayzen-bot PID {} (lock: {}).",
                            pid,
                            lock_path
                        );
                        return false;
                    }
                }
            }
            let _ = std::fs::remove_file(&lock_path);
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&lock_path)
            {
                let _ = writeln!(f, "{}", std::process::id());
                return true;
            }
            log::warn!("не вышло поставить lock-файл: {}", lock_path);
            true
        }
        Err(_) => true,
    }
}

fn spawn_heartbeat(base_path: String) {
    tokio::spawn(async move {
        let heartbeat_path = format!("{}/rayzen-bot.heartbeat", base_path.trim_end_matches('/'));
        loop {
            let now = chrono::Utc::now().to_rfc3339();
            if let Err(e) = tokio::fs::write(&heartbeat_path, format!("{}\n", now)).await {
                log::warn!("heartbeat write failed ({}): {}", heartbeat_path, e);
            }
            sleep(Duration::from_secs(30)).await;
        }
    });
}

#[tokio::main]
async fn main() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    pretty_env_logger::init();

    let argv: Vec<String> = std::env::args().collect();
    if argv.len() > 1 {
        let cmd = argv[1].as_str();
        let res = match cmd {
            "add-token" => cli_admin::add_token(&argv),
            "remove-token" => cli_admin::remove_token(&argv),
            _ => Ok(()),
        };
        if cmd == "add-token" || cmd == "remove-token" {
            if let Err(e) = res {
                eprintln!("{:#}", e);
                panic!("fatal error, restarting");
            }
            return;
        }
    }

    log::info!("Rayzen unified: ТОЛЬКО токен-боты (Teloxide из bots.json)");

    let base_path = ".".to_string();

    if !acquire_single_instance_lock(&base_path) {
        std::process::exit(0);
    }
    spawn_heartbeat(base_path.clone());

    let mut token_tasks: tokio::task::JoinSet<Result<(), String>> = tokio::task::JoinSet::new();

    let bots_json_path = format!("{}/bots.json", base_path.trim_end_matches('/'));
    let bot_entries: Vec<serde_json::Value> = fs::read_to_string(&bots_json_path)
        .ok()
        .and_then(|c| serde_json::from_str::<serde_json::Value>(&c).ok())
        .and_then(|j| j.as_array().cloned())
        .unwrap_or_default();

    for (idx, bot_val) in bot_entries.into_iter().enumerate() {
        let cfg: token::config::Config = match serde_json::from_value(bot_val) {
            Ok(c) => c,
            Err(e) => {
                log::warn!("bots.json[{}] пропуск: {}", idx, e);
                continue;
            }
        };
        if cfg.teloxide_token.trim().is_empty() {
            continue;
        }
        let instance = idx + 1;
        token_tasks.spawn(async move {
            log::info!("Teloxide бот #{} → {}", instance, cfg.base_path);
            token::runner::run_teloxide_bot(cfg)
                .await
                .map_err(|e| format!("Teloxide #{}: {:#}", instance, e))
        });
    }

    if token_tasks.is_empty() {
        log::warn!("В bots.json нет активных ботов!");
    }

    loop {
        tokio::select! {
            t = token_tasks.join_next(), if !token_tasks.is_empty() => {
                match t {
                    Some(Ok(Ok(()))) => {
                        log::error!("Token task завершился без ошибки — рестарт.");
                        panic!("fatal error, restarting");
                    }
                    Some(Ok(Err(e))) => {
                        log::error!("{} — рестарт.", e);
                        panic!("fatal error, restarting");
                    }
                    Some(Err(e)) => {
                        log::error!("Token task panic/join error: {} — рестарт.", e);
                        panic!("fatal error, restarting");
                    }
                    None => {}
                }
            }
            else => {
                sleep(Duration::from_secs(3600)).await;
            }
        }
    }
}