pub mod token;
mod account;
mod cli_admin;
mod host_uptime;
mod logger_prompt;
mod ntfy;

mod token;

use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use account::config::Config as AccountConfig;
use account::state::BotState as AccountBotState;
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
            "add-account" => cli_admin::add_account(&argv),
            "remove-account" => cli_admin::remove_account(&argv),
            _ => Ok(()),
        };
        if cmd == "add-token"
            || cmd == "remove-token"
            || cmd == "add-account"
            || cmd == "remove-account"
        {
            if let Err(e) = res {
                eprintln!("{:#}", e);
                panic!("fatal error, restarting");
            }
            return;
        }
    }

    log::info!("Rayzen unified: аккаунты (Grammers) + токен-боты (Teloxide из bots.json)");

    let config = match AccountConfig::from_env() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            log::warn!("Не прочитан .env: {}. Дефолтные пути.", e);
            Arc::new(AccountConfig {
                owner_id: 8681126108,
                base_path: ".".to_string(),
            })
        }
    };

    if !acquire_single_instance_lock(&config.base_path) {
        std::process::exit(0);
    }
    spawn_heartbeat(config.base_path.clone());

    let account_path = config.base_path.clone();
    let owner_id_cfg = config.owner_id;
    let token_bot_root = config.base_path.clone();

    let mut account_tasks: tokio::task::JoinSet<Result<(), String>> = tokio::task::JoinSet::new();
    let mut token_tasks: tokio::task::JoinSet<Result<(), String>> = tokio::task::JoinSet::new();
    #[allow(unused_assignments)]
    let mut account_started = false;

    let spawn_account = |set: &mut tokio::task::JoinSet<Result<(), String>>,
                         name: String,
                         abs_path: String,
                         oid: Option<u64>,
                         stem: Option<String>| {
        set.spawn(async move {
            run_account_instance(name, abs_path, oid, stem)
                .await
                .map_err(|e| format!("{:#}", e))
        });
    };

    let sessions_dir = format!("{}/sessions", account_path.trim_end_matches('/'));
    log::info!("Сессии (легаси): {}", sessions_dir);
    let _ = fs::create_dir_all(&sessions_dir);

    let multi = discover_account_folders(account_path.trim_end_matches('/'));
    if !multi.is_empty() {
        log::info!(
            "Режим папок accounts/* ({} шт.) — корневые sessions/*.session не сканируются",
            multi.len()
        );
        for (name, abs_path, oid, stem) in multi {
            log::info!("Аккаунт {} → {} (stem={})", name, abs_path, stem);
            spawn_account(&mut account_tasks, name, abs_path, Some(oid), Some(stem));
        }
    } else {
        let stems_path = format!(
            "{}/sessions/session_stems.txt",
            account_path.trim_end_matches('/')
        );
        let mut stems_from_file: Vec<String> = Vec::new();
        if let Ok(stems_raw) = fs::read_to_string(&stems_path) {
            for line in stems_raw.lines() {
                let t = line.trim();
                if !t.is_empty() && !t.starts_with('#') {
                    stems_from_file.push(t.to_string());
                }
            }
        }

        if !stems_from_file.is_empty() {
            for session_stem in stems_from_file {
                let account_path_clone = account_path.clone();
                let name = format!("Acc_{}", session_stem);
                let oid = owner_id_cfg;
                let stem = session_stem.clone();
                log::info!("Слот session_stems.txt: {}", session_stem);
                spawn_account(
                    &mut account_tasks,
                    name,
                    account_path_clone,
                    Some(oid),
                    Some(stem),
                );
                account_started = true;
            }
        } else if let Ok(entries) = fs::read_dir(&sessions_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("session") {
                    let session_stem = path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("account")
                        .to_string();
                    let account_path_clone = account_path.clone();
                    let name = format!("Acc_{}", session_stem);
                    let oid = owner_id_cfg;
                    log::info!("Найдена сессия: {}", session_stem);
                    spawn_account(
                        &mut account_tasks,
                        name,
                        account_path_clone,
                        Some(oid),
                        Some(session_stem.clone()),
                    );
                    account_started = true;
                }
            }
        }

        if !account_started {
            log::info!(
                "Нет accounts/*/account.json и нет sessions/*.session — первый вход: {}/sessions/rayzen_bot.session",
                account_path.trim_end_matches('/')
            );
            let ap = account_path.clone();
            let oid = owner_id_cfg;
            spawn_account(
                &mut account_tasks,
                "Account".to_string(),
                ap,
                Some(oid),
                None,
            );
        }
    }

    let bots_json_path = format!("{}/bots.json", token_bot_root.trim_end_matches('/'));
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

    loop {
        tokio::select! {
            r = account_tasks.join_next() => {
                match r {
                    None => {
                        log::error!("Все аккаунты завершились — рестарт.");
                        panic!("fatal error, restarting");
                    }
                    Some(Ok(Ok(()))) => {
                        log::error!("Аккаунт завершился без ошибки — рестарт.");
                        panic!("fatal error, restarting");
                    }
                    Some(Ok(Err(e))) => {
                        log::error!("Аккаунт упал: {} — рестарт.", e);
                        panic!("fatal error, restarting");
                    }
                    Some(Err(e)) => {
                        log::error!("Аккаунт panic/join error: {} — рестарт.", e);
                        panic!("fatal error, restarting");
                    }
                }
            }
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
        }
    }
}

fn discover_account_folders(base: &str) -> Vec<(String, String, u64, String)> {
    let mut out = Vec::new();
    let root = Path::new(base).join("accounts");
    if !root.is_dir() {
        return out;
    }
    let Ok(rd) = fs::read_dir(&root) else {
        return out;
    };
    for ent in rd.flatten() {
        let p = ent.path();
        if !p.is_dir() {
            continue;
        }
        let aj = p.join("account.json");
        if !aj.is_file() {
            continue;
        }
        let Ok(raw) = fs::read_to_string(&aj) else {
            continue;
        };
        let Ok(j) = serde_json::from_str::<serde_json::Value>(&raw) else {
            continue;
        };
        let oid = j
            .get("owner_id")
            .and_then(|v| v.as_u64())
            .or_else(|| j.get("owner_id").and_then(|v| v.as_i64()).map(|i| i as u64));
        let Some(oid) = oid else {
            log::warn!("account.json без owner_id: {:?}", aj);
            continue;
        };
        let slug = ent.file_name().to_string_lossy().to_string();
        let stem = j
            .get("session_stem")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();
        let stem = if stem.is_empty() {
            slug.clone()
        } else {
            stem.to_string()
        };
        let abs = p.to_string_lossy().replace('\\', "/");
        out.push((format!("Acc_{}", slug), abs, oid, stem));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn apply_account_json_logger(
    path_clean: &str,
    bot_token: &mut String,
    settings: &dashmap::DashMap<String, String>,
) {
    let p = format!("{}/account.json", path_clean);
    let Ok(raw) = fs::read_to_string(&p) else {
        return;
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return;
    };
    if let Some(t) = v.get("logger_bot_token").and_then(|x| x.as_str()) {
        let t = t.trim();
        if !t.is_empty() {
            *bot_token = t.to_string();
        }
    }
    if let Some(l) = v.get("log_id").and_then(|x| x.as_str()) {
        let l = l.trim();
        if !l.is_empty() {
            settings.insert("log_id".to_string(), l.to_string());
        }
    }
}

fn normalize_key_file_content(s: &str) -> String {
    s.strip_prefix('\u{FEFF}')
        .unwrap_or(s)
        .lines()
        .find(|l| !l.trim().is_empty())
        .map(|l| l.trim().to_string())
        .unwrap_or_default()
}

async fn run_account_instance(
    name: String,
    path: String,
    owner_id: Option<u64>,
    session_name: Option<String>,
) -> anyhow::Result<()> {
    let mut bot_state = AccountBotState::new(name, path.clone());
    let path_clean = path.trim_end_matches('/');

    if let Some(oid) = owner_id {
        bot_state
            .settings
            .insert("owner_id".to_string(), oid.to_string());
    }

    // ЖЕСТКО ЗАШИТЫЕ КЛЮЧИ — ПРОБЛЕМА С ПОИСКОМ ФАЙЛОВ РЕШЕНА РАЗ И НАВСЕГДА
    let api_id: i32 = 2040;
    let api_hash: String = "b18441a1ff607e10a989891a5462e627".to_string();
    log::info!("Используются встроенные API ID и API Hash для аккаунта.");

    bot_state.load_persistence();
    logger_prompt::load_logger_files_account(
        path_clean,
        &mut bot_state.bot_token,
        &bot_state.settings,
    );
    apply_account_json_logger(path_clean, &mut bot_state.bot_token, &bot_state.settings);
    if bot_state.bot_token.is_empty() {
        if let Ok(t) = std::env::var("LOGGER_BOT_TOKEN") {
            let tok = normalize_key_file_content(&t);
            if !tok.is_empty() {
                bot_state.bot_token = tok;
            }
        }
    }
    logger_prompt::interactive_fill_account(
        path_clean,
        &mut bot_state.bot_token,
        &bot_state.settings,
    )
    .await;

    let state = Arc::new(bot_state);

    let state_worker = state.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(60)).await;
            state_worker.save_persistence();
        }
    });

    let session_file = match session_name {
        Some(ref s_name) => format!("{}/sessions/{}.session", path_clean, s_name),
        None => format!("{}/sessions/rayzen_bot.session", path_clean),
    };

    if let Some(parent) = std::path::Path::new(&session_file).parent() {
        let _ = fs::create_dir_all(parent);
    }
    let path_norm = path_clean.replace('\\', "/");
    let is_folder_mode = path_norm.contains("/accounts/")
        && std::path::Path::new(path_clean)
            .join("account.json")
            .is_file();
    let base_path_root = if is_folder_mode {
        path_norm
            .split("/accounts/")
            .next()
            .unwrap_or(path_clean)
            .trim_end_matches('/')
            .to_string()
    } else {
        path_clean.to_string()
    };

    let is_fatal_grammers_error = |msg: &str| {
        let m = msg.to_lowercase();
        m.contains("session_revoked")
            || m.contains("auth_key_unregistered")
            || m.contains("auth key")
            || m.contains("user_deactivated")
            || m.contains("phone_code_invalid")
            || m.contains("api_id_invalid")
            || m.contains("requires interactive login")
            || m.contains("token bot requires token")
    };

    let mut attempt = 0;
    loop {
        attempt = attempt + 1;
        let session = grammers_session::Session::load_file_or_create(&session_file)?;
        let client = match grammers_client::Client::connect(grammers_client::Config {
            session,
            api_id,
            api_hash: api_hash.clone(),
            params: grammers_client::InitParams {
                update_queue_limit: Some(10_000),
                ..Default::default()
            },
        })
        .await
        {
            Ok(c) => c,
            Err(e) => {
                let s = format!("{:#}", e);
                log::error!(
                    "Аккаунт {}: Client::connect failed (attempt={}): {}",
                    state.bot_username,
                    attempt,
                    s
                );
                let should_drop_slot = is_fatal_grammers_error(&s)
                    || account::session_cleanup::err_indicates_session_gone(&s);
                if should_drop_slot {
                    let removed_target = if is_folder_mode {
                        path_clean.to_string()
                    } else {
                        session_file.clone()
                    };
                    if is_folder_mode {
                        let _ = account::session_cleanup::remove_invalid_account_dir(
                            path_clean,
                            &base_path_root,
                            "Client::connect",
                        );
                    } else {
                        let _ = account::session_cleanup::remove_invalid_session_file(
                            &session_file,
                            "Client::connect",
                        );
                    }
                    account::userbot::log_slot_removed_to_owner(
                        &state,
                        &state.bot_username,
                        &s,
                        &removed_target,
                    )
                    .await;
                    log::error!(
                        "Аккаунт {} акк мертв - слот удаляем",
                        state.bot_username
                    );
                    return Err(anyhow::anyhow!("{}", s));
                }
                let backoff = (attempt.min(12) as u64).saturating_mul(5).max(5);
                sleep(Duration::from_secs(backoff)).await;
                continue;
            }
        };

        match account::userbot::run_bot(
            client,
            state.clone(),
            false,
            api_id,
            api_hash.clone(),
            session_name.clone(),
        )
        .await
        {
            Ok(_) => {
                let s = "grammers userbot loop stopped without error";
                log::error!(
                    "Аккаунт {}: {} (attempt={})",
                    state.bot_username,
                    s,
                    attempt
                );
                return Err(anyhow::anyhow!("{}", s));
            }
            Err(e) => {
                let s = format!("{:#}", e);
                log::error!(
                    "Аккаунт {}: run_bot failed (attempt={}): {}",
                    state.bot_username,
                    attempt,
                    s
                );
                let should_drop_slot = is_fatal_grammers_error(&s)
                    || account::session_cleanup::err_indicates_session_gone(&s);
                if should_drop_slot {
                    let removed_target = if is_folder_mode {
                        path_clean.to_string()
                    } else {
                        session_file.clone()
                    };
                    if is_folder_mode {
                        let _ = account::session_cleanup::remove_invalid_account_dir(
                            path_clean,
                            &base_path_root,
                            "run_bot",
                        );
                    } else {
                        let _ = account::session_cleanup::remove_invalid_session_file(
                            &session_file,
                            "run_bot",
                        );
                    }
                    account::userbot::log_slot_removed_to_owner(
                        &state,
                        &state.bot_username,
                        &s,
                        &removed_target,
                    )
                    .await;
                    log::error!(
                        "Аккаунт {} акк мертв - слот удаляем",
                        state.bot_username
                    );
                    return Err(anyhow::anyhow!("{}", s));
                }
                return Err(anyhow::anyhow!("{}", s));
            }
        }
    }
}