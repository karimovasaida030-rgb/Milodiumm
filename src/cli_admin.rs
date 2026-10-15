// TODO: переписать это говно
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(target_family = "unix")]
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use serde_json::json;

fn base_path_from_env() -> Result<String> {
    let content = fs::read_to_string(".env").context("прочитать .env")?;
    for line in content.lines() {
        let line = line.trim();
        if let Some(v) = line.strip_prefix("BASE_PATH=") {
            return Ok(v.trim().trim_matches('"').to_string());
        }
    }
    Err(anyhow!("BASE_PATH= не найден в .env"))
}

fn bots_json_path(base: &str) -> PathBuf {
    PathBuf::from(base.trim_end_matches('/')).join("bots.json")
}

fn mkdir_instance_layout(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir.join("config"))?;
    fs::create_dir_all(dir.join("templates"))?;
    fs::create_dir_all(dir.join("TOKEN"))?;
    Ok(())
}

fn mkdir_account_layout(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir.join("api_keys"))?;
    fs::create_dir_all(dir.join("sessions"))?;
    fs::create_dir_all(dir.join("config"))?;
    fs::create_dir_all(dir.join("templates"))?;
    fs::create_dir_all(dir.join("TOKEN"))?;
    Ok(())
}

fn normalize_base_path_key(s: &str) -> String {
    s.trim().trim_end_matches('/').to_string()
}

fn safe_remove_dir_all(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    fs::remove_dir_all(path).with_context(|| format!("remove_dir_all {:?}", path))?;
    if path.exists() {
        #[cfg(target_family = "unix")]
        {
            let status = Command::new("rm")
                .arg("-rf")
                .arg(path)
                .status()
                .with_context(|| format!("rm -rf {:?}", path))?;
            if !status.success() {
                return Err(anyhow!("не удалось удалить каталог {:?} (rm -rf)", path));
            }
        }
    }
    if path.exists() {
        return Err(anyhow!("каталог остался после удаления: {:?}", path));
    }
    Ok(())
}

pub fn add_token(args: &[String]) -> Result<()> {
    if args.len() < 4 {
        return Err(anyhow!(
            "Использование: rayzen-bot add-token <TOKEN> <OWNER_ID> [slug]\n\
             Без slug папка будет instances/<OWNER_ID>/"
        ));
    }
    let token = args[2].trim();
    if token.is_empty() {
        return Err(anyhow!("пустой токен"));
    }
    let owner_id: u64 = args[3]
        .parse()
        .map_err(|_| anyhow!("OWNER_ID должен быть числом"))?;
    let base = base_path_from_env()?;
    let slug = if args.len() >= 5 {
        let s = sanitize_slug(&args[4]);
        if s == "slot1" {
            return Err(anyhow!(
                "slug содержит недопустимые символы; используй только [a-zA-Z0-9_-]"
            ));
        }
        s
    } else {
        owner_id.to_string()
    };
    let base_path = format!("instances/{}/", slug);
    let abs = Path::new(base.trim_end_matches('/')).join(&base_path);
    mkdir_instance_layout(&abs)?;

    let bots_path = bots_json_path(&base);
    let mut arr: Vec<serde_json::Value> = fs::read_to_string(&bots_path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|j| j.as_array().cloned())
        .unwrap_or_default();

    let want = normalize_base_path_key(&base_path);
    for v in &arr {
        if let Some(bp) = v["base_path"].as_str() {
            if normalize_base_path_key(bp) == want {
                return Err(anyhow!("запись с base_path {} уже есть", base_path));
            }
        }
    }

    arr.push(json!({
        "teloxide_token": token,
        "owner_id": owner_id,
        "admin_ids": [],
        "bot_username": "",
        "base_path": base_path,
        "mistral_api_key": ""
    }));

    fs::write(&bots_path, serde_json::to_string_pretty(&arr)?)?;
    log::info!(
        "OK: Teloxide-инстанс «{}» → {} (добавлено в {:?})",
        slug,
        base_path,
        bots_path
    );
    println!(
        "OK slug={} base_path={} bots.json обновлён",
        slug, base_path
    );
    Ok(())
}

pub fn remove_token(args: &[String]) -> Result<()> {
    if args.len() < 3 {
        return Err(anyhow!(
            "Использование: rayzen-bot remove-token <slug|OWNER_ID>\n\
             Пример: 44287845 — удалит запись с owner_id и/или instances/44287845/"
        ));
    }
    let raw_arg = args[2].trim();
    let slug = sanitize_slug(raw_arg);
    let owner_match: Option<u64> = raw_arg.parse().ok();
    let base = base_path_from_env()?;
    let bots_path = bots_json_path(&base);
    let raw = fs::read_to_string(&bots_path).context("bots.json")?;
    let arr: Vec<serde_json::Value> = serde_json::from_str(&raw).context("parse bots.json")?;
    let old_len = arr.len();
    let needle_folder = normalize_base_path_key(&format!("instances/{}/", slug));
    let base_pb = Path::new(base.trim_end_matches('/'));

    let mut matched_indexes: Vec<usize> = arr
        .iter()
        .enumerate()
        .filter_map(|(idx, v)| {
            let bp = v["base_path"].as_str()?;
            (normalize_base_path_key(bp) == needle_folder).then_some(idx)
        })
        .collect();

    if matched_indexes.is_empty() {
        if let Some(oid) = owner_match {
            matched_indexes = arr
                .iter()
                .enumerate()
                .filter_map(|(idx, v)| (v["owner_id"].as_u64() == Some(oid)).then_some(idx))
                .collect();
            if matched_indexes.len() > 1 {
                return Err(anyhow!(
                    "найдено несколько записей с owner_id={}; удаляй по slug/имени папки instances/<slug>",
                    oid
                ));
            }
        }
    }

    if matched_indexes.is_empty() {
        return Err(anyhow!(
            "в bots.json нет записи: instances/{}/ или owner_id={}",
            slug,
            raw_arg
        ));
    }

    let mut dirs_to_rm: Vec<PathBuf> = Vec::new();
    let mut filtered: Vec<serde_json::Value> = Vec::with_capacity(old_len);
    for (idx, v) in arr.into_iter().enumerate() {
        if matched_indexes.contains(&idx) {
            if let Some(bp) = v["base_path"].as_str() {
                let bp_clean = bp.trim().trim_end_matches('/');
                if !bp_clean.is_empty() && !bp_clean.contains("..") {
                    if bp_clean.starts_with('/') {
                        dirs_to_rm.push(PathBuf::from(bp_clean));
                    } else {
                        dirs_to_rm.push(base_pb.join(bp_clean.trim_start_matches('/')));
                    }
                }
            }
        } else {
            filtered.push(v);
        }
    }

    fs::write(&bots_path, serde_json::to_string_pretty(&filtered)?)?;
    dirs_to_rm.sort();
    dirs_to_rm.dedup();
    for p in &dirs_to_rm {
        safe_remove_dir_all(p)?;
    }
    println!(
        "OK: удалены записи из bots.json; каталоги: {:?}",
        dirs_to_rm
    );
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AccountMeta {
    owner_id: u64,
    #[serde(default)]
    session_stem: String,
    #[serde(default)]
    logger_bot_token: String,
    #[serde(default)]
    log_id: String,
}

pub fn add_account(args: &[String]) -> Result<()> {
    if args.len() < 6 {
        return Err(anyhow!(
            "Использование: rayzen-bot add-account <slug> <owner_id> <api_id> <api_hash> [logger_bot_token] [log_id]\n\
             Сессия будет: accounts/<slug>/sessions/<slug>.session (создай через rayzen-session)"
        ));
    }
    let slug = sanitize_slug(&args[2]);
    let owner_id: u64 = args[3].parse().map_err(|_| anyhow!("owner_id"))?;
    let api_id: i32 = args[4].parse().map_err(|_| anyhow!("api_id"))?;
    let api_hash = args[5].trim().to_string();
    if api_id == 0 || api_hash.is_empty() {
        return Err(anyhow!("api_id/api_hash"));
    }
    let logger_bot_token = args.get(6).map(|s| s.clone()).unwrap_or_default();
    let log_id = args.get(7).map(|s| s.clone()).unwrap_or_default();

    let base = base_path_from_env()?;
    let acc = Path::new(base.trim_end_matches('/'))
        .join("accounts")
        .join(&slug);
    if acc.exists() {
        return Err(anyhow!("папка {:?} уже существует", acc));
    }
    mkdir_account_layout(&acc)?;
    fs::write(acc.join("api_keys").join("api_id.txt"), api_id.to_string())?;
    fs::write(acc.join("api_keys").join("api_hash.txt"), &api_hash)?;

    let meta = AccountMeta {
        owner_id,
        session_stem: slug.clone(),
        logger_bot_token,
        log_id,
    };
    fs::write(
        acc.join("account.json"),
        serde_json::to_string_pretty(&meta)?,
    )?;

    if !meta.logger_bot_token.is_empty() {
        fs::write(
            acc.join("TOKEN").join("logger_bot_token.txt"),
            format!("{}\n", meta.logger_bot_token.trim()),
        )?;
    }
    if !meta.log_id.is_empty() {
        fs::write(
            acc.join("TOKEN").join("log_id.txt"),
            format!("{}\n", meta.log_id.trim()),
        )?;
    }

    println!(
        "OK: accounts/{} — api_keys + account.json. Сессию создай:\n  cargo run --release --bin rayzen-session -- \"{}\" {}",
        slug,
        acc.display(),
        slug
    );
    Ok(())
}

pub fn remove_account(args: &[String]) -> Result<()> {
    if args.len() < 3 {
        return Err(anyhow!("Использование: rayzen-bot remove-account <slug>"));
    }
    let slug = sanitize_slug(&args[2]);
    let base = base_path_from_env()?;
    let acc = Path::new(base.trim_end_matches('/'))
        .join("accounts")
        .join(&slug);
    if !acc.exists() {
        return Err(anyhow!("нет {:?}", acc));
    }
    fs::remove_dir_all(&acc).with_context(|| format!("remove {:?}", acc))?;
    println!("OK: удалена папка accounts/{}", slug);
    Ok(())
}

fn sanitize_slug(s: &str) -> String {
    let t = s.trim();
    let mut out = String::new();
    for c in t.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
            out.push(c);
        }
    }
    if out.is_empty() {
        "slot1".to_string()
    } else {
        out
    }
}
