use chrono::{NaiveDate, NaiveDateTime};
use reqwest;
// хз почему это тут, вроде надо
use std::time::{SystemTime, UNIX_EPOCH};
use sysinfo::System;
use tokio::time::Duration;

use crate::host_uptime::{format_uptime_compact, os_uptime_secs};

const LEGACY_TEMPLATES_DIR: &str = "Шаблоны & Автоответчик";
const MAX_TEMPLATE_FILE_NAME_LEN: usize = 128;
pub const MAX_TEMPLATE_FILE_SIZE_BYTES: u64 = 5 * 1024 * 1024;
pub const DROP_COOLDOWN_SECS: u64 = 5;

pub fn format_template_size_limit() -> String {
    format!("{} МБ", MAX_TEMPLATE_FILE_SIZE_BYTES / 1024 / 1024)
}

pub fn get_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub fn templates_dir(base_path: &str) -> String {
    format!("{}/templates", base_path.trim_end_matches('/'))
}

pub fn safe_template_file_name(raw: &str) -> Option<String> {
    let name = raw.trim();
    if name.is_empty()
        || name.len() > MAX_TEMPLATE_FILE_NAME_LEN
        || name.contains("..")
        || name.contains('/')
        || name.contains('\\')
    {
        return None;
    }
    let file_name = std::path::Path::new(name).file_name()?.to_str()?.trim();
    if file_name.is_empty() || file_name != name || !file_name.to_lowercase().ends_with(".txt") {
        return None;
    }
    Some(file_name.to_string())
}

pub fn template_file_path(base_path: &str, file_name: &str) -> String {
    format!("{}/{}", templates_dir(base_path), file_name.trim())
}

pub fn template_candidate_paths(base_path: &str, file_name: &str) -> Vec<String> {
    let base = base_path.trim_end_matches('/');
    vec![
        format!("{}/templates/{}", base, file_name),
        format!("templates/{}", file_name),
        format!("{}/{}/{}", base, LEGACY_TEMPLATES_DIR, file_name),
        format!("{}/{}", LEGACY_TEMPLATES_DIR, file_name),
    ]
}

pub async fn read_template_file(base_path: &str, file_name: &str) -> Option<String> {
    let p = template_file_path(base_path, file_name);
    tokio::fs::read_to_string(&p).await.ok()
}

pub async fn resolve_existing_template_path(base_path: &str, file_name: &str) -> Option<String> {
    for path in template_candidate_paths(base_path, file_name) {
        if tokio::fs::metadata(&path).await.is_ok() {
            return Some(path);
        }
    }
    None
}

pub async fn remove_all_template_copies(base_path: &str, file_name: &str) -> usize {
    let mut removed = 0;
    for path in template_candidate_paths(base_path, file_name) {
        if tokio::fs::remove_file(&path).await.is_ok() {
            removed += 1;
        }
    }
    removed
}

pub async fn upload_to_x0(data: Vec<u8>, filename: String) -> anyhow::Result<String> {
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0")
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .build()?;
    let part = reqwest::multipart::Part::bytes(data).file_name(filename);
    let form = reqwest::multipart::Form::new().part("file", part);
    let res = client.post("https://x0.at/").multipart(form).send().await?;
    if !res.status().is_success() {
        anyhow::bail!("x0.at status {}", res.status());
    }
    let body = res.text().await?.trim().to_string();
    if body.contains("Anon Uploads are temporarily paused") {
        anyhow::bail!("x0.at paused");
    }
    if !(body.starts_with("http://") || body.starts_with("https://")) {
        anyhow::bail!("x0.at unexpected response: {}", body);
    }
    Ok(body)
}

pub async fn upload_to_catbox(data: Vec<u8>, filename: String) -> anyhow::Result<String> {
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0")
        .connect_timeout(Duration::from_secs(8))
        .timeout(Duration::from_secs(40))
        .build()?;
    let part = reqwest::multipart::Part::bytes(data).file_name(filename);
    let form = reqwest::multipart::Form::new()
        .part("reqtype", reqwest::multipart::Part::text("fileupload"))
        .part("fileToUpload", part);
    let res = client
        .post("https://catbox.moe/user/api.php")
        .multipart(form)
        .send()
        .await?;
    let status = res.status();
    let body = res.text().await?.trim().to_string();
    if !status.is_success() {
        anyhow::bail!("catbox status {}: {}", status, body);
    }
    if body.contains("Invalid uploader") || body.contains("ERROR") {
        anyhow::bail!("catbox error: {}", body);
    }
    if !(body.starts_with("http://") || body.starts_with("https://")) {
        anyhow::bail!("catbox unexpected response: {}", body);
    }
    Ok(body)
}

pub async fn upload_to_litterbox(data: Vec<u8>, filename: String) -> anyhow::Result<String> {
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0")
        .connect_timeout(Duration::from_secs(8))
        .timeout(Duration::from_secs(40))
        .build()?;
    let part = reqwest::multipart::Part::bytes(data).file_name(filename);
    let form = reqwest::multipart::Form::new()
        .part("reqtype", reqwest::multipart::Part::text("fileupload"))
        .part("time", reqwest::multipart::Part::text("72h"))
        .part("fileToUpload", part);
    let res = client
        .post("https://litterbox.catbox.moe/resources/internals/api.php")
        .multipart(form)
        .send()
        .await?;
    let status = res.status();
    let body = res.text().await?.trim().to_string();
    if !status.is_success() {
        anyhow::bail!("litterbox status {}: {}", status, body);
    }
    if !(body.starts_with("http://") || body.starts_with("https://")) {
        anyhow::bail!("litterbox unexpected response: {}", body);
    }
    Ok(body)
}

pub async fn upload_to_uguu(data: Vec<u8>, filename: String) -> anyhow::Result<String> {
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 RayzenBot/1.0")
        .connect_timeout(Duration::from_secs(8))
        .timeout(Duration::from_secs(45))
        .build()?;
    let part = reqwest::multipart::Part::bytes(data).file_name(filename);
    let form = reqwest::multipart::Form::new().part("files[]", part);
    let res = client
        .post("https://uguu.se/upload.php")
        .multipart(form)
        .send()
        .await?;
    let status = res.status();
    let body = res.text().await?;
    if !status.is_success() {
        anyhow::bail!("uguu status {}", status);
    }

    let url = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("files")?
                .as_array()?
                .first()?
                .get("url")?
                .as_str()
                .map(str::to_owned)
        })
        .filter(|url| url.starts_with("https://"))
        .ok_or_else(|| anyhow::anyhow!("uguu unexpected response"))?;
    Ok(url)
}

pub fn extension_from_mime_str(mime: &str) -> Option<&'static str> {
    let m = mime.split(';').next().unwrap_or(mime).trim();
    let lower = m.to_ascii_lowercase();
    match lower.as_str() {
        "video/mp4" | "application/mp4" => Some("mp4"),
        "video/webm" => Some("webm"),
        "video/quicktime" => Some("mov"),
        "image/jpeg" | "image/jpg" => Some("jpg"),
        "image/png" => Some("png"),
        "image/gif" => Some("gif"),
        "image/webp" => Some("webp"),
        "audio/ogg" | "application/ogg" => Some("ogg"),
        "audio/mpeg" | "audio/mp3" => Some("mp3"),
        "video/x-matroska" => Some("mkv"),
        _ => None,
    }
}

pub fn grammers_upload_extension(doc: &grammers_client::types::media::Document) -> String {
    let name = doc.name();
    if name.contains('.') {
        let ext = name.rsplit('.').next().unwrap_or("");
        if !ext.is_empty() && ext.len() <= 32 {
            return ext.to_string();
        }
    }
    if let Some(mime) = doc.mime_type() {
        if let Some(ext) = extension_from_mime_str(mime) {
            return ext.to_string();
        }
    }
    if doc.duration().is_some() {
        let mt: &str = doc.mime_type().unwrap_or("");
        if mt.contains("audio") {
            return "ogg".to_string();
        }
        return "mp4".to_string();
    }
    "bin".to_string()
}

pub fn grammers_upload_extension_from_media(media: &grammers_client::types::Media) -> String {
    use grammers_client::types::Media;
    match media {
        Media::Photo(_) => "jpg".to_string(),
        Media::Document(d) => grammers_upload_extension(d),
        Media::Sticker(s) => grammers_upload_extension(&s.document),
        _ => "bin".to_string(),
    }
}

pub async fn upload_media_fast(data: Vec<u8>, filename: String) -> anyhow::Result<String> {
    let data = std::sync::Arc::new(data);
    let filename = std::sync::Arc::new(filename);
    let mut set = tokio::task::JoinSet::new();

    {
        let d = data.clone();
        let f = filename.clone();
        set.spawn(async move { upload_to_x0((*d).clone(), (*f).clone()).await });
    }
    {
        let d = data.clone();
        let f = filename.clone();
        set.spawn(async move { upload_to_catbox((*d).clone(), (*f).clone()).await });
    }
    {
        let d = data.clone();
        let f = filename.clone();
        set.spawn(async move { upload_to_litterbox((*d).clone(), (*f).clone()).await });
    }
    {
        let d = data.clone();
        let f = filename.clone();
        set.spawn(async move { upload_to_uguu((*d).clone(), (*f).clone()).await });
    }

    let mut errs: Vec<String> = Vec::new();
    while let Some(res) = set.join_next().await {
        match res {
            Ok(Ok(url)) => {
                set.abort_all();
                return Ok(url);
            }
            Ok(Err(e)) => errs.push(e.to_string()),
            Err(e) => errs.push(format!("join error: {}", e)),
        }
    }

    anyhow::bail!("upload failed: {}", errs.join(" | "))
}

pub fn escape(text: &str) -> String {
    text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
}

pub fn strip_specific_url(text: &str, url: &str) -> String {
    let u = url.trim();
    if u.is_empty() {
        return text.to_string();
    }
    text.replace(u, "")
        .replace(&u.replace("&", "&amp;"), "")
        .trim()
        .to_string()
}

pub fn get_random_choice(lines: Vec<String>) -> Option<String> {
    use ::rand::seq::SliceRandom;
    let mut rng = ::rand::thread_rng();
    lines.choose(&mut rng).cloned()
}

#[allow(dead_code)]
pub fn format_uptime_duration_secs(total_secs: u64) -> String {
    let days = total_secs / 86400;
    let rem = total_secs % 86400;
    let hours = rem / 3600;
    let rem = rem % 3600;
    let minutes = rem / 60;
    let seconds = rem % 60;
    format!("{} days, {:02}:{:02}:{:02}", days, hours, minutes, seconds)
}

pub fn estimate_registration_date(user_id: u64) -> NaiveDate {
    let uid = user_id as f64;
    let ranges: &[(f64, NaiveDateTime)] = &[
        (
            1.0,
            NaiveDate::from_ymd_opt(2013, 8, 14)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            5_000_000.0,
            NaiveDate::from_ymd_opt(2014, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            50_000_000.0,
            NaiveDate::from_ymd_opt(2014, 12, 8)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            100_000_000.0,
            NaiveDate::from_ymd_opt(2015, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            200_000_000.0,
            NaiveDate::from_ymd_opt(2016, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            350_000_000.0,
            NaiveDate::from_ymd_opt(2017, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            600_000_000.0,
            NaiveDate::from_ymd_opt(2018, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            800_000_000.0,
            NaiveDate::from_ymd_opt(2019, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            1_100_000_000.0,
            NaiveDate::from_ymd_opt(2020, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            1_500_000_000.0,
            NaiveDate::from_ymd_opt(2021, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            5_000_000_000.0,
            NaiveDate::from_ymd_opt(2022, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            6_000_000_000.0,
            NaiveDate::from_ymd_opt(2023, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            7_000_000_000.0,
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            8_000_000_000.0,
            NaiveDate::from_ymd_opt(2025, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        (
            9_000_000_000.0,
            NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
    ];
    for i in 0..ranges.len().saturating_sub(1) {
        let (start_id, start_date) = ranges[i];
        let (end_id, end_date) = ranges[i + 1];
        if uid >= start_id && uid < end_id {
            let proportion = (uid - start_id) / (end_id - start_id);
            let delta = end_date.signed_duration_since(start_date);
            let secs = (delta.num_seconds() as f64 * proportion) as i64;
            return start_date
                .checked_add_signed(chrono::Duration::seconds(secs))
                .unwrap_or(start_date)
                .date();
        }
    }
    chrono::Utc::now().naive_utc().date()
}

pub fn get_sys_metrics() -> (String, String, String) {
    let mut sys = System::new_all();
    sys.refresh_all();
    let sys_u = format_uptime_compact(os_uptime_secs());
    (
        sys_u,
        format!("{:.1}%", sys.global_cpu_info().cpu_usage()),
        format!(
            "{:.2} / {:.2} GB",
            sys.used_memory() as f64 / 1024.0 / 1024.0 / 1024.0,
            sys.total_memory() as f64 / 1024.0 / 1024.0 / 1024.0
        ),
    )
}

pub fn list_directory_content(base_path: &str) -> (String, String, String) {
    list_directory_content_html(base_path)
}

pub fn list_directory_content_html(base_path: &str) -> (String, String, String) {
    let base = base_path.trim_end_matches('/');
    let fmt_files = |subdir: &str, txt_only: bool, empty_msg: &str| -> String {
        let p = format!("{}/{}", base, subdir);
        let path = std::path::Path::new(&p);
        if !path.is_dir() {
            return empty_msg.to_string();
        }
        let Ok(rd) = std::fs::read_dir(path) else {
            return empty_msg.to_string();
        };
        let mut files: Vec<String> = rd
            .filter_map(|e| e.ok())
            .filter(|x| x.path().is_file())
            .map(|x| x.file_name().to_string_lossy().to_string())
            .filter(|s| !s.is_empty() && (!txt_only || s.ends_with(".txt")))
            .collect();
        files.sort();
        if files.is_empty() {
            empty_msg.to_string()
        } else {
            files
                .iter()
                .map(|f| format!("<code>{}</code>", escape(f)))
                .collect::<Vec<_>>()
                .join("\n")
        }
    };
    (
        fmt_files("sessions", false, "нет файлов в папке Сессии"),
        fmt_files("api_keys", false, "нет файлов в папке Api Ключи"),
        fmt_files("templates", true, "нет шаблонов"),
    )
}
