// крашнется - и хер с ним
use std::path::Path;
use std::sync::OnceLock;

use chrono::{DateTime, FixedOffset, Utc};
use reqwest;
use sysinfo::System;
use teloxide::errors::AsResponseParameters;
use teloxide::prelude::*;
use teloxide::types::{ChatId, InputFile, Message, MessageId, ParseMode, ResponseParameters};
use tokio::io::AsyncWriteExt;
use tokio::time::{sleep, timeout, Duration};

fn log_retry_after(kind: &str, chat_id: ChatId, wait: Duration) {
    log::warn!(
        "telegram floodwait/retry_after kind={} chat={} wait={}s; сообщение пока не отправлено",
        kind,
        chat_id.0,
        wait.as_secs()
    );
}

fn log_send_retry(kind: &str, chat_id: ChatId, err: &str, wait: Duration) {
    log::warn!(
        "telegram send retry kind={} chat={} wait={}s err={}",
        kind,
        chat_id.0,
        wait.as_secs(),
        err
    );
}

fn is_missing_reply_error(err: &str) -> bool {
    let err = err.to_lowercase();
    err.contains("message to be replied not found")
        || err.contains("reply message not found")
        || err.contains("replied message not found")
}

fn is_permanent_send_error(err: &str) -> bool {
    let err = err.to_lowercase();
    err.contains("chat not found")
        || err.contains("chat_write_forbidden")
        || err.contains("bot was blocked")
        || err.contains("bot was kicked")
        || err.contains("user_banned_in_channel")
        || err.contains("peer_id_invalid")
        || err.contains("forbidden")
        || err.contains("not enough rights")
}

const SEND_RETRY_DELAY: Duration = Duration::from_secs(3);

const LEGACY_TEMPLATES_DIR: &str = "Шаблоны & Автоответчик";
const MAX_TEMPLATE_FILE_NAME_LEN: usize = 128;
pub const MAX_TEMPLATE_FILE_SIZE_BYTES: u64 = 5 * 1024 * 1024;
pub const DROP_COOLDOWN_SECS: u64 = 5;

pub fn format_template_size_limit() -> String {
    format!("{} МБ", MAX_TEMPLATE_FILE_SIZE_BYTES / 1024 / 1024)
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
    let file_name = Path::new(name).file_name()?.to_str()?.trim();
    if file_name.is_empty() || file_name != name || !file_name.to_lowercase().ends_with(".txt") {
        return None;
    }
    Some(file_name.to_string())
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

pub async fn pick_template_path(base_path: &str, file_name: &str) -> String {
    resolve_existing_template_path(base_path, file_name)
        .await
        .unwrap_or_else(|| {
            format!(
                "{}/templates/{}",
                base_path.trim_end_matches('/'),
                file_name
            )
        })
}

fn catbox_http_client() -> &'static reqwest::Client {
    static C: OnceLock<reqwest::Client> = OnceLock::new();
    C.get_or_init(|| {
        reqwest::Client::builder()
            .user_agent("Mozilla/5.0 RayzenBot/1.0")
            .timeout(Duration::from_secs(300))
            .connect_timeout(Duration::from_secs(8))
            .pool_max_idle_per_host(8)
            .build()
            .expect("reqwest client")
    })
}

pub const TELEGRAM_MEDIA_CAPTION_MAX: usize = 1024;

pub fn telegram_caption_utf16_len(s: &str) -> usize {
    s.encode_utf16().count()
}

fn strip_simple_html_tags(s: &str) -> String {
    let mut out = String::new();
    let mut skip = false;
    for ch in s.chars() {
        if skip {
            if ch == '>' {
                skip = false;
            }
            continue;
        }
        if ch == '<' {
            skip = true;
            continue;
        }
        out.push(ch);
    }
    out
}

fn truncate_utf16_plain_prefix(plain_escaped: &str, max_utf16: usize) -> String {
    let mut out = String::new();
    for ch in plain_escaped.chars() {
        let trial = format!("{}{}", out, ch);
        if telegram_caption_utf16_len(&trial) > max_utf16 {
            break;
        }
        out.push(ch);
    }
    out
}

pub fn telegram_caption_menu_plus_footer(
    menu_html: &str,
    footer_html: &str,
    max_utf16: usize,
) -> String {
    let combined = format!("{}{}", menu_html, footer_html);
    if telegram_caption_utf16_len(&combined) <= max_utf16 {
        return combined;
    }
    let footer_len = telegram_caption_utf16_len(footer_html);
    let budget = max_utf16.saturating_sub(footer_len);
    if budget < 32 {
        return footer_html.to_string();
    }
    let plain_menu = strip_simple_html_tags(menu_html);
    let esc_menu = escape(&plain_menu);
    let body = truncate_utf16_plain_prefix(&esc_menu, budget);
    format!("{}{}", body, footer_html)
}

enum MediaKind {
    Photo,
    Video,
}

const HTTP_DOWNLOAD_MAX_BYTES: u64 = 180 * 1024 * 1024;

async fn input_file_from_media_ref(
    media_ref: &str,
    _default_filename: &str,
) -> Result<InputFile, ()> {
    let s = media_ref.trim();
    if s.starts_with("http://") || s.starts_with("https://") {
        let u = s.parse::<reqwest::Url>().map_err(|_| ())?;
        return Ok(InputFile::url(u));
    }
    let p = Path::new(s);
    if p.is_file() {
        return Ok(InputFile::file(p.to_path_buf()));
    }
    Ok(InputFile::file_id(s.to_string()))
}

async fn download_http_to_temp_file(
    url: &reqwest::Url,
    default_filename: &str,
) -> Result<std::path::PathBuf, ()> {
    let name = url
        .path_segments()
        .and_then(|mut segs| segs.next_back())
        .filter(|n| !n.is_empty())
        .map(|n| n.split('?').next().unwrap_or(n).to_string())
        .unwrap_or_else(|| default_filename.to_string());
    let safe = name.replace('/', "_").replace('\\', "_");
    let p = std::env::temp_dir().join(format!("rayzen_media_{}_{}", rand::random::<u64>(), safe));

    let client = catbox_http_client();
    let resp = client
        .get(url.clone())
        .timeout(Duration::from_secs(300))
        .send()
        .await
        .map_err(|_| ())?;
    if !resp.status().is_success() {
        return Err(());
    }
    let mut file = tokio::fs::File::create(&p).await.map_err(|_| ())?;
    let mut total: u64 = 0;
    let mut stream = resp.bytes_stream();
    use futures::StreamExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| ())?;
        total = total.saturating_add(chunk.len() as u64);
        if total > HTTP_DOWNLOAD_MAX_BYTES {
            let _ = tokio::fs::remove_file(&p).await;
            return Err(());
        }
        file.write_all(&chunk).await.map_err(|_| ())?;
    }
    let _ = file.flush().await;
    Ok(p)
}

async fn send_resilient_media_long_caption(
    bot: &Bot,
    chat_id: ChatId,
    media_url: String,
    cap_s: String,
    reply_to: Option<MessageId>,
    kind: MediaKind,
) -> ResponseResult<Message> {
    let default_name = match kind {
        MediaKind::Photo => "photo.jpg",
        MediaKind::Video => "clip.mp4",
    };
    let input_file = match input_file_from_media_ref(&media_url, default_name).await {
        Ok(f) => f,
        Err(_) => return send_resilient(bot, chat_id, cap_s, reply_to).await,
    };
    let mut retries = 0u32;
    loop {
        let timeout_dur = match kind {
            MediaKind::Photo => Duration::from_secs(55),
            MediaKind::Video => Duration::from_secs(90),
        };
        let send_result = timeout(timeout_dur, async {
            match kind {
                MediaKind::Photo => {
                    let mut req = bot.send_photo(chat_id, input_file.clone());
                    if let Some(rid) = reply_to {
                        req = req.reply_to_message_id(rid);
                    }
                    req.await
                }
                MediaKind::Video => {
                    let mut req = bot.send_video(chat_id, input_file.clone());
                    if let Some(rid) = reply_to {
                        req = req.reply_to_message_id(rid);
                    }
                    req.await
                }
            }
        })
        .await;
        match send_result {
            Ok(Ok(media_msg)) => {
                return send_resilient(bot, chat_id, cap_s, Some(media_msg.id)).await;
            }
            Ok(Err(e)) => {
                if let Some(params) = e.response_parameters() {
                    if let ResponseParameters::RetryAfter(wait) = params {
                        log_retry_after("media_caption", chat_id, wait);
                        sleep(wait).await;
                        continue;
                    }
                }
                return send_resilient(bot, chat_id, cap_s, reply_to).await;
            }
            Err(_) => {}
        }
        retries += 1;
        if retries >= 2 {
            retries = 0;
            if let Ok(m) = send_resilient(bot, chat_id, cap_s.clone(), reply_to).await {
                return Ok(m);
            }
        }
        sleep(Duration::from_millis(150)).await;
    }
}

pub async fn send_resilient(
    bot: &Bot,
    chat_id: ChatId,
    text: String,
    reply_to: Option<MessageId>,
) -> ResponseResult<Message> {
    let mut reply_to = reply_to;
    loop {
        let mut req = bot
            .send_message(chat_id, text.clone())
            .parse_mode(ParseMode::Html)
            .disable_web_page_preview(true);
        if let Some(rid) = reply_to {
            req = req.reply_to_message_id(rid);
        }
        match req.await {
            Ok(m) => return Ok(m),
            Err(e) => {
                if let Some(params) = e.response_parameters() {
                    if let ResponseParameters::RetryAfter(wait) = params {
                        log_retry_after("text", chat_id, wait);
                        sleep(wait).await;
                        continue;
                    }
                }
                let err = e.to_string();
                if reply_to.is_some() && is_missing_reply_error(&err) {
                    log::warn!(
                        "telegram send fallback without reply kind=text chat={} err={}",
                        chat_id.0,
                        err
                    );
                    reply_to = None;
                    continue;
                }
                if is_permanent_send_error(&err) {
                    return Err(e);
                }
                log_send_retry("text", chat_id, &err, SEND_RETRY_DELAY);
                sleep(SEND_RETRY_DELAY).await;
            }
        }
    }
}

pub async fn send_resilient_photo(
    bot: &Bot,
    chat_id: ChatId,
    photo_url: String,
    caption: String,
    reply_to: Option<MessageId>,
) -> ResponseResult<Message> {
    let mut reply_to = reply_to;
    let photo_url = photo_url.trim().to_string();
    let cap_s = strip_specific_url(&caption, &photo_url);
    if telegram_caption_utf16_len(&cap_s) > TELEGRAM_MEDIA_CAPTION_MAX {
        return send_resilient_media_long_caption(
            bot,
            chat_id,
            photo_url,
            cap_s,
            reply_to,
            MediaKind::Photo,
        )
        .await;
    }
    let input_file = match input_file_from_media_ref(&photo_url, "photo.jpg").await {
        Ok(f) => f,
        Err(_) => {
            return send_resilient(bot, chat_id, cap_s, reply_to).await;
        }
    };
    let mut retries = 0;
    loop {
        let mut req = bot
            .send_photo(chat_id, input_file.clone())
            .caption(cap_s.clone())
            .parse_mode(ParseMode::Html);
        if let Some(rid) = reply_to {
            req = req.reply_to_message_id(rid);
        }
        match timeout(Duration::from_secs(55), req).await {
            Ok(Ok(m)) => return Ok(m),
            Ok(Err(e)) => {
                if let Some(params) = e.response_parameters() {
                    if let ResponseParameters::RetryAfter(wait) = params {
                        log_retry_after("photo", chat_id, wait);
                        sleep(wait).await;
                        continue;
                    }
                }
                let err = e.to_string();
                if reply_to.is_some() && is_missing_reply_error(&err) {
                    log::warn!(
                        "telegram send fallback without reply kind=photo chat={} err={}",
                        chat_id.0,
                        err
                    );
                    reply_to = None;
                    continue;
                }
                if is_permanent_send_error(&err) {
                    log::warn!(
                        "telegram media unavailable; sending text fallback kind=photo chat={} err={}",
                        chat_id.0,
                        err
                    );
                    return send_resilient(bot, chat_id, cap_s, reply_to).await;
                }
                if photo_url.starts_with("http://") || photo_url.starts_with("https://") {
                    if let Ok(u) = photo_url.parse::<reqwest::Url>() {
                        if let Ok(p) = download_http_to_temp_file(&u, "photo.jpg").await {
                            let mut req2 = bot
                                .send_photo(chat_id, InputFile::file(p.clone()))
                                .caption(cap_s.clone())
                                .parse_mode(ParseMode::Html);
                            if let Some(rid) = reply_to {
                                req2 = req2.reply_to_message_id(rid);
                            }
                            let res = timeout(Duration::from_secs(55), req2).await;
                            let _ = tokio::fs::remove_file(&p).await;
                            if let Ok(Ok(m2)) = res {
                                return Ok(m2);
                            }
                        }
                    }
                }
            }
            Err(_) => {}
        }
        retries += 1;
        if retries >= 2 {
            retries = 0;
            if let Ok(m) = send_resilient(bot, chat_id, cap_s.clone(), reply_to).await {
                return Ok(m);
            }
        }
        sleep(Duration::from_millis(150)).await;
    }
}

pub async fn send_resilient_video(
    bot: &Bot,
    chat_id: ChatId,
    video_url: String,
    caption: String,
    reply_to: Option<MessageId>,
) -> ResponseResult<Message> {
    let mut reply_to = reply_to;
    let video_url = video_url.trim().to_string();
    let cap_s = strip_specific_url(&caption, &video_url);
    if telegram_caption_utf16_len(&cap_s) > TELEGRAM_MEDIA_CAPTION_MAX {
        return send_resilient_media_long_caption(
            bot,
            chat_id,
            video_url,
            cap_s,
            reply_to,
            MediaKind::Video,
        )
        .await;
    }
    let input_file = match input_file_from_media_ref(&video_url, "clip.mp4").await {
        Ok(f) => f,
        Err(_) => {
            return send_resilient(bot, chat_id, cap_s, reply_to).await;
        }
    };
    let mut retries = 0;
    loop {
        let mut req = bot
            .send_video(chat_id, input_file.clone())
            .caption(cap_s.clone())
            .parse_mode(ParseMode::Html);
        if let Some(rid) = reply_to {
            req = req.reply_to_message_id(rid);
        }
        match timeout(Duration::from_secs(90), req).await {
            Ok(Ok(m)) => return Ok(m),
            Ok(Err(e)) => {
                if let Some(params) = e.response_parameters() {
                    if let ResponseParameters::RetryAfter(wait) = params {
                        log_retry_after("video", chat_id, wait);
                        sleep(wait).await;
                        continue;
                    }
                }
                let err = e.to_string();
                if reply_to.is_some() && is_missing_reply_error(&err) {
                    log::warn!(
                        "telegram send fallback without reply kind=video chat={} err={}",
                        chat_id.0,
                        err
                    );
                    reply_to = None;
                    continue;
                }
                if is_permanent_send_error(&err) {
                    log::warn!(
                        "telegram media unavailable; sending text fallback kind=video chat={} err={}",
                        chat_id.0,
                        err
                    );
                    return send_resilient(bot, chat_id, cap_s, reply_to).await;
                }
                if video_url.starts_with("http://") || video_url.starts_with("https://") {
                    if let Ok(u) = video_url.parse::<reqwest::Url>() {
                        if let Ok(p) = download_http_to_temp_file(&u, "clip.mp4").await {
                            let mut req2 = bot
                                .send_video(chat_id, InputFile::file(p.clone()))
                                .caption(cap_s.clone())
                                .parse_mode(ParseMode::Html);
                            if let Some(rid) = reply_to {
                                req2 = req2.reply_to_message_id(rid);
                            }
                            let res = timeout(Duration::from_secs(120), req2).await;
                            let _ = tokio::fs::remove_file(&p).await;
                            if let Ok(Ok(m2)) = res {
                                return Ok(m2);
                            }
                        }
                    }
                }
            }
            Err(_) => {}
        }
        retries += 1;
        if retries >= 2 {
            retries = 0;
            if let Ok(m) = send_resilient(bot, chat_id, cap_s.clone(), reply_to).await {
                return Ok(m);
            }
        }
        sleep(Duration::from_millis(150)).await;
    }
}

pub async fn send_resilient_document(
    bot: &Bot,
    chat_id: ChatId,
    path: String,
) -> ResponseResult<Message> {
    loop {
        match bot
            .send_document(chat_id, InputFile::file(std::path::PathBuf::from(&path)))
            .await
        {
            Ok(m) => return Ok(m),
            Err(e) => {
                if let Some(params) = e.response_parameters() {
                    if let ResponseParameters::RetryAfter(wait) = params {
                        log_retry_after("document", chat_id, wait);
                        sleep(wait).await;
                        continue;
                    }
                }
                let err = e.to_string();
                if is_permanent_send_error(&err) {
                    return Err(e);
                }
                log_send_retry("document", chat_id, &err, SEND_RETRY_DELAY);
                sleep(SEND_RETRY_DELAY).await;
            }
        }
    }
}

pub fn escape(text: &str) -> String {
    text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
}

pub fn format_unix_local_datetime(ts: i64) -> String {
    if ts <= 0 {
        return "—".to_string();
    }
    let msk = FixedOffset::east_opt(3 * 3600).unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|utc| {
            utc.with_timezone(&msk)
                .format("%d.%m.%Y %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "—".to_string())
}
pub fn is_video_url(url: &str) -> bool {
    let u = url.split(['?', '#']).next().unwrap_or(url).to_lowercase();
    u.ends_with(".mp4")
        || u.ends_with(".mov")
        || u.ends_with(".avi")
        || u.ends_with(".mkv")
        || u.ends_with(".gif")
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

pub fn clean_human_text(text: String) -> String {
    let mut cleaned = String::new();
    let parts: Vec<&str> = text.split("```").collect();
    for (i, part) in parts.iter().enumerate() {
        if i % 2 == 1 {
            cleaned.push_str("```");
            cleaned.push_str(part);
            cleaned.push_str("```");
        } else {
            cleaned.push_str(
                &part
                    .replace(",", "")
                    .replace(".", "")
                    .replace("!", "")
                    .replace("?", "")
                    .replace(":", "")
                    .replace(";", ""),
            );
        }
    }
    cleaned
}

pub fn get_random_choice(lines: Vec<String>) -> Option<String> {
    use ::rand::seq::SliceRandom;
    let mut rng = ::rand::thread_rng();
    lines.choose(&mut rng).cloned()
}

pub fn get_sys_metrics() -> (String, String, String) {
    let mut sys = System::new_all();
    sys.refresh_all();
    let host_u = crate::host_uptime::format_uptime_compact(crate::host_uptime::os_uptime_secs());
    (
        host_u,
        format!("{:.1}%", sys.global_cpu_info().cpu_usage()),
        format!(
            "{:.2} / {:.2} GB",
            sys.used_memory() as f64 / 1024.0 / 1024.0 / 1024.0,
            sys.total_memory() as f64 / 1024.0 / 1024.0 / 1024.0
        ),
    )
}

fn read_dir_list_first_existing(base: &str, subdirs: &[&str]) -> String {
    let base = base.trim_end_matches('/');
    for sub in subdirs {
        let p = format!("{}/{}", base, sub);
        if !std::path::Path::new(&p).exists() {
            continue;
        }
        let Ok(rd) = std::fs::read_dir(&p) else {
            return "ошибка чтения".to_string();
        };
        let mut files: Vec<String> = rd
            .filter_map(|e| e.ok().map(|x| x.file_name().to_string_lossy().to_string()))
            .filter(|s| !s.is_empty())
            .collect();
        files.sort();
        return if files.is_empty() {
            "папка пуста".to_string()
        } else {
            files.join("\n")
        };
    }
    "папка не найдена".to_string()
}

pub fn list_directory_content(base_path: &str) -> (String, String, String) {
    let base = base_path.trim_end_matches('/');
    let s = read_dir_list_first_existing(base, &["Сессии", "sessions"]);
    let k = read_dir_list_first_existing(base, &["Api Ключи", "api_keys"]);
    let t = read_dir_list_first_existing(base, &["templates"]);
    (s, k, t)
}
