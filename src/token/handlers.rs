use crate::account::utils::{
    estimate_registration_date, extension_from_mime_str, upload_media_fast,
};
use crate::token::commands::Command;
use crate::token::config::Config;
use crate::token::state::{
    AfkConfig, AutoReplyConfig, BotState, Check1Task, LastAutoReplyTarget, PersistentTask,
};
use crate::token::utils::*;
use chrono::Datelike;
// ебучий вк апи
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use teloxide::net::Download;
use teloxide::prelude::*;
use teloxide::types::{
    Chat, ChatId, ChatKind, ChatPermissions, InputFile, Message, MessageEntity, MessageEntityKind,
    MessageId, ParseMode, PublicChatKind, UserId,
};
use teloxide::utils::command::BotCommands;
use tokio::time::{sleep, Duration};

fn strip_invisible(s: &str) -> String {
    s.trim()
        .chars()
        .filter(|c| {
            !matches!(
                *c,
                '\u{200B}' | '\u{200C}' | '\u{200D}' | '\u{FEFF}' | '\u{2060}'
            )
        })
        .collect()
}

fn auto_reply_rate_key(sender_id: u64, chat_id: i64) -> String {
    format!("{}:{}", sender_id, chat_id)
}

const AUTO_REPLY_MIN_DELAY_SECS: u64 = 1;
const AUTO_REPLY_SEND_TIMEOUT_SECS: u64 = 45;
const AUTO_REPLY_TRANSIENT_RETRY_SECS: u64 = 60;
const AUTO_REPLY_MAX_SEND_ATTEMPTS: u32 = 50;
const AUTO_REPLY_RESTART_REPLAY_MAX_AGE_SECS: i64 = 15 * 60;

fn token_auto_reply_is_flood_wait(err: &str) -> bool {
    let e = err.to_lowercase();
    e.contains("flood")
        || e.contains("retry_after")
        || e.contains("retry after")
        || e.contains("too many requests")
        || e.contains("slowmode")
        || e.contains("wait")
}

fn parse_retry_after_secs(err: &str) -> Option<u64> {
    let lower = err.to_lowercase();
    for marker in ["retry_after", "retry after", "retry-after"] {
        let Some(pos) = lower.find(marker) else {
            continue;
        };
        let tail = &lower[pos + marker.len()..];
        let digits: String = tail
            .chars()
            .skip_while(|c| !c.is_ascii_digit())
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if let Ok(secs) = digits.parse::<u64>() {
            if secs > 0 {
                return Some(secs);
            }
        }
    }
    None
}

fn token_auto_reply_retry_delay(err: &str) -> Duration {
    parse_retry_after_secs(err)
        .map(|secs| Duration::from_secs(secs.saturating_add(1).min(3600)))
        .unwrap_or_else(|| Duration::from_secs(AUTO_REPLY_TRANSIENT_RETRY_SECS))
}

fn token_auto_reply_is_permanent_error(err: &str) -> bool {
    let e = err.to_lowercase();
    e.contains("channel_invalid")
        || e.contains("peer_id_invalid")
        || e.contains("chat_write_forbidden")
        || e.contains("user_banned_in_channel")
        || e.contains("bot was blocked")
        || e.contains("bot was kicked")
        || e.contains("chat not found")
        || e.contains("message to reply not found")
        || e.contains("message_id_invalid")
        || e.contains("media_invalid")
        || e.contains("not enough rights")
        || e.contains("forbidden")
}

fn token_auto_reply_is_server_error(err: &str) -> bool {
    let e = err.to_lowercase();
    e.contains("bad gateway")
        || e.contains("gateway")
        || e.contains("service unavailable")
        || e.contains("unavailable")
        || e.contains("internal server error")
        || e.contains("server error")
}

fn token_auto_reply_is_transient_error(err: &str) -> bool {
    let e = err.to_lowercase();
    e.contains("timeout")
        || e.contains("timed out")
        || e.contains("deadline")
        || token_auto_reply_is_flood_wait(err)
        || e.contains("network")
        || e.contains("connection")
        || e.contains("connect")
        || e.contains("temporarily")
        || token_auto_reply_is_server_error(err)
        || (!token_auto_reply_is_permanent_error(err) && e.contains("request error"))
}

fn token_auto_reply_missing_reply_error(err: &str) -> bool {
    let e = err.to_lowercase();
    e.contains("message to be replied not found")
        || e.contains("reply message not found")
        || e.contains("replied message not found")
        || e.contains("message_id_invalid")
}

async fn send_token_auto_reply_text_once(
    bot: &Bot,
    chat_id: ChatId,
    text: String,
    reply_to: Option<MessageId>,
) -> Result<Message, String> {
    let mut req = bot
        .send_message(chat_id, text.clone())
        .parse_mode(ParseMode::Html)
        .disable_web_page_preview(true);
    if let Some(reply_to) = reply_to {
        req = req.reply_to_message_id(reply_to);
    }
    match req.await {
        Ok(message) => Ok(message),
        Err(err)
            if reply_to.is_some() && token_auto_reply_missing_reply_error(&err.to_string()) =>
        {
            bot.send_message(chat_id, text)
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .await
                .map_err(|e| e.to_string())
        }
        Err(err) => Err(err.to_string()),
    }
}

fn token_media_input_file(media_ref: &str) -> Result<InputFile, String> {
    let media_ref = media_ref.trim();
    if media_ref.starts_with("http://") || media_ref.starts_with("https://") {
        return media_ref
            .parse::<reqwest::Url>()
            .map(InputFile::url)
            .map_err(|e| e.to_string());
    }
    Ok(InputFile::file_id(media_ref.to_string()))
}

async fn send_token_auto_reply_media_once(
    bot: &Bot,
    chat_id: ChatId,
    media_url: String,
    caption: String,
    reply_to: Option<MessageId>,
) -> Result<Message, String> {
    let media_url = media_url.trim().to_string();
    let caption = strip_specific_url(&caption, &media_url);
    let file = token_media_input_file(&media_url)?;
    let is_video = is_video_url(&media_url);
    let send = |reply_to: Option<MessageId>| {
        let file = file.clone();
        let caption = caption.clone();
        async move {
            if is_video {
                let mut req = bot
                    .send_video(chat_id, file)
                    .caption(caption)
                    .parse_mode(ParseMode::Html);
                if let Some(reply_to) = reply_to {
                    req = req.reply_to_message_id(reply_to);
                }
                req.await.map_err(|e| e.to_string())
            } else {
                let mut req = bot
                    .send_photo(chat_id, file)
                    .caption(caption)
                    .parse_mode(ParseMode::Html);
                if let Some(reply_to) = reply_to {
                    req = req.reply_to_message_id(reply_to);
                }
                req.await.map_err(|e| e.to_string())
            }
        }
    };
    match send(reply_to).await {
        Ok(message) => Ok(message),
        Err(err) if reply_to.is_some() && token_auto_reply_missing_reply_error(&err) => {
            send(None).await
        }
        Err(err) => Err(err),
    }
}

fn token_auto_reply_error_log(kind: &str, chat_id: i64, user_id: u64, err: &str) -> String {
    let headline = if token_auto_reply_is_flood_wait(err) {
        "floodwait: auto-reply was not sent"
    } else {
        "auto-reply was not sent"
    };
    format!(
        "{} | type=<code>{}</code> | chat=<code>{}</code> | user=<code>{}</code>\nReason: <code>{}</code>",
        headline,
        escape(kind),
        chat_id,
        user_id,
        escape(err)
    )
}

fn token_auto_reply_access_loss_reason(err: &str) -> Option<&'static str> {
    let e = err.to_lowercase();
    if e.contains("chat_write_forbidden") {
        Some("бот не может писать в чат: вероятно мут, запрет отправки или нет прав")
    } else if e.contains("user_banned_in_channel")
        || e.contains("bot was kicked")
        || e.contains("bot was blocked")
    {
        Some("бот забанен/удален или заблокирован")
    } else if e.contains("channel_invalid") || e.contains("chat not found") {
        Some("чат недоступен для бота")
    } else if e.contains("peer_id_invalid") {
        Some("peer/chat недоступен для бота")
    } else if e.contains("not enough rights") || e.contains("forbidden") {
        Some("недостаточно прав для отправки в чат")
    } else {
        None
    }
}

fn token_auto_reply_access_lost_log(
    kind: &str,
    chat_id: i64,
    user_id: u64,
    err: &str,
    reason: &str,
) -> String {
    format!(
        "auto-reply stopped: bot cannot send in chat | type=<code>{}</code> | chat=<code>{}</code> | user=<code>{}</code>\nStatus: <code>{}</code>\nReason: <code>{}</code>",
        escape(kind),
        chat_id,
        user_id,
        escape(reason),
        escape(err)
    )
}

fn token_message_url(chat_id: i64, message_id: i32) -> Option<String> {
    if message_id <= 0 {
        return None;
    }
    if chat_id > 0 {
        return Some(format!(
            "tg://openmessage?user_id={}&message_id={}",
            chat_id, message_id
        ));
    }
    if chat_id > -1_000_000_000_000 {
        return None;
    }
    let internal = chat_id.saturating_neg().checked_sub(1_000_000_000_000)?;
    (internal > 0).then(|| format!("https://t.me/c/{}/{}", internal, message_id))
}

fn token_tme_internal(chat_id: i64) -> Option<i64> {
    if chat_id > -1_000_000_000_000 {
        return None;
    }
    let internal = chat_id.saturating_neg().checked_sub(1_000_000_000_000)?;
    (internal > 0).then_some(internal)
}

fn token_chat_public_username(chat: &Chat) -> Option<String> {
    chat.username()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_start_matches('@').to_string())
}

fn message_url_from_parts(
    chat_username: Option<&str>,
    tme_internal: Option<i64>,
    message_id: Option<i32>,
) -> Option<String> {
    if let Some(u) = chat_username.map(str::trim).filter(|s| !s.is_empty()) {
        let Some(mid) = message_id.filter(|&m| m > 0) else {
            return Some(format!("https://t.me/{}", u.trim_start_matches('@')));
        };
        return Some(format!(
            "https://t.me/{}/{}",
            u.trim_start_matches('@'),
            mid
        ));
    }
    let internal = tme_internal.filter(|&i| i > 0)?;
    let mid = message_id.filter(|&m| m > 0).unwrap_or(1);
    Some(format!("https://t.me/c/{}/{}", internal, mid))
}

fn token_chat_id_html(state: &BotState, chat_id: i64) -> String {
    if chat_id > 0 {
        let profile = token_user_id_html(state, chat_id as u64);
        return format!("{profile}, {profile}");
    }
    let username = state
        .last_seen_chat_username
        .get(&chat_id)
        .map(|u| u.value().clone());
    let mid = state
        .last_seen_message_id_by_chat
        .get(&chat_id)
        .map(|m| m.value().0);
    let url = message_url_from_parts(username.as_deref(), token_tme_internal(chat_id), mid);
    match url {
        Some(url) => format!(r#"<a href="{}">{}</a>"#, escape(&url), chat_id),
        None => format!("<code>{}</code>", chat_id),
    }
}

fn token_user_id_html(state: &BotState, user_id: u64) -> String {
    if let Some(username) = state.last_seen_user_username.get(&user_id) {
        let username = username.value().trim().trim_start_matches('@').to_string();
        if !username.is_empty() {
            return format!(
                r#"<a href="https://t.me/{username}">{user_id}</a>"#,
                username = escape(&username),
                user_id = user_id
            );
        }
    }
    format!(r#"<a href="tg://user?id={}">{}</a>"#, user_id, user_id)
}

async fn token_user_id_html_async(bot: &Bot, state: &BotState, user_id: u64) -> String {
    if state.last_seen_user_username.contains_key(&user_id) {
        return token_user_id_html(state, user_id);
    }
    if let Ok(Ok(chat)) = tokio::time::timeout(
        Duration::from_millis(250),
        bot.get_chat(ChatId(user_id as i64)),
    )
    .await
    {
        if let Some(username) = token_chat_public_username(&chat) {
            state.last_seen_user_username.insert(user_id, username);
            return token_user_id_html(state, user_id);
        }
    }
    token_user_id_html(state, user_id)
}

async fn token_chat_id_html_async(bot: &Bot, state: &BotState, chat_id: i64) -> String {
    if chat_id > 0 {
        let profile = token_user_id_html_async(bot, state, chat_id as u64).await;
        return format!("{profile}, {profile}");
    }

    if let Ok(Ok(chat)) =
        tokio::time::timeout(Duration::from_millis(700), bot.get_chat(ChatId(chat_id))).await
    {
        if let Some(username) = token_chat_public_username(&chat) {
            state
                .last_seen_chat_username
                .insert(chat_id, username.clone());
            if let Some(url) = message_url_from_parts(Some(&username), None, None) {
                return format!(r#"<a href="{}">{}</a>"#, escape(&url), chat_id);
            }
        }
        if let ChatKind::Public(p) = &chat.kind {
            if let Some(invite) = p.invite_link.as_deref().filter(|s| !s.trim().is_empty()) {
                return format!(r#"<a href="{}">{}</a>"#, escape(invite.trim()), chat_id);
            }
        }
    }

    token_chat_id_html(state, chat_id)
}

async fn token_linked_chat_ids_for_map(
    bot: &Bot,
    state: &BotState,
    map: &dashmap::DashMap<ChatId, tokio::task::JoinHandle<()>>,
) -> String {
    let raw_ids: Vec<i64> = map.iter().map(|kv| kv.key().0).collect();
    if raw_ids.is_empty() {
        return "None".to_string();
    }
    let mut seen = HashSet::new();
    let mut ids = Vec::with_capacity(raw_ids.len());
    for id in raw_ids {
        if seen.insert(id) {
            ids.push(token_chat_id_html_async(bot, state, id).await);
        }
    }
    ids.join(", ")
}

fn token_append_text(out: &mut String, entities: &mut Vec<MessageEntity>, text: &str) {
    let mut rest = text;
    while let Some(pos) = rest.find('&') {
        out.push_str(&rest[..pos]);
        rest = &rest[pos..];
        let decoded = if let Some(tail) = rest.strip_prefix("&lt;") {
            rest = tail;
            Some('<')
        } else if let Some(tail) = rest.strip_prefix("&gt;") {
            rest = tail;
            Some('>')
        } else if let Some(tail) = rest.strip_prefix("&amp;") {
            rest = tail;
            Some('&')
        } else if let Some(tail) = rest.strip_prefix("&quot;") {
            rest = tail;
            Some('"')
        } else {
            None
        };
        if let Some(ch) = decoded {
            out.push(ch);
        } else {
            out.push('&');
            rest = &rest[1..];
        }
    }
    out.push_str(rest);
    entities.retain(|e| e.length > 0);
}

fn token_utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn token_parse_href(tag: &str) -> Option<String> {
    let marker = "href=\"";
    let start = tag.find(marker)? + marker.len();
    let end = tag[start..].find('"')? + start;
    Some(
        tag[start..end]
            .replace("&amp;", "&")
            .replace("&quot;", "\"")
            .trim()
            .to_string(),
    )
    .filter(|s| !s.is_empty())
}

fn token_html_to_entities(html: &str) -> (String, Vec<MessageEntity>) {
    enum OpenEntity {
        Link { offset: usize, url: String },
        Code { offset: usize },
    }

    let mut text = String::new();
    let mut entities = Vec::new();
    let mut stack: Vec<OpenEntity> = Vec::new();
    let mut rest = html;

    while let Some(tag_start) = rest.find('<') {
        token_append_text(&mut text, &mut entities, &rest[..tag_start]);
        rest = &rest[tag_start..];
        let Some(tag_end) = rest.find('>') else {
            token_append_text(&mut text, &mut entities, rest);
            return (text, entities);
        };
        let tag = &rest[..=tag_end];
        let tag_l = tag.to_ascii_lowercase();
        let offset = token_utf16_len(&text);

        if tag_l.starts_with("<a ") {
            if let Some(url) = token_parse_href(tag) {
                stack.push(OpenEntity::Link { offset, url });
            }
        } else if tag_l == "</a>" {
            if let Some(pos) = stack
                .iter()
                .rposition(|e| matches!(e, OpenEntity::Link { .. }))
            {
                if let OpenEntity::Link { offset, url } = stack.remove(pos) {
                    let length = token_utf16_len(&text).saturating_sub(offset);
                    if length > 0 {
                        if let Ok(url) = reqwest::Url::parse(&url) {
                            entities.push(MessageEntity::new(
                                MessageEntityKind::TextLink { url },
                                offset,
                                length,
                            ));
                        }
                    }
                }
            }
        } else if tag_l == "<code>" {
            stack.push(OpenEntity::Code { offset });
        } else if tag_l == "</code>" {
            if let Some(pos) = stack
                .iter()
                .rposition(|e| matches!(e, OpenEntity::Code { .. }))
            {
                if let OpenEntity::Code { offset } = stack.remove(pos) {
                    let length = token_utf16_len(&text).saturating_sub(offset);
                    if length > 0 {
                        entities.push(MessageEntity::code(offset, length));
                    }
                }
            }
        }

        rest = &rest[tag_end + 1..];
    }

    token_append_text(&mut text, &mut entities, rest);
    (text, entities)
}

async fn send_token_check_chunk(bot: &Bot, chat_id: ChatId, html: String) {
    let (text, entities) = token_html_to_entities(&html);
    let mut req = bot
        .send_message(chat_id, text)
        .disable_web_page_preview(true);
    if !entities.is_empty() {
        req = req.entities(entities);
    }
    let _ = req.await;
}

async fn send_token_check_html(bot: &Bot, chat_id: ChatId, body: String, footer_html: String) {
    let full = format!("{}{}", body, footer_html);
    if telegram_caption_utf16_len(&full) <= 3900 {
        send_token_check_chunk(bot, chat_id, full).await;
        return;
    }

    let mut chunk = String::new();
    for line in body.lines() {
        let next = if chunk.is_empty() {
            line.to_string()
        } else {
            format!("{}\n{}", chunk, line)
        };
        if telegram_caption_utf16_len(&next) > 3500 && !chunk.is_empty() {
            send_token_check_chunk(bot, chat_id, chunk).await;
            chunk = line.to_string();
        } else {
            chunk = next;
        }
    }
    if !chunk.is_empty() {
        send_token_check_chunk(bot, chat_id, format!("{}{}", chunk, footer_html)).await;
    }
}

fn token_auto_reply_retry_log(kind: &str, chat_id: i64, user_id: u64, err: &str) -> String {
    format!(
        "auto-reply temporary send problem | type=<code>{}</code> | chat=<code>{}</code> | user=<code>{}</code>\nReason: <code>{}</code>\nStatus: <code>автоповтор активен, подтверждение не требуется</code>",
        escape(kind),
        chat_id,
        user_id,
        escape(err)
    )
}

fn token_auto_reply_sent_after_retry_log(
    kind: &str,
    chat_id: i64,
    user_id: u64,
    message_id: i32,
    delay_secs: u64,
    actual_secs: u64,
) -> String {
    let over = actual_secs.saturating_sub(delay_secs);
    let link = token_message_url(chat_id, message_id)
        .map(|u| format!("<a href=\"{}\">сообщение</a>", escape(&u)))
        .unwrap_or_else(|| format!("message_id <code>{}</code>", message_id));
    format!(
        "auto-reply sent after retry | type=<code>{}</code> | chat=<code>{}</code> | user=<code>{}</code>\nDelay: <code>{}s</code> | sent after: <code>{}s</code> | late by: <code>{}s</code>\nLink: {}",
        escape(kind),
        chat_id,
        user_id,
        delay_secs,
        actual_secs,
        over,
        link
    )
}

enum TokenAutoReplyDeliveryPayload {
    Text(String),
    Media {
        caption: String,
        fallback_text: String,
    },
}

fn normalize_token_auto_reply_body(raw: &str) -> String {
    let mut plain = String::with_capacity(raw.len());
    let mut in_tag = false;
    for ch in raw.chars() {
        match ch {
            '<' => in_tag = true,
            '>' if in_tag => in_tag = false,
            _ if !in_tag => plain.push(ch),
            _ => {}
        }
    }
    plain
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn token_message_matches_payload(msg: &Message, payload: &TokenAutoReplyDeliveryPayload) -> bool {
    let actual =
        normalize_token_auto_reply_body(msg.text().or_else(|| msg.caption()).unwrap_or(""));
    match payload {
        TokenAutoReplyDeliveryPayload::Text(expected) => {
            actual == normalize_token_auto_reply_body(expected)
        }
        TokenAutoReplyDeliveryPayload::Media {
            caption,
            fallback_text,
        } => {
            let caption = normalize_token_auto_reply_body(caption);
            let fallback_text = normalize_token_auto_reply_body(fallback_text);
            let text_ok = actual == caption || actual == fallback_text;
            let media_ok = msg.photo().is_some() || msg.video().is_some();
            text_ok && (media_ok || actual == fallback_text)
        }
    }
}

async fn notify_token_auto_reply_error(
    bot: &Bot,
    owner_id: u64,
    kind: &str,
    chat_id: i64,
    user_id: u64,
    err: &str,
) {
    log::warn!(
        "token auto-reply {} failed user={} chat={} err={}",
        kind,
        user_id,
        chat_id,
        err
    );
    let body = if let Some(reason) = token_auto_reply_access_loss_reason(err) {
        token_auto_reply_access_lost_log(kind, chat_id, user_id, err, reason)
    } else {
        token_auto_reply_error_log(kind, chat_id, user_id, err)
    };
    let _ = bot
        .send_message(ChatId(owner_id as i64), body)
        .parse_mode(ParseMode::Html)
        .await;
}

async fn notify_token_auto_reply_status(bot: &Bot, owner_id: u64, text: String) {
    let _ = bot
        .send_message(ChatId(owner_id as i64), text)
        .parse_mode(ParseMode::Html)
        .await;
}

fn spawn_token_text_auto_reply_worker(
    bot: Bot,
    state: Arc<BotState>,
    config: Arc<Config>,
    sender_id: u64,
    chat_id: ChatId,
    cfg: AutoReplyConfig,
    delay_secs: u64,
) {
    let rate_key = auto_reply_rate_key(sender_id, chat_id.0);
    if state.text_auto_reply_running.contains_key(&rate_key) {
        return;
    }
    state.text_auto_reply_running.insert(rate_key.clone(), ());
    tokio::spawn(async move {
        loop {
            let wait_secs = state
                .last_auto_reply_seen_at
                .get(&rate_key)
                .map(|t| {
                    delay_secs.saturating_sub(Instant::now().duration_since(*t.value()).as_secs())
                })
                .unwrap_or(delay_secs);
            if wait_secs > 0 {
                sleep(Duration::from_secs(wait_secs)).await;
            }

            let reply_mid = state
                .last_auto_reply_message_id
                .get(&rate_key)
                .map(|v| *v.value());
            let Some(reply_mid) = reply_mid else {
                state.text_auto_reply_running.remove(&rate_key);
                return;
            };
            let path = pick_template_path(&config.base_path, &cfg.file_name).await;
            let Ok(content) = tokio::fs::read_to_string(&path).await else {
                notify_token_auto_reply_error(
                    &bot,
                    config.owner_id,
                    "text-template",
                    chat_id.0,
                    sender_id,
                    "template file is missing or unreadable",
                )
                .await;
                state.text_auto_reply_running.remove(&rate_key);
                return;
            };
            let lines: Vec<String> = content
                .lines()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            let Some(l) = get_random_choice(lines) else {
                notify_token_auto_reply_error(
                    &bot,
                    config.owner_id,
                    "text-template",
                    chat_id.0,
                    sender_id,
                    "template file is empty",
                )
                .await;
                state.text_auto_reply_running.remove(&rate_key);
                return;
            };
            let tv = if cfg.shapka.is_empty() {
                escape(&l)
            } else {
                format!("{} {}", escape(&cfg.shapka), escape(&l))
            };
            let mut had_retry_error = false;
            let mut send_attempts = 0u32;
            let sent_message: Message;
            loop {
                let latest_reply = state
                    .last_auto_reply_message_id
                    .get(&rate_key)
                    .map(|v| *v.value())
                    .unwrap_or(reply_mid);
                let send_result = match tokio::time::timeout(
                    Duration::from_secs(AUTO_REPLY_SEND_TIMEOUT_SECS),
                    send_token_auto_reply_text_once(&bot, chat_id, tv.clone(), Some(latest_reply)),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(format!("timeout after {}s", AUTO_REPLY_SEND_TIMEOUT_SECS)),
                };
                if let Ok(message) = send_result {
                    sent_message = message;
                    break;
                }
                let err = send_result.err().unwrap_or_default();
                if !token_auto_reply_is_transient_error(&err) {
                    notify_token_auto_reply_error(
                        &bot,
                        config.owner_id,
                        "text",
                        chat_id.0,
                        sender_id,
                        &err,
                    )
                    .await;
                    state.last_auto_reply_targets.remove(&rate_key);
                    state.save_persistence();
                    state.text_auto_reply_running.remove(&rate_key);
                    return;
                }
                send_attempts = send_attempts.saturating_add(1);
                if send_attempts >= AUTO_REPLY_MAX_SEND_ATTEMPTS {
                    notify_token_auto_reply_error(
                        &bot,
                        config.owner_id,
                        "text",
                        chat_id.0,
                        sender_id,
                        &format!(
                            "stopped after {} attempts; last error: {}",
                            AUTO_REPLY_MAX_SEND_ATTEMPTS, err
                        ),
                    )
                    .await;
                    state.last_auto_reply_targets.remove(&rate_key);
                    state.save_persistence();
                    state.text_auto_reply_running.remove(&rate_key);
                    return;
                }
                if !had_retry_error {
                    had_retry_error = true;
                    notify_token_auto_reply_status(
                        &bot,
                        config.owner_id,
                        token_auto_reply_retry_log("text", chat_id.0, sender_id, &err),
                    )
                    .await;
                }
                sleep(token_auto_reply_retry_delay(&err)).await;
            }
            state.last_reply_at.insert(rate_key.clone(), Instant::now());
            if had_retry_error {
                let actual_secs = state
                    .last_auto_reply_seen_at
                    .get(&rate_key)
                    .map(|t| Instant::now().duration_since(*t.value()).as_secs())
                    .unwrap_or(delay_secs);
                if token_message_matches_payload(
                    &sent_message,
                    &TokenAutoReplyDeliveryPayload::Text(tv.clone()),
                ) {
                    notify_token_auto_reply_status(
                        &bot,
                        config.owner_id,
                        token_auto_reply_sent_after_retry_log(
                            "text",
                            chat_id.0,
                            sender_id,
                            sent_message.id.0,
                            delay_secs,
                            actual_secs,
                        ),
                    )
                    .await;
                } else {
                    notify_token_auto_reply_error(
                        &bot,
                        config.owner_id,
                        "text",
                        chat_id.0,
                        sender_id,
                        "delivery verification mismatch after retry",
                    )
                    .await;
                }
            }
            let latest_reply = state
                .last_auto_reply_message_id
                .get(&rate_key)
                .map(|v| *v.value())
                .unwrap_or(reply_mid);
            if latest_reply != reply_mid {
                continue;
            }
            state.last_auto_reply_targets.remove(&rate_key);
            state.save_persistence();
            state.text_auto_reply_running.remove(&rate_key);
            return;
        }
    });
}

fn spawn_token_media_auto_reply_worker(
    bot: Bot,
    state: Arc<BotState>,