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
    config: Arc<Config>,
    sender_id: u64,
    chat_id: ChatId,
    cfg: AutoReplyConfig,
    delay_secs: u64,
) {
    let rate_key = auto_reply_rate_key(sender_id, chat_id.0);
    if state.media_auto_reply_running.contains_key(&rate_key) {
        return;
    }
    state.media_auto_reply_running.insert(rate_key.clone(), ());
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
                state.media_auto_reply_running.remove(&rate_key);
                return;
            };
            let path = pick_template_path(&config.base_path, &cfg.file_name).await;
            let Ok(content) = tokio::fs::read_to_string(&path).await else {
                notify_token_auto_reply_error(
                    &bot,
                    config.owner_id,
                    "media-template",
                    chat_id.0,
                    sender_id,
                    "template file is missing or unreadable",
                )
                .await;
                state.media_auto_reply_running.remove(&rate_key);
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
                    "media-template",
                    chat_id.0,
                    sender_id,
                    "template file is empty",
                )
                .await;
                state.media_auto_reply_running.remove(&rate_key);
                return;
            };
            let Some(ref url) = cfg.media_url else {
                notify_token_auto_reply_error(
                    &bot,
                    config.owner_id,
                    "media-config",
                    chat_id.0,
                    sender_id,
                    "media_url is empty",
                )
                .await;
                state.media_auto_reply_running.remove(&rate_key);
                return;
            };
            let text_val_pre = if cfg.shapka.is_empty() {
                escape(&l)
            } else {
                format!("{} {}", escape(&cfg.shapka), escape(&l))
            };
            let send_once = |reply_to: MessageId, caption: String| {
                let b = bot.clone();
                let url = url.clone();
                async move {
                    send_token_auto_reply_media_once(&b, chat_id, url, caption, Some(reply_to))
                        .await
                }
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
                    send_once(latest_reply, text_val_pre.clone()),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(format!("timeout after {}s", AUTO_REPLY_SEND_TIMEOUT_SECS)),
                };
                match send_result {
                    Ok(m) => {
                        sent_message = m;
                        break;
                    }
                    Err(err) => {
                        send_attempts = send_attempts.saturating_add(1);
                        if send_attempts >= AUTO_REPLY_MAX_SEND_ATTEMPTS {
                            notify_token_auto_reply_error(
                                &bot,
                                config.owner_id,
                                "media",
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
                            state.media_auto_reply_running.remove(&rate_key);
                            return;
                        }
                        if !token_auto_reply_is_transient_error(&err) {
                            let latest_reply = state
                                .last_auto_reply_message_id
                                .get(&rate_key)
                                .map(|v| *v.value())
                                .unwrap_or(reply_mid);
                            let fallback_result = match tokio::time::timeout(
                                Duration::from_secs(AUTO_REPLY_SEND_TIMEOUT_SECS),
                                send_token_auto_reply_text_once(
                                    &bot,
                                    chat_id,
                                    text_val_pre.clone(),
                                    Some(latest_reply),
                                ),
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(_) => {
                                    Err(format!("timeout after {}s", AUTO_REPLY_SEND_TIMEOUT_SECS))
                                }
                            };
                            match fallback_result {
                                Ok(m) => {
                                    sent_message = m;
                                    break;
                                }
                                Err(fallback_err) => {
                                    notify_token_auto_reply_error(
                                        &bot,
                                        config.owner_id,
                                        "media",
                                        chat_id.0,
                                        sender_id,
                                        &format!("{}; text fallback failed: {}", err, fallback_err),
                                    )
                                    .await;
                                    state.last_auto_reply_targets.remove(&rate_key);
                                    state.save_persistence();
                                    state.media_auto_reply_running.remove(&rate_key);
                                    return;
                                }
                            }
                        }
                        if !had_retry_error {
                            had_retry_error = true;
                            notify_token_auto_reply_status(
                                &bot,
                                config.owner_id,
                                token_auto_reply_retry_log("media", chat_id.0, sender_id, &err),
                            )
                            .await;
                        }
                        sleep(token_auto_reply_retry_delay(&err)).await;
                    }
                }
            }
            state
                .last_reply_at_media
                .insert(rate_key.clone(), Instant::now());
            let latest_reply = state
                .last_auto_reply_message_id
                .get(&rate_key)
                .map(|v| *v.value())
                .unwrap_or(reply_mid);
            if latest_reply != reply_mid {
                continue;
            }
            if had_retry_error {
                let actual_secs = state
                    .last_auto_reply_seen_at
                    .get(&rate_key)
                    .map(|t| Instant::now().duration_since(*t.value()).as_secs())
                    .unwrap_or(delay_secs);
                let sent_caption = strip_specific_url(&text_val_pre, url);
                if token_message_matches_payload(
                    &sent_message,
                    &TokenAutoReplyDeliveryPayload::Media {
                        caption: sent_caption,
                        fallback_text: text_val_pre.clone(),
                    },
                ) {
                    notify_token_auto_reply_status(
                        &bot,
                        config.owner_id,
                        token_auto_reply_sent_after_retry_log(
                            "media",
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
                        "media",
                        chat_id.0,
                        sender_id,
                        "delivery verification mismatch after retry",
                    )
                    .await;
                }
            }
            state.last_auto_reply_targets.remove(&rate_key);
            state.save_persistence();
            state.media_auto_reply_running.remove(&rate_key);
            return;
        }
    });
}

fn clear_auto_reply_cooldowns(state: &BotState, uid: u64, media: bool) {
    let prefix = format!("{}:", uid);
    if media {
        state
            .last_reply_at_media
            .retain(|key, _| !key.starts_with(&prefix));
        state
            .last_auto_reply_seen_at
            .retain(|key, _| !key.starts_with(&prefix));
        state
            .media_auto_reply_running
            .retain(|key, _| !key.starts_with(&prefix));
    } else {
        state
            .last_reply_at
            .retain(|key, _| !key.starts_with(&prefix));
        state
            .last_auto_reply_seen_at
            .retain(|key, _| !key.starts_with(&prefix));
        state
            .text_auto_reply_running
            .retain(|key, _| !key.starts_with(&prefix));
    }
}

fn local_auto_reply_config(cfg: AutoReplyConfig, current_chat_id: i64) -> Option<AutoReplyConfig> {
    if cfg.chat_id.map_or(true, |id| id == current_chat_id) {
        Some(cfg)
    } else {
        None
    }
}

fn text_auto_reply_config(
    state: &BotState,
    sender_id: u64,
    current_chat_id: i64,
) -> Option<AutoReplyConfig> {
    state
        .global_auto_replies_text
        .get(&sender_id)
        .map(|cfg| cfg.value().clone())
        .or_else(|| {
            state
                .auto_replies_text
                .get(&sender_id)
                .and_then(|cfg| local_auto_reply_config(cfg.value().clone(), current_chat_id))
        })
}

fn media_auto_reply_config(
    state: &BotState,
    sender_id: u64,
    current_chat_id: i64,
) -> Option<AutoReplyConfig> {
    state
        .global_auto_replies_media
        .get(&sender_id)
        .map(|cfg| cfg.value().clone())
        .or_else(|| {
            state
                .auto_replies_media
                .get(&sender_id)
                .and_then(|cfg| local_auto_reply_config(cfg.value().clone(), current_chat_id))
        })
}

fn has_auto_reply_media_trigger(msg: &Message) -> bool {
    msg.photo().is_some()
        || msg.video().is_some()
        || msg.animation().is_some()
        || msg.document().is_some()
        || msg.voice().is_some()
        || msg.audio().is_some()
        || msg.video_note().is_some()
        || msg.sticker().is_some()
}

pub(crate) fn replay_saved_token_auto_replies(bot: Bot, state: Arc<BotState>, config: Arc<Config>) {
    tokio::spawn(async move {
        sleep(Duration::from_secs(2)).await;
        let targets: Vec<LastAutoReplyTarget> = state
            .last_auto_reply_targets
            .iter()
            .map(|e| e.value().clone())
            .collect();
        let mut started = 0usize;
        let now_unix = chrono::Utc::now().timestamp();
        for target in targets {
            if target.chat_id == 0 || target.user_id == 0 || target.message_id <= 0 {
                continue;
            }
            let rate_key = auto_reply_rate_key(target.user_id, target.chat_id);
            if target.seen_unix > 0
                && now_unix.saturating_sub(target.seen_unix)
                    > AUTO_REPLY_RESTART_REPLAY_MAX_AGE_SECS
            {
                state.last_auto_reply_targets.remove(&rate_key);
                state.save_persistence();
                continue;
            }
            state
                .last_auto_reply_message_id
                .insert(rate_key.clone(), MessageId(target.message_id));
            state
                .last_auto_reply_seen_at
                .insert(rate_key, Instant::now());

            if target.has_text {
                if let Some(cfg) = text_auto_reply_config(&state, target.user_id, target.chat_id) {
                    let delay_secs = cfg.delay.max(AUTO_REPLY_MIN_DELAY_SECS);
                    spawn_token_text_auto_reply_worker(
                        bot.clone(),
                        state.clone(),
                        config.clone(),
                        target.user_id,
                        ChatId(target.chat_id),
                        cfg,
                        delay_secs,
                    );
                    started = started.saturating_add(1);
                }
            }
            if target.has_media {
                if let Some(cfg) = media_auto_reply_config(&state, target.user_id, target.chat_id) {
                    let delay_secs = cfg.delay.max(AUTO_REPLY_MIN_DELAY_SECS);
                    spawn_token_media_auto_reply_worker(
                        bot.clone(),
                        state.clone(),
                        config.clone(),
                        target.user_id,
                        ChatId(target.chat_id),
                        cfg,
                        delay_secs,
                    );
                    started = started.saturating_add(1);
                }
            }
            sleep(Duration::from_millis(80)).await;
        }
        if started > 0 {
            log::info!(
                "token auto-reply restart replay: started {} saved replies",
                started
            );
        }
    });
}

fn slash_command_line_for_parse(text: &str, bot_username: &str) -> Option<String> {
    let t = strip_invisible(text);
    let s = t.trim();
    if !s.starts_with('/') {
        return None;
    }
    Some(sanitize_command_line_for_parse(s, bot_username))
}

fn sanitize_command_line_for_parse(cmd_line: &str, bot_username: &str) -> String {
    let bot_user = bot_username.trim();
    let parts: Vec<&str> = cmd_line.split_whitespace().collect();
    if parts.is_empty() {
        return cmd_line.to_string();
    }
    let first = parts[0];
    if !first.starts_with('/') {
        return cmd_line.to_string();
    }
    let after_slash = &first[1..];
    if let Some((cmd, mentioned)) = after_slash.split_once('@') {
        let strip = bot_user.is_empty() || !mentioned.eq_ignore_ascii_case(bot_user);
        if strip {
            let mut out = format!("/{}", cmd);
            for p in parts.iter().skip(1) {
                out.push(' ');
                out.push_str(p);
            }
            return out;
        }
    }
    cmd_line.to_string()
}

fn chat_supports_restrict_member(chat: &Chat) -> bool {
    let ChatKind::Public(p) = &chat.kind else {
        return false;
    };
    matches!(
        &p.kind,
        PublicChatKind::Group(_) | PublicChatKind::Supergroup(_)
    )
}

fn parse_command_loose(
    cmd_line: &str,
    bot_username: &str,
) -> Result<Command, teloxide::utils::command::ParseError> {
    let sanitized = sanitize_command_line_for_parse(cmd_line, bot_username);
    Command::parse(&sanitized, bot_username).or_else(|_| {
        let head = sanitized
            .split_whitespace()
            .next()
            .unwrap_or(sanitized.as_str());
        Command::parse(head, bot_username)
    })
}

pub async fn message_handler(
    bot: Bot,
    msg: Message,
    state: Arc<BotState>,
    config: Arc<Config>,
) -> ResponseResult<()> {
    let sender_id = msg.from().map(|u| u.id.0).unwrap_or(0);
    let owner_id = config.owner_id;
    let chat_id = msg.chat.id;
    let chat_id_raw = chat_id.0;
    state
        .last_seen_message_id_by_chat
        .insert(chat_id_raw, msg.id);
    if let Some(username) = token_chat_public_username(&msg.chat) {
        state.last_seen_chat_username.insert(chat_id_raw, username);
    }
    if let Some(user) = msg.from() {
        if let Some(username) = user
            .username
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
        {
            state
                .last_seen_user_username
                .insert(user.id.0, username.trim_start_matches('@').to_string());
        }
    }

    let privileged = msg
        .from()
        .map(|u| u.id.0 == config.owner_id || state.admins.contains_key(&u.id.0))
        .unwrap_or(false);

    let ck = format!("{}_{}", chat_id_raw, sender_id);
    let check1_clear_notify = if let Some(mut t) = state.check1_tasks.get_mut(&ck) {
        let had_silence_alert = t.silence_notified;
        let msg_ts = msg.date.timestamp();
        if msg_ts >= t.last_seen_unix {
            t.last_seen_unix = msg_ts;
            t.last_message_id = Some(msg.id.0);
            t.chat_username = token_chat_public_username(&msg.chat);
            t.tme_internal = token_tme_internal(chat_id_raw);
            t.silence_notified = false;
            had_silence_alert
        } else {
            false
        }
    } else {
        false
    };
    if check1_clear_notify {
        state.save_persistence();
    }

    if state.muted_users.contains_key(&sender_id) && sender_id != owner_id {
        let _ = bot.delete_message(chat_id, msg.id).await;
        return Ok(());
    }

    let cmd_source = msg.text().or_else(|| msg.caption());
    if let Some(text) = cmd_source {
        if let Some(cmd_line) = slash_command_line_for_parse(text, &state.bot_username) {
            if privileged {
                match parse_command_loose(&cmd_line, &state.bot_username) {
                    Ok(cmd) => {
                        if let Err(e) = handle_command(
                            bot.clone(),
                            msg.clone(),
                            cmd,
                            state.clone(),
                            config.clone(),
                            sender_id,
                        )
                        .await
                        {
                            log::warn!("handle_command: {}", e);
                        }
                    }
                    Err(e) => log::warn!("Command::parse {:?} for {:?}", e, cmd_line),
                }
            }
            return Ok(());
        }
    }

    if privileged && sender_id != owner_id {
        if let Some(r) = state.afk_state.get(&owner_id) {
            let v = r.value();
            let elapsed = v.start_time.elapsed();
            let t_str = format!(
                "{} days, {:02}:{:02}:{:02}",
                elapsed.as_secs() / 86400,
                (elapsed.as_secs() / 3600) % 24,
                (elapsed.as_secs() / 60) % 60,
                elapsed.as_secs() % 60
            );
            let m_text = format!(
                "<b>⛧ Я в AFK режиме ⛧\n⛧ Время нахождения в AFK: <code>{}</code> ⛧\n⛧ Причина AFK: <code>{}</code> ⛧</b>",
                t_str, v.reason
            );
            if let Some(ref url) = v.media_url {
                let _ = send_resilient_photo(&bot, chat_id, url.clone(), m_text, None).await;
            } else {
                let _ = send_resilient(&bot, chat_id, m_text, None).await;
            }
        }
    }

    let auto_reply_text = msg.text().or_else(|| msg.caption());
    let has_media_trigger = has_auto_reply_media_trigger(&msg);
    let is_auto_reply_command = auto_reply_text
        .map(|text| text.trim_start().starts_with('/'))
        .unwrap_or(false);
    if !is_auto_reply_command && (auto_reply_text.is_some() || has_media_trigger) {
        let current_chat_id = msg.chat.id.0;
        let last_auto_reply_key = auto_reply_rate_key(sender_id, current_chat_id);
        state
            .last_auto_reply_message_id
            .insert(last_auto_reply_key.clone(), msg.id);
        state
            .last_auto_reply_seen_at
            .insert(last_auto_reply_key.clone(), Instant::now());
        let text_auto_reply = text_auto_reply_config(&state, sender_id, current_chat_id);
        let media_auto_reply = media_auto_reply_config(&state, sender_id, current_chat_id);
        if text_auto_reply.is_some() || media_auto_reply.is_some() {
            state.last_auto_reply_targets.insert(
                last_auto_reply_key,
                LastAutoReplyTarget {
                    chat_id: current_chat_id,
                    user_id: sender_id,
                    message_id: msg.id.0,
                    has_text: text_auto_reply.is_some(),
                    has_media: media_auto_reply.is_some(),
                    seen_unix: chrono::Utc::now().timestamp(),
                },
            );
            state.save_persistence();
        }
        if let Some(c) = text_auto_reply {
            let delay_secs = c.delay.max(AUTO_REPLY_MIN_DELAY_SECS);
            spawn_token_text_auto_reply_worker(
                bot.clone(),
                state.clone(),
                config.clone(),
                sender_id,
                msg.chat.id,
                c,
                delay_secs,
            );
        }
        if let Some(c) = media_auto_reply {
            let delay_secs = c.delay.max(AUTO_REPLY_MIN_DELAY_SECS);
            spawn_token_media_auto_reply_worker(
                bot.clone(),
                state.clone(),
                config.clone(),
                sender_id,
                msg.chat.id,
                c,
                delay_secs,
            );
        }
    }

    Ok(())
}

const DEFAULT_MENU_MEDIA: &str = "";

fn effective_menu_media_token(state: &BotState) -> String {
    let m = state.get_setting("media_link").trim().to_string();
    if m.is_empty() {
        DEFAULT_MENU_MEDIA.to_string()
    } else {
        m
    }
}

fn capitalize_first_char(s: &str) -> String {
    let mut it = s.chars();
    match it.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().chain(it).collect(),
    }
}

fn display_name_bot(state: &BotState) -> String {
    let s = state.get_setting("name_bot");
    let t = s.trim();
    if t.is_empty() {
        "Rayzen".to_string()
    } else {
        capitalize_first_char(t)
    }
}

async fn send_bad_args(bot: &Bot, chat_id: ChatId) {
    let _ = send_resilient(
        bot,
        chat_id,
        "<b>⛧ Недостаточно аргументов, введи команду правильно ⛧</b>".to_string(),
        None,
    )
    .await;
}

async fn handle_command(
    bot: Bot,
    msg: Message,
    cmd: Command,
    state: Arc<BotState>,
    config: Arc<Config>,
    sender_id: u64,
) -> ResponseResult<()> {
    let owner_id = config.owner_id;
    let name_bot_disp = display_name_bot(&state);
    let media_for_menu = effective_menu_media_token(&state);
    let footer_menu = format!(
        "\n\n⛧ User: {}\n⛧ ID: {}\n⛧ Разработчик бота — <a href=\"https://t.me/Arab_Trolling\">Arab</a>",
        escape(&msg.from().map(|u| u.first_name.clone()).unwrap_or_default()),
        sender_id,
    );
    let send_menu = |bot: Bot,
                     chat_id: ChatId,
                     menu_text: String,
                     media_url_raw: String,
                     footer_html: String| async move {
        let media_url = media_url_raw.trim().to_string();
        let block =
            telegram_caption_menu_plus_footer(&menu_text, &footer_html, TELEGRAM_MEDIA_CAPTION_MAX);
        if media_url.is_empty() {
            let _ = send_resilient(&bot, chat_id, block, None).await;
            return;
        }
        let cap = block;
        if is_video_url(&media_url) {
            let _ = send_resilient_video(&bot, chat_id, media_url.clone(), cap, None).await;
            return;
        }
        let _ = send_resilient_photo(&bot, chat_id, media_url.clone(), cap, None).await;
    };

    let cmd_str = match cmd {
        Command::tag(_) => "tag",
        Command::tagp(_) => "tagp",
        Command::tagv(_) => "tagv",
        Command::kal(_) => "kal",
        Command::kalp(_) => "kalp",
        Command::kalv(_) => "kalv",
        Command::ar(_) => "ar",
        Command::arp(_) => "arp",
        Command::arv(_) => "arv",
        Command::sp(_) => "sp",
        _ => "unknown",
    }
    .to_string();

    match cmd {
        Command::ping => {
            let start = Instant::now();
            let s =
                send_resilient(&bot, msg.chat.id, "<b>⛧ Пинг... ⛧</b>".to_string(), None).await?;
            let _ = bot
                .edit_message_text(
                    msg.chat.id,
                    s.id,
                    format!("пинг сервера — {} ms", start.elapsed().as_millis()),
                )
                .await;
        }
        Command::help => {
            let nb = escape(&name_bot_disp);
            let menu = format!(
                r#"⛧ {} ⛧
⛧ <code>/ar</code> + chat id + time + file.txt
⛧ <code>/arp</code> + chat id + time + file.txt + url photo
⛧ <code>/arv</code> + chat id + time + file.txt + url video
⛧ <code>/tag</code> + chat id + user id + time + file.txt
⛧ <code>/tagp</code> + chat id + user id + time + file.txt + url photo
⛧ <code>/tagv</code> + chat id + user id + time + file.txt + url video
⛧ <code>/kal</code> + chat id + time + file.txt — календарь
⛧ <code>/kalp</code> + chat id + time + file.txt + url photo
⛧ <code>/kalv</code> + chat id + time + file.txt + url video
⛧ <code>/settings</code> — настройка
⛧ <code>/direct</code> — шаблоны
⛧ <code>/id</code> — chat id
⛧ <code>/user</code> + reply — user id
⛧ <code>/check</code> — статус команд
⛧ <code>/clear</code> — стоп спамеры
⛧ <code>/cclean</code> — стоп автоответчики
⛧ <code>/x0</code> + reply — хостинг"#,
                nb
            );
            send_menu(
                bot.clone(),
                msg.chat.id,
                menu,
                media_for_menu.clone(),
                footer_menu.clone(),
            )
            .await;
        }
        Command::settings => {
            let menu = format!(
                r#"⛧ Settings ⛧
⛧ <code>/us</code> + user id + time + file.txt
⛧ <code>/usm</code> + user id + time + file.txt + url media
⛧ <code>/mute</code> + user id — мьют
⛧ <code>/check1</code> + chat id + user id + time
⛧ <code>/ping</code> — пинг
⛧ <code>/uptime</code> — аптайм
⛧ <code>/online</code> — онлайн
⛧ <code>/admin</code> [id] — админ
⛧ <code>/ai</code> + запрос — Melody AI

⛧ Афк режима ⛧
⛧ <code>/afk</code> 1 + reason
⛧ <code>/afk</code> 2 — выкл

⛧ Custom ⛧
⛧ <code>/bot</code> — имя
⛧ <code>/link</code> — медиа"#
            );
            send_menu(
                bot.clone(),
                msg.chat.id,
                menu,
                media_for_menu.clone(),
                footer_menu.clone(),
            )
            .await;
        }
        Command::uptime => {
            let (host_u, cpu, mem) = get_sys_metrics();
            let d_b = state.start_time.elapsed().as_secs();
            let bot_u = format!(
                "{}d {:02}:{:02}:{:02}",
                d_b / 86400,
                (d_b % 86400) / 3600,
                (d_b % 3600) / 60,
                d_b % 60
            );
            let menu = format!("⛧ СТАТУС СЕРВЕРА ⛧\n⛧ Процесс бота: <code>{}</code> ⛧\n⛧ Аптайм дедика: <code>{}</code> ⛧\n⛧ Процессор: <code>{}</code> ⛧\n⛧ Память: <code>{}</code> ⛧", bot_u, host_u, cpu, mem);
            let _ = send_resilient(&bot, msg.chat.id, menu, None).await;
        }
        Command::direct => {
            let (s, k, t) = list_directory_content(&state.base_path);
            let list_html = |body: &str| {
                let b = body.trim();
                if b.is_empty() || b == "папка пуста" || b == "папка не найдена"
                {
                    escape(b)
                } else {
                    b.lines()
                        .filter(|l| !l.is_empty())
                        .map(|l| format!("<code>{}</code>", escape(l)))
                        .collect::<Vec<_>>()
                        .join("\n")
                }
            };
            let menu = format!(
                r#"<b>⛧ Директория ⛧</b>
<b>⛧ Сессии:</b>
{}

<b>⛧ Ключи:</b>
{}

<b>⛧ Шаблоны:</b>
{}

<b>⛧ Управление файлами ⛧</b>
<b>⛧</b> <code>/save</code> + reply — скачать файл
<b>⛧</b> <code>/unsave</code> + file.txt — удалить файл
<b>⛧</b> <code>/drop</code> + file.txt — отправить файл шаблона в чат"#,
                list_html(&s),
                list_html(&k),
                list_html(&t)
            );
            send_menu(
                bot.clone(),
                msg.chat.id,
                menu,
                media_for_menu.clone(),
                footer_menu.clone(),
            )
            .await;
        }
        Command::check => {
            let mut list = "⛧ Статус команд ⛧\n\n".to_string();
            list.push_str(&format!(
                "⛧ Чаты где работает tag: ⛧\n{}\n\n",
                token_linked_chat_ids_for_map(&bot, &state, &state.spam_tasks_tag).await
            ));
            list.push_str(&format!(
                "⛧ Чаты где работает tagp: ⛧\n{}\n\n",
                token_linked_chat_ids_for_map(&bot, &state, &state.spam_tasks_tagp).await
            ));
            list.push_str(&format!(
                "⛧ Чаты где работает tagv: ⛧\n{}\n\n",
                token_linked_chat_ids_for_map(&bot, &state, &state.spam_tasks_tagv).await
            ));
            list.push_str(&format!(
                "⛧ Чаты где работает ar: ⛧\n{}\n\n",
                token_linked_chat_ids_for_map(&bot, &state, &state.spam_tasks_ar).await
            ));
            list.push_str(&format!(
                "⛧ Чаты где работает arp: ⛧\n{}\n\n",
                token_linked_chat_ids_for_map(&bot, &state, &state.spam_tasks_arp).await
            ));
            list.push_str(&format!(
                "⛧ Чаты где работает arv: ⛧\n{}\n\n",
                token_linked_chat_ids_for_map(&bot, &state, &state.spam_tasks_arv).await
            ));
            list.push_str(&format!(
                "⛧ Чаты где работает sp: ⛧\n{}\n\n",
                token_linked_chat_ids_for_map(&bot, &state, &state.spam_tasks_sp).await
            ));
            let us_ids = {
                let keys: Vec<u64> = state.auto_replies_text.iter().map(|kv| *kv.key()).collect();
                let mut ids = Vec::with_capacity(keys.len());
                for id in keys {
                    ids.push(token_user_id_html_async(&bot, &state, id).await);
                }
                if ids.is_empty() {
                    "None".into()
                } else {
                    ids.join(", ")
                }
            };
            list.push_str(&format!(
                "⛧ На кого работает автоответчик us: ⛧\n{}\n\n",
                us_ids
            ));
            let global_us_ids = {
                let keys: Vec<u64> = state
                    .global_auto_replies_text
                    .iter()
                    .map(|kv| *kv.key())
                    .collect();
                let mut ids = Vec::with_capacity(keys.len());
                for id in keys {
                    ids.push(token_user_id_html_async(&bot, &state, id).await);
                }
                if ids.is_empty() {
                    "None".into()
                } else {
                    ids.join(", ")
                }
            };
            list.push_str(&format!(
                "⛧ На кого работает глобальный автоответчик us: ⛧\n{}\n\n",
                global_us_ids
            ));
            let usm_ids = {
                let keys: Vec<u64> = state
                    .auto_replies_media
                    .iter()
                    .map(|kv| *kv.key())
                    .collect();
                let mut ids = Vec::with_capacity(keys.len());
                for id in keys {
                    ids.push(token_user_id_html_async(&bot, &state, id).await);
                }
                if ids.is_empty() {
                    "None".into()
                } else {
                    ids.join(", ")
                }
            };
            list.push_str(&format!(
                "⛧ На кого работает автоответчик usm: ⛧\n{}\n\n",
                usm_ids
            ));
            let global_usm_ids = {
                let keys: Vec<u64> = state
                    .global_auto_replies_media
                    .iter()
                    .map(|kv| *kv.key())
                    .collect();
                let mut ids = Vec::with_capacity(keys.len());
                for id in keys {
                    ids.push(token_user_id_html_async(&bot, &state, id).await);
                }
                if ids.is_empty() {
                    "None".into()
                } else {
                    ids.join(", ")
                }
            };
            list.push_str(&format!(
                "⛧ На кого работает глобальный автоответчик usm: ⛧\n{}\n\n",
                global_usm_ids
            ));
            let mutes = {
                let keys: Vec<u64> = state.muted_users.iter().map(|kv| *kv.key()).collect();
                let mut ids = Vec::with_capacity(keys.len());
                for id in keys {
                    ids.push(token_user_id_html_async(&bot, &state, id).await);
                }
                if ids.is_empty() {
                    "None".into()
                } else {
                    ids.join(", ")
                }
            };
            list.push_str(&format!(
                "⛧ Замученные пользователи (mute): ⛧\n{}\n\n",
                mutes
            ));
            if !state.check1_tasks.is_empty() {
                let mut items = Vec::new();
                for entry in state.check1_tasks.iter() {
                    let c = entry.value();
                    let seen = format_unix_local_datetime(c.last_seen_unix);
                    let chat_url = message_url_from_parts(
                        c.chat_username.as_deref(),
                        c.tme_internal.or_else(|| token_tme_internal(c.chat_id)),
                        c.last_message_id,
                    );
                    let chat_id = match chat_url {
                        Some(url) => format!(r#"<a href="{}">{}</a>"#, escape(&url), c.chat_id),
                        None => token_chat_id_html(&state, c.chat_id),
                    };
                    items.push(format!(
                        "Chat: {} | User: {} | порог {} сек | последн. сообщ.: {}",
                        chat_id,
                        token_user_id_html_async(&bot, &state, c.user_id).await,
                        c.delay,
                        seen
                    ));
                }
                list.push_str(&format!(
                    "⛧ Активные чекеры (check1): ⛧\n{}\n",
                    items.join("\n")
                ));
            } else {
                list.push_str("⛧ Активные чекеры (check1): ⛧\nNone\n");
            }
            send_token_check_html(&bot, msg.chat.id, list, footer_menu).await;
        }
        Command::link(url) => {
            state.settings.insert("media_link".to_string(), url.clone());
            state.save_persistence();
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                format!(
                    "⛧ Ссылка на медиа изменена на: <code>{}</code> ⛧",
                    escape(&url)
                ),
                None,
            )
            .await;
        }
        Command::bot(name) => {
            state.settings.insert("name_bot".to_string(), name.clone());
            state.save_persistence();
            let _ =
                send_resilient(&bot, msg.chat.id, format!("⛧ Имя бота: {} ⛧", name), None).await;
        }
        Command::online => {
            state
                .online_enabled
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                "<b>⛧ Online статус включен ⛧</b>".to_string(),
                None,
            )
            .await;
        }
        Command::offline => {
            state
                .online_enabled
                .store(false, std::sync::atomic::Ordering::SeqCst);
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                "<b>⛧ Online статус выключен ⛧</b>".to_string(),
                None,
            )
            .await;
        }
        Command::id => {
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                format!("⛧ Chat ID: <code>{}</code> ⛧", msg.chat.id),
                None,
            )
            .await;
        }
        Command::user => {
            if let Some(r) = msg.reply_to_message() {
                if let Some(u) = r.from() {
                    let _ = send_resilient(
                        &bot,
                        msg.chat.id,
                        format!("⛧ User ID: <code>{}</code> ⛧", u.id),
                        None,
                    )
                    .await;
                }
            }
        }
        Command::reg => {
            if let Some(r) = msg.reply_to_message() {
                if let Some(u) = r.from() {
                    let uid = u.id.0;
                    let reg = estimate_registration_date(uid);
                    let reg_str = format!("{:02}.{:02}.{}", reg.day(), reg.month(), reg.year());
                    let res_text = format!(
                        "⛧ Пользователь: <a href=\"tg://user?id={}\">{}</a> ⛧\n⛧ Примерная дата регистрации: <code>{}</code> ⛧",
                        uid,
                        escape(&u.first_name),
                        reg_str
                    );
                    let _ = send_resilient(&bot, msg.chat.id, res_text, None).await;
                } else {
                    let _ = send_resilient(
                        &bot,
                        msg.chat.id,
                        "⛧ не вышло узнать регистрацию акка ⛧".to_string(),
                        None,
                    )
                    .await;
                }
            } else {
                let _ =
                    send_resilient(&bot, msg.chat.id, "⛧ Нужен реплай ⛧".to_string(), None).await;
            }
        }
        Command::admin(id) => {
            state.admins.insert(id, true);
            state.save_persistence();
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                format!("⛧ Админ добавлен: {} ⛧", id),
                None,
            )
            .await;
        }
        Command::unadmin(id) => {
            let existed = state.admins.remove(&id).is_some();
            state.save_persistence();
            let text = if existed {
                format!("⛧ Админ удалён: {} ⛧", id)
            } else {
                format!("⛧ Не был админом: {} ⛧", id)
            };
            let _ = send_resilient(&bot, msg.chat.id, text, None).await;
        }
        Command::mute(id) => {
            state.muted_users.insert(id, true);
            state.save_persistence();
            if chat_supports_restrict_member(&msg.chat) {
                let _ = bot
                    .restrict_chat_member(msg.chat.id, UserId(id), ChatPermissions::empty())
                    .await;
            }
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                format!("⛧ Пользователь <code>{}</code> замучен ⛧", id),
                None,
            )
            .await;
        }
        Command::unmute(id) => {
            state.muted_users.remove(&id);
            state.save_persistence();
            if chat_supports_restrict_member(&msg.chat) {
                let _ = bot
                    .restrict_chat_member(msg.chat.id, UserId(id), ChatPermissions::all())
                    .await;
            }
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                format!("⛧ Пользователь <code>{}</code> размучен ⛧", id),
                None,
            )
            .await;
        }
        Command::afk(reason) => {
            state.afk_state.insert(
                sender_id,
                AfkConfig {
                    reason: if reason.is_empty() {
                        "Сплю".to_string()
                    } else {
                        reason
                    },
                    start_time: Instant::now(),
                    media_url: None,
                },
            );
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                "<b>⛧ AFK ВКЛЮЧЕН ⛧</b>".to_string(),
                None,
            )
            .await;
        }
        Command::afk2 => {
            state.afk_state.remove(&sender_id);
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                "<b>⛧ AFK ВЫКЛЮЧЕН ⛧</b>".to_string(),
                None,
            )
            .await;
        }
        Command::x0 => {
            if let Some(reply) = msg.reply_to_message() {
                let loading =
                    send_resilient(&bot, msg.chat.id, "⛧ Загружаю... ⛧".to_string(), None).await?;
                let chat_id = msg.chat.id;
                let loading_id = loading.id;

                let fid_res = if let Some(p) = reply.photo() {
                    p.last().map(|pp| (pp.file.id.clone(), "jpg".to_string()))
                } else if let Some(v) = reply.video() {
                    let ext = v
                        .mime_type
                        .as_ref()
                        .and_then(|m| extension_from_mime_str(m.essence_str()))
                        .unwrap_or("mp4");
                    Some((v.file.id.clone(), ext.to_string()))
                } else if let Some(vn) = reply.video_note() {
                    Some((vn.file.id.clone(), "mp4".to_string()))
                } else if let Some(a) = reply.animation() {
                    Some((a.file.id.clone(), "mp4".to_string()))
                } else if let Some(vo) = reply.voice() {
                    let ext = vo
                        .mime_type
                        .as_ref()
                        .and_then(|m| extension_from_mime_str(m.essence_str()))
                        .unwrap_or("ogg");
                    Some((vo.file.id.clone(), ext.to_string()))
                } else if let Some(d) = reply.document() {
                    let ext = d
                        .file_name
                        .as_deref()
                        .and_then(|n| n.rsplit('.').next())
                        .filter(|e| !e.is_empty())
                        .map(|e| e.to_string())
                        .or_else(|| {
                            d.mime_type.as_ref().and_then(|m| {
                                extension_from_mime_str(m.essence_str()).map(str::to_string)
                            })
                        })
                        .unwrap_or_else(|| "bin".to_string());
                    Some((d.file.id.clone(), ext))
                } else {
                    None
                };

                if let Some((fid, ext)) = fid_res {
                    match bot.get_file(fid).await {
                        Ok(f) => {
                            let mut buf = Vec::new();
                            if f.size > 0 && f.size != u32::MAX {
                                buf.reserve(f.size as usize);
                            }
                            match tokio::time::timeout(
                                Duration::from_secs(300),
                                bot.download_file(&f.path, &mut buf),
                            )
                            .await
                            {
                                Ok(Ok(_)) => {
                                    let fname = format!("file.{}", ext);
                                    let url = upload_media_fast(buf, fname).await;
                                    let _ = bot.delete_message(chat_id, loading_id).await;
                                    match url {
                                        Ok(u) => {
                                            let _ = send_resilient(
                                                &bot,
                                                chat_id,
                                                format!("<code>{}</code>", escape(&u)),
                                                None,
                                            )
                                            .await;
                                        }
                                        Err(e) => {
                                            let _ = send_resilient(
                                                &bot,
                                                chat_id,
                                                format!(
                                                    "<b>⛧ ошибка загрузки ⛧</b>\n<code>{}</code>",
                                                    escape(&e.to_string())
                                                ),
                                                None,
                                            )
                                            .await;
                                        }
                                    }
                                }
                                Ok(Err(_)) | Err(_) => {
                                    let _ = bot.delete_message(chat_id, loading_id).await;
                                    let _ = send_resilient(
                                        &bot,
                                        chat_id,
                                        "<b>⛧ ошибка скачивания (лимит ТГ 20 МБ или таймаут) ⛧</b>"
                                            .to_string(),
                                        None,
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(_) => {
                            let _ = bot.delete_message(chat_id, loading_id).await;
                            let _ = send_resilient(
                                &bot,
                                chat_id,
                                "<b>⛧ ошибка доступа к файлу ⛧</b>".to_string(),
                                None,
                            )
                            .await;
                        }
                    }
                } else {
                    let _ = bot.delete_message(chat_id, loading_id).await;
                    let _ = send_resilient(
                        &bot,
                        chat_id,
                        "<b>⛧ Медиа не найдено ⛧</b>".to_string(),
                        None,
                    )
                    .await;
                }
            }
        }
        Command::ai(query) => {
            if query.to_lowercase().contains("нарисуй") || query.to_lowercase().contains("фото")
            {
                let url = format!("https://image.pollinations.ai/prompt/{}?width=1024&height=1024&nologo=true&seed={}", urlencoding::encode(&query), rand::random::<u32>());
                let _ = send_resilient_photo(
                    &bot,
                    msg.chat.id,
                    url,
                    "<b>⛧ Ваша генерация готова ⛧</b>".to_string(),
                    None,
                )
                .await;
            } else {
                let loading = send_resilient(
                    &bot,
                    msg.chat.id,
                    "<b>⛧ Melody думает... 0% ⛧</b>".to_string(),
                    None,
                )
                .await?;
                let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();
                let b_p = bot.clone();
                let ch_p = msg.chat.id;
                let mid_p = loading.id;
                tokio::spawn(async move {
                    let mut p = 0;
                    loop {
                        tokio::select! { _ = &mut rx => break, _ = sleep(Duration::from_millis(1500)) => { p += (rand::random::<u8>() % 20) + 10; if p > 99 { p = 99; } let _ = b_p.edit_message_text(ch_p, mid_p, format!("<b>⛧ Melody думает... {}% ⛧</b>", p)).parse_mode(ParseMode::Html).await; if p >= 99 { break; } } }
                    }
                });
                let mut h = state.ai_history.get_mut(&sender_id).map(|v| v.value().clone()).unwrap_or_else(|| vec![serde_json::json!({"role":"system","content":"Ты AI помощник Melody. Твой владелец засекречен. Отвечай кратко и в стиле хакера."})]);
                h.push(serde_json::json!({"role": "user", "content": query.clone()}));
                let r = tokio::time::timeout(
                    Duration::from_secs(45),
                    bot.client()
                        .post("https://api.mistral.ai/v1/chat/completions")
                        .header(
                            "Authorization",
                            format!("Bearer {}", config.mistral_api_key),
                        )
                        .json(&serde_json::json!({"model":"mistral-small-latest","messages":h}))
                        .send(),
                )
                .await;
                let _ = tx.send(());
                match r {
                    Ok(Ok(res)) if res.status().is_success() => {
                        let json: serde_json::Value = res.json().await.unwrap_or_default();
                        let content = json["choices"][0]["message"]["content"]
                            .as_str()
                            .unwrap_or("ошибка");
                        h.push(serde_json::json!({"role": "assistant", "content": content}));
                        if h.len() > 15 {
                            let sys = h.remove(0);
                            let h_len = h.len();
                            h = h
                                .into_iter()
                                .skip(h_len.saturating_sub(10))
                                .collect::<Vec<_>>();
                            h.insert(0, sys);
                        }
                        state.ai_history.insert(sender_id, h);
                        state.save_persistence();
                        let _ = bot.delete_message(msg.chat.id, loading.id).await;
                        let _ = send_resilient(
                            &bot,
                            msg.chat.id,
                            escape(&clean_human_text(content.to_string())),
                            None,
                        )
                        .await;
                    }
                    Ok(Ok(res)) => {
                        let status = res.status();
                        let body = res.text().await.unwrap_or_default();
                        let body_lc = body.to_lowercase();
                        let is_rate_limited = status.as_u16() == 429
                            || body_lc.contains("rate limit")
                            || body_lc.contains("rate_limited")
                            || body_lc.contains("\"code\":\"1300\"");
                        log::warn!(
                            "mistral ai non-success status={} body={} chat_id={} user_id={}",
                            status,
                            body,
                            msg.chat.id,
                            sender_id
                        );
                        let _ = bot.delete_message(msg.chat.id, loading.id).await;
                        let user_msg = if is_rate_limited {
                            "<b>⛧ Лимит Mistral исчерпан (429). Попробуй позже или смени ключ ⛧</b>"
                                .to_string()
                        } else {
                            "<b>⛧ ошибка API. Попробуй позже ⛧</b>".to_string()
                        };
                        let _ = send_resilient(&bot, msg.chat.id, user_msg, None).await;
                    }
                    Ok(Err(err)) => {
                        log::warn!(
                            "mistral ai request failed: {} chat_id={} user_id={}",
                            err,
                            msg.chat.id,
                            sender_id
                        );
                        let _ = bot.delete_message(msg.chat.id, loading.id).await;
                        let _ = send_resilient(
                            &bot,
                            msg.chat.id,
                            "<b>⛧ ошибка сети/API. Попробуй позже ⛧</b>".to_string(),
                            None,
                        )
                        .await;
                    }
                    Err(_) => {
                        log::warn!(
                            "mistral ai timeout chat_id={} user_id={}",
                            msg.chat.id,
                            sender_id
                        );
                        let _ = bot.delete_message(msg.chat.id, loading.id).await;
                        let _ = send_resilient(
                            &bot,
                            msg.chat.id,
                            "<b>⛧ Таймаут AI запроса. Попробуй позже ⛧</b>".to_string(),
                            None,
                        )
                        .await;
                    }
                }
            }
        }
        Command::save => {
            let Some(reply) = msg.reply_to_message() else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    "⛧ Реплай на файл нужен ⛧".to_string(),
                    None,
                )
                .await;
                return Ok(());
            };
            let Some(doc) = reply.document() else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    "⛧ Реплай на файл нужен ⛧".to_string(),
                    None,
                )
                .await;
                return Ok(());
            };
            let Some(fname_raw) = doc.file_name.as_deref().map(str::trim) else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    "⛧ Нужен реплай на .txt файл ⛧".to_string(),
                    None,
                )
                .await;
                return Ok(());
            };
            let Some(safe_name) = safe_template_file_name(fname_raw) else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    "⛧ Недопустимое имя файла или не .txt шаблон ⛧".to_string(),
                    None,
                )
                .await;
                return Ok(());
            };
            if doc.file.size as u64 > MAX_TEMPLATE_FILE_SIZE_BYTES {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!(
                        "⛧ Файл слишком большой, лимит {} ⛧",
                        format_template_size_limit()
                    ),
                    None,
                )
                .await;
                return Ok(());
            }
            let file = match bot.get_file(doc.file.id.clone()).await {
                Ok(file) => file,
                Err(err) => {
                    let err_text = err.to_string();
                    if err_text.to_lowercase().contains("file is too big") {
                        let _ = send_resilient(
                            &bot,
                            msg.chat.id,
                            format!(
                                "⛧ Файл слишком большой, лимит {} ⛧",
                                format_template_size_limit()
                            ),
                            None,
                        )
                        .await;
                        return Ok(());
                    }
                    let _ = send_resilient(
                        &bot,
                        msg.chat.id,
                        format!(
                            "<b>⛧ ошибка доступа к файлу ⛧</b>\n<code>{}</code>",
                            escape(&err_text)
                        ),
                        None,
                    )
                    .await;
                    return Ok(());
                }
            };
            if file.size as u64 > MAX_TEMPLATE_FILE_SIZE_BYTES {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!(
                        "⛧ Файл слишком большой, лимит {} ⛧",
                        format_template_size_limit()
                    ),
                    None,
                )
                .await;
                return Ok(());
            }
            remove_all_template_copies(&state.base_path, &safe_name).await;
            let base = state.base_path.trim_end_matches('/');
            let dir = format!("{}/templates", base);
            if let Err(err) = tokio::fs::create_dir_all(&dir).await {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!(
                        "<b>⛧ ошибка создания папки templates ⛧</b>\n<code>{}</code>",
                        escape(&err.to_string())
                    ),
                    None,
                )
                .await;
                return Ok(());
            }
            let path = format!("{}/{}", dir, safe_name);
            let mut dest = match tokio::fs::File::create(&path).await {
                Ok(file) => file,
                Err(err) => {
                    let _ = send_resilient(
                        &bot,
                        msg.chat.id,
                        format!(
                            "<b>⛧ ошибка создания файла ⛧</b>\n<code>{}</code>",
                            escape(&err.to_string())
                        ),
                        None,
                    )
                    .await;
                    return Ok(());
                }
            };
            if bot.download_file(&file.path, &mut dest).await.is_err() {
                let _ = tokio::fs::remove_file(&path).await;
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    "⛧ ошибка скачивания файла ⛧".to_string(),
                    None,
                )
                .await;
                return Ok(());
            }
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                format!(
                    "⛧ Файл <code>{}</code> успешно сохранен ⛧",
                    escape(&safe_name)
                ),
                None,
            )
            .await;
        }
        Command::unsave(file_name) => {
            let raw = file_name.trim();
            let Some(safe) = safe_template_file_name(raw) else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    "<b>⛧ Недопустимое имя файла ⛧</b>".to_string(),
                    None,
                )
                .await;
                return Ok(());
            };
            if remove_all_template_copies(&state.base_path, &safe).await > 0 {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!("⛧ Файл <code>{}</code> удалён ⛧", escape(&safe)),
                    None,
                )
                .await;
            } else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!("⛧ Файл <code>{}</code> не найден ⛧", escape(&safe)),
                    None,
                )
                .await;
            }
        }
        Command::drop(file_name) => {
            let raw = file_name.trim();
            let Some(safe) = safe_template_file_name(raw) else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    "<b>⛧ Недопустимое имя файла ⛧</b>".to_string(),
                    None,
                )
                .await;
                return Ok(());
            };
            if let Some(path) = resolve_existing_template_path(&state.base_path, &safe).await {
                match tokio::fs::metadata(&path).await {
                    Ok(meta) if meta.len() > MAX_TEMPLATE_FILE_SIZE_BYTES => {
                        let _ = send_resilient(
                            &bot,
                            msg.chat.id,
                            format!(
                                "<b>⛧ Файл слишком большой, лимит {} ⛧</b>",
                                format_template_size_limit()
                            ),
                            None,
                        )
                        .await;
                        return Ok(());
                    }
                    Err(_) => {
                        let _ = send_resilient(
                            &bot,
                            msg.chat.id,
                            "<b>⛧ Файл не найден ⛧</b>".to_string(),
                            None,
                        )
                        .await;
                        return Ok(());
                    }
                    _ => {}
                }
                let now = Instant::now();
                if let Some(last) = state.last_drop_at.insert(sender_id, now) {
                    let elapsed = last.elapsed();
                    if elapsed < Duration::from_secs(DROP_COOLDOWN_SECS) {
                        let wait = DROP_COOLDOWN_SECS.saturating_sub(elapsed.as_secs());
                        let _ = send_resilient(
                            &bot,
                            msg.chat.id,
                            format!("<b>⛧ Подожди {} сек перед следующим drop ⛧</b>", wait),
                            None,
                        )
                        .await;
                        return Ok(());
                    }
                }
                let _ = send_resilient_document(&bot, msg.chat.id, path).await;
            } else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    "<b>⛧ Файл не найден ⛧</b>".to_string(),
                    None,
                )
                .await;
            }
        }
        Command::cclean => {
            state.auto_replies_text.clear();
            state.auto_replies_media.clear();
            state.global_auto_replies_text.clear();
            state.global_auto_replies_media.clear();
            state.last_reply_at.clear();
            state.last_reply_at_media.clear();
            state.save_persistence();
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                "<b>⛧ Автоответчики очищены ⛧</b>".to_string(),
                None,
            )
            .await;
        }
        Command::clear => {
            let checks = [
                &state.spam_tasks_ar,
                &state.spam_tasks_sp,
                &state.spam_tasks_arp,
                &state.spam_tasks_arv,
                &state.spam_tasks_tag,
                &state.spam_tasks_tagp,
                &state.spam_tasks_tagv,
                &state.calendar_tasks,
                &state.calendar_p_tasks,
                &state.calendar_v_tasks,
            ];
            for map in checks {
                for entry in map.iter() {
                    entry.value().abort();
                }
                map.clear();
            }
            state.missions.clear();
            state.save_persistence();
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                "<b>⛧ Все спамеры, тагеры и календари остановлены ⛧</b>".to_string(),
                None,
            )
            .await;
        }
        Command::uss(uid) => {
            let removed_local = state.auto_replies_text.remove(&uid).is_some();
            let removed_global = state.global_auto_replies_text.remove(&uid).is_some();
            clear_auto_reply_cooldowns(&state, uid, false);
            if removed_local || removed_global {
                state.save_persistence();
                let _ = send_resilient(&bot, msg.chat.id, "⛧ Выключено ⛧".to_string(), None).await;
            } else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!(
                        "⛧ Пользователь не добавлен в автоответчик с ID: <code>{}</code> ⛧",
                        uid
                    ),
                    None,
                )
                .await;
            }
        }
        Command::ussm(uid) => {
            let removed_local = state.auto_replies_media.remove(&uid).is_some();
            let removed_global = state.global_auto_replies_media.remove(&uid).is_some();
            clear_auto_reply_cooldowns(&state, uid, true);
            if removed_local || removed_global {
                state.save_persistence();
                let _ = send_resilient(&bot, msg.chat.id, "⛧ Выключено ⛧".to_string(), None).await;
            } else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!(
                        "⛧ Пользователь не добавлен в автоответчик с ID: <code>{}</code> ⛧",
                        uid
                    ),
                    None,
                )
                .await;
            }
        }
        Command::all(uid) => {
            let mut moved_text = false;
            let mut moved_media = false;

            if let Some((_, mut cfg)) = state.auto_replies_text.remove(&uid) {
                cfg.chat_id = None;
                state.global_auto_replies_text.insert(uid, cfg);
                moved_text = true;
            } else if state.global_auto_replies_text.contains_key(&uid) {
                moved_text = true;
            }

            if let Some((_, mut cfg)) = state.auto_replies_media.remove(&uid) {
                cfg.chat_id = None;
                state.global_auto_replies_media.insert(uid, cfg);
                moved_media = true;
            } else if state.global_auto_replies_media.contains_key(&uid) {
                moved_media = true;
            }

            if moved_text || moved_media {
                state.save_persistence();
                let delete_cmd = match (moved_text, moved_media) {
                    (false, true) => format!("<code>/ussm {}</code>", uid),
                    (true, false) => format!("<code>/uss {}</code>", uid),
                    _ => format!("<code>/uss {}</code> / <code>/ussm {}</code>", uid, uid),
                };
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!(
                        "⛧ Пользователь добавлен в глобальный автоответчик ⛧\n⛧ Чтобы удалить {} ⛧\n⛧ Выключить глобальный автоответчик <code>/unall {}</code> ⛧",
                        delete_cmd, uid
                    ),
                    None,
                )
                .await;
            } else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!(
                        "⛧ Пользователь не добавлен в автоответчик с ID: <code>{}</code> ⛧",
                        uid
                    ),
                    None,
                )
                .await;
            }
        }
        Command::unall(uid) => {
            let current_chat_id = msg.chat.id.0;
            let mut removed = false;

            if let Some((_, mut cfg)) = state.global_auto_replies_text.remove(&uid) {
                cfg.chat_id = Some(cfg.chat_id.unwrap_or(current_chat_id));
                state.auto_replies_text.insert(uid, cfg);
                removed = true;
            }
            if let Some((_, mut cfg)) = state.global_auto_replies_media.remove(&uid) {
                cfg.chat_id = Some(cfg.chat_id.unwrap_or(current_chat_id));
                state.auto_replies_media.insert(uid, cfg);
                removed = true;
            }

            if removed {
                state.save_persistence();
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    "⛧ Глобальный автоответчик выключен ⛧".to_string(),
                    None,
                )
                .await;
            } else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!(
                        "⛧ Пользователь не добавлен в глобальный автоответчик с ID: <code>{}</code> ⛧",
                        uid
                    ),
                    None,
                )
                .await;
            }
        }
        Command::us(ref args) => {
            let p: Vec<&str> = args.split_whitespace().collect();
            if p.len() < 3 {
                send_bad_args(&bot, msg.chat.id).await;
                return Ok(());
            }
            let uid = p[0].parse().unwrap_or(0);
            let delay = p[1].parse().unwrap_or(1);
            let f_name = p[2].to_string();
            let s = p.get(3..).unwrap_or(&[]).join(" ");
            let tpl_path = pick_template_path(&config.base_path, &f_name).await;
            if tokio::fs::metadata(&tpl_path).await.is_err() {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!("⛧ Файл <code>{}</code> не найден ⛧", escape(&f_name)),
                    None,
                )
                .await;
                return Ok(());
            }
            state.auto_replies_text.insert(
                uid,
                AutoReplyConfig {
                    file_name: f_name,
                    delay,
                    shapka: s,
                    media_url: None,
                    chat_id: Some(msg.chat.id.0),
                },
            );
            state.global_auto_replies_text.remove(&uid);
            state.save_persistence();
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                format!(
                    "⛧ Пользователь добавлен в автоответчик ⛧\n⛧ Чтобы удалить <code>/uss {}</code> ⛧\n⛧ Глобальный ответ <code>/all {}</code> ⛧",
                    uid, uid
                ),
                None,
            )
            .await;
        }
        Command::usm(ref args) => {
            let p: Vec<&str> = args.split_whitespace().collect();
            if p.len() < 4 {
                send_bad_args(&bot, msg.chat.id).await;
                return Ok(());
            }
            let uid = p[0].parse().unwrap_or(0);
            let delay = p[1].parse().unwrap_or(1);
            let f_name = p[2].to_string();
            let url = Some(p[3].to_string());
            let s = p.get(4..).unwrap_or(&[]).join(" ");
            let tpl_path = pick_template_path(&config.base_path, &f_name).await;
            if tokio::fs::metadata(&tpl_path).await.is_err() {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!("⛧ Файл <code>{}</code> не найден ⛧", escape(&f_name)),
                    None,
                )
                .await;
                return Ok(());
            }
            state.auto_replies_media.insert(
                uid,
                AutoReplyConfig {
                    file_name: f_name,
                    delay,
                    shapka: s,
                    media_url: url,
                    chat_id: Some(msg.chat.id.0),
                },
            );
            state.global_auto_replies_media.remove(&uid);
            state.save_persistence();
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                format!(
                    "⛧ Пользователь добавлен в автоответчик ⛧\n⛧ Чтобы удалить <code>/ussm {}</code> ⛧\n⛧ Глобальный ответ <code>/all {}</code> ⛧",
                    uid, uid
                ),
                None,
            )
            .await;
        }
        Command::check1(args) => {
            let p: Vec<&str> = args.split_whitespace().collect();
            if p.len() < 3 {
                send_bad_args(&bot, msg.chat.id).await;
                return Ok(());
            }
            let cid: i64 = p[0].parse().unwrap_or(0);
            let uid: u64 = p[1].parse().unwrap_or(0);
            let delay: u64 = p[2].parse().unwrap_or(60);
            let now_ts = chrono::Utc::now().timestamp();
            state.check1_tasks.insert(
                format!("{}_{}", cid, uid),
                Check1Task {
                    chat_id: cid,
                    user_id: uid,
                    delay,
                    last_seen_unix: now_ts,
                    last_message_id: None,
                    chat_username: None,
                    tme_internal: token_tme_internal(cid),
                    silence_notified: false,
                },
            );
            state.save_persistence();
            let _ = send_resilient(
                &bot,
                msg.chat.id,
                format!(
                    "⛧ Чекер включен ⛧\n⛧ Пользователь: <a href=\"tg://user?id={uid}\">{uid}</a> ⛧\n⛧ Чат: <code>{cid}</code> ⛧\n⛧ Чтобы выключить — <code>/uncheck {cid} {uid}</code> ⛧",
                    uid = uid,
                    cid = cid
                ),
                None,
            )
            .await;
        }
        Command::uncheck(args) => {
            let p: Vec<&str> = args.split_whitespace().collect();
            if p.len() < 2 {
                send_bad_args(&bot, msg.chat.id).await;
                return Ok(());
            }
            let key = format!("{}_{}", p[0], p[1]);
            if state.check1_tasks.remove(&key).is_some() {
                state.save_persistence();
                let _ =
                    send_resilient(&bot, msg.chat.id, "⛧ Чекер выключен ⛧".to_string(), None).await;
            }
        }
        Command::ar(ref args)
        | Command::arp(ref args)
        | Command::arv(ref args)
        | Command::tag(ref args)
        | Command::tagp(ref args)
        | Command::tagv(ref args)
        | Command::kal(ref args)
        | Command::kalp(ref args)
        | Command::kalv(ref args)
        | Command::sp(ref args) => {
            let p: Vec<&str> = args.split_whitespace().collect();
            if p.is_empty() {
                send_bad_args(&bot, msg.chat.id).await;
                return Ok(());
            }
            let cid_parsed = p[0].parse::<i64>().unwrap_or(msg.chat.id.0);
            let cid = ChatId(cid_parsed);

            let (interval_idx, file, url, is_video, uid, is_kal) = match cmd {
                Command::ar(_) | Command::sp(_) => {
                    if p.len() < 3 {
                        send_bad_args(&bot, msg.chat.id).await;
                        return Ok(());
                    }
                    (1usize, p[2].to_string(), None, false, None, false)
                }
                Command::arp(_) => {
                    if p.len() < 4 {
                        send_bad_args(&bot, msg.chat.id).await;
                        return Ok(());
                    }
                    (
                        1usize,
                        p[2].to_string(),
                        Some(p[3].to_string()),
                        false,
                        None,
                        false,
                    )
                }
                Command::arv(_) => {
                    if p.len() < 4 {
                        send_bad_args(&bot, msg.chat.id).await;
                        return Ok(());
                    }
                    (
                        1usize,
                        p[2].to_string(),
                        Some(p[3].to_string()),
                        true,
                        None,
                        false,
                    )
                }
                Command::tag(_) => {
                    if p.len() < 4 {
                        send_bad_args(&bot, msg.chat.id).await;
                        return Ok(());
                    }
                    (
                        2usize,
                        p[3].to_string(),
                        None,
                        false,
                        Some(p[1].parse().unwrap_or(0)),
                        false,
                    )
                }
                Command::tagp(_) => {
                    if p.len() < 5 {
                        send_bad_args(&bot, msg.chat.id).await;
                        return Ok(());
                    }
                    (
                        2usize,
                        p[3].to_string(),
                        Some(p[4].to_string()),
                        false,
                        Some(p[1].parse().unwrap_or(0)),
                        false,
                    )
                }
                Command::tagv(_) => {
                    if p.len() < 5 {
                        send_bad_args(&bot, msg.chat.id).await;
                        return Ok(());
                    }
                    (
                        2usize,
                        p[3].to_string(),
                        Some(p[4].to_string()),
                        true,
                        Some(p[1].parse().unwrap_or(0)),
                        false,
                    )
                }
                Command::kal(_) => {
                    if p.len() < 3 {
                        send_bad_args(&bot, msg.chat.id).await;
                        return Ok(());
                    }
                    (1usize, p[2].to_string(), None, false, None, true)
                }
                Command::kalp(_) => {
                    if p.len() < 4 {
                        send_bad_args(&bot, msg.chat.id).await;
                        return Ok(());
                    }
                    (
                        1usize,
                        p[2].to_string(),
                        Some(p[3].to_string()),
                        false,
                        None,
                        true,
                    )
                }
                Command::kalv(_) => {
                    if p.len() < 4 {
                        send_bad_args(&bot, msg.chat.id).await;
                        return Ok(());
                    }
                    (
                        1usize,
                        p[2].to_string(),
                        Some(p[3].to_string()),
                        true,
                        None,
                        true,
                    )
                }
                _ => (1usize, "".to_string(), None, false, None, false),
            };
            let tpl_path = pick_template_path(&config.base_path, &file).await;
            if tokio::fs::metadata(&tpl_path).await.is_err() {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!("⛧ Файл <code>{}</code> не найден ⛧", escape(&file)),
                    None,
                )
                .await;
                return Ok(());
            }
            let (delay, wait_ms) = if is_kal {
                let dm = p
                    .get(interval_idx)
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(1)
                    .max(1);
                (dm, 0u64)
            } else {
                let sec = p
                    .get(interval_idx)
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(1.0)
                    .max(0.0);
                let wm = (sec * 1000.0).round().max(1.0) as u64;
                let d = ((wm + 999) / 1000).max(1);
                (d, wm)
            };

            if is_kal && (delay == 0 || delay > 1440) {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    "<b>⛧ Календарь: 1–1440 мин между постами 1..99; +1–59 сек; 1-й сразу; 100-й — в чат команды.</b>".to_string(),
                    None,
                )
                .await;
                return Ok(());
            }

            let s = match cmd {
                Command::arp(_) | Command::arv(_) | Command::kalp(_) | Command::kalv(_) => {
                    p.get(4..).unwrap_or(&[]).join(" ")
                }
                Command::tagp(_) | Command::tagv(_) => p.get(5..).unwrap_or(&[]).join(" "),
                Command::tag(_) => p.get(4..).unwrap_or(&[]).join(" "),
                _ => p.get(3..).unwrap_or(&[]).join(" "),
            };

            let b = Arc::new(bot.clone());
            let ow = owner_id;
            let origin_cid = msg.chat.id;
            let kal_echo = msg
                .text()
                .map(|t| t.to_string())
                .or_else(|| msg.caption().map(|c| c.to_string()))
                .unwrap_or_default();
            let h = if is_kal {
                tokio::spawn(crate::token::spam_engine::run_calendar(
                    b,
                    state.clone(),
                    cid,
                    origin_cid,
                    delay,
                    file.clone(),
                    s.clone(),
                    url.as_ref().map(|u| (u.clone(), is_video)),
                    ow,
                    kal_echo.clone(),
                    cmd_str.clone(),
                ))
            } else {
                let log_sr = matches!(cmd, Command::sp(_));
                tokio::spawn(crate::token::spam_engine::run_spam(
                    b,
                    state.clone(),
                    cid,
                    wait_ms,
                    file.clone(),
                    s.clone(),
                    uid,
                    url.as_ref().map(|u| (u.clone(), is_video)),
                    cmd_str.clone(),
                    log_sr,
                    owner_id,
                ))
            };

            match cmd {
                Command::ar(_) => {
                    state.spam_tasks_ar.insert(cid, h);
                }
                Command::sp(_) => {
                    state.spam_tasks_sp.insert(cid, h);
                }
                Command::arp(_) => {
                    state.spam_tasks_arp.insert(cid, h);
                }
                Command::arv(_) => {
                    state.spam_tasks_arv.insert(cid, h);
                }
                Command::tag(_) => {
                    state.spam_tasks_tag.insert(cid, h);
                }
                Command::tagp(_) => {
                    state.spam_tasks_tagp.insert(cid, h);
                }
                Command::tagv(_) => {
                    state.spam_tasks_tagv.insert(cid, h);
                }
                Command::kal(_) => {
                    if let Some((_, old)) = state.calendar_tasks.remove(&cid) {
                        old.abort();
                    }
                    state.calendar_tasks.insert(cid, h);
                }
                Command::kalp(_) => {
                    if let Some((_, old)) = state.calendar_p_tasks.remove(&cid) {
                        old.abort();
                    }
                    state.calendar_p_tasks.insert(cid, h);
                }
                Command::kalv(_) => {
                    if let Some((_, old)) = state.calendar_v_tasks.remove(&cid) {
                        old.abort();
                    }
                    state.calendar_v_tasks.insert(cid, h);
                }
                _ => {}
            }
            state.missions.insert(
                format!("{}_{}", cid.0, cmd_str),
                PersistentTask {
                    chat_id: cid.0,
                    delay,
                    file_name: file,
                    shapka: s,
                    user_id: uid,
                    media: url.map(|u| (u, is_video)),
                    task_type: cmd_str,
                    origin_chat_id: if is_kal { Some(origin_cid.0) } else { None },
                    kal_command_echo: if is_kal { Some(kal_echo) } else { None },
                },
            );
            state.save_persistence();
            let off_cmd = match cmd {
                Command::tag(_) => format!("/tagoff {}", cid),
                Command::tagp(_) => format!("/tagpoff {}", cid),
                Command::tagv(_) => format!("/tagvoff {}", cid),
                Command::kal(_) => format!("/koff {}", cid),
                Command::kalp(_) => format!("/kpoff {}", cid),
                Command::kalv(_) => format!("/kvoff {}", cid),
                Command::arp(_) => format!("/offp {}", cid),
                Command::arv(_) => format!("/offv {}", cid),
                Command::sp(_) => format!("/offsr {}", cid),
                _ => format!("/off {}", cid),
            };
            let ack = if is_kal {
                format!(
                    "⛧ Календарь включен ⛧\n⛧ Чтобы выключить — <code>{}</code> ⛧",
                    off_cmd
                )
            } else {
                format!(
                    "⛧ Включено ⛧\n⛧ Чтобы выключить — <code>{}</code> ⛧",
                    off_cmd
                )
            };
            let _ = send_resilient(&bot, msg.chat.id, ack, None).await;
        }
        Command::off(cid) => {
            let cid_chat = ChatId(cid);
            let mut found = false;
            if let Some(kv) = state.spam_tasks_ar.remove(&cid_chat) {
                kv.1.abort();
                found = true;
            }
            let mkey = format!("{}_{}", cid, "ar");
            let mission_exists = state.missions.contains_key(&mkey);
            if found || mission_exists {
                state.missions.remove(&format!("{}_{}", cid, "ar"));
                state.save_persistence();
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!("⛧ Спам выключен в чате с ID: <code>{}</code> ⛧", cid),
                    None,
                )
                .await;
            } else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!("⛧ Спам не был включен в чате с ID: <code>{}</code> ⛧", cid),
                    None,
                )
                .await;
            }
        }
        Command::offsr(cid) => {
            let cid_chat = ChatId(cid);
            let mut found = false;
            if let Some(kv) = state.spam_tasks_sp.remove(&cid_chat) {
                kv.1.abort();
                found = true;
            }
            let mkey = format!("{}_{}", cid, "sp");
            let mission_exists = state.missions.contains_key(&mkey);
            if found || mission_exists {
                state.missions.remove(&format!("{}_{}", cid, "sp"));
                state.save_persistence();
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!("⛧ Спам в чате (<code>{}</code>) остановлен.", cid),
                    None,
                )
                .await;
            } else {
                let _ = send_resilient(
                    &bot,
                    msg.chat.id,
                    format!("⛧ Спам не был включен в чате с ID: <code>{}</code> ⛧", cid),
                    None,
                )
                .await;
            }
        }
        Command::koff(cid)
        | Command::tagoff(cid)
        | Command::offp(cid)
        | Command::kpoff(cid)
        | Command::tagpoff(cid)
        | Command::offv(cid)
        | Command::kvoff(cid)
        | Command::tagvoff(cid) => {
            let cid_chat = ChatId(cid);
            let mut found = false;
            let t_type = match cmd {
                Command::koff(_) => "kal",
                Command::tagoff(_) => "tag",
                Command::offp(_) => "arp",
                Command::kpoff(_) => "kalp",
                Command::tagpoff(_) => "tagp",
                Command::offv(_) => "arv",
                Command::kvoff(_) => "kalv",
                Command::tagvoff(_) => "tagv",
                _ => "ar",
            };
            let map = match cmd {
                Command::koff(_) => &state.calendar_tasks,
                Command::tagoff(_) => &state.spam_tasks_tag,
                Command::offp(_) => &state.spam_tasks_arp,
                Command::kpoff(_) => &state.calendar_p_tasks,
                Command::tagpoff(_) => &state.spam_tasks_tagp,
                Command::offv(_) => &state.spam_tasks_arv,
                Command::kvoff(_) => &state.calendar_v_tasks,
                Command::tagvoff(_) => &state.spam_tasks_tagv,
                _ => &state.spam_tasks_ar,
            };

            if let Some(kv) = map.remove(&cid_chat) {
                kv.1.abort();
                found = true;
            }
            let mkey = format!("{}_{}", cid, t_type);
            let mission_exists = state.missions.contains_key(&mkey);
            if found || mission_exists {
                state.missions.remove(&mkey);
                state.save_persistence();
                let stopped = match cmd {
                    Command::offp(_) => {
                        format!("⛧ Спам с фото выключен в чате с ID: <code>{}</code> ⛧", cid)
                    }
                    Command::offv(_) => format!(
                        "⛧ Спам с видео выключен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    Command::tagoff(_) => {
                        format!("⛧ Остановил теггер в чате с ID: <code>{}</code> ⛧", cid)
                    }
                    Command::tagpoff(_) => format!(
                        "⛧ Теггер с фото выключен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    Command::tagvoff(_) => format!(
                        "⛧ Теггер с видео выключен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    Command::koff(_) => {
                        format!("⛧ Календарь выключен в чате с ID: <code>{}</code> ⛧", cid)
                    }
                    Command::kpoff(_) => format!(
                        "⛧ Календарь с фото выключен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    Command::kvoff(_) => format!(
                        "⛧ Календарь с видео выключен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    _ => format!("⛧ Спам выключен в чате с ID: <code>{}</code> ⛧", cid),
                };
                let _ = send_resilient(&bot, msg.chat.id, stopped, None).await;
            } else {
                let not_running = match cmd {
                    Command::offp(_) => format!(
                        "⛧ Спам с фото не был включен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    Command::offv(_) => format!(
                        "⛧ Спам с видео не был включен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    Command::tagoff(_) => format!(
                        "⛧ Теггер не был запущен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    Command::tagpoff(_) => format!(
                        "⛧ Теггер с фото не был запущен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    Command::tagvoff(_) => format!(
                        "⛧ Теггер с видео не был включен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    Command::koff(_) => format!(
                        "⛧ Календарь не был включен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    Command::kpoff(_) => format!(
                        "⛧ Календарь с фото не был включен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    Command::kvoff(_) => format!(
                        "⛧ Календарь с видео не был включен в чате с ID: <code>{}</code> ⛧",
                        cid
                    ),
                    _ => format!("⛧ Спам не был включен в чате с ID: <code>{}</code> ⛧", cid),
                };
                let _ = send_resilient(&bot, msg.chat.id, not_running, None).await;
            }
        }
    }
    Ok(())
}
