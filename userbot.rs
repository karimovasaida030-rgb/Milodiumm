use crate::account::session_cleanup;
use crate::account::state::{AfkConfig, BotState};
use crate::account::utils::{
    escape, estimate_registration_date, format_template_size_limit, get_now, get_random_choice,
    get_sys_metrics, grammers_upload_extension_from_media, list_directory_content,
    read_template_file, remove_all_template_copies, resolve_existing_template_path,
    safe_template_file_name, strip_specific_url, templates_dir, upload_media_fast,
    DROP_COOLDOWN_SECS, MAX_TEMPLATE_FILE_SIZE_BYTES,
};
use chrono::{Datelike, Local};
use futures::StreamExt;
use grammers_client::types::{Chat, Downloadable, Message};
use grammers_client::{Client, InputMessage, Update};
use grammers_tl_types as tl;
// TODO: переписать это говно
use std::collections::{HashMap, HashSet};
use std::io::IsTerminal;
use std::io::{self, BufRead};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{sleep, sleep_until, Duration, Instant as TokioInstant};

async fn prompt(message: &str) -> std::io::Result<String> {
    println!("{}", message);
    let mut line = String::new();
    let stdin = io::stdin();
    stdin.lock().read_line(&mut line)?;
    Ok(line.trim().to_string())
}

fn html_msg(html: &str) -> InputMessage {
    let html = html.replace("href=\"tg://user?id=", "href=\"rayzen-tg-user://id=");
    let (text, mut entities) = grammers_client::parsers::parse_html_message(&html);
    for entity in &mut entities {
        if let tl::enums::MessageEntity::TextUrl(e) = entity {
            if let Some(user_id) = e.url.strip_prefix("rayzen-tg-user://id=") {
                e.url = format!("tg://user?id={}", user_id);
            }
        }
    }
    InputMessage::text(text).fmt_entities(entities)
}

fn auto_reply_rate_key(sender_id: u64, chat_id: i64) -> String {
    format!("{}:{}", sender_id, chat_id)
}

const AUTO_REPLY_MIN_DELAY_SECS: u64 = 1;
const AUTO_REPLY_SEND_TIMEOUT_SECS: u64 = 45;
const AUTO_REPLY_TRANSIENT_RETRY_SECS: u64 = 60;
const AUTO_REPLY_MAX_SEND_ATTEMPTS: u32 = 50;
const PING_TARGET: &str = "149.154.167.91:443";
const SEND_WIN_SECS: u64 = 90;
const SEND_RESTART_MAX: u32 = 2;
const GRAMMERS_RESTART_COOLDOWN_SECS: u64 = 300;
const GRAMMERS_RESTART_STAMP: &str = "/var/run/rayzen-bot-watchdog-restart";
const HC_SECS: u64 = 45;
const GRAMMERS_HEALTHCHECK_TIMEOUT_SECS: u64 = 20;
const GRAMMERS_HEALTHCHECK_FAILURE_LIMIT: u32 = 3;
static GRAMMERS_BROKEN_SEND_WINDOW_START: AtomicU64 = AtomicU64::new(0);
static GRAMMERS_BROKEN_SEND_COUNT: AtomicU32 = AtomicU32::new(0);

async fn ping_telegram_dc() -> Option<f64> {
    let addr: SocketAddr = PING_TARGET.parse().ok()?;
    let started = Instant::now();
    match tokio::time::timeout(Duration::from_millis(1000), TcpStream::connect(addr)).await {
        Ok(Ok(stream)) => {
            let _ = stream.set_nodelay(true);
            drop(stream);
            Some(started.elapsed().as_secs_f64() * 1000.0)
        }
        _ => None,
    }
}

async fn build_account_ping_reply() -> String {
    let ping = ping_telegram_dc().await;
    let ping = ping
        .map(|ms| format!("{:.0} ms", ms))
        .unwrap_or_else(|| "timeout".to_string());
    format!("пинг сервера — {}", ping)
}

fn clear_auto_reply_cooldowns(state: &BotState, uid: u64, media: bool) {
    let prefix = format!("{}:", uid);
    if media {
        state
            .last_reply_at_media
            .retain(|key, _| !key.starts_with(&prefix));
        state
            .media_auto_reply_running
            .retain(|key, _| !key.starts_with(&prefix));
        state
            .last_auto_reply_seen_at
            .retain(|key, _| !key.starts_with(&prefix));
        state
            .handled_auto_reply_message_id
            .retain(|key, _| !key.starts_with(&prefix));
    } else {
        state
            .last_reply_at
            .retain(|key, _| !key.starts_with(&prefix));
        state
            .text_auto_reply_running
            .retain(|key, _| !key.starts_with(&prefix));
        state
            .last_auto_reply_seen_at
            .retain(|key, _| !key.starts_with(&prefix));
    }
}

fn mission_key(chat_id: i64, task_type: &str) -> String {
    format!("{}_{}", chat_id, task_type)
}

fn legacy_mission_key(chat_id: i64) -> String {
    chat_id.to_string()
}

fn stop_command_task_type(cmd: &str) -> &'static str {
    match cmd {
        "offp" => "arp",
        "offv" => "arv",
        "tagoff" => "tag",
        "tagpoff" => "tagp",
        "tagvoff" => "tagv",
        "koff" => "kal",
        "kpoff" => "kalp",
        "kvoff" => "kalv",
        "offsr" => "sp",
        _ => "ar",
    }
}

const TG_MIN_CHANNEL_PEER: i64 = 1_000_000_000_000;

fn chat_public_username(chat: &Chat) -> Option<String> {
    if let Some(u) = chat.username() {
        let t = u.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    chat.usernames().first().map(|s| s.to_string())
}

fn check1_stored_public_username(chat: &Chat) -> Option<String> {
    match chat {
        Chat::User(_) => None,
        Chat::Group(_) | Chat::Channel(_) => chat_public_username(chat),
    }
}

fn compute_tme_internal_from_chat(chat: &Chat, raw_id: i64) -> Option<i64> {
    let from_minus100 = || {
        if raw_id <= -TG_MIN_CHANNEL_PEER {
            let neg = raw_id.saturating_neg();
            neg.checked_sub(TG_MIN_CHANNEL_PEER).filter(|&x| x > 0)
        } else {
            None
        }
    };
    let from_positive_internal = || (raw_id > 0 && raw_id < TG_MIN_CHANNEL_PEER).then_some(raw_id);

    match chat {
        Chat::User(_) => None,
        Chat::Channel(_) => from_minus100().or_else(from_positive_internal),
        Chat::Group(g) => {
            if let Some(i) = from_minus100() {
                return Some(i);
            }
            if g.is_megagroup() {
                return from_positive_internal();
            }
            None
        }
    }
}

fn compute_tme_internal_from_raw(raw_id: i64) -> Option<i64> {
    if raw_id <= -TG_MIN_CHANNEL_PEER {
        let neg = raw_id.saturating_neg();
        return neg.checked_sub(TG_MIN_CHANNEL_PEER).filter(|&x| x > 0);
    }
    (raw_id > 0 && raw_id < TG_MIN_CHANNEL_PEER).then_some(raw_id)
}

fn check1_message_url(
    chat_username: Option<&str>,
    tme_internal: Option<i64>,
    message_id: i32,
) -> Option<String> {
    if let Some(u) = chat_username.map(str::trim).filter(|s| !s.is_empty()) {
        let u = u.trim_start_matches('@');
        if message_id <= 0 {
            return Some(format!("https://t.me/{}", u));
        }
        return Some(format!("https://t.me/{}/{}", u, message_id));
    }
    let i = tme_internal.filter(|&x| x > 0)?;
    let message_id = if message_id > 0 { message_id } else { 1 };
    Some(format!("https://t.me/c/{}/{}", i, message_id))
}

fn check1_chat_line_html(
    chat_username: Option<&str>,
    tme_internal: Option<i64>,
    display_chat_id: i64,
    last_message_id: Option<i32>,
) -> String {
    let mid = last_message_id.filter(|&m| m > 0).unwrap_or(1);
    if let Some(url) = check1_message_url(chat_username, tme_internal, mid) {
        return format!(
            r#"Чат: <a href="{url}">{cid}</a>"#,
            url = url,
            cid = display_chat_id
        );
    }
    format!(r#"Чат: <code>{cid}</code>"#, cid = display_chat_id)
}

fn user_id_link_html(user_id: u64) -> String {
    format!(r#"<a href="tg://user?id={uid}">{uid}</a>"#, uid = user_id)
}

fn normalized_username(username: &str) -> Option<String> {
    let username = username.trim().trim_start_matches('@');
    if username.is_empty() {
        None
    } else {
        Some(username.to_string())
    }
}

fn remember_user_username(state: &BotState, user_id: u64, username: Option<&str>) {
    if user_id == 0 {
        return;
    }
    if let Some(username) = username.and_then(normalized_username) {
        state.known_usernames.insert(user_id, username);
    }
}

fn remember_chat_username(state: &BotState, chat: &Chat) {
    if let Some(username) = chat_public_username(chat).and_then(|u| normalized_username(&u)) {
        state.known_chat_usernames.insert(chat.id(), username);
    }
}

fn username_profile_link_html(user_id: u64, username: &str) -> String {
    let username = username.trim_start_matches('@');
    format!(r#"<a href="https://t.me/{username}">{user_id}</a>"#)
}

#[derive(Default)]
struct DialogSnapshot {
    chats: HashMap<i64, Chat>,
    usernames: HashMap<u64, String>,
}

fn remember_snapshot_chat(snapshot: &mut DialogSnapshot, state: &BotState, chat: &Chat) {
    snapshot
        .chats
        .entry(chat.id())
        .or_insert_with(|| chat.clone());
    remember_chat_username(state, chat);
    if let Chat::User(user) = chat {
        if let Some(username) = user.username().and_then(normalized_username) {
            let user_id = user.id() as u64;
            snapshot.usernames.insert(user_id, username.clone());
            state.known_usernames.insert(user_id, username);
        }
    }
}

async fn collect_dialog_snapshot(
    client: &Client,
    state: &BotState,
    current_chat: &Chat,
    desired_chat_ids: &HashSet<i64>,
) -> DialogSnapshot {
    let mut snapshot = DialogSnapshot::default();
    remember_snapshot_chat(&mut snapshot, state, current_chat);

    let started = TokioInstant::now();
    let mut dialogs = client.iter_dialogs();
    for _ in 0..5000 {
        if !desired_chat_ids.is_empty()
            && desired_chat_ids.iter().all(|id| {
                snapshot.chats.contains_key(id) || state.known_chat_usernames.contains_key(id)
            })
        {
            break;
        }
        if started.elapsed() >= Duration::from_millis(2500) {
            break;
        }
        match tokio::time::timeout(Duration::from_millis(120), dialogs.next()).await {
            Ok(Ok(Some(dialog))) => {
                remember_snapshot_chat(&mut snapshot, state, dialog.chat());
            }
            Ok(Ok(None)) => break,
            Ok(Err(err)) => {
                log::debug!("check dialog snapshot failed: {}", err);
                break;
            }
            Err(_) => break,
        }
    }
    snapshot
}

fn user_link_html_from_snapshot(
    state: &BotState,
    snapshot: &DialogSnapshot,
    user_id: u64,
) -> String {
    if let Some(username) = snapshot.usernames.get(&user_id).cloned().or_else(|| {
        state
            .known_usernames
            .get(&user_id)
            .map(|v| v.value().clone())
    }) {
        username_profile_link_html(user_id, &username)
    } else {
        user_id_link_html(user_id)
    }
}

fn linked_user_ids_for_check<I>(state: &BotState, snapshot: &DialogSnapshot, ids: I) -> Vec<String>
where
    I: IntoIterator<Item = u64>,
{
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for id in ids {
        if seen.insert(id) {
            out.push(user_link_html_from_snapshot(state, snapshot, id));
        }
    }
    out
}

async fn latest_outgoing_message_id_in_chat(client: &Client, chat: &Chat) -> Option<i32> {
    tokio::time::timeout(Duration::from_millis(1800), async {
        let mut it = client.iter_messages(chat);
        for _ in 0..250 {
            match it.next().await {
                Ok(Some(m)) if m.outgoing() => return Some(m.id()),
                Ok(Some(_)) => {}
                _ => break,
            }
        }
        None
    })
    .await
    .ok()
    .flatten()
}

fn private_message_url(user_id: u64, message_id: i32) -> String {
    if message_id > 0 {
        format!("tg://openmessage?user_id={user_id}&message_id={message_id}")
    } else {
        format!("tg://openmessage?user_id={user_id}")
    }
}

async fn chat_link_html(
    client: &Client,
    state: &BotState,
    snapshot: &DialogSnapshot,
    chat_id: i64,
) -> String {
    if let Some(username) = state.known_chat_usernames.get(&chat_id) {
        let username = username.value().clone();
        return format!(r#"<a href="https://t.me/{username}">{chat_id}</a>"#);
    }

    let Some(chat) = snapshot.chats.get(&chat_id) else {
        if let Some(mid) = state
            .last_outgoing_message_id_by_chat
            .get(&chat_id)
            .map(|m| *m.value())
        {
            if chat_id > 0 {
                let url = format!("https://t.me/c/{chat_id}/{mid}");
                return format!(r#"<a href="{url}">{chat_id}</a>"#);
            }
            if let Some(internal) = compute_tme_internal_from_raw(chat_id) {
                let url = format!("https://t.me/c/{internal}/{mid}");
                return format!(r#"<a href="{url}">{chat_id}</a>"#);
            }
        }
        if chat_id > 0 {
            return user_link_html_from_snapshot(state, snapshot, chat_id as u64);
        }
        return format!(r#"<code>{}</code>"#, chat_id);
    };
    if let Some(username) = chat_public_username(chat) {
        let username = username.trim_start_matches('@').to_string();
        state.known_chat_usernames.insert(chat_id, username.clone());
        return format!(r#"<a href="https://t.me/{username}">{chat_id}</a>"#);
    }
    if let Some(mid) = latest_outgoing_message_id_in_chat(client, chat).await {
        state.last_outgoing_message_id_by_chat.insert(chat_id, mid);
        if let Chat::User(user) = chat {
            let url = private_message_url(user.id() as u64, mid);
            return format!(r#"<a href="{url}">{chat_id}</a>"#);
        }
        let internal = compute_tme_internal_from_chat(chat, chat_id);
        if let Some(url) = check1_message_url(None, internal, mid) {
            return format!(r#"<a href="{url}">{chat_id}</a>"#);
        }
    }
    format!(r#"<code>{}</code>"#, chat_id)
}

async fn linked_chat_ids_for_map(
    client: &Client,
    state: &BotState,
    snapshot: &DialogSnapshot,
    map: &dashmap::DashMap<i64, tokio::task::JoinHandle<()>>,
) -> String {
    let ids: Vec<i64> = map.iter().map(|kv| *kv.key()).collect();
    if ids.is_empty() {
        return "None".to_string();
    }
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if seen.insert(id) {
            out.push(chat_link_html(client, state, snapshot, id).await);
        }
    }
    out.join(", ")
}

fn check1_silence_alert_html(
    chat_username: Option<String>,
    tme_internal: Option<i64>,
    display_chat_id: i64,
    user_id: u64,
    delay_threshold: u64,
    silence_secs: u64,
    last_message_id: Option<i32>,
) -> String {
    let profile = format!(r#"<a href="tg://user?id={uid}">{uid}</a>"#, uid = user_id);
    let un = chat_username.as_deref();
    let last_part = match last_message_id {
        Some(mid) => match check1_message_url(un, tme_internal, mid) {
            Some(url) => format!(r#"<a href="{url}">последнее сообщение</a>"#),
            None => format!(
                "последнее сообщение (id {mid}): нет публичного username и не удалось собрать t.me/c для chat_id <code>{cid}</code>",
                mid = mid,
                cid = display_chat_id
            ),
        },
        None => "последнее сообщение: пока нет id (пользователь ещё не писал после включения чекера)".to_string(),
    };
    let chat_line = check1_chat_line_html(un, tme_internal, display_chat_id, last_message_id);
    format!(
        "<b>⛧ Чекер ⛧</b>\nПользователь {profile} не писал <code>{silence}</code> сек (порог <code>{delay}</code> сек).\n{last_part}\n{chat_line}",
        profile = profile,
        silence = silence_secs,
        delay = delay_threshold,
        last_part = last_part,
        chat_line = chat_line,
    )
}

const GRAMMERS_CALENDAR_MIN_DELAY_MINUTES: u64 = 1;

fn spam_interval_arg_to_wait_ms_stored(arg: impl AsRef<str>) -> (u64, u64) {
    let sec = arg.as_ref().parse::<f64>().unwrap_or(1.0).max(0.0);
    let wait_ms = (sec * 1000.0).round().max(1.0) as u64;
    let stored = ((wait_ms + 999) / 1000).max(1);
    (wait_ms, stored)
}

async fn resolve_chat(client: &Client, current_chat: &Chat, target_str: &str) -> Option<Chat> {
    if let Ok(id) = target_str.parse::<i64>() {
        if id == current_chat.id() || id == 0 {
            return Some(current_chat.clone());
        }
        let mut dialogs = client.iter_dialogs();
        while let Ok(Some(dialog)) = dialogs.next().await {
            if dialog.chat().id() == id {
                return Some(dialog.chat().clone());
            }
        }
    } else {
        if let Ok(Some(c)) = client
            .resolve_username(target_str.trim_start_matches('@'))
            .await
        {
            return Some(c);
        }
    }
    None
}

async fn find_chat_by_id(client: &Client, chat_id: i64) -> Option<Chat> {
    if chat_id == 0 {
        return None;
    }
    let mut dialogs = client.iter_dialogs();
    while let Ok(Some(dialog)) = dialogs.next().await {
        if dialog.chat().id() == chat_id {
            return Some(dialog.chat().clone());
        }
    }
    None
}

async fn resync_check1_tasks_on_start(
    client: &Client,
    state: &Arc<BotState>,
    dialog_cache: &HashMap<i64, Chat>,
) -> usize {
    let keys: Vec<String> = state.check1_tasks.iter().map(|e| e.key().clone()).collect();
    if keys.is_empty() {
        return 0;
    }
    let now = get_now();
    let mut touched: usize = 0;

    for key in keys {
        let (chat_id, user_id) = match state.check1_tasks.get(&key) {
            Some(t) => (t.chat_id, t.user_id),
            None => continue,
        };

        if let Some(mut t) = state.check1_tasks.get_mut(&key) {
            t.last_time = Some(now);
            t.silence_notified = false;
            t.link_chat_id = Some(chat_id);
        }
        touched = touched.saturating_add(1);

        let Some(chat) = dialog_cache.get(&chat_id).cloned() else {
            continue;
        };

        let wanted = HashSet::from([user_id]);
        let empty = HashSet::new();
        let (latest_by_user, _) =
            collect_recent_auto_reply_candidates(client, state, &chat, &wanted, &empty, 50, 700)
                .await;
        let last_mid = latest_by_user.get(&user_id).copied();

        if let Some(mut t) = state.check1_tasks.get_mut(&key) {
            t.last_message_id = last_mid;
            t.silence_notified = false;
            t.link_chat_id = Some(chat_id);
            t.chat_username = check1_stored_public_username(&chat);
            t.tme_internal = compute_tme_internal_from_chat(&chat, chat_id);
        }
        sleep(Duration::from_millis(120)).await;
    }

    state.save_persistence();
    touched
}

async fn delete_scheduled_ids_in_chat(client: &Client, chat: &Chat, ids: &[i32]) {
    if ids.is_empty() {
        return;
    }
    let peer = chat.pack().to_input_peer();
    for chunk in ids.chunks(100) {
        let _ = client
            .invoke(&tl::functions::messages::DeleteScheduledMessages {
                peer: peer.clone(),
                id: chunk.to_vec(),
            })
            .await;
    }
}

fn scheduled_message_matches_calendar_type(msg: &tl::types::Message, task_type: &str) -> bool {
    match task_type {
        "kal" => msg.media.is_none(),
        "kalp" => matches!(msg.media, Some(tl::enums::MessageMedia::Photo(_))),
        "kalv" => match &msg.media {
            Some(tl::enums::MessageMedia::Document(doc)) => match &doc.document {
                Some(tl::enums::Document::Document(d)) => d
                    .attributes
                    .iter()
                    .any(|a| matches!(a, tl::enums::DocumentAttribute::Video(_))),
                _ => false,
            },
            _ => false,
        },
        _ => false,
    }
}

async fn delete_scheduled_calendar_payloads_in_chat(
    client: &Client,
    chat: &Chat,
    task_type: &str,
) -> usize {
    let peer = chat.pack().to_input_peer();
    let response = client
        .invoke(&tl::functions::messages::GetScheduledHistory {
            peer: peer.clone(),
            hash: 0,
        })
        .await;

    let Ok(messages) = response else {
        return 0;
    };

    let raw_messages = match messages {
        tl::enums::messages::Messages::Messages(m) => m.messages,
        tl::enums::messages::Messages::Slice(m) => m.messages,
        tl::enums::messages::Messages::ChannelMessages(m) => m.messages,
        tl::enums::messages::Messages::NotModified(_) => Vec::new(),
    };

    let ids: Vec<i32> = raw_messages
        .into_iter()
        .filter_map(|m| match m {
            tl::enums::Message::Message(m)
                if scheduled_message_matches_calendar_type(&m, task_type) =>
            {
                Some(m.id)
            }
            _ => None,
        })
        .collect();
    delete_scheduled_ids_in_chat(client, chat, &ids).await;
    ids.len()
}

fn scheduled_calendar_echo_task_type(text: &str, target_cid: i64) -> Option<String> {
    let mut parts = text.split_whitespace();
    let first = parts.next()?;
    let cmd = first
        .trim_start_matches('!')
        .trim_start_matches('/')
        .split('@')
        .next()
        .unwrap_or(first)
        .to_lowercase();
    if !matches!(cmd.as_str(), "kal" | "kalp" | "kalv") {
        return None;
    }
    let chat_arg = parts.next()?;
    if chat_arg == target_cid.to_string() {
        Some(cmd)
    } else {
        None
    }
}

struct CalendarEchoScan {
    ids: Vec<i32>,
    due_at: Option<u64>,
}

async fn scan_scheduled_calendar_echoes_in_chat(
    client: &Client,
    chat: &Chat,
    task_type: &str,
    target_cid: i64,
) -> Option<CalendarEchoScan> {
    let peer = chat.pack().to_input_peer();
    let response = client
        .invoke(&tl::functions::messages::GetScheduledHistory {
            peer: peer.clone(),
            hash: 0,
        })
        .await
        .ok()?;

    let raw_messages = match response {
        tl::enums::messages::Messages::Messages(m) => m.messages,
        tl::enums::messages::Messages::Slice(m) => m.messages,
        tl::enums::messages::Messages::ChannelMessages(m) => m.messages,
        tl::enums::messages::Messages::NotModified(_) => Vec::new(),
    };

    let mut ids = Vec::new();
    let mut due_at = None;
    for msg in raw_messages {
        let tl::enums::Message::Message(m) = msg else {
            continue;
        };
        let Some(found_type) = scheduled_calendar_echo_task_type(&m.message, target_cid) else {
            continue;
        };
        if found_type != task_type {
            continue;
        }
        ids.push(m.id);
        let date = m.date.max(0) as u64;
        due_at = Some(due_at.map_or(date, |old: u64| old.max(date)));
    }

    Some(CalendarEchoScan { ids, due_at })
}

async fn scheduled_calendar_payload_count_in_chat(
    client: &Client,
    chat: &Chat,
    task_type: &str,
) -> Option<usize> {
    let peer = chat.pack().to_input_peer();
    let response = client
        .invoke(&tl::functions::messages::GetScheduledHistory {
            peer: peer.clone(),
            hash: 0,
        })
        .await
        .ok()?;

    let raw_messages = match response {
        tl::enums::messages::Messages::Messages(m) => m.messages,
        tl::enums::messages::Messages::Slice(m) => m.messages,
        tl::enums::messages::Messages::ChannelMessages(m) => m.messages,
        tl::enums::messages::Messages::NotModified(_) => Vec::new(),
    };

    Some(
        raw_messages
            .into_iter()
            .filter(|m| match m {
                tl::enums::Message::Message(m) => {
                    scheduled_message_matches_calendar_type(m, task_type)
                }
                _ => false,
            })
            .count(),
    )
}

async fn prune_scheduled_calendar_echoes_in_chat(
    client: &Client,
    chat: &Chat,
    task_type: &str,
    target_cid: i64,
    keep_ids: &[i32],
) -> Option<usize> {
    let scan = scan_scheduled_calendar_echoes_in_chat(client, chat, task_type, target_cid).await?;
    let remove_ids: Vec<i32> = scan
        .ids
        .into_iter()
        .filter(|id| !keep_ids.contains(id))
        .collect();
    let removed = remove_ids.len();
    delete_scheduled_ids_in_chat(client, chat, &remove_ids).await;
    Some(removed)
}

async fn delete_scheduled_calendar_echoes_in_chat(
    client: &Client,
    chat: &Chat,
    task_type: &str,
    target_cid: i64,
) -> (usize, bool) {
    let peer = chat.pack().to_input_peer();
    let response = client
        .invoke(&tl::functions::messages::GetScheduledHistory {
            peer: peer.clone(),
            hash: 0,
        })
        .await;

    let Ok(messages) = response else {
        return (0, false);
    };

    let raw_messages = match messages {
        tl::enums::messages::Messages::Messages(m) => m.messages,
        tl::enums::messages::Messages::Slice(m) => m.messages,
        tl::enums::messages::Messages::ChannelMessages(m) => m.messages,
        tl::enums::messages::Messages::NotModified(_) => Vec::new(),
    };

    let mut wrong_type_seen = false;
    let ids: Vec<i32> = raw_messages
        .into_iter()
        .filter_map(|m| match m {
            tl::enums::Message::Message(m) => {
                let found_type = scheduled_calendar_echo_task_type(&m.message, target_cid)?;
                if found_type == task_type {
                    Some(m.id)
                } else {
                    wrong_type_seen = true;
                    None
                }
            }
            _ => None,
        })
        .collect();

    delete_scheduled_ids_in_chat(client, chat, &ids).await;
    (ids.len(), wrong_type_seen)
}

fn media_http_client(menu_quick: bool) -> &'static reqwest::Client {
    static MENU: OnceLock<reqwest::Client> = OnceLock::new();
    static FULL: OnceLock<reqwest::Client> = OnceLock::new();
    let ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
    if menu_quick {
        MENU.get_or_init(|| {
            reqwest::Client::builder()
                .user_agent(ua)
                .tcp_nodelay(true)
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(14))
                .pool_max_idle_per_host(16)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        })
    } else {
        FULL.get_or_init(|| {
            reqwest::Client::builder()
                .user_agent(ua)
                .tcp_nodelay(true)
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(150))
                .pool_max_idle_per_host(16)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        })
    }
}

const MEDIA_DOWNLOAD_MAX_BYTES: u64 = 280_000_000;

async fn download_media_url_to_temp(
    http_client: &reqwest::Client,
    url: &str,
    chat_id: i64,
) -> Result<String, ()> {
    let response = http_client.get(url).send().await.map_err(|_| ())?;
    if !response.status().is_success() {
        return Err(());
    }
    let ext = url.split('.').last().unwrap_or("dat");
    let ext = ext.split('?').next().unwrap_or(ext);
    let t = format!("temp_{}_{}.{}", chat_id, rand::random::<u32>(), ext);
    let file = tokio::fs::File::create(&t).await.map_err(|_| ())?;
    let mut writer = BufWriter::with_capacity(512 * 1024, file);
    let mut stream = response.bytes_stream();
    let mut total: u64 = 0;
    while let Some(item) = stream.next().await {
        let chunk = item.map_err(|_| ())?;
        total += chunk.len() as u64;
        if total > MEDIA_DOWNLOAD_MAX_BYTES {
            let _ = tokio::fs::remove_file(&t).await;
            return Err(());
        }
        writer.write_all(&chunk).await.map_err(|_| ())?;
    }
    writer.flush().await.map_err(|_| ())?;
    let mut inner = writer.into_inner();
    inner.flush().await.map_err(|_| ())?;
    Ok(t)
}

static MEDIA_URL_CACHE: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();

fn media_url_cache() -> &'static Mutex<HashMap<String, String>> {
    MEDIA_URL_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn is_public_http_url(url: &str) -> bool {
    url.starts_with("http://") || url.starts_with("https://")
}

fn media_url_as_document(url: &str, as_doc: bool) -> bool {
    if as_doc {
        return true;
    }
    let lower = url.to_lowercase();
    let path = lower.split('?').next().unwrap_or(&lower);
    path.ends_with(".mp4")
        || path.ends_with(".mov")
        || path.ends_with(".avi")
        || path.ends_with(".mkv")
        || path.ends_with(".webm")
        || path.ends_with(".gif")
}

fn media_url_looks_like_photo(url: &str) -> bool {
    let lower = url.to_lowercase();
    let path = lower.split('?').next().unwrap_or(&lower);
    path.ends_with(".jpg")
        || path.ends_with(".jpeg")
        || path.ends_with(".png")
        || path.ends_with(".webp")
        || path.ends_with(".bmp")
}

async fn send_media_msg_result(
    client: &Client,
    chat: &Chat,
    url: &str,
    caption: &str,
    as_doc: bool,
    menu_quick: bool,
    reply_to: Option<i32>,
) -> Result<i32, String> {
    let url = url.trim();
    if url.is_empty() {
        return client
            .send_message(chat, html_msg(caption).reply_to(reply_to))
            .await
            .map(|m| m.id())
            .map_err(|e| e.to_string());
    }

    if is_public_http_url(url) {
        let mut last_err = None;
        let build_msg = || {
            let mut msg = html_msg(caption);
            msg = if media_url_as_document(url, as_doc) {
                msg.document_url(url.to_string())
            } else if media_url_looks_like_photo(url) {
                msg.photo_url(url.to_string())
            } else {
                msg.document_url(url.to_string())
            };
            msg.reply_to(reply_to)
        };
        for attempt in 0u32..2u32 {
            if attempt == 1 {
                sleep(Duration::from_millis(60)).await;
            }
            match client.send_message(chat, build_msg()).await {
                Ok(m) => return Ok(m.id()),
                Err(e) => {
                    let err = e.to_string();
                    if is_flood_wait_error(&err) {
                        let wait = flood_wait_secs(&err);
                        log::warn!(
                            "grammers media flood wait chat={} wait={}s err={}",
                            chat.id(),
                            wait,
                            err
                        );
                        sleep(Duration::from_secs(wait.saturating_add(1))).await;
                        last_err = Some(err);
                        continue;
                    }
                    last_err = Some(err);
                }
            }
        }
        if let Some(ref err) = last_err {
            log::warn!(
                "grammers media url send failed chat={} url={} err={}",
                chat.id(),
                url,
                err
            );
        }
    }

    let http_client = media_http_client(menu_quick);

    let temp_name = if let Ok(guard) = media_url_cache().lock() {
        guard.get(url).cloned()
    } else {
        None
    };

    let temp_name = if let Some(ref p) = temp_name {
        if tokio::fs::metadata(p).await.is_ok() {
            p.clone()
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    let temp_name = if !temp_name.is_empty() {
        temp_name
    } else if let Ok(t) = download_media_url_to_temp(http_client, url, chat.id()).await {
        if let Ok(mut g) = media_url_cache().lock() {
            g.insert(url.to_string(), t.clone());
        }
        t
    } else {
        String::new()
    };

    if temp_name.is_empty() {
        return client
            .send_message(chat, html_msg(caption).reply_to(reply_to))
            .await
            .map(|m| m.id())
            .map_err(|e| e.to_string());
    }

    let ext = url.split('.').last().unwrap_or("dat");
    if let Ok(uploaded) = client.upload_file(&temp_name).await {
        let mut msg = html_msg(caption);
        if as_doc || ext == "mp4" || ext == "mov" || ext == "avi" || ext == "gif" {
            msg = msg.document(uploaded);
        } else {
            msg = msg.photo(uploaded);
        }
        client
            .send_message(chat, msg.reply_to(reply_to))
            .await
            .map(|m| m.id())
            .map_err(|e| e.to_string())
    } else {
        client
            .send_message(chat, html_msg(caption).reply_to(reply_to))
            .await
            .map(|m| m.id())
            .map_err(|e| e.to_string())
    }
}

async fn send_media_msg(
    client: &Client,
    chat: &Chat,
    url: &str,
    caption: &str,
    as_doc: bool,
    menu_quick: bool,
    reply_to: Option<i32>,
) -> bool {
    send_media_msg_result(client, chat, url, caption, as_doc, menu_quick, reply_to)
        .await
        .is_ok()
}

const DEFAULT_MENU_MEDIA: &str = "";

fn effective_menu_media(state: &BotState) -> String {
    let m = state.get_setting("media_link");
    let t = m.trim();
    if t.is_empty() {
        DEFAULT_MENU_MEDIA.to_string()
    } else {
        m
    }
}

async fn send_rayzen_style_menu(
    client: &Client,
    chat: &Chat,
    message: &Message,
    is_token_bot: bool,
    _is_out: bool,
    media_url: &str,
    caption: &str,
) {
    let url = if media_url.trim().is_empty() {
        DEFAULT_MENU_MEDIA
    } else {
        media_url.trim()
    };
    if !is_token_bot {
        let _ = message.delete().await;
    }
    let _ = send_media_msg(client, chat, url, caption, false, true, None).await;
}

async fn log_to_bot(state: &BotState, message: &str) {
    if state.bot_token.is_empty() {
        return;
    }
    let log_id = state.get_setting("log_id");
    if log_id.is_empty() {
        return;
    }
    let url = format!(
        "https://api.telegram.org/bot{}/sendMessage",
        state.bot_token
    );
    let resp = reqwest::Client::new()
        .post(url)
        .json(&serde_json::json!({
            "chat_id": log_id,
            "text": message,
            "parse_mode": "HTML"
        }))
        .send()
        .await;

    match resp {
        Ok(r) => {
            if !r.status().is_success() {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                log::error!("log_to_bot failed status={} body={}", status.as_u16(), body);
            }
        }
        Err(e) => {
            log::warn!("log_to_bot request failed: {}", e);
        }
    }
}

pub async fn log_slot_removed_to_owner(
    state: &BotState,
    account_name: &str,
    reason: &str,
    removed_target: &str,
) {
    let msg = format!(
        "Акк <code>{}</code>: сессия мертва (<code>{}</code>). Удалено: <code>{}</code>. Создай заново через <code>rayzen-session</code>.",
        escape(account_name),
        escape(reason),
        escape(removed_target)
    );
    log_to_bot(state, &msg).await;
}

fn startup_banner_html(total_tasks: usize) -> String {
    format!(
        "<b>⛧ БОТ В СЕТИ ⛧</b>\n\
         <b>⛧ Сервис был перезапущен ⛧</b>\n\
         <b>⛧ Восстановлено задач: {} ⛧</b>",
        total_tasks
    )
}

#[allow(dead_code)]
async fn send_startup_dm_to_owner(client: &Client, state: &BotState, html: &str) {
    let owner_id = state.get_setting("owner_id").parse::<u64>().unwrap_or(0);
    if owner_id == 0 {
        return;
    }
    let mut dialogs = client.iter_dialogs();
    while let Ok(Some(d)) = dialogs.next().await {
        if let Chat::User(u) = d.chat() {
            if u.id() as u64 == owner_id {
                let _ = client.send_message(d.chat(), html_msg(html)).await;
                return;
            }
        }
    }
}

async fn announce_account_startup(client: &Client, state: &BotState, missions_restored: usize) {
    let total = missions_restored
        + state.check1_tasks.len()
        + state.auto_replies_text.len()
        + state.auto_replies_media.len()
        + state.global_auto_replies_text.len()
        + state.global_auto_replies_media.len();
    let msg = startup_banner_html(total);
    log_to_bot(state, &msg).await;
    let _ = (client, msg);
    log::info!(
        "Старт акка {}: восстановлено задач (всего): {}",
        state.bot_username,
        total
    );
}

fn record_auto_reply_candidate(
    state: &BotState,
    message: &Message,
    text_wanted: &HashSet<u64>,
    media_wanted: &HashSet<u64>,
    text_latest: &mut HashMap<u64, i32>,
    media_latest: &mut HashMap<u64, i32>,
) {
    if message.outgoing() {
        return;
    }
    let Some(Chat::User(user)) = message.sender() else {
        return;
    };
    let sid = user.id() as u64;
    remember_user_username(state, sid, user.username());
    if text_wanted.contains(&sid) {
        text_latest.entry(sid).or_insert_with(|| message.id());
    }
    if media_wanted.contains(&sid) {
        media_latest.entry(sid).or_insert_with(|| message.id());
    }
}

async fn collect_recent_auto_reply_candidates(
    client: &Client,
    state: &BotState,
    chat: &Chat,
    text_wanted: &HashSet<u64>,
    media_wanted: &HashSet<u64>,
    limit: usize,
    timeout_ms: u64,
) -> (HashMap<u64, i32>, HashMap<u64, i32>) {
    tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        let mut text_latest: HashMap<u64, i32> = HashMap::new();
        let mut media_latest: HashMap<u64, i32> = HashMap::new();
        let mut it = client.iter_messages(chat);
        for _ in 0..limit {
            let Ok(Some(m)) = it.next().await else {
                break;
            };
            record_auto_reply_candidate(
                state,
                &m,
                text_wanted,
                media_wanted,
                &mut text_latest,
                &mut media_latest,
            );
            if text_latest.len() >= text_wanted.len() && media_latest.len() >= media_wanted.len() {
                break;
            }
        }
        (text_latest, media_latest)
    })
    .await
    .unwrap_or_default()
}

fn spawn_media_auto_reply_history_poller(
    client: Client,
    state: Arc<BotState>,
    dialog_cache: HashMap<i64, Chat>,
) {
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(4)).await;
            let mut by_chat: HashMap<
                i64,
                (Chat, Vec<(u64, crate::account::state::AutoReplyConfig)>),
            > = HashMap::new();
            for entry in state.auto_replies_media.iter() {
                let uid = *entry.key();
                let cfg = entry.value().clone();
                let Some(chat_id) = cfg.chat_id else {
                    continue;
                };
                let Some(chat) = dialog_cache.get(&chat_id).cloned() else {
                    continue;
                };
                by_chat
                    .entry(chat_id)
                    .or_insert_with(|| (chat, Vec::new()))
                    .1
                    .push((uid, cfg));
            }

            for (chat_id, (chat, configs)) in by_chat {
                if configs.is_empty() {
                    continue;
                }
                let wanted: HashSet<u64> = configs.iter().map(|(uid, _)| *uid).collect();
                let empty = HashSet::new();
                let (_, latest_by_user) = collect_recent_auto_reply_candidates(
                    &client, &state, &chat, &empty, &wanted, 12, 500,
                )
                .await;

                for (uid, cfg) in configs {
                    let Some(mid) = latest_by_user.get(&uid).copied() else {
                        continue;
                    };
                    let rate_key = auto_reply_rate_key(uid, chat_id);
                    if state
                        .handled_auto_reply_message_id
                        .get(&rate_key)
                        .map(|v| *v.value() == mid)
                        .unwrap_or(false)
                    {
                        continue;
                    }
                    let prev = state
                        .last_auto_reply_message_id
                        .get(&rate_key)
                        .map(|v| *v.value())
                        .unwrap_or_default();
                    if mid <= prev && state.media_auto_reply_running.contains_key(&rate_key) {
                        continue;
                    }
                    state
                        .last_auto_reply_message_id
                        .insert(rate_key.clone(), mid);
                    state
                        .last_auto_reply_seen_at
                        .insert(rate_key, Instant::now());
                    let delay_secs = cfg.delay.max(AUTO_REPLY_MIN_DELAY_SECS);
                    spawn_media_auto_reply_worker(
                        client.clone(),
                        chat.clone(),
                        state.clone(),
                        uid,
                        chat_id,
                        cfg,
                        delay_secs,
                    );
                }
                sleep(Duration::from_millis(120)).await;
            }
        }
    });
}

fn account_text_auto_reply_config(
    state: &BotState,
    sender_id: u64,
    chat_id: i64,
) -> Option<crate::account::state::AutoReplyConfig> {
    state
        .global_auto_replies_text
        .get(&sender_id)
        .map(|cfg| cfg.value().clone())
        .or_else(|| {
            state.auto_replies_text.get(&sender_id).and_then(|cfg| {
                let c = cfg.value().clone();
                if c.chat_id.map_or(true, |id| id == chat_id) {
                    Some(c)
                } else {
                    None
                }
            })
        })
}

fn account_media_auto_reply_config(
    state: &BotState,
    sender_id: u64,
    chat_id: i64,
) -> Option<crate::account::state::AutoReplyConfig> {
    state
        .global_auto_replies_media
        .get(&sender_id)
        .map(|cfg| cfg.value().clone())
        .or_else(|| {
            state.auto_replies_media.get(&sender_id).and_then(|cfg| {
                let c = cfg.value().clone();
                if c.chat_id.map_or(true, |id| id == chat_id) {
                    Some(c)
                } else {
                    None
                }
            })
        })
}

fn spawn_auto_reply_restart_replay(
    client: Client,
    state: Arc<BotState>,
    dialog_cache: HashMap<i64, Chat>,
    dialog_last_messages: HashMap<i64, Message>,
) {
    tokio::spawn(async move {
        sleep(Duration::from_secs(2)).await;
        let has_global_text = !state.global_auto_replies_text.is_empty();
        let has_global_media = !state.global_auto_replies_media.is_empty();
        let mut scan_chat_ids: HashSet<i64> = HashSet::new();

        if has_global_text || has_global_media {
            scan_chat_ids.extend(dialog_cache.keys().copied());
        }
        for entry in state.auto_replies_text.iter() {
            if let Some(chat_id) = entry.value().chat_id {
                scan_chat_ids.insert(chat_id);
            }
        }
        for entry in state.auto_replies_media.iter() {
            if let Some(chat_id) = entry.value().chat_id {
                scan_chat_ids.insert(chat_id);
            }
        }

        if scan_chat_ids.is_empty() {
            return;
        }

        let mut started = 0usize;
        for chat_id in scan_chat_ids {
            let Some(chat) = dialog_cache.get(&chat_id).cloned() else {
                continue;
            };

            let has_local_text = state
                .auto_replies_text
                .iter()
                .any(|e| e.value().chat_id.map_or(false, |id| id == chat_id));
            let has_local_media = state
                .auto_replies_media
                .iter()
                .any(|e| e.value().chat_id.map_or(false, |id| id == chat_id));
            let text_wanted: HashSet<u64> = state
                .global_auto_replies_text
                .iter()
                .map(|e| *e.key())
                .chain(state.auto_replies_text.iter().filter_map(|e| {
                    if e.value().chat_id.map_or(false, |id| id == chat_id) {
                        Some(*e.key())
                    } else {
                        None
                    }
                }))
                .collect();
            let media_wanted: HashSet<u64> = state
                .global_auto_replies_media
                .iter()
                .map(|e| *e.key())
                .chain(state.auto_replies_media.iter().filter_map(|e| {
                    if e.value().chat_id.map_or(false, |id| id == chat_id) {
                        Some(*e.key())
                    } else {
                        None
                    }
                }))
                .collect();

            if text_wanted.is_empty() && media_wanted.is_empty() {
                continue;
            }

            let mut text_latest: HashMap<u64, i32> = HashMap::new();
            let mut media_latest: HashMap<u64, i32> = HashMap::new();
            if let Some(last) = dialog_last_messages.get(&chat_id) {
                record_auto_reply_candidate(
                    &state,
                    last,
                    &text_wanted,
                    &media_wanted,
                    &mut text_latest,
                    &mut media_latest,
                );
            }
            if (has_local_text || has_local_media)
                && (text_latest.len() < text_wanted.len()
                    || media_latest.len() < media_wanted.len())
            {
                let (extra_text, extra_media) = collect_recent_auto_reply_candidates(
                    &client,
                    &state,
                    &chat,
                    &text_wanted,
                    &media_wanted,
                    50,
                    700,
                )
                .await;
                for (uid, mid) in extra_text {
                    text_latest.entry(uid).or_insert(mid);
                }
                for (uid, mid) in extra_media {
                    media_latest.entry(uid).or_insert(mid);
                }
            }

            for (uid, mid) in text_latest {
                let Some(cfg) = account_text_auto_reply_config(&state, uid, chat_id) else {
                    continue;
                };
                let rate_key = auto_reply_rate_key(uid, chat_id);
                state
                    .last_auto_reply_message_id
                    .insert(rate_key.clone(), mid);
                state
                    .last_auto_reply_seen_at
                    .insert(rate_key, Instant::now());
                let delay_secs = cfg.delay.max(AUTO_REPLY_MIN_DELAY_SECS);
                spawn_text_auto_reply_worker(
                    client.clone(),
                    chat.clone(),
                    state.clone(),
                    uid,
                    chat_id,
                    cfg,
                    delay_secs,
                );
                started = started.saturating_add(1);
            }

            for (uid, mid) in media_latest {
                let Some(cfg) = account_media_auto_reply_config(&state, uid, chat_id) else {
                    continue;
                };
                let rate_key = auto_reply_rate_key(uid, chat_id);
                state
                    .last_auto_reply_message_id
                    .insert(rate_key.clone(), mid);
                state
                    .last_auto_reply_seen_at
                    .insert(rate_key.clone(), Instant::now());
                let delay_secs = cfg.delay.max(AUTO_REPLY_MIN_DELAY_SECS);
                spawn_media_auto_reply_worker(
                    client.clone(),
                    chat.clone(),
                    state.clone(),
                    uid,
                    chat_id,
                    cfg,
                    delay_secs,
                );
                started = started.saturating_add(1);
            }

            sleep(Duration::from_millis(120)).await;
        }

        if started > 0 {
            log::info!(
                "account auto-reply restart replay: started {} history replies",
                started
            );
        }
    });
}

async fn handle_spam_loop(
    client: Client,
    packed_chat: Chat,
    state: Arc<BotState>,
    wait_ms: u64,
    file_name: String,
    shapka: String,
    user_id: Option<u64>,
    media_url: Option<String>,
    is_video: bool,
    task_map_type: String,
) {
    let chat_id = packed_chat.id();
    let base = state.base_path.clone();
    let fname = file_name.clone();
    let interval = Duration::from_millis(wait_ms.max(1));
    let mut next_tick = TokioInstant::now();
    loop {
        let should_run = match task_map_type.as_str() {
            "ar" => state.spam_tasks_ar.contains_key(&chat_id),
            "arp" => state.spam_tasks_arp.contains_key(&chat_id),
            "arv" => state.spam_tasks_arv.contains_key(&chat_id),
            "tag" => state.spam_tasks_tag.contains_key(&chat_id),
            "tagp" => state.spam_tasks_tagp.contains_key(&chat_id),
            "tagv" => state.spam_tasks_tagv.contains_key(&chat_id),
            "sp" => state.spam_tasks_sp.contains_key(&chat_id),
            _ => false,
        };
        if !should_run {
            break;
        }
        let now = TokioInstant::now();
        if next_tick > now {
            sleep_until(next_tick).await;
        }
        next_tick = TokioInstant::now() + interval;

        if let Some(content) = read_template_file(&base, &fname).await {
            let lines: Vec<String> = content
                .lines()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            if let Some(l) = get_random_choice(lines) {
                let mut text = if let Some(uid) = user_id {
                    format!(
                        "<a href=\"tg://user?id={}\">{} {}</a>",
                        uid,
                        escape(&shapka),
                        escape(&l)
                    )
                } else {
                    if shapka.is_empty() {
                        escape(&l)
                    } else {
                        format!("{} {}", escape(&shapka), escape(&l))
                    }
                };

                if let Some(ref url) = media_url {
                    text = strip_specific_url(&text, url);
                    if let Err(err) = send_spam_media_with_retry(
                        &client,
                        &packed_chat,
                        url,
                        &text,
                        is_video,
                        false,
                        None,
                    )
                    .await
                    {
                        let ts = Local::now().format("%H:%M:%S.%d.%m.%Y").to_string();
                        let title = chat_title_preview(&packed_chat);
                        if let Some(reason) = auto_reply_access_loss_reason(&err) {
                            log::warn!(
                                "grammers spam send stopped (access lost) task={} chat_id={} mode=media err={}",
                                task_map_type,
                                chat_id,
                                err
                            );
                            log_to_bot(
                                &state,
                                &rayzen_spam_access_lost_log(
                                    &task_map_type,
                                    &ts,
                                    &title,
                                    chat_id,
                                    reason,
                                    &err,
                                ),
                            )
                            .await;
                            stop_spam_task_for_chat(&state, &task_map_type, chat_id);
                            break;
                        }
                        let log_line =
                            rayzen_spam_send_error_log(&task_map_type, &ts, &title, chat_id, &err);
                        log::warn!(
                            "grammers spam send failed task={} chat_id={} mode=media err={}",
                            task_map_type,
                            chat_id,
                            err
                        );
                        log_to_bot(&state, &log_line).await;
                        continue;
                    }
                } else {
                    if let Err(err) = send_spam_text_with_retry(&client, &packed_chat, &text).await
                    {
                        let ts = Local::now().format("%H:%M:%S.%d.%m.%Y").to_string();
                        let title = chat_title_preview(&packed_chat);
                        if let Some(reason) = auto_reply_access_loss_reason(&err) {
                            log::warn!(
                                "grammers spam send stopped (access lost) task={} chat_id={} mode=text err={}",
                                task_map_type,
                                chat_id,
                                err
                            );
                            log_to_bot(
                                &state,
                                &rayzen_spam_access_lost_log(
                                    &task_map_type,
                                    &ts,
                                    &title,
                                    chat_id,
                                    reason,
                                    &err,
                                ),
                            )
                            .await;
                            stop_spam_task_for_chat(&state, &task_map_type, chat_id);
                            break;
                        }
                        let log_line =
                            rayzen_spam_send_error_log(&task_map_type, &ts, &title, chat_id, &err);
                        log::warn!(
                            "grammers spam send failed task={} chat_id={} mode=text err={}",
                            task_map_type,
                            chat_id,
                            err
                        );
                        log_to_bot(&state, &log_line).await;
                        continue;
                    }
                }
                let ts = Local::now().format("%H:%M:%S.%d.%m.%Y").to_string();
                let title = chat_title_preview(&packed_chat);
                let log_line = rayzen_spam_send_log(&task_map_type, &ts, &title, chat_id);
                log_to_bot(&state, &log_line).await;
            }
        }
    }
}

fn chat_title_preview(chat: &Chat) -> String {
    match chat {
        Chat::User(u) => u.first_name().to_string(),
        Chat::Group(g) => g.title().to_string(),
        Chat::Channel(c) => c.title().to_string(),
    }
}

fn rayzen_spam_send_log(task_type: &str, ts: &str, chat_title: &str, chat_id: i64) -> String {
    let t = escape(chat_title);
    match task_type {
        "ar" => format!(
            "<code>{}</code> — было отправлено сообщение в чат <code>{}</code> | <code>{}</code> | Спам команда — [ar]",
            ts, t, chat_id
        ),
        "arp" => format!(
            "<code>{}</code> — было отправлено фото в чат <code>{}</code> | <code>{}</code> | Спам команда — [arp]",
            ts, t, chat_id
        ),
        "arv" => format!(
            "<code>{}</code> — было отправлено видео в чат <code>{}</code> | <code>{}</code> | Спам команда — [arv]",
            ts, t, chat_id
        ),
        "tag" => format!(
            "<code>{}</code> — было отправлено сообщение в чат <code>{}</code> | <code>{}</code> | Спам команда — [tag]",
            ts, t, chat_id
        ),
        "tagp" => format!(
            "<code>{}</code> — было отправлено фото в чат <code>{}</code> | <code>{}</code> | Команда спама — [tagp]",
            ts, t, chat_id
        ),
        "tagv" => format!(
            "<code>{}</code> — было отправлено видео в чат <code>{}</code> | <code>{}</code> | Команда спама — [tagv]",
            ts, t, chat_id
        ),
        _ => format!(
            "<code>{}</code> — было отправлено сообщение в чат <code>{}</code> | <code>{}</code> | Спам команда — [{}]",
            ts,
            t,
            chat_id,
            escape(task_type)
        ),
    }
}

fn is_flood_wait_error(err: &str) -> bool {
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

fn is_transient_spam_send_error(err: &str) -> bool {
    let e = err.to_lowercase();
    is_flood_wait_error(err)
        || e.contains("timeout")
        || e.contains("timed out")
        || e.contains("deadline")
        || e.contains("network")
        || e.contains("connection")
        || e.contains("connect")
        || e.contains("temporarily")
        || e.contains("read error")
        || e.contains("io failed")
        || e.contains("read 0 bytes")
        || (!is_permanent_auto_reply_error(err) && e.contains("request error"))
}

fn is_grammers_connection_broken_error(err: &str) -> bool {
    if is_flood_wait_error(err) {
        return false;
    }
    let e = err.to_lowercase();
    e.contains("read 0 bytes")
        || e.contains("io failed")
        || e.contains("connection reset")
        || e.contains("connection aborted")
        || e.contains("connection closed")
        || e.contains("broken pipe")
        || (e.contains("request error") && e.contains("read error"))
}

fn reset_grammers_broken_send_failures() {
    GRAMMERS_BROKEN_SEND_COUNT.store(0, Ordering::Relaxed);
    GRAMMERS_BROKEN_SEND_WINDOW_START.store(0, Ordering::Relaxed);
}

fn claim_grammers_restart(now: u64) -> bool {
    let last = std::fs::read_to_string(GRAMMERS_RESTART_STAMP)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0);
    if now.saturating_sub(last) < GRAMMERS_RESTART_COOLDOWN_SECS {
        return false;
    }
    std::fs::write(GRAMMERS_RESTART_STAMP, format!("{}\n", now)).is_ok()
}

fn record_grammers_broken_send_failure(err: &str, context: &str, chat_id: i64) {
    if !is_grammers_connection_broken_error(err) {
        return;
    }

    let now = unix_now_secs();
    let start = GRAMMERS_BROKEN_SEND_WINDOW_START.load(Ordering::Relaxed);
    let count = if start == 0 || now.saturating_sub(start) > SEND_WIN_SECS {
        GRAMMERS_BROKEN_SEND_WINDOW_START.store(now, Ordering::Relaxed);
        GRAMMERS_BROKEN_SEND_COUNT.store(1, Ordering::Relaxed);
        1
    } else {
        GRAMMERS_BROKEN_SEND_COUNT
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    };

    log::warn!(
        "grammers sender looks broken context={} chat_id={} count={}/{} err={}",
        context,
        chat_id,
        count,
        SEND_RESTART_MAX,
        err
    );

    if count >= SEND_RESTART_MAX {
        reset_grammers_broken_send_failures();
        if claim_grammers_restart(now) {
            log::error!(
                "grammers sender is stuck after repeated send failures; exiting for systemd restart"
            );
            std::process::exit(12);
        }
        log::warn!(
            "grammers restart suppressed by {}s cooldown",
            GRAMMERS_RESTART_COOLDOWN_SECS
        );
    }
}

fn spam_send_retry_delay(err: &str, attempt: u32) -> Duration {
    if is_flood_wait_error(err) {
        return Duration::from_secs(flood_wait_secs(err).saturating_add(1));
    }
    Duration::from_secs(match attempt {
        0 => 2,
        1 => 5,
        _ => 10,
    })
}

fn auto_reply_retry_delay(err: &str) -> Duration {
    parse_retry_after_secs(err)
        .map(|secs| Duration::from_secs(secs.saturating_add(1).min(3600)))
        .unwrap_or_else(|| Duration::from_secs(AUTO_REPLY_TRANSIENT_RETRY_SECS))
}

async fn send_spam_text_with_retry(client: &Client, chat: &Chat, text: &str) -> Result<(), String> {
    let mut attempt = 0u32;
    loop {
        match client.send_message(chat, html_msg(text)).await {
            Ok(_) => {
                reset_grammers_broken_send_failures();
                return Ok(());
            }
            Err(err) => {
                let err = err.to_string();
                if attempt >= 3 || !is_transient_spam_send_error(&err) {
                    record_grammers_broken_send_failure(&err, "spam_text", chat.id());
                    return Err(err);
                }
                let wait = spam_send_retry_delay(&err, attempt);
                log::warn!(
                    "grammers spam text retry chat_id={} attempt={} wait={}s err={}",
                    chat.id(),
                    attempt + 1,
                    wait.as_secs(),
                    err
                );
                attempt = attempt + 1;
                sleep(wait).await;
            }
        }
    }
}

async fn send_spam_media_with_retry(
    client: &Client,
    chat: &Chat,
    url: &str,
    caption: &str,
    as_doc: bool,
    menu_quick: bool,
    reply_to: Option<i32>,
) -> Result<(), String> {
    let mut attempt = 0u32;
    loop {
        match send_media_msg_result(client, chat, url, caption, as_doc, menu_quick, reply_to).await
        {
            Ok(_) => {
                reset_grammers_broken_send_failures();
                return Ok(());
            }
            Err(err) => {
                if attempt >= 3 || !is_transient_spam_send_error(&err) {
                    record_grammers_broken_send_failure(&err, "spam_media", chat.id());
                    return Err(err);
                }
                let wait = spam_send_retry_delay(&err, attempt);
                log::warn!(
                    "grammers spam media retry chat_id={} attempt={} wait={}s err={}",
                    chat.id(),
                    attempt + 1,
                    wait.as_secs(),
                    err
                );
                attempt = attempt + 1;
                sleep(wait).await;
            }
        }
    }
}

fn flood_wait_secs(err: &str) -> u64 {
    if let Some(secs) = parse_retry_after_secs(err) {
        return secs.clamp(1, 60);
    }
    if let Some(pos) = err.find("value:") {
        let digits: String = err[pos + 6..]
            .chars()
            .skip_while(|c| !c.is_ascii_digit())
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if let Ok(v) = digits.parse::<u64>() {
            return v.clamp(1, 60);
        }
    }
    3
}

fn rayzen_spam_send_error_log(
    task_type: &str,
    ts: &str,
    chat_title: &str,
    chat_id: i64,
    err: &str,
) -> String {
    let t = escape(chat_title);
    let headline = if is_flood_wait_error(err) {
        "флудвайт: сообщение не отправлено"
    } else {
        "сообщение не отправлено"
    };
    format!(
        "<code>{}</code> — {} в чат <code>{}</code> | <code>{}</code> | Спам команда — [{}]\nПричина: <code>{}</code>",
        ts,
        headline,
        t,
        chat_id,
        escape(task_type),
        escape(err)
    )
}

fn rayzen_auto_reply_error_log(kind: &str, chat_id: i64, user_id: u64, err: &str) -> String {
    let ts = Local::now().format("%H:%M:%S.%d.%m.%Y").to_string();
    let headline = if is_flood_wait_error(err) {
        "floodwait: auto-reply was not sent"
    } else {
        "auto-reply was not sent"
    };
    format!(
        "<code>{}</code> - {} | type=<code>{}</code> | chat=<code>{}</code> | user=<code>{}</code>\nReason: <code>{}</code>",
        ts,
        headline,
        escape(kind),
        chat_id,
        user_id,
        escape(err)
    )
}

fn auto_reply_access_loss_reason(err: &str) -> Option<&'static str> {
    let e = err.to_lowercase();
    if e.contains("chat_write_forbidden") {
        Some("акк не может писать в чат: вероятно мут, запрет отправки или нет прав")
    } else if e.contains("user_banned_in_channel") {
        Some("акк забанен в чате")
    } else if e.contains("channel_invalid") {
        Some("чат недоступен для акка")
    } else if e.contains("peer_id_invalid") {
        Some("peer/chat недоступен для акка")
    } else if e.contains("not enough rights") || e.contains("forbidden") {
        Some("недостаточно прав для отправки в чат")
    } else {
        None
    }
}

fn rayzen_auto_reply_access_lost_log(
    kind: &str,
    chat_id: i64,
    user_id: u64,
    err: &str,
    reason: &str,
) -> String {
    let ts = Local::now().format("%H:%M:%S.%d.%m.%Y").to_string();
    format!(
        "<code>{}</code> - auto-reply stopped: account cannot send in chat | type=<code>{}</code> | chat=<code>{}</code> | user=<code>{}</code>\nStatus: <code>{}</code>\nReason: <code>{}</code>",
        ts,
        escape(kind),
        chat_id,
        user_id,
        escape(reason),
        escape(err)
    )
}

fn rayzen_auto_reply_retry_log(kind: &str, chat_id: i64, user_id: u64, err: &str) -> String {
    let ts = Local::now().format("%H:%M:%S.%d.%m.%Y").to_string();
    format!(
        "<code>{}</code> - auto-reply temporary send problem | type=<code>{}</code> | chat=<code>{}</code> | user=<code>{}</code>\nReason: <code>{}</code>\nStatus: <code>автоповтор активен, подтверждение не требуется</code>",
        ts,
        escape(kind),
        chat_id,
        user_id,
        escape(err)
    )
}

fn is_permanent_auto_reply_error(err: &str) -> bool {
    let e = err.to_lowercase();
    e.contains("channel_invalid")
        || e.contains("peer_id_invalid")
        || e.contains("chat_write_forbidden")
        || e.contains("user_banned_in_channel")
        || e.contains("user_is_blocked")
        || e.contains("input_user_deactivated")
        || e.contains("media_invalid")
        || e.contains("message_id_invalid")
        || e.contains("message to reply not found")
        || e.contains("not enough rights")
        || e.contains("forbidden")
}

fn is_server_error_auto_reply(err: &str) -> bool {
    let e = err.to_lowercase();
    e.contains("bad gateway")
        || e.contains("gateway")
        || e.contains("service unavailable")
        || e.contains("unavailable")
        || e.contains("internal server error")
        || e.contains("server error")
}

fn is_transient_auto_reply_error(err: &str) -> bool {
    let e = err.to_lowercase();
    e.contains("timeout")
        || e.contains("timed out")
        || e.contains("deadline")
        || e.contains("flood")
        || e.contains("retry_after")
        || e.contains("retry after")
        || e.contains("too many requests")
        || e.contains("slowmode")
        || e.contains("wait")
        || e.contains("network")
        || e.contains("connection")
        || e.contains("connect")
        || e.contains("temporarily")
        || is_server_error_auto_reply(err)
        || (!is_permanent_auto_reply_error(err) && e.contains("request error"))
}

fn rayzen_auto_reply_sent_after_retry_log(
    kind: &str,
    chat: &Chat,
    user_id: u64,
    message_id: i32,
    delay_secs: u64,
    actual_secs: u64,
) -> String {
    let ts = Local::now().format("%H:%M:%S.%d.%m.%Y").to_string();
    let over = actual_secs.saturating_sub(delay_secs);
    let link = auto_reply_message_url(chat, message_id)
        .map(|u| format!("<a href=\"{}\">сообщение</a>", escape(&u)))
        .unwrap_or_else(|| format!("message_id <code>{}</code>", message_id));
    format!(
        "<code>{}</code> - auto-reply sent after retry | type=<code>{}</code> | chat=<code>{}</code> | user=<code>{}</code>\nDelay: <code>{}s</code> | sent after: <code>{}s</code> | late by: <code>{}s</code>\nLink: {}",
        ts,
        escape(kind),
        chat.id(),
        user_id,
        delay_secs,
        actual_secs,
        over,
        link
    )
}

fn auto_reply_message_url(chat: &Chat, message_id: i32) -> Option<String> {
    if message_id <= 0 {
        return None;
    }
    if let Chat::User(user) = chat {
        return Some(private_message_url(user.id() as u64, message_id));
    }
    check1_message_url(
        check1_stored_public_username(chat).as_deref(),
        compute_tme_internal_from_chat(chat, chat.id())
            .or_else(|| compute_tme_internal_from_raw(chat.id())),
        message_id,
    )
}

#[derive(Clone)]
enum AutoReplyDeliveryPayload {
    Text(String),
    Media { url: String, caption: String },
}

fn normalize_auto_reply_body(raw: &str) -> String {
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

impl AutoReplyDeliveryPayload {
    fn matches_message(&self, msg: &Message) -> bool {
        let actual = normalize_auto_reply_body(msg.text());
        match self {
            AutoReplyDeliveryPayload::Text(expected) => {
                actual == normalize_auto_reply_body(expected)
            }
            AutoReplyDeliveryPayload::Media { caption, .. } => {
                let expected = normalize_auto_reply_body(caption);
                let caption_ok = expected.is_empty() || actual == expected;
                let media_ok = msg.media().is_some();
                caption_ok && (media_ok || !actual.is_empty())
            }
        }
    }
}

fn spawn_auto_reply_delivery_verifier(
    client: Client,
    state: Arc<BotState>,
    chat: Chat,
    kind: &'static str,
    sender_id: u64,
    sent_message_id: i32,
    reply_to: i32,
    payload: AutoReplyDeliveryPayload,
    delay_secs: u64,
    seen_at: Instant,
) {
    tokio::spawn(async move {
        sleep(Duration::from_secs(2)).await;
        let mut expected_id = sent_message_id;
        let mut last_err = String::new();
        for repair_attempt in 0..3 {
            let mut verified_id = None;
            for _ in 0..5 {
                let found = tokio::time::timeout(Duration::from_millis(2500), async {
                    match client.get_messages_by_id(&chat, &[expected_id]).await {
                        Ok(messages)
                            if messages.iter().any(|m| {
                                m.as_ref()
                                    .map(|msg| {
                                        msg.outgoing()
                                            && msg.id() == expected_id
                                            && payload.matches_message(msg)
                                    })
                                    .unwrap_or(false)
                            }) =>
                        {
                            Some(expected_id)
                        }
                        _ => None,
                    }
                })
                .await
                .ok()
                .flatten();
                if found.is_some() {
                    verified_id = found;
                    break;
                }
                sleep(Duration::from_secs(3)).await;
            }

            if let Some(mid) = verified_id {
                let actual_secs = Instant::now().duration_since(seen_at).as_secs();
                log_to_bot(
                    &state,
                    &rayzen_auto_reply_sent_after_retry_log(
                        kind,
                        &chat,
                        sender_id,
                        mid,
                        delay_secs,
                        actual_secs,
                    ),
                )
                .await;
                return;
            }

            if repair_attempt >= 2 {
                break;
            }

            let repaired = match &payload {
                AutoReplyDeliveryPayload::Text(text) => {
                    send_auto_reply_text_with_plain_fallback(
                        &client,
                        chat.id(),
                        &chat,
                        text,
                        reply_to,
                    )
                    .await
                }
                AutoReplyDeliveryPayload::Media { url, caption } => {
                    let media_result = tokio::time::timeout(
                        Duration::from_secs(AUTO_REPLY_SEND_TIMEOUT_SECS),
                        send_media_msg_result(
                            &client,
                            &chat,
                            url,
                            caption,
                            false,
                            false,
                            Some(reply_to),
                        ),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        Err(format!("timeout after {}s", AUTO_REPLY_SEND_TIMEOUT_SECS))
                    });
                    match media_result {
                        Ok(mid) => Ok(mid),
                        Err(media_err) => {
                            let fallback = send_auto_reply_text_with_plain_fallback(
                                &client,
                                chat.id(),
                                &chat,
                                caption,
                                reply_to,
                            )
                            .await;
                            fallback.map_err(|fallback_err| {
                                format!(
                                    "{}; verifier text fallback failed: {}",
                                    media_err, fallback_err
                                )
                            })
                        }
                    }
                }
            };

            match repaired {
                Ok(mid) => {
                    expected_id = mid;
                    sleep(Duration::from_secs(2)).await;
                }
                Err(err) => {
                    last_err = err;
                    if is_permanent_auto_reply_error(&last_err) {
                        break;
                    }
                    sleep(auto_reply_retry_delay(&last_err)).await;
                }
            }
        }

        let reason = if last_err.is_empty() {
            format!(
                "delivery verification failed after retry; expected message_id {}",
                expected_id
            )
        } else {
            format!(
                "delivery verification failed after retry; expected message_id {}; last repair error: {}",
                expected_id, last_err
            )
        };
        log_auto_reply_send_error(&state, kind, chat.id(), sender_id, &reason).await;
    });
}

async fn log_auto_reply_send_error(
    state: &BotState,
    kind: &str,
    chat_id: i64,
    user_id: u64,
    err: &str,
) {
    log::warn!(
        "grammers auto-reply {} failed user={} chat={} err={}",
        kind,
        user_id,
        chat_id,
        err
    );
    if let Some(reason) = auto_reply_access_loss_reason(err) {
        log_to_bot(
            state,
            &rayzen_auto_reply_access_lost_log(kind, chat_id, user_id, err, reason),
        )
        .await;
    } else {
        log_to_bot(
            state,
            &rayzen_auto_reply_error_log(kind, chat_id, user_id, err),
        )
        .await;
    }
}

async fn send_auto_reply_text_with_plain_fallback(
    client: &Client,
    chat_id: i64,
    chat: &Chat,
    text: &str,
    reply_to: i32,
) -> Result<i32, String> {
    let first = match tokio::time::timeout(
        Duration::from_secs(AUTO_REPLY_SEND_TIMEOUT_SECS),
        client.send_message(chat, html_msg(text).reply_to(Some(reply_to))),
    )
    .await
    {
        Ok(result) => result.map(|m| m.id()).map_err(|e| e.to_string()),
        Err(_) => Err(format!("timeout after {}s", AUTO_REPLY_SEND_TIMEOUT_SECS)),
    };
    if let Ok(id) = first {
        return Ok(id);
    }

    let first_err = first.err().unwrap_or_default();
    if !is_permanent_auto_reply_error(&first_err) {
        return Err(first_err);
    }

    if let Some(fresh_chat) = find_chat_by_id(client, chat_id).await {
        let fresh_reply = match tokio::time::timeout(
            Duration::from_secs(AUTO_REPLY_SEND_TIMEOUT_SECS),
            client.send_message(&fresh_chat, html_msg(text).reply_to(Some(reply_to))),
        )
        .await
        {
            Ok(result) => result.map(|m| m.id()).map_err(|e| e.to_string()),
            Err(_) => Err(format!("timeout after {}s", AUTO_REPLY_SEND_TIMEOUT_SECS)),
        };
        match fresh_reply {
            Ok(id) => return Ok(id),
            Err(err) if !is_permanent_auto_reply_error(&err) => return Err(err),
            Err(_) => {
                let fresh_plain = match tokio::time::timeout(
                    Duration::from_secs(AUTO_REPLY_SEND_TIMEOUT_SECS),
                    client.send_message(&fresh_chat, html_msg(text)),
                )
                .await
                {
                    Ok(result) => result.map(|m| m.id()).map_err(|e| e.to_string()),
                    Err(_) => Err(format!("timeout after {}s", AUTO_REPLY_SEND_TIMEOUT_SECS)),
                };
                if fresh_plain.is_ok() {
                    return fresh_plain;
                }
            }
        }
    }

    match tokio::time::timeout(
        Duration::from_secs(AUTO_REPLY_SEND_TIMEOUT_SECS),
        client.send_message(chat, html_msg(text)),
    )
    .await
    {
        Ok(result) => result.map(|m| m.id()).map_err(|e| e.to_string()),
        Err(_) => Err(format!("timeout after {}s", AUTO_REPLY_SEND_TIMEOUT_SECS)),
    }
}

fn spawn_text_auto_reply_worker(
    client: Client,
    chat: Chat,
    state: Arc<BotState>,
    sender_id: u64,
    chat_id: i64,
    cfg: crate::account::state::AutoReplyConfig,
    delay_secs: u64,
) {
    let rate_key = auto_reply_rate_key(sender_id, chat_id);
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
                .map(|v| *v.value())
                .unwrap_or_default();
            if reply_mid <= 0 {
                state.text_auto_reply_running.remove(&rate_key);
                return;
            }
            let Some(ct) = read_template_file(&state.base_path, &cfg.file_name).await else {
                log_auto_reply_send_error(
                    &state,
                    "text-template",
                    chat.id(),
                    sender_id,
                    "template file is missing or unreadable",
                )
                .await;
                state.text_auto_reply_running.remove(&rate_key);
                return;
            };
            let lines: Vec<String> = ct
                .lines()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            let Some(l) = get_random_choice(lines) else {
                log_auto_reply_send_error(
                    &state,
                    "text-template",
                    chat.id(),
                    sender_id,
                    "template file is empty",
                )
                .await;
                state.text_auto_reply_running.remove(&rate_key);
                return;
            };
            let msg_text = if cfg.shapka.is_empty() {
                escape(&l)
            } else {
                format!("{} {}", escape(&cfg.shapka), escape(&l))
            };
            let mut had_retry_error = false;
            let mut send_attempts = 0u32;
            let sent_message_id: i32;
            loop {
                let latest_reply = state
                    .last_auto_reply_message_id
                    .get(&rate_key)
                    .map(|v| *v.value())
                    .unwrap_or(reply_mid);
                let send_result = send_auto_reply_text_with_plain_fallback(
                    &client,
                    chat_id,
                    &chat,
                    &msg_text,
                    latest_reply,
                )
                .await
                .map(|id| id);
                if let Ok(id) = send_result {
                    sent_message_id = id;
                    break;
                }
                let err = send_result.err().unwrap_or_default();
                if !is_transient_auto_reply_error(&err) {
                    log_auto_reply_send_error(&state, "text", chat.id(), sender_id, &err).await;
                    state.text_auto_reply_running.remove(&rate_key);
                    return;
                }
                record_grammers_broken_send_failure(&err, "auto_reply_text", chat.id());
                send_attempts = send_attempts.saturating_add(1);
                if send_attempts >= AUTO_REPLY_MAX_SEND_ATTEMPTS {
                    log_auto_reply_send_error(
                        &state,
                        "text",
                        chat.id(),
                        sender_id,
                        &format!(
                            "stopped after {} attempts; last error: {}",
                            AUTO_REPLY_MAX_SEND_ATTEMPTS, err
                        ),
                    )
                    .await;
                    state.text_auto_reply_running.remove(&rate_key);
                    return;
                }
                if !had_retry_error {
                    had_retry_error = true;
                    log_to_bot(
                        &state,
                        &rayzen_auto_reply_retry_log("text", chat.id(), sender_id, &err),
                    )
                    .await;
                }
                sleep(auto_reply_retry_delay(&err)).await;
            }
            reset_grammers_broken_send_failures();
            state.last_reply_at.insert(rate_key.clone(), Instant::now());
            if had_retry_error {
                let seen_at = state
                    .last_auto_reply_seen_at
                    .get(&rate_key)
                    .map(|t| *t.value())
                    .unwrap_or_else(Instant::now);
                spawn_auto_reply_delivery_verifier(
                    client.clone(),
                    state.clone(),
                    chat.clone(),
                    "text",
                    sender_id,
                    sent_message_id,
                    reply_mid,
                    AutoReplyDeliveryPayload::Text(msg_text.clone()),
                    delay_secs,
                    seen_at,
                );
            }
            let latest_reply = state
                .last_auto_reply_message_id
                .get(&rate_key)
                .map(|v| *v.value())
                .unwrap_or(reply_mid);
            if latest_reply == reply_mid {
                state.text_auto_reply_running.remove(&rate_key);
                return;
            }
        }
    });
}

fn spawn_media_auto_reply_worker(
    client: Client,
    chat: Chat,
    state: Arc<BotState>,
    sender_id: u64,
    chat_id: i64,
    cfg: crate::account::state::AutoReplyConfig,
    delay_secs: u64,
) {
    let rate_key = auto_reply_rate_key(sender_id, chat_id);
    if state.media_auto_reply_running.contains_key(&rate_key) {
        return;
    }
    state.media_auto_reply_running.insert(rate_key.clone(), ());
    tokio::spawn(async move {
        let mut chat = chat;
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
                .map(|v| *v.value())
                .unwrap_or_default();
            if reply_mid <= 0
                || state
                    .handled_auto_reply_message_id
                    .get(&rate_key)
                    .map(|v| *v.value() == reply_mid)
                    .unwrap_or(false)
            {
                state.media_auto_reply_running.remove(&rate_key);
                return;
            }
            let Some(ct) = read_template_file(&state.base_path, &cfg.file_name).await else {
                log_auto_reply_send_error(
                    &state,
                    "media-template",
                    chat.id(),
                    sender_id,
                    "template file is missing or unreadable",
                )
                .await;
                state.media_auto_reply_running.remove(&rate_key);
                return;
            };
            let lines: Vec<String> = ct
                .lines()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            let Some(l) = get_random_choice(lines) else {
                log_auto_reply_send_error(
                    &state,
                    "media-template",
                    chat.id(),
                    sender_id,
                    "template file is empty",
                )
                .await;
                state.media_auto_reply_running.remove(&rate_key);
                return;
            };
            let Some(ref u) = cfg.media_url else {
                log_auto_reply_send_error(
                    &state,
                    "media-config",
                    chat.id(),
                    sender_id,
                    "media_url is empty",
                )
                .await;
                state.media_auto_reply_running.remove(&rate_key);
                return;
            };
            let caption = format!("{} {}", escape(&cfg.shapka), escape(&l));
            let mut had_retry_error = false;
            let mut send_attempts = 0u32;
            let sent_message_id: i32;
            loop {
                let latest_reply = state
                    .last_auto_reply_message_id
                    .get(&rate_key)
                    .map(|v| *v.value())
                    .unwrap_or(reply_mid);
                let send_result = match tokio::time::timeout(
                    Duration::from_secs(AUTO_REPLY_SEND_TIMEOUT_SECS),
                    send_media_msg_result(
                        &client,
                        &chat,
                        u,
                        &caption,
                        false,
                        false,
                        Some(latest_reply),
                    ),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(format!("timeout after {}s", AUTO_REPLY_SEND_TIMEOUT_SECS)),
                };
                match send_result {
                    Ok(mid) => {
                        reset_grammers_broken_send_failures();
                        sent_message_id = mid;
                        break;
                    }
                    Err(mut err) => {
                        record_grammers_broken_send_failure(&err, "auto_reply_media", chat.id());
                        send_attempts = send_attempts.saturating_add(1);
                        if send_attempts >= AUTO_REPLY_MAX_SEND_ATTEMPTS {
                            log_auto_reply_send_error(
                                &state,
                                "media",
                                chat.id(),
                                sender_id,
                                &format!(
                                    "stopped after {} attempts; last error: {}",
                                    AUTO_REPLY_MAX_SEND_ATTEMPTS, err
                                ),
                            )
                            .await;
                            state
                                .handled_auto_reply_message_id
                                .insert(rate_key.clone(), reply_mid);
                            state.media_auto_reply_running.remove(&rate_key);
                            return;
                        }
                        if !is_transient_auto_reply_error(&err) {
                            if let Some(fresh_chat) = find_chat_by_id(&client, chat_id).await {
                                match tokio::time::timeout(
                                    Duration::from_secs(AUTO_REPLY_SEND_TIMEOUT_SECS),
                                    send_media_msg_result(
                                        &client,
                                        &fresh_chat,
                                        u,
                                        &caption,
                                        false,
                                        false,
                                        Some(latest_reply),
                                    ),
                                )
                                .await
                                {
                                    Ok(Ok(mid)) => {
                                        chat = fresh_chat;
                                        sent_message_id = mid;
                                        break;
                                    }
                                    Ok(Err(fresh_err))
                                        if is_transient_auto_reply_error(&fresh_err) =>
                                    {
                                        chat = fresh_chat;
                                        err = fresh_err;
                                    }
                                    Ok(Err(fresh_err)) => {
                                        chat = fresh_chat;
                                        err = format!("{}; fresh media failed: {}", err, fresh_err);
                                    }
                                    Err(_) => {
                                        chat = fresh_chat;
                                        err = format!(
                                            "timeout after {}s",
                                            AUTO_REPLY_SEND_TIMEOUT_SECS
                                        );
                                    }
                                }
                                if is_transient_auto_reply_error(&err) {
                                    if !had_retry_error {
                                        had_retry_error = true;
                                        log_to_bot(
                                            &state,
                                            &rayzen_auto_reply_retry_log(
                                                "media",
                                                chat.id(),
                                                sender_id,
                                                &err,
                                            ),
                                        )
                                        .await;
                                    }
                                    sleep(auto_reply_retry_delay(&err)).await;
                                    continue;
                                }
                            }
                            let latest_reply = state
                                .last_auto_reply_message_id
                                .get(&rate_key)
                                .map(|v| *v.value())
                                .unwrap_or(reply_mid);
                            let fallback_result = send_auto_reply_text_with_plain_fallback(
                                &client,
                                chat_id,
                                &chat,
                                &caption,
                                latest_reply,
                            )
                            .await;
                            match fallback_result {
                                Ok(mid) => {
                                    sent_message_id = mid;
                                    break;
                                }
                                Err(fallback_err)
                                    if is_transient_auto_reply_error(&fallback_err) =>
                                {
                                    if !had_retry_error {
                                        had_retry_error = true;
                                        log_to_bot(
                                            &state,
                                            &rayzen_auto_reply_retry_log(
                                                "media-text-fallback",
                                                chat.id(),
                                                sender_id,
                                                &fallback_err,
                                            ),
                                        )
                                        .await;
                                    }
                                    sleep(auto_reply_retry_delay(&fallback_err)).await;
                                    continue;
                                }
                                Err(fallback_err) => {
                                    log_auto_reply_send_error(
                                        &state,
                                        "media",
                                        chat.id(),
                                        sender_id,
                                        &format!("{}; text fallback failed: {}", err, fallback_err),
                                    )
                                    .await;
                                    state
                                        .handled_auto_reply_message_id
                                        .insert(rate_key.clone(), reply_mid);
                                    state.media_auto_reply_running.remove(&rate_key);
                                    return;
                                }
                            }
                        }
                        if !had_retry_error {
                            had_retry_error = true;
                            log_to_bot(
                                &state,
                                &rayzen_auto_reply_retry_log("media", chat.id(), sender_id, &err),
                            )
                            .await;
                        }
                        sleep(auto_reply_retry_delay(&err)).await;
                    }
                }
            }
            reset_grammers_broken_send_failures();
            state
                .last_reply_at_media
                .insert(rate_key.clone(), Instant::now());
            state
                .handled_auto_reply_message_id
                .insert(rate_key.clone(), reply_mid);
            if had_retry_error {
                let seen_at = state
                    .last_auto_reply_seen_at
                    .get(&rate_key)
                    .map(|t| *t.value())
                    .unwrap_or_else(Instant::now);
                spawn_auto_reply_delivery_verifier(
                    client.clone(),
                    state.clone(),
                    chat.clone(),
                    "media",
                    sender_id,
                    sent_message_id,
                    reply_mid,
                    AutoReplyDeliveryPayload::Media {
                        url: u.clone(),
                        caption: caption.clone(),
                    },
                    delay_secs,
                    seen_at,
                );
            }
            let latest_reply = state
                .last_auto_reply_message_id
                .get(&rate_key)
                .map(|v| *v.value())
                .unwrap_or(reply_mid);
            if latest_reply == reply_mid {
                state.media_auto_reply_running.remove(&rate_key);
                return;
            }
        }
    });
}

fn rayzen_stop_log(cmd: &str, prefix: char, target_cid: i64) -> String {
    let p = prefix;
    match cmd {
        "offsr" => format!(
            "Команда <code>{}sp</code> остановлена.\nChat ID: <code>{}</code>",
            p, target_cid
        ),
        "tagoff" => format!(
            "Команда <code>{}tagoff</code> была выключена.\nChat id: <code>{}</code>",
            p, target_cid
        ),
        c => format!(
            "Команда <code>{}{}</code> была выключена.\nChat ID: <code>{}</code>",
            p, c, target_cid
        ),
    }
}

fn stop_success_text(cmd: &str, target_cid: i64) -> String {
    match cmd {
        "off" => format!("⛧ Спам выключен в чате с ID: <code>{}</code> ⛧", target_cid),
        "offp" => format!(
            "⛧ Спам с фото выключен в чате с ID: <code>{}</code> ⛧",
            target_cid
        ),
        "offv" => format!(
            "⛧ Спам с видео выключен в чате с ID: <code>{}</code> ⛧",
            target_cid
        ),
        "tagoff" => format!(
            "⛧ Остановил теггер в чате с ID: <code>{}</code> ⛧",
            target_cid
        ),
        "tagpoff" => format!(
            "⛧ Теггер с фото выключен в чате с ID: <code>{}</code> ⛧",
            target_cid
        ),
        "tagvoff" => format!(
            "⛧ Теггер с видео выключен в чате с ID: <code>{}</code> ⛧",
            target_cid
        ),
        "offsr" => format!("⛧ Спам в чате (<code>{}</code>) остановлен.", target_cid),
        "koff" => format!(
            "⛧ Календарь выключен в чате с ID: <code>{}</code> ⛧",
            target_cid
        ),
        "kpoff" => format!(
            "⛧ Календарь с фото выключен в чате с ID: <code>{}</code> ⛧",
            target_cid
        ),
        "kvoff" => format!(
            "⛧ Календарь с видео выключен в чате с ID: <code>{}</code> ⛧",
            target_cid
        ),
        _ => format!("⛧ Спам выключен в чате с ID: <code>{}</code> ⛧", target_cid),
    }
}

async fn resolve_self_dm_chat(client: &Client) -> Option<Chat> {
    let me = client.get_me().await.ok()?;
    let mut dialogs = client.iter_dialogs();
    while let Ok(Some(d)) = dialogs.next().await {
        if let Chat::User(u) = d.chat() {
            if u.id() == me.id() {
                return Some(d.chat().clone());
            }
        }
    }
    None
}

async fn handle_sp_forward_loop(
    client: Client,
    target_chat: Chat,
    state: Arc<BotState>,
    wait_ms: u64,
    file_name: String,
    shapka: String,
) {
    let me_chat = match resolve_self_dm_chat(&client).await {
        Some(c) => c,
        None => return,
    };
    let base = state.base_path.clone();
    let fname = file_name.clone();
    let target_cid = target_chat.id();
    let interval = Duration::from_millis(wait_ms.max(1));
    let mut next_tick = TokioInstant::now();
    loop {
        if !state.spam_tasks_sp.contains_key(&target_cid) {
            break;
        }
        let now = TokioInstant::now();
        if next_tick > now {
            sleep_until(next_tick).await;
        }
        next_tick = TokioInstant::now() + interval;

        if let Some(content) = read_template_file(&base, &fname).await {
            let lines: Vec<String> = content
                .lines()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            if let Some(l) = get_random_choice(lines) {
                let text = if shapka.is_empty() {
                    l.clone()
                } else {
                    format!("{} {}", shapka, l)
                };
                match client
                    .send_message(&me_chat, InputMessage::text(text))
                    .await
                {
                    Ok(sent) => match sent.forward_to(&target_chat).await {
                        Ok(_) => {
                            let ts = Local::now().format("%H:%M:%S.%d.%m.%Y").to_string();
                            let title = chat_title_preview(&target_chat);
                            let log_line = format!(
                                "<code>{}</code> — сообщение отправлено в чат <code>{}</code> | <code>{}</code> | Спам команда — [sr]",
                                ts, escape(&title), target_cid
                            );
                            log_to_bot(&state, &log_line).await;
                        }
                        Err(err) => {
                            let ts = Local::now().format("%H:%M:%S.%d.%m.%Y").to_string();
                            let title = chat_title_preview(&target_chat);
                            let log_line = rayzen_spam_send_error_log(
                                "sr",
                                &ts,
                                &title,
                                target_cid,
                                &err.to_string(),
                            );
                            log::warn!(
                                "grammers sr forward failed chat={} err={}",
                                target_cid,
                                err
                            );
                            log_to_bot(&state, &log_line).await;
                        }
                    },
                    Err(err) => {
                        let ts = Local::now().format("%H:%M:%S.%d.%m.%Y").to_string();
                        let title = chat_title_preview(&target_chat);
                        let log_line = rayzen_spam_send_error_log(
                            "sr",
                            &ts,
                            &title,
                            target_cid,
                            &err.to_string(),
                        );
                        log::warn!("grammers sr prepare failed chat={} err={}", target_cid, err);
                        log_to_bot(&state, &log_line).await;
                    }
                }
            }
        }
    }
}

fn stop_spam_task_for_chat(state: &BotState, task_map_type: &str, chat_id: i64) {
    let removed = match task_map_type {
        "ar" => state.spam_tasks_ar.remove(&chat_id),
        "arp" => state.spam_tasks_arp.remove(&chat_id),
        "arv" => state.spam_tasks_arv.remove(&chat_id),
        "tag" => state.spam_tasks_tag.remove(&chat_id),
        "tagp" => state.spam_tasks_tagp.remove(&chat_id),
        "tagv" => state.spam_tasks_tagv.remove(&chat_id),
        "sp" => state.spam_tasks_sp.remove(&chat_id),
        _ => None,
    };
    if let Some((_, handle)) = removed {
        handle.abort();
    }
    let canonical_removed = state
        .missions
        .remove(&mission_key(chat_id, task_map_type))
        .is_some();
    let legacy_removed = state
        .missions
        .remove(&legacy_mission_key(chat_id))
        .is_some();
    if canonical_removed || legacy_removed {
        state.save_persistence();
    }
}

fn rayzen_spam_access_lost_log(
    task_type: &str,
    ts: &str,
    chat_title: &str,
    chat_id: i64,
    reason: &str,
    err: &str,
) -> String {
    format!(
        "<code>{}</code> — спам остановлен: акк не может писать в чат <code>{}</code> | <code>{}</code> | Спам команда — [{}]\nStatus: <code>{}</code>\nПричина: <code>{}</code>",
        ts,
        escape(chat_title),
        chat_id,
        escape(task_type),
        escape(reason),
        escape(err)
    )
}

fn abort_all_spam_and_calendars(state: &BotState) {
    for e in state.spam_tasks_ar.iter() {
        e.value().abort();
    }
    state.spam_tasks_ar.clear();
    for e in state.spam_tasks_sp.iter() {
        e.value().abort();
    }
    state.spam_tasks_sp.clear();
    for e in state.spam_tasks_arp.iter() {
        e.value().abort();
    }
    state.spam_tasks_arp.clear();
    for e in state.spam_tasks_arv.iter() {
        e.value().abort();
    }
    state.spam_tasks_arv.clear();
    for e in state.spam_tasks_tag.iter() {
        e.value().abort();
    }
    state.spam_tasks_tag.clear();
    for e in state.spam_tasks_tagp.iter() {
        e.value().abort();
    }
    state.spam_tasks_tagp.clear();
    for e in state.spam_tasks_tagv.iter() {
        e.value().abort();
    }
    state.spam_tasks_tagv.clear();
    for e in state.calendar_tasks.iter() {
        e.value().abort();
    }
    state.calendar_tasks.clear();
    for e in state.calendar_p_tasks.iter() {
        e.value().abort();
    }
    state.calendar_p_tasks.clear();
    for e in state.calendar_v_tasks.iter() {
        e.value().abort();
    }
    state.calendar_v_tasks.clear();
}

fn calendar_task_active(state: &BotState, chat_id: i64, task_map_type: &str) -> bool {
    match task_map_type {
        "kal" => state.calendar_tasks.contains_key(&chat_id),
        "kalp" => state.calendar_p_tasks.contains_key(&chat_id),
        "kalv" => state.calendar_v_tasks.contains_key(&chat_id),
        _ => false,
    }
}

fn calendar_schedule_base_minute() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    ((now / 60) + 1) * 60
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn calendar_schedule_time(base_minute: u64, delay_minutes: u64, index: u64) -> SystemTime {
    let jitter = (rand::random::<u64>() % 59) + 1;
    UNIX_EPOCH
        + Duration::from_secs(
            base_minute
                .saturating_add(delay_minutes.saturating_mul(60).saturating_mul(index))
                .saturating_add(jitter),
        )
}

fn begin_calendar_cycle_state(state: &BotState, key: &str, generation: u64) {
    if let Some(mut task) = state.missions.get_mut(key) {
        task.scheduled_target_ids.clear();
        task.scheduled_origin_ids.clear();
        task.calendar_restart_due_at = None;
        task.calendar_cycle_started_at = Some(generation);
        task.calendar_watchdog_generation = Some(generation);
    }
    state.save_persistence();
}

fn mark_calendar_restart_echo_state(
    state: &BotState,
    key: &str,
    id: i32,
    due_at: u64,
    generation: u64,
) {
    if let Some(mut task) = state.missions.get_mut(key) {
        task.scheduled_origin_ids = vec![id];
        task.calendar_restart_due_at = Some(due_at);
        task.calendar_watchdog_generation = Some(generation);
    }
    state.save_persistence();
}

fn calendar_restart_echo(
    command_echo: &str,
    task_type: &str,
    chat_id: i64,
    delay_minutes: u64,
    file_name: &str,
    shapka: &str,
    media: &Option<(String, bool)>,
) -> String {
    let saved = command_echo.trim();
    if !saved.is_empty() {
        return saved.to_string();
    }

    let mut parts = vec![
        format!("!{}", task_type),
        chat_id.to_string(),
        delay_minutes.to_string(),
        file_name.to_string(),
    ];
    if matches!(task_type, "kalp" | "kalv") {
        if let Some((url, _)) = media {
            parts.push(url.clone());
        }
    }
    let shapka = shapka.trim();
    if !shapka.is_empty() {
        parts.push(shapka.to_string());
    }
    parts.join(" ")
}

fn replace_calendar_task(state: &BotState, chat_id: i64, task_type: &str, handle: JoinHandle<()>) {
    match task_type {
        "kal" => {
            if let Some((_, old)) = state.calendar_tasks.remove(&chat_id) {
                old.abort();
            }
            state.calendar_tasks.insert(chat_id, handle);
        }
        "kalp" => {
            if let Some((_, old)) = state.calendar_p_tasks.remove(&chat_id) {
                old.abort();
            }
            state.calendar_p_tasks.insert(chat_id, handle);
        }
        "kalv" => {
            if let Some((_, old)) = state.calendar_v_tasks.remove(&chat_id) {
                old.abort();
            }
            state.calendar_v_tasks.insert(chat_id, handle);
        }
        _ => {}
    }
}

async fn send_calendar_scheduled(
    client: &Client,
    chat: &Chat,
    lines: &[String],
    shapka: &str,
    media: &Option<(String, bool)>,
    when: SystemTime,
) -> Result<i32, String> {
    let Some(l) = get_random_choice(lines.to_vec()) else {
        return Ok(0);
    };
    let mut text = format!("{} {}", escape(shapka), escape(&l));
    if let Some((ref url, _)) = media {
        text = strip_specific_url(&text, url);
    }
    let im = match media {
        None => html_msg(&text).schedule_date(Some(when)),
        Some((url, is_v)) => {
            let mut m = html_msg(&text);
            m = if *is_v || media_url_as_document(url, false) {
                m.document_url(url.clone())
            } else if media_url_looks_like_photo(url) {
                m.photo_url(url.clone())
            } else {
                m.document_url(url.clone())
            };
            m.schedule_date(Some(when))
        }
    };
    client
        .send_message(chat, im)
        .await
        .map(|m| m.id())
        .map_err(|e| e.to_string())
}

async fn send_calendar_now(
    client: &Client,
    chat: &Chat,
    lines: &[String],
    shapka: &str,
    media: &Option<(String, bool)>,
) -> Result<(), String> {
    let Some(l) = get_random_choice(lines.to_vec()) else {
        return Ok(());
    };
    let mut text = format!("{} {}", escape(shapka), escape(&l));
    if let Some((ref url, _)) = media {
        text = strip_specific_url(&text, url);
    }
    match media {
        None => client
            .send_message(chat, html_msg(&text))
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        Some((url, is_v)) => send_media_msg_result(client, chat, url, &text, *is_v, false, None)
            .await
            .map(|_| ()),
    }
}

async fn notify_calendar_owner(_state: &BotState, msg: &str) {
    log::info!("{}", msg);
}

async fn schedule_calendar_restart_echo(
    client: &Client,
    origin_chat: &Chat,
    state: &BotState,
    mission_id: &str,
    base_minute: u64,
    delay_minutes: u64,
    task_type: &str,
    target_chat_id: i64,
    file_name: &str,
    shapka: &str,
    media: &Option<(String, bool)>,
    command_echo: &str,
    generation: u64,
    schedule_index: u64,
) -> Option<u64> {
    let echo = calendar_restart_echo(
        command_echo,
        task_type,
        target_chat_id,
        delay_minutes,
        file_name,
        shapka,
        media,
    );
    if echo.trim().is_empty() {
        log::warn!(
            "calendar restart echo empty task={} target_chat={} origin_chat={}",
            task_type,
            target_chat_id,
            origin_chat.id()
        );
        return None;
    }

    let when = calendar_schedule_time(base_minute, delay_minutes, schedule_index);
    let due_at = when
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    match client
        .send_message(
            origin_chat,
            InputMessage::text(echo.clone()).schedule_date(Some(when)),
        )
        .await
    {
        Ok(m) => {
            mark_calendar_restart_echo_state(state, mission_id, m.id(), due_at, generation);
            log::info!(
                "calendar restart echo scheduled task={} target_chat={} origin_chat={} message_id={}",
                task_type,
                target_chat_id,
                origin_chat.id(),
                m.id()
            );
            Some(due_at)
        }
        Err(first_err) => {
            if let Some(fresh_origin) = find_chat_by_id(client, origin_chat.id()).await {
                match client
                    .send_message(
                        &fresh_origin,
                        InputMessage::text(echo.clone()).schedule_date(Some(when)),
                    )
                    .await
                {
                    Ok(m) => {
                        mark_calendar_restart_echo_state(
                            state,
                            mission_id,
                            m.id(),
                            due_at,
                            generation,
                        );
                        log::info!(
                            "calendar restart echo scheduled after origin refresh task={} target_chat={} origin_chat={} message_id={}",
                            task_type,
                            target_chat_id,
                            fresh_origin.id(),
                            m.id()
                        );
                        return Some(due_at);
                    }
                    Err(second_err) => {
                        log::warn!(
                            "calendar restart echo failed after origin refresh task={} target_chat={} origin_chat={} err={} first_err={}",
                            task_type,
                            target_chat_id,
                            fresh_origin.id(),
                            second_err,
                            first_err
                        );
                    }
                }
            } else {
                log::warn!(
                    "calendar restart echo failed and origin not found task={} target_chat={} origin_chat={} err={}",
                    task_type,
                    target_chat_id,
                    origin_chat.id(),
                    first_err
                );
            }
            None
        }
    }
}

fn remember_calendar_scheduled_id(state: &BotState, key: &str, id: i32, origin: bool) {
    if let Some(mut task) = state.missions.get_mut(key) {
        let ids = if origin {
            &mut task.scheduled_origin_ids
        } else {
            &mut task.scheduled_target_ids
        };
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    state.save_persistence();
}

async fn handle_kal_loop(
    client: Client,
    target_chat: Chat,
    origin_chat: Chat,
    state: Arc<BotState>,
    delay_minutes: u64,
    file_name: String,
    shapka: String,
    media: Option<(String, bool)>,
    task_map_type: String,
    command_echo: String,
) {
    let orig = delay_minutes.clamp(GRAMMERS_CALENDAR_MIN_DELAY_MINUTES, 1440);
    let chat_id = target_chat.id();
    let mission_id = mission_key(chat_id, &task_map_type);
    while calendar_task_active(&state, chat_id, &task_map_type) {
        let generation = unix_now_secs();
        begin_calendar_cycle_state(&state, &mission_id, generation);
        let content = read_template_file(&state.base_path, &file_name)
            .await
            .unwrap_or_default();
        let lines: Vec<String> = content
            .lines()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        if lines.is_empty() {
            notify_calendar_owner(&state, "⛧ Календарь: файл шаблона пуст или не найден ⛧").await;
            sleep(Duration::from_secs(30)).await;
            continue;
        }

        if let Err(err) = send_calendar_now(&client, &target_chat, &lines, &shapka, &media).await {
            log::warn!(
                "grammers calendar immediate send failed task={} chat={} err={}",
                task_map_type,
                chat_id,
                err
            );
        }
        sleep(Duration::from_millis(180)).await;

        let base_minute = calendar_schedule_base_minute();
        for n in 1u64..=99u64 {
            if !calendar_task_active(&state, chat_id, &task_map_type) {
                break;
            }
            let when = calendar_schedule_time(base_minute, orig, n);
            match send_calendar_scheduled(&client, &target_chat, &lines, &shapka, &media, when)
                .await
            {
                Ok(id) if id > 0 => remember_calendar_scheduled_id(&state, &mission_id, id, false),
                Ok(_) => {}
                Err(err) => {
                    log::warn!(
                        "grammers calendar scheduled send failed task={} chat={} err={}",
                        task_map_type,
                        chat_id,
                        err
                    );
                    if err.to_lowercase().contains("schedule_too_much") {
                        let _ = schedule_calendar_restart_echo(
                            &client,
                            &origin_chat,
                            &state,
                            &mission_id,
                            base_minute,
                            orig,
                            &task_map_type,
                            chat_id,
                            &file_name,
                            &shapka,
                            &media,
                            &command_echo,
                            generation,
                            100,
                        )
                        .await;
                        notify_calendar_owner(
                            &state,
                            &format!(
                                "⛧ Календарь <code>{}</code> остановлен: в чате <code>{}</code> слишком много отложенных сообщений ⛧",
                                escape(&task_map_type),
                                chat_id
                            ),
                        )
                        .await;
                        match task_map_type.as_str() {
                            "kal" => {
                                state.calendar_tasks.remove(&chat_id);
                            }
                            "kalp" => {
                                state.calendar_p_tasks.remove(&chat_id);
                            }
                            "kalv" => {
                                state.calendar_v_tasks.remove(&chat_id);
                            }
                            _ => {}
                        }
                        state.missions.remove(&mission_id);
                        state.save_persistence();
                        break;
                    }
                }
            }
            sleep(Duration::from_millis(180)).await;
        }

        if !calendar_task_active(&state, chat_id, &task_map_type) {
            break;
        }

        if calendar_task_active(&state, chat_id, &task_map_type) {
            let _ = schedule_calendar_restart_echo(
                &client,
                &origin_chat,
                &state,
                &mission_id,
                base_minute,
                orig,
                &task_map_type,
                chat_id,
                &file_name,
                &shapka,
                &media,
                &command_echo,
                generation,
                100,
            )
            .await;
        }

        let cycle_wait = orig
            .saturating_mul(60)
            .saturating_mul(100)
            .saturating_add(60);
        sleep(Duration::from_secs(cycle_wait)).await;
    }

    match task_map_type.as_str() {
        "kal" => {
            state.calendar_tasks.remove(&chat_id);
        }
        "kalp" => {
            state.calendar_p_tasks.remove(&chat_id);
        }
        "kalv" => {
            state.calendar_v_tasks.remove(&chat_id);
        }
        _ => {}
    }
    let mkey = mission_key(chat_id, &task_map_type);
    state.missions.remove(&mkey);
    state.save_persistence();
}

async fn handle_calendar_watchdog(
    client: Client,
    target_chat: Chat,
    origin_chat: Chat,
    state: Arc<BotState>,
    delay_minutes: u64,
    file_name: String,
    shapka: String,
    media: Option<(String, bool)>,
    task_map_type: String,
    command_echo: String,
    generation: u64,
    due_at: u64,
) {
    let orig = delay_minutes.clamp(GRAMMERS_CALENDAR_MIN_DELAY_MINUTES, 1440);
    let chat_id = target_chat.id();
    let mission_id = mission_key(chat_id, &task_map_type);
    let mut watched_due_at = due_at;

    loop {
        if !calendar_task_active(&state, chat_id, &task_map_type) {
            break;
        }

        let wake_at = watched_due_at.saturating_add(150);
        let now = unix_now_secs();
        if wake_at > now {
            sleep(Duration::from_secs(wake_at - now)).await;
        }

        if !calendar_task_active(&state, chat_id, &task_map_type) {
            break;
        }

        let Some(task) = state.missions.get(&mission_id).map(|m| m.value().clone()) else {
            break;
        };
        if task.calendar_watchdog_generation != Some(generation)
            || task.calendar_restart_due_at != Some(watched_due_at)
            || task.scheduled_origin_ids.is_empty()
        {
            log::info!(
                "calendar watchdog: echo fired/rebound detected task={} target_chat={} origin_chat={}",
                task_map_type,
                chat_id,
                origin_chat.id()
            );
            break;
        }

        if let Some(scan) =
            scan_scheduled_calendar_echoes_in_chat(&client, &origin_chat, &task_map_type, chat_id)
                .await
        {
            let now = unix_now_secs();
            if !scan.ids.is_empty() {
                let keep_id = scan.ids.iter().copied().max().unwrap_or_default();
                let keep_ids = vec![keep_id];
                let remove_ids: Vec<i32> = scan
                    .ids
                    .iter()
                    .copied()
                    .filter(|id| *id != keep_id)
                    .collect();
                if !remove_ids.is_empty() {
                    delete_scheduled_ids_in_chat(&client, &origin_chat, &remove_ids).await;
                    log::info!(
                        "calendar watchdog: pruned duplicate restart echoes task={} target_chat={} origin_chat={} removed={}",
                        task_map_type,
                        chat_id,
                        origin_chat.id(),
                        remove_ids.len()
                    );
                }
                if let Some(scan_due) = scan.due_at {
                    if scan_due > now {
                        if let Some(mut task) = state.missions.get_mut(&mission_id) {
                            task.scheduled_origin_ids = keep_ids;
                            task.calendar_restart_due_at = Some(scan_due);
                            task.calendar_watchdog_generation = Some(generation);
                        }
                        state.save_persistence();
                        watched_due_at = scan_due;
                        log::info!(
                            "calendar watchdog: pending echo exists task={} target_chat={} origin_chat={} due_at={}",
                            task_map_type,
                            chat_id,
                            origin_chat.id(),
                            scan_due
                        );
                        continue;
                    }
                }
            }
        } else {
            log::info!(
                "calendar watchdog: pending echo exists task={} target_chat={} origin_chat={} status=history-unavailable",
                task_map_type,
                chat_id,
                origin_chat.id()
            );
            watched_due_at = unix_now_secs().saturating_add(orig.saturating_mul(60).max(300));
            if let Some(mut task) = state.missions.get_mut(&mission_id) {
                task.calendar_restart_due_at = Some(watched_due_at);
                task.calendar_watchdog_generation = Some(generation);
            }
            state.save_persistence();
            continue;
        }

        let target_payloads =
            scheduled_calendar_payload_count_in_chat(&client, &target_chat, &task_map_type)
                .await
                .unwrap_or(0);
        if target_payloads > 0 {
            let base_minute = calendar_schedule_base_minute();
            if let Some(new_due) = schedule_calendar_restart_echo(
                &client,
                &origin_chat,
                &state,
                &mission_id,
                base_minute,
                orig,
                &task_map_type,
                chat_id,
                &file_name,
                &shapka,
                &media,
                &command_echo,
                generation,
                1,
            )
            .await
            {
                watched_due_at = new_due;
                log::info!(
                    "calendar watchdog: echo missing, repaired task={} target_chat={} origin_chat={} mode=echo-only due_at={}",
                    task_map_type,
                    chat_id,
                    origin_chat.id(),
                    new_due
                );
                continue;
            }
        }

        log::info!(
            "calendar watchdog: echo missing, repaired task={} target_chat={} origin_chat={} mode=full-cycle",
            task_map_type,
            chat_id,
            origin_chat.id()
        );
        handle_kal_loop(
            client,
            target_chat,
            origin_chat,
            state,
            orig,
            file_name,
            shapka,
            media,
            task_map_type,
            command_echo,
        )
        .await;
        break;
    }
}

pub async fn run_bot(
    client: Client,
    state: Arc<BotState>,
    is_token_bot: bool,
    api_id: i32,
    api_hash: String,
    session_name: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if api_id == 0 || api_hash.is_empty() {
        log::error!(
            "CRITICAL: API ID (={}) or API HASH is invalid for {}",
            api_id,
            state.bot_username
        );
        return Ok(());
    }

    let session_file = if is_token_bot {
        format!(
            "{}/rayzen_bot_token.session",
            state.base_path.trim_end_matches('/')
        )
    } else {
        match session_name {
            Some(ref s_name) => format!(
                "{}/sessions/{}.session",
                state.base_path.trim_end_matches('/'),
                s_name
            ),
            None => format!(
                "{}/sessions/rayzen_bot.session",
                state.base_path.trim_end_matches('/')
            ),
        }
    };

    let log_name = session_name.as_deref().unwrap_or(if is_token_bot {
        "Token Bot"
    } else {
        "rayzen_bot"
    });
    let authorized = match client.is_authorized().await {
        Ok(v) => v,
        Err(e) => {
            let s = e.to_string();
            if session_cleanup::err_indicates_dead_session(&s) {
                let _ =
                    session_cleanup::remove_invalid_session_file(&session_file, "is_authorized");
            }
            return Err(e.into());
        }
    };
    if !authorized {
        if is_token_bot {
            if !state.bot_token.is_empty() {
                log::info!("Authorizing Token Bot using provided token...");
                match client.bot_sign_in(&state.bot_token).await {
                    Ok(_) => {
                        client.session().save_to_file(&session_file)?;
                        log::info!("Token Bot authorized successfully!");
                    }
                    Err(e) => {
                        log::error!("Token Bot authorization failed: {}", e);
                        return Err(e.into());
                    }
                }
            } else {
                log::error!("Token Bot requires a token, but it's missing in state!");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "token bot requires token but it is missing",
                )
                .into());
            }
        } else {
            println!();
            println!("╔══════════════════════════════════════════════════════════════════╗");
            println!("║  Вход в акк Telegram (как в Rayzen / Telethon)               ║");
            println!("╠══════════════════════════════════════════════════════════════════╣");
            println!("║  1) Введи номер с кодом страны, например +79991234567            ║");
            println!("║  2) В Telegram придёт код — введи его в консоль                   ║");
            println!("║  3) При 2FA — пароль облака Telegram                             ║");
            println!("╚══════════════════════════════════════════════════════════════════╝");
            println!("  Файл сессии после входа: {}", session_file);
            println!();

            if !std::io::stdin().is_terminal() {
                log::error!(
                    "Акк {}: нужен интерактивный вход, но stdin не TTY (systemd). Пропускаю.",
                    log_name
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "account {} requires interactive login but stdin is not TTY",
                        log_name
                    ),
                )
                .into());
            }
            let phone = prompt(&format!("== [{}] Номер телефона (с +): ", log_name)).await?;
            println!("Отправляю запрос кода в Telegram...");
            let token = client.request_login_code(&phone).await?;
            let code = prompt("Код из Telegram (или звонок): ").await?;
            let mut sign_in_res = client.sign_in(&token, &code).await;

            if let Err(ref e) = sign_in_res {
                let err_msg = e.to_string().to_lowercase();
                if err_msg.contains("password") || err_msg.contains("2fa") {
                    println!("== Облачный пароль (2FA) для [{}] ==", log_name);
                    let pwd = prompt("Пароль двухфакторной аутентификации: ").await?;

                    if let Err(grammers_client::SignInError::PasswordRequired(pwd_token)) =
                        sign_in_res
                    {
                        sign_in_res = client.check_password(pwd_token, pwd.trim()).await;
                    } else {
                        println!("== [DEBUG] 2FA String matched but Enum variant did not! == \nError was: {}", err_msg);
                    }
                }
            }

            match sign_in_res {
                Ok(_) => {
                    println!("[{}] Вход выполнен успешно!", log_name);
                }
                Err(e) => {
                    println!("[{}] ошибка входа: {}", log_name, e);
                    return Err(e.into());
                }
            }
            if let Some(parent) = std::path::Path::new(&session_file).parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            client.session().save_to_file(&session_file)?;
            println!();
            println!(">>> Сессия записана. При следующем запуске вход по файлу, номер не спросит.");
            println!(">>> Путь: {}", session_file);
            println!();
        }
    }

    let me = match client.get_me().await {
        Ok(m) => m,
        Err(e) => {
            let s = e.to_string();
            if session_cleanup::err_indicates_dead_session(&s) {
                let _ = session_cleanup::remove_invalid_session_file(&session_file, "get_me");
            }
            return Err(e.into());
        }
    };
    let self_id = me.id() as u64;
    log::info!(
        "Bot instance {} is online as {}!",
        state.bot_username,
        me.full_name()
    );

    let missions_to_restore: Vec<_> = state.missions.iter().map(|m| m.value().clone()).collect();
    let mut dialog_cache: HashMap<i64, Chat> = HashMap::new();
    let mut dialog_last_messages: HashMap<i64, Message> = HashMap::new();
    let needs_dialog_cache = !missions_to_restore.is_empty()
        || !state.check1_tasks.is_empty()
        || !state.auto_replies_text.is_empty()
        || !state.auto_replies_media.is_empty()
        || !state.global_auto_replies_text.is_empty()
        || !state.global_auto_replies_media.is_empty();
    if needs_dialog_cache {
        let mut dialogs = client.iter_dialogs();
        while let Ok(Some(dialog)) = dialogs.next().await {
            remember_chat_username(&state, dialog.chat());
            if let Chat::User(user) = dialog.chat() {
                remember_user_username(&state, user.id() as u64, user.username());
            }
            dialog_cache.insert(dialog.chat().id(), dialog.chat().clone());
            if let Some(last) = dialog.last_message.clone() {
                if last.outgoing() {
                    state
                        .last_outgoing_message_id_by_chat
                        .insert(dialog.chat().id(), last.id());
                }
                dialog_last_messages.insert(dialog.chat().id(), last);
            }
        }
    }
    let mut restored_count: usize = 0;
    for m in missions_to_restore {
        let chat_id = m.chat_id;
        let task_type = m.task_type.as_str();

        if task_type == "kal" || task_type == "kalp" || task_type == "kalv" {
            let target = match dialog_cache.get(&chat_id).cloned() {
                Some(c) => c,
                None => continue,
            };
            let origin_chat = if let Some(mid) = m.mirror_chat_id {
                dialog_cache
                    .get(&mid)
                    .cloned()
                    .unwrap_or_else(|| target.clone())
            } else {
                target.clone()
            };
            let delay_minutes = m.delay;
            let file_name = m.file_name.clone();
            let shapka = m.shapka.clone();
            let media = m.media_url.clone().map(|u| (u, m.is_video));
            let echo = m.kal_command_echo.clone().unwrap_or_default();
            let mission_id = mission_key(chat_id, task_type);
            let generation = m
                .calendar_watchdog_generation
                .or(m.calendar_cycle_started_at)
                .unwrap_or_else(unix_now_secs);
            let now = unix_now_secs();

            if let Some(due_at) = m
                .calendar_restart_due_at
                .filter(|due| *due > now && !m.scheduled_origin_ids.is_empty())
            {
                if let Some(removed) = prune_scheduled_calendar_echoes_in_chat(
                    &client,
                    &origin_chat,
                    task_type,
                    chat_id,
                    &m.scheduled_origin_ids,
                )
                .await
                {
                    if removed > 0 {
                        log::info!(
                            "calendar watchdog: pruned duplicate restart echoes task={} target_chat={} origin_chat={} removed={}",
                            task_type,
                            chat_id,
                            origin_chat.id(),
                            removed
                        );
                    }
                }
                if let Some(mut task) = state.missions.get_mut(&mission_id) {
                    task.calendar_restart_due_at = Some(due_at);
                    task.calendar_watchdog_generation = Some(generation);
                }
                state.save_persistence();
                let c_sub = client.clone();
                let st_sub = state.clone();
                let tm = task_type.to_string();
                let h = tokio::spawn(async move {
                    log::info!(
                        "calendar watchdog: pending echo exists task={} target_chat={} origin_chat={} due_at={}",
                        tm,
                        chat_id,
                        origin_chat.id(),
                        due_at
                    );
                    handle_calendar_watchdog(
                        c_sub,
                        target,
                        origin_chat,
                        st_sub,
                        delay_minutes,
                        file_name,
                        shapka,
                        media,
                        tm,
                        echo,
                        generation,
                        due_at,
                    )
                    .await;
                });
                replace_calendar_task(&state, chat_id, task_type, h);
                restored_count += 1;
                continue;
            }

            let echo_scan =
                scan_scheduled_calendar_echoes_in_chat(&client, &origin_chat, task_type, chat_id)
                    .await;
            if let Some(scan) = echo_scan {
                if !scan.ids.is_empty() {
                    let keep_id = scan.ids.iter().copied().max().unwrap_or_default();
                    let keep_ids = vec![keep_id];
                    let remove_ids: Vec<i32> = scan
                        .ids
                        .iter()
                        .copied()
                        .filter(|id| *id != keep_id)
                        .collect();
                    if !remove_ids.is_empty() {
                        delete_scheduled_ids_in_chat(&client, &origin_chat, &remove_ids).await;
                        log::info!(
                            "calendar watchdog: pruned duplicate restart echoes task={} target_chat={} origin_chat={} removed={}",
                            task_type,
                            chat_id,
                            origin_chat.id(),
                            remove_ids.len()
                        );
                    }
                    let due_at = scan.due_at.unwrap_or_else(|| {
                        now.saturating_add(delay_minutes.saturating_mul(60).max(300))
                    });
                    if let Some(mut task) = state.missions.get_mut(&mission_id) {
                        task.scheduled_origin_ids = keep_ids;
                        task.calendar_restart_due_at = Some(due_at);
                        task.calendar_watchdog_generation = Some(generation);
                    }
                    state.save_persistence();
                    let c_sub = client.clone();
                    let st_sub = state.clone();
                    let tm = task_type.to_string();
                    let h = tokio::spawn(async move {
                        log::info!(
                            "calendar watchdog: pending echo exists task={} target_chat={} origin_chat={} due_at={}",
                            tm,
                            chat_id,
                            origin_chat.id(),
                            due_at
                        );
                        handle_calendar_watchdog(
                            c_sub,
                            target,
                            origin_chat,
                            st_sub,
                            delay_minutes,
                            file_name,
                            shapka,
                            media,
                            tm,
                            echo,
                            generation,
                            due_at,
                        )
                        .await;
                    });
                    replace_calendar_task(&state, chat_id, task_type, h);
                    restored_count += 1;
                    continue;
                }
            } else if !m.scheduled_origin_ids.is_empty() {
                let due_at = m.calendar_restart_due_at.unwrap_or_else(|| {
                    now.saturating_add(delay_minutes.saturating_mul(60).max(300))
                });
                if let Some(mut task) = state.missions.get_mut(&mission_id) {
                    task.calendar_restart_due_at = Some(due_at);
                    task.calendar_watchdog_generation = Some(generation);
                }
                state.save_persistence();
                let c_sub = client.clone();
                let st_sub = state.clone();
                let tm = task_type.to_string();
                let h = tokio::spawn(async move {
                    log::info!(
                        "calendar watchdog: pending echo exists task={} target_chat={} origin_chat={} due_at={} status=history-unavailable",
                        tm,
                        chat_id,
                        origin_chat.id(),
                        due_at
                    );
                    handle_calendar_watchdog(
                        c_sub,
                        target,
                        origin_chat,
                        st_sub,
                        delay_minutes,
                        file_name,
                        shapka,
                        media,
                        tm,
                        echo,
                        generation,
                        due_at,
                    )
                    .await;
                });
                replace_calendar_task(&state, chat_id, task_type, h);
                restored_count += 1;
                continue;
            }

            let target_payload_count =
                scheduled_calendar_payload_count_in_chat(&client, &target, task_type)
                    .await
                    .unwrap_or_else(|| m.scheduled_target_ids.len());
            if target_payload_count > 0 {
                let base_minute = calendar_schedule_base_minute();
                if let Some(due_at) = schedule_calendar_restart_echo(
                    &client,
                    &origin_chat,
                    &state,
                    &mission_id,
                    base_minute,
                    delay_minutes.clamp(GRAMMERS_CALENDAR_MIN_DELAY_MINUTES, 1440),
                    task_type,
                    chat_id,
                    &file_name,
                    &shapka,
                    &media,
                    &echo,
                    generation,
                    1,
                )
                .await
                {
                    let c_sub = client.clone();
                    let st_sub = state.clone();
                    let tm = task_type.to_string();
                    let h = tokio::spawn(async move {
                        log::info!(
                            "calendar watchdog: echo missing, repaired task={} target_chat={} origin_chat={} mode=startup-echo-only due_at={}",
                            tm,
                            chat_id,
                            origin_chat.id(),
                            due_at
                        );
                        handle_calendar_watchdog(
                            c_sub,
                            target,
                            origin_chat,
                            st_sub,
                            delay_minutes,
                            file_name,
                            shapka,
                            media,
                            tm,
                            echo,
                            generation,
                            due_at,
                        )
                        .await;
                    });
                    replace_calendar_task(&state, chat_id, task_type, h);
                    restored_count += 1;
                    continue;
                }
            }

            let c_sub = client.clone();
            let st_sub = state.clone();
            let tm = task_type.to_string();
            let h = tokio::spawn(async move {
                handle_kal_loop(
                    c_sub,
                    target,
                    origin_chat,
                    st_sub,
                    delay_minutes,
                    file_name,
                    shapka,
                    media,
                    tm,
                    echo,
                )
                .await;
            });
            replace_calendar_task(&state, chat_id, task_type, h);
            restored_count += 1;
            continue;
        }

        let chat = match dialog_cache.get(&chat_id).cloned() {
            Some(c) => c,
            None => continue,
        };
        let c_sub = client.clone();
        let st_sub = state.clone();
        let cmd_str = m.task_type.clone();
        let m_clone = m.clone();
        let wait_ms = m_clone.delay.saturating_mul(1000).max(1);
        let h = if m.task_type == "sp" {
            tokio::spawn(async move {
                handle_sp_forward_loop(
                    c_sub,
                    chat,
                    st_sub,
                    wait_ms,
                    m_clone.file_name,
                    m_clone.shapka,
                )
                .await;
            })
        } else {
            tokio::spawn(async move {
                handle_spam_loop(
                    c_sub,
                    chat,
                    st_sub,
                    wait_ms,
                    m_clone.file_name,
                    m_clone.shapka,
                    m_clone.user_id,
                    m_clone.media_url,
                    m_clone.is_video,
                    cmd_str,
                )
                .await;
            })
        };
        match m.task_type.as_str() {
            "ar" => {
                state.spam_tasks_ar.insert(chat_id, h);
            }
            "sp" => {
                state.spam_tasks_sp.insert(chat_id, h);
            }
            "arp" => {
                state.spam_tasks_arp.insert(chat_id, h);
            }
            "arv" => {
                state.spam_tasks_arv.insert(chat_id, h);
            }
            "tag" => {
                state.spam_tasks_tag.insert(chat_id, h);
            }
            "tagp" => {
                state.spam_tasks_tagp.insert(chat_id, h);
            }
            "tagv" => {
                state.spam_tasks_tagv.insert(chat_id, h);
            }
            _ => {}
        }
        restored_count += 1;
    }

    if !is_token_bot {
        let c = client.clone();
        let s = state.clone();
        let cache = dialog_cache.clone();
        tokio::spawn(async move {
            let synced = resync_check1_tasks_on_start(&c, &s, &cache).await;
            if synced > 0 {
                log::info!(
                    "check1: resync on start touched {} tasks via startup dialog cache",
                    synced
                );
            }
        });
        spawn_auto_reply_restart_replay(
            client.clone(),
            state.clone(),
            dialog_cache.clone(),
            dialog_last_messages.clone(),
        );
        spawn_media_auto_reply_history_poller(client.clone(), state.clone(), dialog_cache.clone());
    }
    announce_account_startup(&client, &state, restored_count).await;

    let check_state = state.clone();
    let check_client = client.clone();
    tokio::spawn(async move {
        loop {
            let now = get_now();
            struct Check1Fired {
                key: String,
                chat_id: i64,
                chat_username: Option<String>,
                tme_internal: Option<i64>,
                user_id: u64,
                delay: u64,
                last: u64,
                last_message_id: Option<i32>,
            }
            let mut fired: Vec<Check1Fired> = Vec::new();
            for mut entry in check_state.check1_tasks.iter_mut() {
                let key = entry.key().clone();
                let task = entry.value_mut();
                let Some(last) = task.last_time else {
                    continue;
                };
                if task.silence_notified {
                    continue;
                }
                if now < last.saturating_add(task.delay) {
                    continue;
                }
                task.silence_notified = true;
                fired.push(Check1Fired {
                    key,
                    chat_id: task.chat_id,
                    chat_username: task.chat_username.clone(),
                    tme_internal: task.tme_internal,
                    user_id: task.user_id,
                    delay: task.delay,
                    last,
                    last_message_id: task.last_message_id,
                });
            }
            if !fired.is_empty() {
                let me_chat = resolve_self_dm_chat(&check_client).await;
                let mut log_chunks: Vec<String> = Vec::new();
                for f in &fired {
                    let silence = now.saturating_sub(f.last);
                    let body = check1_silence_alert_html(
                        f.chat_username.clone(),
                        f.tme_internal,
                        f.chat_id,
                        f.user_id,
                        f.delay,
                        silence,
                        f.last_message_id,
                    );
                    if let Some(ref c) = me_chat {
                        let _ = check_client.send_message(c, html_msg(&body)).await;
                    }
                    let push = format!(
                        "check1: user {} молчит {} сек (порог {} сек), chat {}, последнее unix {}",
                        f.user_id, silence, f.delay, f.chat_id, f.last
                    );
                    crate::ntfy::send_check1_alert(
                        &check_state.base_path,
                        check_state
                            .settings
                            .get("ntfy_topic")
                            .map(|v| v.value().clone()),
                        &push,
                    )
                    .await;
                    log_chunks.push(body);
                }
                let mut removed_any = false;
                for f in &fired {
                    if check_state.check1_tasks.remove(&f.key).is_some() {
                        removed_any = true;
                    }
                }
                if removed_any {
                    check_state.save_persistence();
                }

                let text = log_chunks.join("\n\n");
                log_to_bot(&check_state, &text).await;
            }
            sleep(Duration::from_secs(1)).await;
        }
    });

    let prefix = if is_token_bot { '/' } else { '!' };
    let mut health_tick =
        tokio::time::interval(Duration::from_secs(HC_SECS));
    health_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    health_tick.tick().await;
    let mut health_failures = 0u32;

    loop {
        let update = tokio::select! {
            upd = client.next_update() => {
                match upd {
                    Ok(u) => u,
                    Err(e) => {
                        log::error!("grammers next_update failed: {}", e);
                        return Err(Box::new(e));
                    }
                }
            }
            _ = health_tick.tick() => {
                let health_result = tokio::time::timeout(
                    Duration::from_secs(GRAMMERS_HEALTHCHECK_TIMEOUT_SECS),
                    client.is_authorized(),
                )
                .await;
                match health_result {
                    Ok(Ok(true)) => {
                        if health_failures > 0 {
                            log::info!(
                                "Акк {}: MTProto healthcheck recovered after {} failure(s)",
                                log_name,
                                health_failures
                            );
                        }
                        health_failures = 0;
                        reset_grammers_broken_send_failures();
                        continue;
                    }
                    Ok(Ok(false)) => {
                        let reason = format!("session_revoked MTProto healthcheck unauthorized ({})", log_name);
                        let text = format!(
                            "Акк <code>{}</code>: проверка MTProto показала невалидную сессию, удаляю слот.",
                            escape(log_name)
                        );
                        log_to_bot(&state, &text).await;
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, reason).into());
                    }
                    Ok(Err(e)) => {
                        let s = format!("{:#}", e);
                        if session_cleanup::err_indicates_session_gone(&s) {
                            let text = format!(
                                "Акк <code>{}</code>: проверка MTProto вернула фатальную ошибку, удаляю слот.",
                                escape(log_name)
                            );
                            log_to_bot(&state, &text).await;
                            return Err(Box::new(e));
                        }
                        health_failures = health_failures.saturating_add(1);
                        log::warn!(
                            "Акк {}: MTProto healthcheck transient error {}/{}: {}",
                            log_name,
                            health_failures,
                            GRAMMERS_HEALTHCHECK_FAILURE_LIMIT,
                            s
                        );
                        if health_failures >= GRAMMERS_HEALTHCHECK_FAILURE_LIMIT {
                            let reason = format!(
                                "MTProto healthcheck failed {} times: {}",
                                health_failures, s
                            );
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::ConnectionAborted,
                                reason,
                            )
                            .into());
                        }
                        continue;
                    }
                    Err(_) => {
                        health_failures = health_failures.saturating_add(1);
                        log::warn!(
                            "Акк {}: MTProto healthcheck timeout {}/{} after {}s",
                            log_name,
                            health_failures,
                            GRAMMERS_HEALTHCHECK_FAILURE_LIMIT,
                            GRAMMERS_HEALTHCHECK_TIMEOUT_SECS
                        );
                        if health_failures >= GRAMMERS_HEALTHCHECK_FAILURE_LIMIT {
                            let reason = format!(
                                "MTProto healthcheck timed out {} times",
                                health_failures
                            );
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                reason,
                            )
                            .into());
                        }
                        continue;
                    }
                }
            }
        };
        match update {
            Update::NewMessage(message) => {
                let text = message.text().to_string();
                let is_out = message.outgoing();
                let sender_id = message.sender().map_or(0, |s| s.id()) as u64;
                if let Some(Chat::User(user)) = message.sender() {
                    remember_user_username(&state, user.id() as u64, user.username());
                }
                let chat_id = message.chat().id();
                let current_chat = message.chat();
                remember_chat_username(&state, &current_chat);
                if is_out {
                    state
                        .last_outgoing_message_id_by_chat
                        .insert(chat_id, message.id());
                }
                let owner_id = state
                    .get_setting("owner_id")
                    .parse::<u64>()
                    .unwrap_or(598802932);

                let is_privileged = sender_id == owner_id || state.admins.contains_key(&sender_id);
                if state.muted_users.contains_key(&sender_id) && !is_out && !is_privileged {
                    let _ = message.delete().await;
                    continue;
                }

                if !is_token_bot && state.online_enabled.load(Ordering::SeqCst) && !is_out {
                    let mark = match message.chat() {
                        Chat::Channel(_) => true,
                        Chat::Group(g) => g.is_megagroup(),
                        _ => false,
                    };
                    if mark {
                        let _ = message.mark_as_read().await;
                    }
                }

                let key_chk = format!("{}_{}", chat_id, sender_id);
                let check1_user_spoke = if let Some(mut t) = state.check1_tasks.get_mut(&key_chk) {
                    t.last_time = Some(get_now());
                    t.last_message_id = Some(message.id());
                    t.silence_notified = false;
                    t.link_chat_id = Some(chat_id);
                    let ch = message.chat();
                    t.chat_username = check1_stored_public_username(&ch);
                    t.tme_internal = compute_tme_internal_from_chat(&ch, chat_id);
                    true
                } else {
                    false
                };
                if check1_user_spoke {
                    state.save_persistence();
                }

                if !is_token_bot && !is_out && matches!(message.chat(), Chat::User(_)) {
                    let peer_id = message.sender().map(|s| s.id() as u64).unwrap_or(0);
                    if peer_id != 0 && peer_id != self_id {
                        if let Some(v) = state.afk_state.get(&self_id).map(|e| e.value().clone()) {
                            if state.afk_notified.insert(peer_id, ()).is_none() {
                                let elapsed = get_now().saturating_sub(v.start_time);
                                let t_str = format!(
                                    "{} days, {:02}:{:02}:{:02}",
                                    elapsed / 86400,
                                    (elapsed % 86400) / 3600,
                                    (elapsed % 3600) / 60,
                                    elapsed % 60
                                );
                                let m_text = format!(
                                    "Я в AFK режиме\nВремя нахождения в AFK: <code>{}</code>\nПричина AFK: <code>{}</code>",
                                    t_str,
                                    escape(&v.reason)
                                );
                                let reply_mid = Some(message.id());
                                let _ = if let Some(ref url) = v.media_url {
                                    send_media_msg(
                                        &client,
                                        &message.chat(),
                                        url,
                                        &m_text,
                                        url.to_lowercase().ends_with(".mp4"),
                                        false,
                                        reply_mid,
                                    )
                                    .await;
                                    true
                                } else {
                                    client
                                        .send_message(
                                            &message.chat(),
                                            html_msg(&m_text).reply_to(reply_mid),
                                        )
                                        .await
                                        .is_ok()
                                };
                            }
                        }
                    }
                }

                // Command triggers:
                let is_command = if is_token_bot {
                    !is_out
                        && text.starts_with(prefix)
                        && (sender_id == owner_id || state.admins.contains_key(&sender_id))
                } else {
                    let is_from_owner =
                        sender_id == owner_id || state.admins.contains_key(&sender_id);
                    (is_out || is_from_owner) && text.starts_with(prefix)
                };

                if !is_out && !is_command {
                    let last_auto_reply_key = auto_reply_rate_key(sender_id, chat_id);
                    state
                        .last_auto_reply_message_id
                        .insert(last_auto_reply_key.clone(), message.id());
                    state
                        .last_auto_reply_seen_at
                        .insert(last_auto_reply_key.clone(), Instant::now());
                    let text_auto_reply =
                        account_text_auto_reply_config(&state, sender_id, chat_id);
                    if let Some(c) = text_auto_reply {
                        let delay_secs = c.delay.max(AUTO_REPLY_MIN_DELAY_SECS);
                        spawn_text_auto_reply_worker(
                            client.clone(),
                            message.chat().clone(),
                            state.clone(),
                            sender_id,
                            chat_id,
                            c,
                            delay_secs,
                        );
                    }
                    let media_auto_reply =
                        account_media_auto_reply_config(&state, sender_id, chat_id);
                    if let Some(c) = media_auto_reply {
                        let delay_secs = c.delay.max(AUTO_REPLY_MIN_DELAY_SECS);
                        spawn_media_auto_reply_worker(
                            client.clone(),
                            message.chat().clone(),
                            state.clone(),
                            sender_id,
                            chat_id,
                            c,
                            delay_secs,
                        );
                    }
                }

                if is_command {
                    let parts: Vec<String> =
                        text.split_whitespace().map(|s| s.to_string()).collect();
                    if parts.is_empty() {
                        continue;
                    }
                    let first = parts[0].as_str();
                    if !first.starts_with(prefix) {
                        continue;
                    }
                    let rest = first[prefix.len_utf8()..].to_string();
                    let cmd = rest.split('@').next().unwrap_or(&rest).to_lowercase();
                    let cmd_dedupe_key = format!("{}:{}:{}", chat_id, message.id(), cmd);
                    let now = Instant::now();
                    state
                        .processed_commands
                        .retain(|_, seen| seen.elapsed() < Duration::from_secs(120));
                    if state
                        .processed_commands
                        .insert(cmd_dedupe_key, now)
                        .is_some()
                    {
                        continue;
                    }
                    let namebot_setting = state.get_setting("name_bot");
                    let namebot_display = if namebot_setting.trim().is_empty() {
                        "Rayzen x Arab Bot".to_string()
                    } else {
                        namebot_setting
                    };
                    let media_for_menu = effective_menu_media(&state);
                    let footer_menu = format!(
                         "\n\n⛧User: <code>{}</code>\n⛧ID: <code>{}</code>\n⛧Разработчик бота — <a href=\"https://t.me/Arab_Trolling\">Arab</a> &amp; <a href=\"https://t.me/rayzen_ezz\">rayzen</a>",
                         escape(me.first_name()),
                         self_id
                     );
                    match cmd.as_str() {
                        "online" => {
                            state.online_enabled.store(true, Ordering::SeqCst);
                            let res_text = format!(
                                "вечный онлайн включен чтобы выключить — <code>{}offline</code>",
                                prefix
                            );
                            if is_token_bot {
                                let _ = client
                                    .send_message(message.chat(), html_msg(&res_text))
                                    .await;
                            } else {
                                let _ = message.edit(html_msg(&res_text)).await;
                            }
                        }
                        "offline" => {
                            state.online_enabled.store(false, Ordering::SeqCst);
                            let res_text = "вечный онлайн выключен";
                            if is_token_bot {
                                let _ = client
                                    .send_message(message.chat(), html_msg(res_text))
                                    .await;
                            } else {
                                let _ = message.edit(html_msg(res_text)).await;
                            }
                        }
                        "ping" => {
                            let res_text = build_account_ping_reply().await;
                            if is_token_bot {
                                let _ = client
                                    .send_message(message.chat(), html_msg(&res_text))
                                    .await;
                            } else {
                                let _ = message.edit(html_msg(&res_text)).await;
                            }
                        }
                        "uptime" => {
                            let d_b = state.start_time.elapsed().as_secs();
                            let bot_u = format!(
                                "{}d {:02}:{:02}:{:02}",
                                d_b / 86400,
                                (d_b % 86400) / 3600,
                                (d_b % 3600) / 60,
                                d_b % 60
                            );
                            let (host_u, cpu, mem) = get_sys_metrics();
                            let res_text = format!(
                                "⛧ СТАТУС СЕРВЕРА ⛧\n⛧ Процесс бота: <code>{}</code> ⛧\n⛧ Аптайм дедика: <code>{}</code> ⛧\n⛧ Процессор: <code>{}</code> ⛧\n⛧ Память: <code>{}</code> ⛧",
                                 bot_u, host_u, cpu, mem
                             );
                            if is_token_bot {
                                let _ = client
                                    .send_message(message.chat(), html_msg(&res_text))
                                    .await;
                            } else {
                                let _ = message.edit(html_msg(&res_text)).await;
                            }
                        }
                        "id" | "user" => {
                            let res_text = if let Some(reply_id) = message.reply_to_message_id() {
                                if let Ok(msgs) = client
                                    .get_messages_by_id(&message.chat(), &[reply_id])
                                    .await
                                {
                                    if let Some(Some(rm)) = msgs.get(0) {
                                        if let Some(Chat::User(u)) = rm.sender() {
                                            format!("⛧ User ID: <code>{}</code> ⛧", u.id())
                                        } else {
                                            "пользователь не определен".to_string()
                                        }
                                    } else {
                                        "пользователь не определен".to_string()
                                    }
                                } else {
                                    "пользователь не определен".to_string()
                                }
                            } else {
                                format!("⛧ Chat ID: <code>{}</code> ⛧", chat_id)
                            };
                            if is_token_bot {
                                let _ = client
                                    .send_message(message.chat(), html_msg(&res_text))
                                    .await;
                            } else {
                                let _ = message.edit(html_msg(&res_text)).await;
                            }
                        }
                        "reg" => {
                            if message.reply_to_message_id().is_none() {
                                let res_text = "⛧ Нужен реплай ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                            } else if let Some(reply_id) = message.reply_to_message_id() {
                                if let Ok(msgs) = client
                                    .get_messages_by_id(&message.chat(), &[reply_id])
                                    .await
                                {
                                    if let Some(Some(rm)) = msgs.get(0) {
                                        if let Some(Chat::User(u)) = rm.sender() {
                                            let uid = u.id() as u64;
                                            let reg = estimate_registration_date(uid);
                                            let reg_str = format!(
                                                "{:02}.{:02}.{}",
                                                reg.day(),
                                                reg.month(),
                                                reg.year()
                                            );
                                            let res_text = format!(
                                                 "⛧ Пользователь: <a href=\"tg://user?id={}\">{}</a> ⛧\n⛧ Примерная дата регистрации: <code>{}</code> ⛧",
                                                 uid, escape(u.first_name()), reg_str
                                             );
                                            if is_token_bot {
                                                let _ = client
                                                    .send_message(
                                                        message.chat(),
                                                        html_msg(&res_text),
                                                    )
                                                    .await;
                                            } else {
                                                let _ = message.edit(html_msg(&res_text)).await;
                                            }
                                        } else {
                                            let res_text =
                                                "⛧ не вышло узнать регистрацию акка ⛧";
                                            if is_token_bot {
                                                let _ = client
                                                    .send_message(
                                                        message.chat(),
                                                        html_msg(res_text),
                                                    )
                                                    .await;
                                            } else {
                                                let _ = message.edit(html_msg(res_text)).await;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        "help" | "settings" => {
                            let menu = if cmd == "help" {
                                format!(
                                    r#"⛧ {nb} ⛧
⛧ <code>{p}ar</code> + chat id + time + file.txt + обращение
⛧ <code>{p}arp</code> + chat id + time + file.txt + url photo + обращение
⛧ <code>{p}arv</code> + chat id + time + file.txt + url video + обращение
⛧ <code>{p}tag</code> + chat id + user id + time + file.txt + обращение
⛧ <code>{p}tagp</code> + chat id + user id + time + file.txt + url photo + обращение
⛧ <code>{p}tagv</code> + chat id + user id + time + file.txt + url video + обращение
⛧ <code>{p}kal</code> + chat id + time + file.txt + обращение — календарь
⛧ <code>{p}kalp</code> + chat id + time + file.txt + url photo + обращение — календарь с фото
⛧ <code>{p}kalv</code> + chat id + time + file.txt + url video + обращение — календарь с видео
⛧ <code>{p}settings</code> — настройка дополнительных команд
⛧ <code>{p}direct</code> — покажет все файлы бота, для просмотра шаблонов
⛧ <code>{p}id</code> — покажет chat id
⛧ <code>{p}user</code> + reply — покажет user id
⛧ <code>{p}check</code> — покажет статус команд
⛧ <code>{p}clear</code> — очистит все списки спамеров
⛧ <code>{p}cclean</code> — очистит все списки автоответчиков
⛧ <code>{p}x0</code> + reply — загрузить фото/видео на хостинг{foot}"#,
                                    nb = escape(&namebot_display),
                                    foot = footer_menu,
                                    p = prefix
                                )
                            } else {
                                format!(
                                    r#"⛧ Settings ⛧
⛧ <code>{p}us</code> + user id + time + file.txt + обращение
⛧ <code>{p}usm</code> + user id + time + file.txt + url media + обращение
⛧ <code>{p}mute</code> + user id - удаляет сообщения пользователя
⛧ <code>{p}check1</code> + chat id + user id + time - отслеживает последнее сообщение юзера в чате
⛧ <code>{p}ping</code> - пинг бота
⛧ <code>{p}uptime</code> — время работы бота
⛧ <code>{p}sp</code> + chat id + time + file.txt + обращение - спам в чат через флуд вейт
⛧ <code>{p}reg</code> + reply - узнать дату регистрации акка
⛧ <code>{p}online</code> - включить вечный онлайн

⛧ Настройка Афк режима ⛧
⛧ <code>{p}afk</code> 1 + reason + url media — включить афк режим
⛧ <code>{p}afk</code> 2 — выключить афк режим

⛧ Custom ⛧
⛧ <code>{p}bot</code> + название — сменить название бота
⛧ <code>{p}link</code> + url media — изменить медиа для {p}help и {p}settings{foot}"#,
                                    foot = footer_menu,
                                    p = prefix
                                )
                            };
                            send_rayzen_style_menu(
                                &client,
                                &message.chat(),
                                &message,
                                is_token_bot,
                                is_out,
                                &media_for_menu,
                                &menu,
                            )
                            .await;
                        }
                        "check" => {
                            let check_chat = message.chat();
                            let mut check_chat_ids: HashSet<i64> = HashSet::new();
                            check_chat_ids.extend(state.spam_tasks_tag.iter().map(|kv| *kv.key()));
                            check_chat_ids.extend(state.spam_tasks_tagp.iter().map(|kv| *kv.key()));
                            check_chat_ids.extend(state.spam_tasks_tagv.iter().map(|kv| *kv.key()));
                            check_chat_ids.extend(state.spam_tasks_ar.iter().map(|kv| *kv.key()));
                            check_chat_ids.extend(state.spam_tasks_arp.iter().map(|kv| *kv.key()));
                            check_chat_ids.extend(state.spam_tasks_arv.iter().map(|kv| *kv.key()));
                            check_chat_ids.extend(state.spam_tasks_sp.iter().map(|kv| *kv.key()));
                            check_chat_ids.extend(state.check1_tasks.iter().map(|kv| kv.chat_id));
                            let dialog_snapshot = collect_dialog_snapshot(
                                &client,
                                &state,
                                &check_chat,
                                &check_chat_ids,
                            )
                            .await;
                            let us_i = linked_user_ids_for_check(
                                &state,
                                &dialog_snapshot,
                                state.auto_replies_text.iter().map(|kv| *kv.key()),
                            );
                            let global_us_i = linked_user_ids_for_check(
                                &state,
                                &dialog_snapshot,
                                state.global_auto_replies_text.iter().map(|kv| *kv.key()),
                            );
                            let usm_i = linked_user_ids_for_check(
                                &state,
                                &dialog_snapshot,
                                state.auto_replies_media.iter().map(|kv| *kv.key()),
                            );
                            let global_usm_i = linked_user_ids_for_check(
                                &state,
                                &dialog_snapshot,
                                state.global_auto_replies_media.iter().map(|kv| *kv.key()),
                            );
                            let list_mutes = linked_user_ids_for_check(
                                &state,
                                &dialog_snapshot,
                                state.muted_users.iter().map(|kv| *kv.key()),
                            );
                            let mut check_lines: Vec<String> = Vec::new();
                            let check_items: Vec<crate::account::state::Check1Task> = state
                                .check1_tasks
                                .iter()
                                .map(|kv| kv.value().clone())
                                .collect();
                            for task in check_items {
                                let chat_html = if let Some(mid) =
                                    task.last_message_id.filter(|&mid| mid > 0)
                                {
                                    if let Some(url) = check1_message_url(
                                        task.chat_username.as_deref(),
                                        task.tme_internal,
                                        mid,
                                    ) {
                                        format!(r#"<a href="{url}">{}</a>"#, task.chat_id)
                                    } else {
                                        chat_link_html(
                                            &client,
                                            &state,
                                            &dialog_snapshot,
                                            task.chat_id,
                                        )
                                        .await
                                    }
                                } else if let Some(username) =
                                    task.chat_username.as_deref().and_then(normalized_username)
                                {
                                    format!(
                                        r#"<a href="https://t.me/{username}">{}</a>"#,
                                        task.chat_id
                                    )
                                } else {
                                    chat_link_html(&client, &state, &dialog_snapshot, task.chat_id)
                                        .await
                                };
                                let user_html = user_link_html_from_snapshot(
                                    &state,
                                    &dialog_snapshot,
                                    task.user_id,
                                );
                                check_lines.push(format!(
                                    "Chat: {} | User: {} | Delay: {}",
                                    chat_html, user_html, task.delay
                                ));
                            }
                            let check_block = if check_lines.is_empty() {
                                "None".to_string()
                            } else {
                                check_lines.join("\n")
                            };

                            let us_s = if us_i.is_empty() {
                                "None".into()
                            } else {
                                us_i.join(", ")
                            };
                            let usm_s = if usm_i.is_empty() {
                                "None".into()
                            } else {
                                usm_i.join(", ")
                            };
                            let global_us_s = if global_us_i.is_empty() {
                                "None".into()
                            } else {
                                global_us_i.join(", ")
                            };
                            let global_usm_s = if global_usm_i.is_empty() {
                                "None".into()
                            } else {
                                global_usm_i.join(", ")
                            };
                            let mutes_s = if list_mutes.is_empty() {
                                "None".into()
                            } else {
                                list_mutes.join(", ")
                            };

                            let tag_ids = linked_chat_ids_for_map(
                                &client,
                                &state,
                                &dialog_snapshot,
                                &state.spam_tasks_tag,
                            )
                            .await;
                            let tagp_ids = linked_chat_ids_for_map(
                                &client,
                                &state,
                                &dialog_snapshot,
                                &state.spam_tasks_tagp,
                            )
                            .await;
                            let tagv_ids = linked_chat_ids_for_map(
                                &client,
                                &state,
                                &dialog_snapshot,
                                &state.spam_tasks_tagv,
                            )
                            .await;
                            let ar_ids = linked_chat_ids_for_map(
                                &client,
                                &state,
                                &dialog_snapshot,
                                &state.spam_tasks_ar,
                            )
                            .await;
                            let arp_ids = linked_chat_ids_for_map(
                                &client,
                                &state,
                                &dialog_snapshot,
                                &state.spam_tasks_arp,
                            )
                            .await;
                            let arv_ids = linked_chat_ids_for_map(
                                &client,
                                &state,
                                &dialog_snapshot,
                                &state.spam_tasks_arv,
                            )
                            .await;
                            let sr_ids = linked_chat_ids_for_map(
                                &client,
                                &state,
                                &dialog_snapshot,
                                &state.spam_tasks_sp,
                            )
                            .await;
                            let list = format!(
                                "⛧ Статус команд ⛧\n\n\
⛧ Чаты где работает tag: ⛧\n{}\n\n\
⛧ Чаты где работает tagp: ⛧\n{}\n\n\
⛧ Чаты где работает tagv: ⛧\n{}\n\n\
⛧ Чаты где работает ar: ⛧\n{}\n\n\
⛧ Чаты где работает arp: ⛧\n{}\n\n\
⛧ Чаты где работает arv: ⛧\n{}\n\n\
⛧ Чаты где работает sr: ⛧\n{}\n\n\
⛧ На кого работает автоответчик us: ⛧\n{}\n\n\
⛧ На кого работает глобальный автоответчик us: ⛧\n{}\n\n\
⛧ На кого работает автоответчик usm: ⛧\n{}\n\n\
⛧ На кого работает глобальный автоответчик usm: ⛧\n{}\n\n\
⛧ Замученные пользователи (mute): ⛧\n{}\n\n\
⛧ Активные чекеры (check1): ⛧\n{}\n{}",
                                tag_ids,
                                tagp_ids,
                                tagv_ids,
                                ar_ids,
                                arp_ids,
                                arv_ids,
                                sr_ids,
                                us_s,
                                global_us_s,
                                usm_s,
                                global_usm_s,
                                mutes_s,
                                check_block,
                                footer_menu,
                            );
                            send_rayzen_style_menu(
                                &client,
                                &message.chat(),
                                &message,
                                is_token_bot,
                                is_out,
                                &media_for_menu,
                                &list,
                            )
                            .await;
                        }
                        "clear" => {
                            abort_all_spam_and_calendars(&state);
                            state.check1_tasks.clear();
                            state.muted_users.clear();
                            state.missions.clear();
                            state.save_persistence();
                            log_to_bot(&state, "Все спамеры, муты и чекеры очищены").await;
                            let res_text = "сделано";
                            if is_token_bot {
                                let _ = client
                                    .send_message(message.chat(), html_msg(res_text))
                                    .await;
                            } else {
                                let _ = message.edit(html_msg(res_text)).await;
                            }
                        }
                        "cclean" => {
                            state.auto_replies_text.clear();
                            state.auto_replies_media.clear();
                            state.global_auto_replies_text.clear();
                            state.global_auto_replies_media.clear();
                            state.last_reply_at.clear();
                            state.last_reply_at_media.clear();
                            state.last_auto_reply_seen_at.clear();
                            state.text_auto_reply_running.clear();
                            state.media_auto_reply_running.clear();
                            state.last_auto_reply_per_chat.clear();
                            state.save_persistence();
                            log_to_bot(&state, "Все списки автоответчиков очищены").await;
                            let res_text = "сделано";
                            if is_token_bot {
                                let _ = client
                                    .send_message(message.chat(), html_msg(res_text))
                                    .await;
                            } else {
                                let _ = message.edit(html_msg(res_text)).await;
                            }
                        }
                        "mute" => {
                            if parts.len() > 1 {
                                if let Ok(uid) = parts[1].parse::<u64>() {
                                    state.muted_users.insert(uid, true);
                                    state.save_persistence();
                                    let res_text = format!("⛧ Пользователь <code>{}</code> добавлен в вечный мут, выключить — <code>{}unmute {}</code> ⛧", uid, prefix, uid);
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(&res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(&res_text)).await;
                                    }
                                }
                            }
                        }
                        "unmute" => {
                            if parts.len() > 1 {
                                if let Ok(uid) = parts[1].parse::<u64>() {
                                    state.muted_users.remove(&uid);
                                    state.save_persistence();
                                    let res_text = format!(
                                        "⛧ Пользователь <code>{}</code> удален из вечного мута ⛧",
                                        uid
                                    );
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(&res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(&res_text)).await;
                                    }
                                }
                            }
                        }
                        "direct" => {
                            let (s, k, t) = list_directory_content(&state.base_path);
                            let menu = format!(
                                "Сессии:\n{}\n\nКлючи:\n{}\n\nШаблоны:\n{}\n\n⛧ Управление файлами ⛧\n⛧ <code>{p}save</code> + reply — скачать файл\n⛧ <code>{p}unsave</code> + file.txt — удалить файл\n⛧ <code>{p}drop</code> + file.txt — отправить файл шаблона в чат{}",
                                s,
                                k,
                                t,
                                footer_menu,
                                p = prefix
                            );
                            send_rayzen_style_menu(
                                &client,
                                &message.chat(),
                                &message,
                                is_token_bot,
                                is_out,
                                &media_for_menu,
                                &menu,
                            )
                            .await;
                        }
                        "bot" => {
                            if parts.len() > 1 {
                                let new_name = parts[1..].join(" ");
                                state
                                    .settings
                                    .insert("name_bot".to_string(), new_name.clone());
                                state.save_persistence();
                                let res_text = format!(
                                    "⛧ Имя бота изменено на: <code>{}</code> ⛧",
                                    escape(&new_name)
                                );
                                log_to_bot(
                                    &state,
                                    &format!(
                                        "Имя бота было изменено.\nНовое имя: <code>{}</code>",
                                        escape(&new_name)
                                    ),
                                )
                                .await;
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(&res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(&res_text)).await;
                                }
                            }
                        }
                        "link" => {
                            if parts.len() > 1 {
                                let new_link = parts[1..].join(" ");
                                state
                                    .settings
                                    .insert("media_link".to_string(), new_link.clone());
                                state.save_persistence();
                                let res_text = format!(
                                    "⛧ Ссылка на медиа изменена на: <code>{}</code> ⛧",
                                    escape(&new_link)
                                );
                                log_to_bot(
                                    &state,
                                    &format!(
                                        "Ссылка на медиа изменено.\nНовая ссылка: <code>{}</code>",
                                        escape(&new_link)
                                    ),
                                )
                                .await;
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(&res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(&res_text)).await;
                                }
                            }
                        }
                        "afk" => {
                            if parts.len() > 1 {
                                if parts[1] == "1" {
                                    let mut reason_parts: Vec<String> = vec![];
                                    let mut media: Option<String> = None;
                                    for w in parts.iter().skip(2) {
                                        if w.starts_with("http://") || w.starts_with("https://") {
                                            if media.is_none() {
                                                media = Some(w.clone());
                                            }
                                        } else {
                                            reason_parts.push(w.clone());
                                        }
                                    }
                                    let reason = reason_parts.join(" ");
                                    state.afk_notified.clear();
                                    state.afk_state.insert(
                                        self_id,
                                        AfkConfig {
                                            reason,
                                            start_time: get_now(),
                                            media_url: media,
                                        },
                                    );
                                    let res_text = "⛧ Режим AFK включен ⛧";
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(res_text)).await;
                                    }
                                } else if parts[1] == "2" {
                                    state.afk_state.remove(&self_id);
                                    state.afk_notified.clear();
                                    let res_text = "⛧ Режим AFK выключен ⛧";
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(res_text)).await;
                                    }
                                }
                            }
                        }
                        "save" => {
                            if let Some(reply_id) = message.reply_to_message_id() {
                                if let Ok(msgs) = client
                                    .get_messages_by_id(&message.chat(), &[reply_id])
                                    .await
                                {
                                    if let Some(Some(reply_msg)) = msgs.get(0) {
                                        if let Some(media) = reply_msg.media() {
                                            let is_txt = match &media {
                                                grammers_client::types::Media::Document(doc) => {
                                                    doc.mime_type()
                                                        .map(|m| m == "text/plain")
                                                        .unwrap_or(false)
                                                        || doc
                                                            .name()
                                                            .to_lowercase()
                                                            .ends_with(".txt")
                                                }
                                                _ => false,
                                            };
                                            let too_big = match &media {
                                                grammers_client::types::Media::Document(doc) => doc
                                                    .size()
                                                    .try_into()
                                                    .map(|s: u64| s > MAX_TEMPLATE_FILE_SIZE_BYTES)
                                                    .unwrap_or(false),
                                                _ => false,
                                            };
                                            if !is_txt {
                                                let res_text = "⛧ Нужен реплай на .txt файл ⛧";
                                                if is_token_bot {
                                                    let _ = client
                                                        .send_message(
                                                            message.chat(),
                                                            html_msg(res_text),
                                                        )
                                                        .await;
                                                } else {
                                                    let _ = message.edit(html_msg(res_text)).await;
                                                }
                                            } else if too_big {
                                                let res_text = format!(
                                                    "⛧ Файл слишком большой, лимит {} ⛧",
                                                    format_template_size_limit()
                                                );
                                                if is_token_bot {
                                                    let _ = client
                                                        .send_message(
                                                            message.chat(),
                                                            html_msg(&res_text),
                                                        )
                                                        .await;
                                                } else {
                                                    let _ = message.edit(html_msg(&res_text)).await;
                                                }
                                            } else {
                                                let mut file_name = if parts.len() > 1 {
                                                    parts[1..].join(" ")
                                                } else {
                                                    "".to_string()
                                                };
                                                if file_name.is_empty() {
                                                    if let grammers_client::types::Media::Document(
                                                        doc,
                                                    ) = &media
                                                    {
                                                        file_name = doc.name().to_string();
                                                        if file_name.is_empty() {
                                                            file_name = "file.txt".to_string();
                                                        }
                                                    } else {
                                                        file_name = format!(
                                                            "file_{}.txt",
                                                            rand::random::<u32>()
                                                        );
                                                    }
                                                }
                                                let Some(file_name) =
                                                    safe_template_file_name(&file_name)
                                                else {
                                                    let res_text =
                                                        "⛧ Недопустимое имя файла или не .txt шаблон ⛧";
                                                    if is_token_bot {
                                                        let _ = client
                                                            .send_message(
                                                                message.chat(),
                                                                html_msg(res_text),
                                                            )
                                                            .await;
                                                    } else {
                                                        let _ =
                                                            message.edit(html_msg(res_text)).await;
                                                    }
                                                    continue;
                                                };
                                                remove_all_template_copies(
                                                    &state.base_path,
                                                    &file_name,
                                                )
                                                .await;
                                                let dir = templates_dir(&state.base_path);
                                                let path = format!(
                                                    "{}/{}",
                                                    dir.trim_end_matches('/'),
                                                    file_name
                                                );
                                                let downloadable = Downloadable::Media(media);
                                                if let Ok(_) = tokio::fs::create_dir_all(&dir).await
                                                {
                                                    if client
                                                        .download_media(
                                                            &downloadable,
                                                            std::path::Path::new(&path),
                                                        )
                                                        .await
                                                        .is_ok()
                                                    {
                                                        let res_text = format!("⛧ Файл <code>{}</code> успешно сохранен ⛧", escape(&file_name));
                                                        log_to_bot(&state, &format!("Файл <code>{}</code> был сохранен!", escape(&file_name))).await;
                                                        if is_token_bot {
                                                            let _ = client
                                                                .send_message(
                                                                    message.chat(),
                                                                    html_msg(&res_text),
                                                                )
                                                                .await;
                                                        } else {
                                                            let _ = message
                                                                .edit(html_msg(&res_text))
                                                                .await;
                                                        }
                                                    } else {
                                                        let res_text =
                                                            "⛧ ошибка скачивания файла ⛧";
                                                        if is_token_bot {
                                                            let _ = client
                                                                .send_message(
                                                                    message.chat(),
                                                                    html_msg(res_text),
                                                                )
                                                                .await;
                                                        } else {
                                                            let _ = message
                                                                .edit(html_msg(res_text))
                                                                .await;
                                                        }
                                                    }
                                                }
                                            }
                                        } else {
                                            let res_text = "⛧ Реплай на файл нужен ⛧";
                                            if is_token_bot {
                                                let _ = client
                                                    .send_message(
                                                        message.chat(),
                                                        html_msg(res_text),
                                                    )
                                                    .await;
                                            } else {
                                                let _ = message.edit(html_msg(res_text)).await;
                                            }
                                        }
                                    }
                                }
                            } else {
                                let res_text = "⛧ Реплай на файл нужен ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                            }
                        }
                        "drop" => {
                            if parts.len() > 1 {
                                let raw_file_name = parts[1..].join(" ");
                                let Some(file_name) = safe_template_file_name(&raw_file_name)
                                else {
                                    let res_text = "⛧ Недопустимое имя файла ⛧";
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(res_text)).await;
                                    }
                                    continue;
                                };
                                if let Some(path) =
                                    resolve_existing_template_path(&state.base_path, &file_name)
                                        .await
                                {
                                    match tokio::fs::metadata(&path).await {
                                        Ok(meta) if meta.len() > MAX_TEMPLATE_FILE_SIZE_BYTES => {
                                            let res_text = format!(
                                                "⛧ Файл слишком большой, лимит {} ⛧",
                                                format_template_size_limit()
                                            );
                                            if is_token_bot {
                                                let _ = client
                                                    .send_message(
                                                        message.chat(),
                                                        html_msg(&res_text),
                                                    )
                                                    .await;
                                            } else {
                                                let _ = message.edit(html_msg(&res_text)).await;
                                            }
                                            continue;
                                        }
                                        Err(_) => {
                                            let res_text = "⛧ Файл не найден ⛧";
                                            if is_token_bot {
                                                let _ = client
                                                    .send_message(
                                                        message.chat(),
                                                        html_msg(res_text),
                                                    )
                                                    .await;
                                            } else {
                                                let _ = message.edit(html_msg(res_text)).await;
                                            }
                                            continue;
                                        }
                                        _ => {}
                                    }
                                    let now = Instant::now();
                                    if let Some(last) = state.last_drop_at.insert(self_id, now) {
                                        let elapsed = last.elapsed();
                                        if elapsed < Duration::from_secs(DROP_COOLDOWN_SECS) {
                                            let wait = DROP_COOLDOWN_SECS
                                                .saturating_sub(elapsed.as_secs());
                                            let res_text = format!(
                                                "⛧ Подожди {} сек перед следующим drop ⛧",
                                                wait
                                            );
                                            if is_token_bot {
                                                let _ = client
                                                    .send_message(
                                                        message.chat(),
                                                        html_msg(&res_text),
                                                    )
                                                    .await;
                                            } else {
                                                let _ = message.edit(html_msg(&res_text)).await;
                                            }
                                            continue;
                                        }
                                    }
                                    if let Ok(uploaded) = client.upload_file(&path).await {
                                        let _ = client
                                            .send_message(
                                                message.chat(),
                                                InputMessage::text("").document(uploaded),
                                            )
                                            .await;
                                        if !is_token_bot {
                                            let _ = message.delete().await;
                                        }
                                    } else {
                                        let res_text = "⛧ ошибка загрузки ⛧";
                                        if is_token_bot {
                                            let _ = client
                                                .send_message(message.chat(), html_msg(res_text))
                                                .await;
                                        } else {
                                            let _ = message.edit(html_msg(res_text)).await;
                                        }
                                    }
                                } else {
                                    let res_text = "⛧ Файл не найден ⛧";
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(res_text)).await;
                                    }
                                }
                            }
                        }
                        "unsave" => {
                            if parts.len() > 1 {
                                let raw_file_name = parts[1..].join(" ");
                                let Some(file_name) = safe_template_file_name(&raw_file_name)
                                else {
                                    let res_text = "⛧ Недопустимое имя файла ⛧";
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(res_text)).await;
                                    }
                                    continue;
                                };
                                if remove_all_template_copies(&state.base_path, &file_name).await
                                    > 0
                                {
                                    let res_text = "⛧ Файл удален ⛧";
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(res_text)).await;
                                    }
                                } else {
                                    let res_text = "⛧ Файл не найден ⛧";
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(res_text)).await;
                                    }
                                }
                            }
                        }
                        "x0" => {
                            let res_text_start = "<b>загружаю...</b>";
                            if is_token_bot {
                                let _ = client
                                    .send_message(message.chat(), html_msg(res_text_start))
                                    .await;
                                if let Some(reply_id) = message.reply_to_message_id() {
                                    if let Ok(msgs) = client
                                        .get_messages_by_id(&message.chat(), &[reply_id])
                                        .await
                                    {
                                        if let Some(Some(reply_msg)) = msgs.get(0) {
                                            if let Some(media) = reply_msg.media() {
                                                let ext =
                                                    grammers_upload_extension_from_media(&media);
                                                let p = format!(
                                                    "temp_x0_{}.{}",
                                                    rand::random::<u64>(),
                                                    ext
                                                );
                                                let filename = format!("upload.{}", ext);
                                                if tokio::fs::File::create(&p).await.is_ok() {
                                                    let downloadable = Downloadable::Media(media);
                                                    match tokio::time::timeout(
                                                        Duration::from_secs(300),
                                                        client.download_media(
                                                            &downloadable,
                                                            std::path::Path::new(&p),
                                                        ),
                                                    )
                                                    .await
                                                    {
                                                        Ok(Ok(_)) => {
                                                            match tokio::fs::read(&p).await {
                                                                Ok(data) => {
                                                                    let up = upload_media_fast(
                                                                        data, filename,
                                                                    )
                                                                    .await;
                                                                    match up {
                                                                        Ok(url) => {
                                                                            let res_text = format!(
                                                                                "<code>{}</code>",
                                                                                url
                                                                            );
                                                                            let _ = client
                                                                                .send_message(
                                                                                    message.chat(),
                                                                                    html_msg(
                                                                                        &res_text,
                                                                                    ),
                                                                                )
                                                                                .await;
                                                                        }
                                                                        Err(e) => {
                                                                            let res_text = format!("<b>ошибка загрузки {}</b>", e);
                                                                            let _ = client
                                                                                .send_message(
                                                                                    message.chat(),
                                                                                    html_msg(
                                                                                        &res_text,
                                                                                    ),
                                                                                )
                                                                                .await;
                                                                        }
                                                                    }
                                                                }
                                                                Err(_) => {
                                                                    let _ = client.send_message(message.chat(), html_msg("<b>ошибка чтения скачанного файла</b>")).await;
                                                                }
                                                            }
                                                        }
                                                        Ok(Err(_)) | Err(_) => {
                                                            let _ = client.send_message(message.chat(), html_msg("<b>ошибка скачивания медиа (слишком большой файл, нет доступа или таймаут)</b>")).await;
                                                        }
                                                    }
                                                    let _ = tokio::fs::remove_file(&p).await;
                                                }
                                            } else {
                                                let res_text = "<b>реплай на файл нужен</b>";
                                                let _ = client
                                                    .send_message(
                                                        message.chat(),
                                                        html_msg(res_text),
                                                    )
                                                    .await;
                                            }
                                        }
                                    }
                                } else {
                                    let res_text = "<b>реплай на файл</b>";
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                }
                            } else {
                                let _ = message.edit(html_msg(res_text_start)).await;
                                let cmd_id = message.id();
                                let chat = message.chat().clone();
                                let client_bg = client.clone();
                                if let Some(reply_id) = message.reply_to_message_id() {
                                    tokio::spawn(async move {
                                        if let Ok(msgs) =
                                            client_bg.get_messages_by_id(&chat, &[reply_id]).await
                                        {
                                            if let Some(Some(reply_msg)) = msgs.get(0) {
                                                if let Some(media) = reply_msg.media() {
                                                    let ext = grammers_upload_extension_from_media(
                                                        &media,
                                                    );
                                                    let p = format!(
                                                        "temp_x0_{}.{}",
                                                        rand::random::<u64>(),
                                                        ext
                                                    );
                                                    let filename = format!("upload.{}", ext);
                                                    if tokio::fs::File::create(&p).await.is_ok() {
                                                        let downloadable =
                                                            Downloadable::Media(media);
                                                        match tokio::time::timeout(
                                                            Duration::from_secs(300),
                                                            client_bg.download_media(
                                                                &downloadable,
                                                                std::path::Path::new(&p),
                                                            ),
                                                        )
                                                        .await
                                                        {
                                                            Ok(Ok(_)) => {
                                                                match tokio::fs::read(&p).await {
                                                                    Ok(data) => {
                                                                        let up = upload_media_fast(
                                                                            data, filename,
                                                                        )
                                                                        .await;
                                                                        match up {
                                                                            Ok(url) => {
                                                                                let res_text = format!("<code>{}</code>", url);
                                                                                let _ = client_bg.edit_message(&chat, cmd_id, html_msg(&res_text)).await;
                                                                            }
                                                                            Err(e) => {
                                                                                let res_text = format!("<b>ошибка загрузки {}</b>", e);
                                                                                let _ = client_bg.edit_message(&chat, cmd_id, html_msg(&res_text)).await;
                                                                            }
                                                                        }
                                                                    }
                                                                    Err(_) => {
                                                                        let _ = client_bg.edit_message(&chat, cmd_id, html_msg("<b>ошибка чтения файла</b>")).await;
                                                                    }
                                                                }
                                                            }
                                                            Ok(Err(_)) | Err(_) => {
                                                                let _ = client_bg.edit_message(&chat, cmd_id, html_msg("<b>ошибка скачивания медиа (слишком большой файл, нет доступа или таймаут)</b>")).await;
                                                            }
                                                        }
                                                        let _ = tokio::fs::remove_file(&p).await;
                                                    } else {
                                                        let _ = client_bg
                                                            .edit_message(
                                                                &chat,
                                                                cmd_id,
                                                                html_msg(
                                                                    "<b>ошибка temp файла</b>",
                                                                ),
                                                            )
                                                            .await;
                                                    }
                                                } else {
                                                    let _ = client_bg
                                                        .edit_message(
                                                            &chat,
                                                            cmd_id,
                                                            html_msg("<b>реплай на файл нужен</b>"),
                                                        )
                                                        .await;
                                                }
                                            } else {
                                                let _ = client_bg
                                                    .edit_message(
                                                        &chat,
                                                        cmd_id,
                                                        html_msg("<b>реплай на файл нужен</b>"),
                                                    )
                                                    .await;
                                            }
                                        } else {
                                            let _ = client_bg
                                                .edit_message(
                                                    &chat,
                                                    cmd_id,
                                                    html_msg("<b>не удалось загрузить реплай</b>"),
                                                )
                                                .await;
                                        }
                                    });
                                } else {
                                    let _ = client
                                        .edit_message(
                                            &chat,
                                            cmd_id,
                                            html_msg("<b>реплай на файл</b>"),
                                        )
                                        .await;
                                }
                            }
                        }
                        "us" => {
                            if parts.len() > 3 {
                                if let Ok(uid) = parts[1].parse::<u64>() {
                                    let delay: u64 = parts[2].parse().unwrap_or(1);
                                    let file_name = parts[3].to_string();
                                    let shapka = parts.get(4..).unwrap_or(&[]).join(" ");
                                    if read_template_file(&state.base_path, &file_name)
                                        .await
                                        .is_none()
                                    {
                                        let res_text = format!(
                                            "⛧ Файл <code>{}</code> не найден ⛧",
                                            escape(&file_name)
                                        );
                                        if is_token_bot {
                                            let _ = client
                                                .send_message(message.chat(), html_msg(&res_text))
                                                .await;
                                        } else {
                                            let _ = message.edit(html_msg(&res_text)).await;
                                        }
                                    } else {
                                        state.auto_replies_text.insert(
                                            uid,
                                            crate::account::state::AutoReplyConfig {
                                                file_name,
                                                delay,
                                                shapka,
                                                media_url: None,
                                                chat_id: Some(chat_id),
                                            },
                                        );
                                        state.global_auto_replies_text.remove(&uid);
                                        state.save_persistence();
                                        let res_text = format!("⛧ Пользователь добавлен в автоответчик ⛧\n⛧ Чтобы удалить <code>{}uss {}</code> ⛧\n⛧ Глобальный ответ <code>{}all {}</code> ⛧", prefix, uid, prefix, uid);
                                        log_to_bot(&state, &format!("Пользователь <code>{}</code> добавлен в автоответчик <code>[us]</code>", uid)).await;
                                        if is_token_bot {
                                            let _ = client
                                                .send_message(message.chat(), html_msg(&res_text))
                                                .await;
                                        } else {
                                            let _ = message.edit(html_msg(&res_text)).await;
                                        }
                                    }
                                }
                            } else {
                                let res_text =
                                    "⛧ Недостаточно аргументов, введи команду правильно ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                            }
                        }
                        "uss" => {
                            if parts.len() > 1 {
                                if let Ok(uid) = parts[1].parse::<u64>() {
                                    let removed_local =
                                        state.auto_replies_text.remove(&uid).is_some();
                                    let removed_global =
                                        state.global_auto_replies_text.remove(&uid).is_some();
                                    let removed = removed_local || removed_global;
                                    clear_auto_reply_cooldowns(&state, uid, false);
                                    let res_text = if removed {
                                        state.save_persistence();
                                        log_to_bot(&state, &format!("Автоответчик <code>.us</code> выключен.\nUser ID: <code>{}</code>", uid)).await;
                                        format!(
                                            "⛧ Пользователь удален с ID: <code>{}</code> ⛧",
                                            uid
                                        )
                                    } else {
                                        format!("⛧ Пользователь не добавлен в автоответчик с ID: <code>{}</code> ⛧", uid)
                                    };
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(&res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(&res_text)).await;
                                    }
                                }
                            } else {
                                let res_text =
                                    "⛧ Недостаточно аргументов, введи команду правильно ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                            }
                        }
                        "usm" => {
                            if parts.len() > 4 {
                                if let Ok(uid) = parts[1].parse::<u64>() {
                                    let delay: u64 = parts[2].parse().unwrap_or(1);
                                    let file_name = parts[3].to_string();
                                    let media_url = Some(parts[4].to_string());
                                    let shapka = parts.get(5..).unwrap_or(&[]).join(" ");
                                    if read_template_file(&state.base_path, &file_name)
                                        .await
                                        .is_none()
                                    {
                                        let res_text = format!(
                                            "⛧ Файл <code>{}</code> не найден ⛧",
                                            escape(&file_name)
                                        );
                                        if is_token_bot {
                                            let _ = client
                                                .send_message(message.chat(), html_msg(&res_text))
                                                .await;
                                        } else {
                                            let _ = message.edit(html_msg(&res_text)).await;
                                        }
                                    } else {
                                        state.auto_replies_media.insert(
                                            uid,
                                            crate::account::state::AutoReplyConfig {
                                                file_name,
                                                delay,
                                                shapka,
                                                media_url,
                                                chat_id: Some(chat_id),
                                            },
                                        );
                                        state.global_auto_replies_media.remove(&uid);
                                        state.save_persistence();
                                        let res_text = format!("⛧ Пользователь добавлен в автоответчик ⛧\n⛧ Чтобы удалить <code>{}ussm {}</code> ⛧\n⛧ Глобальный ответ <code>{}all {}</code> ⛧", prefix, uid, prefix, uid);
                                        log_to_bot(&state, &format!("Пользователь <code>{}</code> добавлен в автоответчик <code>[usm]</code>", uid)).await;
                                        if is_token_bot {
                                            let _ = client
                                                .send_message(message.chat(), html_msg(&res_text))
                                                .await;
                                        } else {
                                            let _ = message.edit(html_msg(&res_text)).await;
                                        }
                                    }
                                }
                            } else {
                                let res_text =
                                    "⛧ Недостаточно аргументов, введи команду правильно ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                            }
                        }
                        "ussm" => {
                            if parts.len() > 1 {
                                if let Ok(uid) = parts[1].parse::<u64>() {
                                    let removed_local =
                                        state.auto_replies_media.remove(&uid).is_some();
                                    let removed_global =
                                        state.global_auto_replies_media.remove(&uid).is_some();
                                    let removed = removed_local || removed_global;
                                    clear_auto_reply_cooldowns(&state, uid, true);
                                    let res_text = if removed {
                                        state.save_persistence();
                                        log_to_bot(&state, &format!("Автоответчик <code>.usm</code> выключен\nUser ID: <code>{}</code>", uid)).await;
                                        format!(
                                            "⛧ Пользователь удален с ID: <code>{}</code> ⛧",
                                            uid
                                        )
                                    } else {
                                        format!("⛧ Пользователь не добавлен в автоответчик с ID: <code>{}</code> ⛧", uid)
                                    };
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(&res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(&res_text)).await;
                                    }
                                }
                            } else {
                                let res_text =
                                    "⛧ Недостаточно аргументов, введи команду правильно ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                            }
                        }
                        "all" => {
                            if parts.len() > 1 {
                                if let Ok(uid) = parts[1].parse::<u64>() {
                                    let mut moved_text = false;
                                    let mut moved_media = false;

                                    if let Some((_, mut cfg)) = state.auto_replies_text.remove(&uid)
                                    {
                                        cfg.chat_id = None;
                                        state.global_auto_replies_text.insert(uid, cfg);
                                        moved_text = true;
                                    } else if state.global_auto_replies_text.contains_key(&uid) {
                                        moved_text = true;
                                    }

                                    if let Some((_, mut cfg)) =
                                        state.auto_replies_media.remove(&uid)
                                    {
                                        cfg.chat_id = None;
                                        state.global_auto_replies_media.insert(uid, cfg);
                                        moved_media = true;
                                    } else if state.global_auto_replies_media.contains_key(&uid) {
                                        moved_media = true;
                                    }

                                    let res_text = if moved_text || moved_media {
                                        state.save_persistence();
                                        let delete_cmd = match (moved_text, moved_media) {
                                            (false, true) => {
                                                format!("<code>{}ussm {}</code>", prefix, uid)
                                            }
                                            (true, false) => {
                                                format!("<code>{}uss {}</code>", prefix, uid)
                                            }
                                            _ => format!(
                                                "<code>{}uss {}</code> / <code>{}ussm {}</code>",
                                                prefix, uid, prefix, uid
                                            ),
                                        };
                                        log_to_bot(
                                            &state,
                                            &format!(
                                                "Пользователь <code>{}</code> добавлен в глобальный автоответчик",
                                                uid
                                            ),
                                        )
                                        .await;
                                        format!(
                                            "⛧ Пользователь добавлен в глобальный автоответчик ⛧\n⛧ Чтобы удалить {} ⛧\n⛧ Выключить глобальный автоответчик <code>{}unall {}</code> ⛧",
                                            delete_cmd, prefix, uid
                                        )
                                    } else {
                                        format!(
                                            "⛧ Пользователь не добавлен в автоответчик с ID: <code>{}</code> ⛧",
                                            uid
                                        )
                                    };
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(&res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(&res_text)).await;
                                    }
                                }
                            } else {
                                let res_text =
                                    "⛧ Недостаточно аргументов, введи команду правильно ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                            }
                        }
                        "unall" => {
                            if parts.len() > 1 {
                                if let Ok(uid) = parts[1].parse::<u64>() {
                                    let mut removed = false;
                                    if let Some((_, mut cfg)) =
                                        state.global_auto_replies_text.remove(&uid)
                                    {
                                        cfg.chat_id = Some(cfg.chat_id.unwrap_or(chat_id));
                                        state.auto_replies_text.insert(uid, cfg);
                                        removed = true;
                                    }
                                    if let Some((_, mut cfg)) =
                                        state.global_auto_replies_media.remove(&uid)
                                    {
                                        cfg.chat_id = Some(cfg.chat_id.unwrap_or(chat_id));
                                        state.auto_replies_media.insert(uid, cfg);
                                        removed = true;
                                    }

                                    let res_text = if removed {
                                        state.save_persistence();
                                        "⛧ Глобальный автоответчик выключен ⛧".to_string()
                                    } else {
                                        format!(
                                            "⛧ Пользователь не добавлен в глобальный автоответчик с ID: <code>{}</code> ⛧",
                                            uid
                                        )
                                    };
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(&res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(&res_text)).await;
                                    }
                                }
                            } else {
                                let res_text =
                                    "⛧ Недостаточно аргументов, введи команду правильно ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                            }
                        }
                        "check1" => {
                            if parts.len() > 3 {
                                if let (Ok(cid), Ok(uid), Ok(delay)) = (
                                    parts[1].parse::<i64>(),
                                    parts[2].parse::<u64>(),
                                    parts[3].parse::<u64>(),
                                ) {
                                    let key = format!("{}_{}", cid, uid);
                                    let resolved =
                                        resolve_chat(&client, &message.chat(), &cid.to_string())
                                            .await;
                                    let chat_username = resolved
                                        .as_ref()
                                        .and_then(|chat| check1_stored_public_username(chat));
                                    let tme_internal = resolved
                                        .as_ref()
                                        .and_then(|chat| compute_tme_internal_from_chat(chat, cid));
                                    state.check1_tasks.insert(
                                        key,
                                        crate::account::state::Check1Task {
                                            chat_id: cid,
                                            user_id: uid,
                                            delay,
                                            last_time: Some(get_now()),
                                            last_alert: None,
                                            last_message_id: None,
                                            silence_notified: false,
                                            link_chat_id: Some(cid),
                                            chat_username,
                                            tme_internal,
                                        },
                                    );
                                    state.save_persistence();
                                    let res_text = format!(
                                        "⛧ Чекер установлен ⛧\n⛧ Пользователь: <a href=\"tg://user?id={uid}\">{uid}</a> ⛧\n⛧ Чат: <code>{cid}</code> ⛧\n⛧ Выключить: <code>{p}uncheck {cid} {uid}</code> ⛧",
                                        uid = uid,
                                        cid = cid,
                                        p = prefix
                                    );
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(&res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(&res_text)).await;
                                    }
                                }
                            } else {
                                let res_text =
                                    "⛧ Недостаточно аргументов, введи команду правильно ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                            }
                        }
                        "uncheck" => {
                            if parts.len() > 2 {
                                if let (Ok(cid), Ok(uid)) =
                                    (parts[1].parse::<i64>(), parts[2].parse::<u64>())
                                {
                                    state.check1_tasks.remove(&format!("{}_{}", cid, uid));
                                    state.save_persistence();
                                    let res_text = "⛧ Чекер выключен ⛧";
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(res_text)).await;
                                    }
                                }
                            } else {
                                let res_text =
                                    "⛧ Недостаточно аргументов, введи команду правильно ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                            }
                        }
                        "sp" => {
                            if parts.len() < 4 {
                                let res_text =
                                    "⛧ Недостаточно аргументов, введи команду правильно ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                                continue;
                            }
                            let target_chat =
                                match resolve_chat(&client, &message.chat(), &parts[1]).await {
                                    Some(c) => c,
                                    None => {
                                        let res_txt = "⛧ Чат не найден в диалогах! ⛧";
                                        if is_token_bot {
                                            let _ = client
                                                .send_message(message.chat(), html_msg(res_txt))
                                                .await;
                                        } else {
                                            let _ = message.edit(html_msg(res_txt)).await;
                                        }
                                        continue;
                                    }
                                };
                            let target_cid = target_chat.id();
                            let (wait_ms, delay_stored) =
                                spam_interval_arg_to_wait_ms_stored(parts[2].as_str());
                            let file = parts[3].clone();
                            let shapka = parts.get(4..).unwrap_or(&[]).join(" ");
                            if read_template_file(&state.base_path, &file).await.is_none() {
                                let res_text =
                                    format!("⛧ Файл <code>{}</code> не найден ⛧", escape(&file));
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(&res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(&res_text)).await;
                                }
                                continue;
                            }
                            let p_chat = target_chat.pack();
                            state.missions.insert(
                                mission_key(target_cid, "sp"),
                                crate::account::state::PersistentTask {
                                    chat_id: target_cid,
                                    packed_ty: p_chat.ty as u8,
                                    access_hash: p_chat.access_hash,
                                    delay: delay_stored,
                                    file_name: file.clone(),
                                    shapka: shapka.clone(),
                                    user_id: None,
                                    media_url: None,
                                    is_video: false,
                                    task_type: "sp".to_string(),
                                    mirror_chat_id: None,
                                    kal_command_echo: None,
                                    scheduled_target_ids: Vec::new(),
                                    scheduled_origin_ids: Vec::new(),
                                    calendar_restart_due_at: None,
                                    calendar_cycle_started_at: None,
                                    calendar_watchdog_generation: None,
                                },
                            );
                            state.save_persistence();
                            let c_sub = client.clone();
                            let st_sub = state.clone();
                            let tc = target_chat.clone();
                            let f = file.clone();
                            let sh = shapka.clone();
                            let h = tokio::spawn(async move {
                                handle_sp_forward_loop(c_sub, tc, st_sub, wait_ms, f, sh).await;
                            });
                            state.spam_tasks_sp.insert(target_cid, h);
                            log_to_bot(&state, &format!("Команда <code>{}sp</code> была запущена.\nChat ID: <code>{}</code>", prefix, target_cid)).await;
                            let res_text = format!(
                                "⛧ Чтобы остановить спам — <code>{}offsr {}</code> ⛧",
                                prefix, target_cid
                            );
                            if is_token_bot {
                                let _ = client
                                    .send_message(message.chat(), html_msg(&res_text))
                                    .await;
                            } else {
                                let _ = message.edit(html_msg(&res_text)).await;
                            }
                        }
                        "ar" | "arp" | "arv" | "tag" | "tagp" | "tagv" => {
                            let min_parts = match cmd.as_str() {
                                "ar" => 4usize,
                                "arp" | "arv" => 5,
                                "tag" => 5,
                                "tagp" | "tagv" => 6,
                                _ => 4,
                            };
                            if parts.len() < min_parts {
                                let res_text =
                                    "⛧ Недостаточно аргументов, введи команду правильно ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                                continue;
                            }
                            let target_chat =
                                match resolve_chat(&client, &message.chat(), &parts[1]).await {
                                    Some(c) => c,
                                    None => {
                                        let res_txt = "⛧ Чат не найден в диалогах! ⛧";
                                        if is_token_bot {
                                            let _ = client
                                                .send_message(message.chat(), html_msg(res_txt))
                                                .await;
                                        } else {
                                            let _ = message.edit(html_msg(res_txt)).await;
                                        }
                                        continue;
                                    }
                                };
                            let target_cid = target_chat.id();
                            let (wait_ms, delay_stored, file, url, is_video, uid) =
                                match cmd.as_str() {
                                    "ar" => {
                                        let (w, d) =
                                            spam_interval_arg_to_wait_ms_stored(parts[2].as_str());
                                        (w, d, parts[3].to_string(), None, false, None)
                                    }
                                    "arp" => {
                                        let (w, d) =
                                            spam_interval_arg_to_wait_ms_stored(parts[2].as_str());
                                        (
                                            w,
                                            d,
                                            parts[3].to_string(),
                                            Some(parts[4].to_string()),
                                            false,
                                            None,
                                        )
                                    }
                                    "arv" => {
                                        let (w, d) =
                                            spam_interval_arg_to_wait_ms_stored(parts[2].as_str());
                                        (
                                            w,
                                            d,
                                            parts[3].to_string(),
                                            Some(parts[4].to_string()),
                                            true,
                                            None,
                                        )
                                    }
                                    "tag" => {
                                        let (w, d) =
                                            spam_interval_arg_to_wait_ms_stored(parts[3].as_str());
                                        (
                                            w,
                                            d,
                                            parts[4].to_string(),
                                            None,
                                            false,
                                            Some(parts[2].parse().unwrap_or(0)),
                                        )
                                    }
                                    "tagp" => {
                                        let (w, d) =
                                            spam_interval_arg_to_wait_ms_stored(parts[3].as_str());
                                        (
                                            w,
                                            d,
                                            parts[4].to_string(),
                                            Some(parts[5].to_string()),
                                            false,
                                            Some(parts[2].parse().unwrap_or(0)),
                                        )
                                    }
                                    "tagv" => {
                                        let (w, d) =
                                            spam_interval_arg_to_wait_ms_stored(parts[3].as_str());
                                        (
                                            w,
                                            d,
                                            parts[4].to_string(),
                                            Some(parts[5].to_string()),
                                            true,
                                            Some(parts[2].parse().unwrap_or(0)),
                                        )
                                    }
                                    _ => (1000, 1, "".to_string(), None, false, None),
                                };
                            let shapka = parts
                                .get(
                                    if cmd.starts_with("tag") {
                                        if url.is_some() {
                                            6
                                        } else {
                                            5
                                        }
                                    } else {
                                        if url.is_some() {
                                            5
                                        } else {
                                            4
                                        }
                                    }..,
                                )
                                .unwrap_or(&[])
                                .join(" ");
                            if read_template_file(&state.base_path, &file).await.is_none() {
                                let res_text =
                                    format!("⛧ Файл <code>{}</code> не найден ⛧", escape(&file));
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(&res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(&res_text)).await;
                                }
                                continue;
                            }
                            let c_sub = client.clone();
                            let st_sub = state.clone();
                            let cmd_str = cmd.clone();
                            let url_cl = url.clone();
                            let shapka_cl = shapka.clone();

                            let p_chat = target_chat.pack();
                            state.missions.insert(
                                mission_key(target_cid, &cmd_str),
                                crate::account::state::PersistentTask {
                                    chat_id: target_cid,
                                    packed_ty: p_chat.ty as u8,
                                    access_hash: p_chat.access_hash,
                                    delay: delay_stored,
                                    file_name: file.clone(),
                                    shapka: shapka_cl,
                                    user_id: uid,
                                    media_url: url_cl,
                                    is_video,
                                    task_type: cmd_str.clone(),
                                    mirror_chat_id: None,
                                    kal_command_echo: None,
                                    scheduled_target_ids: Vec::new(),
                                    scheduled_origin_ids: Vec::new(),
                                    calendar_restart_due_at: None,
                                    calendar_cycle_started_at: None,
                                    calendar_watchdog_generation: None,
                                },
                            );
                            state.save_persistence();

                            let h = tokio::spawn(async move {
                                handle_spam_loop(
                                    c_sub,
                                    target_chat,
                                    st_sub,
                                    wait_ms,
                                    file,
                                    shapka,
                                    uid,
                                    url,
                                    is_video,
                                    cmd_str,
                                )
                                .await;
                            });
                            match cmd.as_str() {
                                "ar" => {
                                    state.spam_tasks_ar.insert(target_cid, h);
                                }
                                "arp" => {
                                    state.spam_tasks_arp.insert(target_cid, h);
                                }
                                "arv" => {
                                    state.spam_tasks_arv.insert(target_cid, h);
                                }
                                "tag" => {
                                    state.spam_tasks_tag.insert(target_cid, h);
                                }
                                "tagp" => {
                                    state.spam_tasks_tagp.insert(target_cid, h);
                                }
                                "tagv" => {
                                    state.spam_tasks_tagv.insert(target_cid, h);
                                }
                                _ => {}
                            }
                            let off_cmd = match cmd.as_str() {
                                "tag" => "tagoff",
                                "tagp" => "tagpoff",
                                "tagv" => "tagvoff",
                                "arp" => "offp",
                                "arv" => "offv",
                                _ => "off",
                            };
                            log_to_bot(&state, &format!("Команда <code>{}{}</code> была запущена.\nChat ID: <code>{}</code>", prefix, cmd, target_cid)).await;
                            let res_text = format!(
                                "⛧ Включено ⛧\n⛧ Чтобы выключить — <code>{p}{} {}</code> ⛧",
                                off_cmd,
                                target_cid,
                                p = prefix
                            );
                            if is_token_bot {
                                let _ = client
                                    .send_message(message.chat(), html_msg(&res_text))
                                    .await;
                            } else {
                                let _ = message.edit(html_msg(&res_text)).await;
                            }
                        }
                        "kal" | "kalp" | "kalv" => {
                            let min_k = if cmd == "kal" { 4 } else { 5 };
                            if parts.len() < min_k {
                                let res_text =
                                    "⛧ Недостаточно аргументов, введи команду правильно ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_text)).await;
                                }
                                continue;
                            }
                            let target_chat =
                                match resolve_chat(&client, &message.chat(), &parts[1]).await {
                                    Some(c) => c,
                                    None => {
                                        let res_txt = "⛧ Чат не найден в диалогах! ⛧";
                                        if is_token_bot {
                                            let _ = client
                                                .send_message(message.chat(), html_msg(res_txt))
                                                .await;
                                        } else {
                                            let _ = message.edit(html_msg(res_txt)).await;
                                        }
                                        continue;
                                    }
                                };
                            let target_cid = target_chat.id();
                            let delay_minutes: u64 = parts[2].parse().unwrap_or(1);
                            if delay_minutes == 0 || delay_minutes > 1440 {
                                let res_txt = "⛧ Интервал календаря: 1–1440 минут (шаг как в Rayzen); к постам добавляются секунды 1–59; всё уходит в «Отправить позже». ⛧";
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(res_txt))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(res_txt)).await;
                                }
                                continue;
                            }
                            let (file, url, shapka_off) = if cmd == "kal" {
                                (parts[3].to_string(), None, 4usize)
                            } else {
                                (parts[3].to_string(), Some(parts[4].to_string()), 5usize)
                            };
                            let shapka = parts.get(shapka_off..).unwrap_or(&[]).join(" ");
                            if read_template_file(&state.base_path, &file).await.is_none() {
                                let res_text =
                                    format!("⛧ Файл <code>{}</code> не найден ⛧", escape(&file));
                                if is_token_bot {
                                    let _ = client
                                        .send_message(message.chat(), html_msg(&res_text))
                                        .await;
                                } else {
                                    let _ = message.edit(html_msg(&res_text)).await;
                                }
                                continue;
                            }
                            let is_video = cmd == "kalv";
                            let origin_chat = message.chat().clone();
                            let mirror_id = Some(origin_chat.id());
                            let p_chat = target_chat.pack();
                            let mkey = mission_key(target_cid, &cmd);
                            let kal_echo = message.text().to_string();
                            let old_cal_meta = state.missions.get(&mkey).map(|r| {
                                (
                                    r.mirror_chat_id,
                                    r.scheduled_target_ids.clone(),
                                    r.scheduled_origin_ids.clone(),
                                )
                            });
                            if let Some((old_origin_id, target_ids, origin_ids)) = old_cal_meta {
                                delete_scheduled_ids_in_chat(&client, &target_chat, &target_ids)
                                    .await;
                                if let Some(old_origin_id) = old_origin_id {
                                    if let Some(old_origin_chat) =
                                        find_chat_by_id(&client, old_origin_id).await
                                    {
                                        delete_scheduled_ids_in_chat(
                                            &client,
                                            &old_origin_chat,
                                            &origin_ids,
                                        )
                                        .await;
                                    }
                                }
                            }
                            state.missions.insert(
                                mkey,
                                crate::account::state::PersistentTask {
                                    chat_id: target_cid,
                                    packed_ty: p_chat.ty as u8,
                                    access_hash: p_chat.access_hash,
                                    delay: delay_minutes,
                                    file_name: file.clone(),
                                    shapka: shapka.clone(),
                                    user_id: None,
                                    media_url: url.clone(),
                                    is_video,
                                    task_type: cmd.clone(),
                                    mirror_chat_id: mirror_id,
                                    kal_command_echo: Some(kal_echo.clone()),
                                    scheduled_target_ids: Vec::new(),
                                    scheduled_origin_ids: Vec::new(),
                                    calendar_restart_due_at: None,
                                    calendar_cycle_started_at: None,
                                    calendar_watchdog_generation: None,
                                },
                            );
                            state.save_persistence();
                            let c_sub = client.clone();
                            let st_sub = state.clone();
                            let cmd_str = cmd.clone();
                            let echo_move = kal_echo.clone();
                            let h = tokio::spawn(async move {
                                handle_kal_loop(
                                    c_sub,
                                    target_chat,
                                    origin_chat,
                                    st_sub,
                                    delay_minutes,
                                    file,
                                    shapka,
                                    url.map(|u| (u, is_video)),
                                    cmd_str,
                                    echo_move,
                                )
                                .await;
                            });
                            replace_calendar_task(&state, target_cid, &cmd, h);
                            let off_cmd = match cmd.as_str() {
                                "kalp" => "kpoff",
                                "kalv" => "kvoff",
                                _ => "koff",
                            };
                            let res_text = format!(
                                "⛧ Календарь включен ⛧\n\
⛧ Чтобы выключить — <code>{p}{} {}</code> ⛧",
                                off_cmd,
                                target_cid,
                                p = prefix
                            );
                            if is_token_bot {
                                let _ = client
                                    .send_message(message.chat(), html_msg(&res_text))
                                    .await;
                            } else {
                                let _ = message.edit(html_msg(&res_text)).await;
                            }
                        }
                        "off" | "offp" | "offv" | "tagoff" | "tagpoff" | "tagvoff" | "koff"
                        | "kpoff" | "kvoff" | "offsr" => {
                            if parts.len() > 1 {
                                let target_cid: i64 = parts[1].parse().unwrap_or(chat_id);
                                let map = match cmd.as_str() {
                                    "tagoff" => &state.spam_tasks_tag,
                                    "offp" => &state.spam_tasks_arp,
                                    "tagpoff" => &state.spam_tasks_tagp,
                                    "offv" => &state.spam_tasks_arv,
                                    "tagvoff" => &state.spam_tasks_tagv,
                                    "koff" => &state.calendar_tasks,
                                    "kpoff" => &state.calendar_p_tasks,
                                    "kvoff" => &state.calendar_v_tasks,
                                    "offsr" => &state.spam_tasks_sp,
                                    _ => &state.spam_tasks_ar,
                                };
                                let task_type = stop_command_task_type(cmd.as_str());
                                let mkey = mission_key(target_cid, task_type);
                                let legacy_mkey = legacy_mission_key(target_cid);
                                let cal_meta = match cmd.as_str() {
                                    "koff" | "kpoff" | "kvoff" => {
                                        state.missions.get(&mkey).map(|r| {
                                            (
                                                r.mirror_chat_id,
                                                r.scheduled_target_ids.clone(),
                                                r.scheduled_origin_ids.clone(),
                                            )
                                        })
                                    }
                                    _ => None,
                                };
                                let mission_exists = state.missions.contains_key(&mkey)
                                    || (!matches!(cmd.as_str(), "koff" | "kpoff" | "kvoff")
                                        && state.missions.contains_key(&legacy_mkey));
                                let found = if let Some(kv) = map.remove(&target_cid) {
                                    kv.1.abort();
                                    true
                                } else {
                                    false
                                };
                                let is_calendar_stop =
                                    matches!(cmd.as_str(), "koff" | "kpoff" | "kvoff");
                                let mut scheduled_echoes_removed = 0usize;
                                let mut scheduled_target_removed = 0usize;
                                if is_calendar_stop {
                                    let wrong_calendar_echo_seen;
                                    (scheduled_echoes_removed, wrong_calendar_echo_seen) =
                                        delete_scheduled_calendar_echoes_in_chat(
                                            &client,
                                            &message.chat(),
                                            task_type,
                                            target_cid,
                                        )
                                        .await;

                                    let has_current_calendar_source =
                                        found || mission_exists || scheduled_echoes_removed > 0;
                                    if !has_current_calendar_source && wrong_calendar_echo_seen {
                                        scheduled_target_removed = 0;
                                    } else {
                                        let mut has_stored_target_ids = false;
                                        if let Some((mirror_id, target_ids, origin_ids)) =
                                            cal_meta.as_ref()
                                        {
                                            has_stored_target_ids = !target_ids.is_empty();
                                            scheduled_target_removed = scheduled_target_removed
                                                .saturating_add(target_ids.len());
                                            if let Some(ch) =
                                                find_chat_by_id(&client, target_cid).await
                                            {
                                                delete_scheduled_ids_in_chat(
                                                    &client, &ch, target_ids,
                                                )
                                                .await;
                                            }
                                            if let Some(o) = *mirror_id {
                                                if let Some(ch) = find_chat_by_id(&client, o).await
                                                {
                                                    delete_scheduled_ids_in_chat(
                                                        &client, &ch, origin_ids,
                                                    )
                                                    .await;
                                                }
                                            }
                                            if target_ids.is_empty() && origin_ids.is_empty() {
                                                log::warn!(
                                                    "calendar stop task={} chat={} has no stored scheduled ids; using typed scheduled cleanup",
                                                    cmd,
                                                    target_cid
                                                );
                                            }
                                        }
                                        if !has_stored_target_ids {
                                            let target_chat_for_cleanup = if target_cid == chat_id {
                                                Some(message.chat().clone())
                                            } else {
                                                find_chat_by_id(&client, target_cid).await
                                            };
                                            if let Some(ch) = target_chat_for_cleanup {
                                                let removed =
                                                    delete_scheduled_calendar_payloads_in_chat(
                                                        &client, &ch, task_type,
                                                    )
                                                    .await;
                                                scheduled_target_removed = scheduled_target_removed
                                                    .saturating_add(removed);
                                                log::info!(
                                                    "calendar stop task={} chat={} typed scheduled cleanup removed {} messages",
                                                    cmd,
                                                    target_cid,
                                                    removed
                                                );
                                            } else {
                                                log::warn!(
                                                    "calendar stop task={} chat={} cannot resolve target chat for typed scheduled cleanup",
                                                    cmd,
                                                    target_cid
                                                );
                                            }
                                        }
                                    }
                                }
                                if found
                                    || mission_exists
                                    || scheduled_echoes_removed > 0
                                    || scheduled_target_removed > 0
                                {
                                    state.missions.remove(&mkey);
                                    if !matches!(cmd.as_str(), "koff" | "kpoff" | "kvoff") {
                                        state.missions.remove(&legacy_mkey);
                                    }
                                    state.save_persistence();
                                    log_to_bot(
                                        &state,
                                        &rayzen_stop_log(cmd.as_str(), prefix, target_cid),
                                    )
                                    .await;
                                    let res_text = stop_success_text(cmd.as_str(), target_cid);
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(&res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(&res_text)).await;
                                    }
                                } else {
                                    let res_text = match cmd.as_str() {
                                        "off" | "offsr" => format!("⛧ Спам не был включен в чате с ID: <code>{}</code> ⛧", target_cid),
                                        "offp" => format!("⛧ Спам с фото не был включен в чате с ID: <code>{}</code> ⛧", target_cid),
                                        "offv" => format!("⛧ Спам с видео не был включен в чате с ID: <code>{}</code> ⛧", target_cid),
                                        "tagoff" => format!("⛧ Теггер не был запущен в чате с ID: <code>{}</code> ⛧", target_cid),
                                        "tagpoff" => format!("⛧ Теггер с фото не был запущен в чате с ID: <code>{}</code> ⛧", target_cid),
                                        "tagvoff" => format!("⛧ Теггер с видео не был включен в чате с ID: <code>{}</code> ⛧", target_cid),
                                        "koff" => format!("⛧ Календарь не был включен в чате с ID: <code>{}</code> ⛧", target_cid),
                                        "kpoff" => format!("⛧ Календарь с фото не был включен в чате с ID: <code>{}</code> ⛧", target_cid),
                                        "kvoff" => format!("⛧ Календарь с видео не был включен в чате с ID: <code>{}</code> ⛧", target_cid),
                                        _ => format!("⛧ Спам не был включен в чате с ID: <code>{}</code> ⛧", target_cid),
                                    };
                                    if is_token_bot {
                                        let _ = client
                                            .send_message(message.chat(), html_msg(&res_text))
                                            .await;
                                    } else {
                                        let _ = message.edit(html_msg(&res_text)).await;
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
}
