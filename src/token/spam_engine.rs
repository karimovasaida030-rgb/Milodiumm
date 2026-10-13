use crate::token::state::BotState;
use crate::token::utils::{
    escape, get_random_choice, pick_template_path, send_resilient, send_resilient_photo,
    send_resilient_video, strip_specific_url,
};
use chrono::Local;
// TODO: переписать это говно
use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::{ChatId, ParseMode};
use tokio::time::{sleep, sleep_until, Duration, Instant as TokioInstant};

fn is_flood_wait_error(err: &str) -> bool {
    let e = err.to_lowercase();
    e.contains("flood")
        || e.contains("retry_after")
        || e.contains("too many requests")
        || e.contains("slowmode")
        || e.contains("wait")
}

fn token_delivery_failure_log(task_type: &str, ts: &str, chat_id: ChatId, err: &str) -> String {
    let headline = if is_flood_wait_error(err) {
        "флудвайт: сообщение не отправлено"
    } else {
        "сообщение не отправлено"
    };
    format!(
        "<code>{}</code> — {}\nChat ID: <code>{}</code>\nКоманда: <code>[{}]</code>\nПричина: <code>{}</code>",
        ts,
        headline,
        chat_id.0,
        escape(task_type),
        escape(err)
    )
}

async fn log_token_delivery_failure(
    bot: &Bot,
    owner_id: u64,
    task_type: &str,
    chat_id: ChatId,
    err: &str,
) {
    log::warn!(
        "token delivery failed task={} chat={} err={}",
        task_type,
        chat_id.0,
        err
    );
    if matches!(task_type, "kal" | "kalp" | "kalv") {
        return;
    }
    let ts = Local::now().format("%H:%M:%S.%d.%m.%Y").to_string();
    let line = token_delivery_failure_log(task_type, &ts, chat_id, err);
    let _ = send_resilient(bot, ChatId(owner_id as i64), line, None).await;
}

pub async fn run_spam(
    bot: Arc<Bot>,
    state: Arc<BotState>,
    chat_id: ChatId,
    wait_ms: u64,
    file_name: String,
    shapka: String,
    user_id: Option<u64>,
    media: Option<(String, bool)>,
    task_type: String,
    log_each_send_sr: bool,
    owner_id: u64,
) {
    let interval = Duration::from_millis(if wait_ms == 0 { 300 } else { wait_ms });
    let mut next_tick = TokioInstant::now();
    loop {
        let now = TokioInstant::now();
        if next_tick > now {
            sleep_until(next_tick).await;
        }
        next_tick = TokioInstant::now() + interval;

        let path = pick_template_path(&state.base_path, &file_name).await;
        let content = match tokio::fs::read_to_string(&path).await {
            Ok(c) => c,
            Err(e) => {
                log::warn!("token spam: template read failed path={} err={}", path, e);
                let _ = send_resilient(
                    &bot,
                    ChatId(owner_id as i64),
                    format!(
                        "⛧ Шаблон не найден/не читается ⛧\nFile: <code>{}</code>\nPath: <code>{}</code>",
                        escape(&file_name),
                        escape(&path)
                    ),
                    None,
                )
                .await;
                sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        {
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
                    format!("{} {}", escape(&shapka), escape(&l))
                };

                if let Some((url, _)) = &media {
                    text = strip_specific_url(&text, url);
                }

                let send_ok = if let Some((url, is_v)) = &media {
                    let r = if *is_v {
                        send_resilient_video(&bot, chat_id, url.clone(), text.clone(), None).await
                    } else {
                        send_resilient_photo(&bot, chat_id, url.clone(), text.clone(), None).await
                    };
                    if let Err(e) = &r {
                        let ts = Local::now().format("%H:%M:%S.%d.%m.%Y").to_string();
                        let err = format!("{}; URL: {}", e, url);
                        let line = token_delivery_failure_log(&task_type, &ts, chat_id, &err);
                        log::warn!(
                            "token spam media send failed task={} chat={} url={} err={}",
                            task_type,
                            chat_id.0,
                            url,
                            e
                        );
                        let _ = send_resilient(&bot, ChatId(owner_id as i64), line, None).await;
                    }
                    r.is_ok()
                } else {
                    match send_resilient(&bot, chat_id, text, None).await {
                        Ok(_) => true,
                        Err(e) => {
                            log_token_delivery_failure(
                                &bot,
                                owner_id,
                                &task_type,
                                chat_id,
                                &e.to_string(),
                            )
                            .await;
                            false
                        }
                    }
                };

                let _ = log_each_send_sr;
                let _ = send_ok;
            }
        }
    }
}

fn calendar_still_active(state: &BotState, chat_id: ChatId, task_type: &str) -> bool {
    match task_type {
        "kal" => state.calendar_tasks.contains_key(&chat_id),
        "kalp" => state.calendar_p_tasks.contains_key(&chat_id),
        "kalv" => state.calendar_v_tasks.contains_key(&chat_id),
        _ => false,
    }
}

pub async fn run_calendar(
    bot: Arc<Bot>,
    state: Arc<BotState>,
    target_chat_id: ChatId,
    origin_chat_id: ChatId,
    delay_minutes: u64,
    file_name: String,
    shapka: String,
    media: Option<(String, bool)>,
    owner_id: u64,
    command_echo: String,
    mission_task_type: String,
) {
    let delay_minutes = delay_minutes.clamp(1, 1440);
    'cycle: loop {
        if !calendar_still_active(&state, target_chat_id, &mission_task_type) {
            break;
        }
        let path = pick_template_path(&state.base_path, &file_name).await;
        let content = match tokio::fs::read_to_string(&path).await {
            Ok(c) => c,
            Err(_) => {
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        let lines: Vec<String> = content
            .lines()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        if lines.is_empty() {
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        for i in 1u32..=100 {
            if !calendar_still_active(&state, target_chat_id, &mission_task_type) {
                break 'cycle;
            }
            if i > 1 {
                let jitter = (rand::random::<u64>() % 59) + 1;
                let wait_secs = delay_minutes.saturating_mul(60).saturating_add(jitter);
                sleep(Duration::from_secs(wait_secs.max(1))).await;
            }
            if !calendar_still_active(&state, target_chat_id, &mission_task_type) {
                break 'cycle;
            }

            if i == 100 {
                let tail = if command_echo.trim().is_empty() {
                    format!(
                        "⛧ Календарь: цикл 100/100 ⛧\n⛧ Цель <code>{}</code> ⛧",
                        target_chat_id.0
                    )
                } else {
                    format!(
                        "⛧ Календарь: цикл 100/100 ⛧\n{}",
                        escape(command_echo.trim())
                    )
                };
                if let Err(e) = send_resilient(&bot, origin_chat_id, tail, None).await {
                    log_token_delivery_failure(
                        &bot,
                        owner_id,
                        &mission_task_type,
                        origin_chat_id,
                        &e.to_string(),
                    )
                    .await;
                }
                let _ = bot
                    .send_message(
                        ChatId(owner_id as i64),
                        "⛧ КАЛЕНДАРЬ ЗАВЕРШЕН (100/100) — цикл повторяется ⛧",
                    )
                    .parse_mode(ParseMode::Html)
                    .await;
                continue;
            }

            if let Some(l) = get_random_choice(lines.clone()) {
                let mut text = format!("{} {}", escape(&shapka), escape(&l));
                if let Some((url, _)) = &media {
                    text = strip_specific_url(&text, url);
                }
                let send_result = if let Some((url, is_v)) = &media {
                    if *is_v {
                        send_resilient_video(&bot, target_chat_id, url.clone(), text, None).await
                    } else {
                        send_resilient_photo(&bot, target_chat_id, url.clone(), text, None).await
                    }
                } else {
                    send_resilient(&bot, target_chat_id, text, None).await
                };
                if let Err(e) = send_result {
                    log_token_delivery_failure(
                        &bot,
                        owner_id,
                        &mission_task_type,
                        target_chat_id,
                        &e.to_string(),
                    )
                    .await;
                }
            }

            if i == 90 {
                let _ = bot
                    .send_message(
                        ChatId(owner_id as i64),
                        "⛧ ВНИМАНИЕ: КАЛЕНДАРЬ ЗАКАНЧИВАЕТСЯ (10/100) ⛧",
                    )
                    .parse_mode(ParseMode::Html)
                    .await;
            }
        }
    }
    state
        .missions
        .remove(&format!("{}_{}", target_chat_id.0, mission_task_type));
    match mission_task_type.as_str() {
        "kal" => {
            state.calendar_tasks.remove(&target_chat_id);
        }
        "kalp" => {
            state.calendar_p_tasks.remove(&target_chat_id);
        }
        "kalv" => {
            state.calendar_v_tasks.remove(&target_chat_id);
        }
        _ => {}
    }
    state.save_persistence();
}
