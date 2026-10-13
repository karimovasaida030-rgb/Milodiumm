use crate::ntfy;
use crate::token::state::BotState;
use crate::token::utils::{escape, format_unix_local_datetime};
// крашнется - и хер с ним
use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::{ChatId, ParseMode};
use tokio::time::{sleep, Duration};

pub async fn run_checker_loop(bot: Arc<Bot>, state: Arc<BotState>, owner_id: u64) {
    loop {
        let now_ts = chrono::Utc::now().timestamp();
        let mut to_notify: Vec<(String, i64, u64, u64, i64)> = Vec::new();

        {
            for entry in state.check1_tasks.iter() {
                let key = entry.key().clone();
                let task = entry.value();
                let last = task.last_seen_unix;
                if last <= 0 {
                    continue;
                }
                if task.silence_notified {
                    continue;
                }
                let silence = now_ts.saturating_sub(last);
                if silence > task.delay as i64 {
                    to_notify.push((key, task.chat_id, task.user_id, task.delay, last));
                }
            }
        }

        let notify_count = to_notify.len();
        for (key, chat_id, target_uid, delay, prev_last_unix) in to_notify {
            let Some((silence, last_human)) =
                state.check1_tasks.get_mut(&key).and_then(|mut task| {
                    if task.silence_notified {
                        return None;
                    }
                    let current_last = task.last_seen_unix.max(prev_last_unix);
                    let current_silence = now_ts.saturating_sub(current_last);
                    if current_silence <= task.delay as i64 {
                        return None;
                    }
                    task.silence_notified = true;
                    Some((current_silence, format_unix_local_datetime(current_last)))
                })
            else {
                continue;
            };

            let msg = format!(
                "<b>⛧ Чекер ⛧</b>\nПользователь <a href=\"tg://user?id={}\">{}</a> не писал <code>{}</code> сек (порог <code>{}</code> сек).\nПоследнее сообщение: <code>{}</code>\nЧат: <code>{}</code>",
                target_uid,
                target_uid,
                silence,
                delay,
                escape(&last_human),
                chat_id
            );
            let push = format!(
                "check1: user {} молчит {} сек (порог {} сек), chat {}, последнее: {}",
                target_uid, silence, delay, chat_id, last_human
            );
            ntfy::send_check1_alert(
                &state.base_path,
                state.get_setting("ntfy_topic").into(),
                &push,
            )
            .await;

            let _ = bot
                .send_message(ChatId(owner_id as i64), msg)
                .parse_mode(ParseMode::Html)
                .await;
        }
        if notify_count > 0 {
            state.save_persistence();
        }

        {
            let mut expired_keys: Vec<String> = Vec::new();
            for entry in state.check1_tasks.iter() {
                let key = entry.key().clone();
                let task = entry.value();
                let last = task.last_seen_unix;
                if last > 0 {
                    let silence = now_ts.saturating_sub(last);
                    if silence > task.delay as i64 {
                        expired_keys.push(key);
                    }
                }
            }

            let expired_count = expired_keys.len();
            if expired_count > 0 {
                for key in expired_keys {
                    state.check1_tasks.remove(&key);
                }
                log::debug!("Removed {} expired check1 tasks", expired_count);
                state.save_persistence();
            }
        }

        {
            let fix_ts = chrono::Utc::now().timestamp();
            for mut entry in state.check1_tasks.iter_mut() {
                if entry.value().last_seen_unix <= 0 {
                    entry.value_mut().last_seen_unix = fix_ts;
                }
            }
        }

        sleep(Duration::from_secs(5)).await;
    }
}
