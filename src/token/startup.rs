// крашнется - и хер с ним
use std::sync::Arc;

use teloxide::prelude::*;
use teloxide::types::ParseMode;

use crate::token::config::Config;
use crate::token::spam_engine::{run_calendar, run_spam};
use crate::token::state::{BotState, PersistentTask};

pub fn inject_token_files(base_path: &str, state: &mut BotState) {
    let base = base_path.trim_end_matches('/');
    let _ = (base, state); // логгер-файлы больше не читаем для токен-бота
}

fn startup_html(total_tasks: usize) -> String {
    format!(
        "<b>⛧ БОТ В СЕТИ ⛧</b>\n\
         <b>⛧ Сервис был перезапущен ⛧</b>\n\
         <b>⛧ Восстановлено задач: {} ⛧</b>",
        total_tasks
    )
}

pub async fn restore_missions(bot: &Bot, state: Arc<BotState>, owner_id: u64) -> usize {
    let missions: Vec<PersistentTask> = state.missions.iter().map(|e| e.value().clone()).collect();
    let mut count = 0usize;
    let b = Arc::new(bot.clone());

    for m in missions {
        let task_type = m.task_type.as_str();
        let cid = ChatId(m.chat_id);
        let delay = m.delay;
        let file = m.file_name.clone();
        let shapka = m.shapka.clone();
        let uid = m.user_id;
        let media = m.media.clone();
        let is_kal = matches!(task_type, "kal" | "kalp" | "kalv");

        let origin = ChatId(m.origin_chat_id.unwrap_or(m.chat_id));
        let echo = m.kal_command_echo.clone().unwrap_or_default();
        let h = if is_kal {
            tokio::spawn(run_calendar(
                b.clone(),
                state.clone(),
                cid,
                origin,
                delay,
                file,
                shapka,
                media,
                owner_id,
                echo,
                task_type.to_string(),
            ))
        } else {
            let wait_ms = delay.saturating_mul(1000).max(1);
            let log_sr = task_type == "sp";
            tokio::spawn(run_spam(
                b.clone(),
                state.clone(),
                cid,
                wait_ms,
                file,
                shapka,
                uid,
                media,
                task_type.to_string(),
                log_sr,
                owner_id,
            ))
        };

        match task_type {
            "ar" => {
                state.spam_tasks_ar.insert(cid, h);
            }
            "sp" => {
                state.spam_tasks_sp.insert(cid, h);
            }
            "arp" => {
                state.spam_tasks_arp.insert(cid, h);
            }
            "arv" => {
                state.spam_tasks_arv.insert(cid, h);
            }
            "tag" => {
                state.spam_tasks_tag.insert(cid, h);
            }
            "tagp" => {
                state.spam_tasks_tagp.insert(cid, h);
            }
            "tagv" => {
                state.spam_tasks_tagv.insert(cid, h);
            }
            "kal" => {
                state.calendar_tasks.insert(cid, h);
            }
            "kalp" => {
                state.calendar_p_tasks.insert(cid, h);
            }
            "kalv" => {
                state.calendar_v_tasks.insert(cid, h);
            }
            _ => {
                h.abort();
                continue;
            }
        }
        count += 1;
    }
    count
}

pub async fn send_startup_announcement(
    bot: &Bot,
    state: &Arc<BotState>,
    config: &Config,
    restored_missions: usize,
) {
    let total = restored_missions
        + state.check1_tasks.len()
        + state.auto_replies_text.len()
        + state.auto_replies_media.len()
        + state.global_auto_replies_text.len()
        + state.global_auto_replies_media.len();
    let text = startup_html(total);
    let owner_cid = ChatId(config.owner_id as i64);
    let _ = bot
        .send_message(owner_cid, &text)
        .parse_mode(ParseMode::Html)
        .await;
}
