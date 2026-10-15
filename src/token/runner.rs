// TODO: переписать это говно
use std::sync::Arc;

use anyhow::Context;
use teloxide::dispatching::Dispatcher;
use teloxide::prelude::*;
use tokio::time::{sleep, Duration};

use crate::token::checker::run_checker_loop;
use crate::token::config::Config;
use crate::token::handlers::{message_handler, replay_saved_token_auto_replies};
use crate::token::startup::{inject_token_files, restore_missions, send_startup_announcement};
use crate::token::state::BotState;

pub async fn run_teloxide_bot(config: Config) -> anyhow::Result<()> {
    let bot = Bot::new(config.teloxide_token.clone());
    let me = bot.get_me().await.context("Teloxide getMe")?;
    let bot_name = if !config.bot_username.trim().is_empty() {
        config.bot_username.clone()
    } else {
        me.user
            .username
            .clone()
            .unwrap_or_else(|| "teloxide_bot".to_string())
    };

    let mut bot_state = BotState::new(bot_name.clone(), config.base_path.clone());
    bot_state.load_persistence();
    let now_ts = chrono::Utc::now().timestamp();
    for mut entry in bot_state.check1_tasks.iter_mut() {
        let t = entry.value_mut();
        t.last_seen_unix = now_ts;
        t.silence_notified = false;
    }
    bot_state.save_persistence();
    inject_token_files(&config.base_path, &mut bot_state);
    for aid in &config.admin_ids {
        bot_state.admins.insert(*aid, true);
    }
    let state = Arc::new(bot_state);

    let restored = restore_missions(&bot, state.clone(), config.owner_id).await;
    send_startup_announcement(&bot, &state, &config, restored).await;
    let total_startup = restored
        + state.check1_tasks.len()
        + state.auto_replies_text.len()
        + state.auto_replies_media.len()
        + state.global_auto_replies_text.len()
        + state.global_auto_replies_media.len();
    log::info!(
        "Teloxide «{}»: восстановлено задач (всего): {}",
        bot_name,
        total_startup
    );
    let config_arc = Arc::new(config.clone());
    replay_saved_token_auto_replies(bot.clone(), state.clone(), config_arc.clone());

    let state_worker = state.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(600)).await;
            state_worker.save_persistence();
        }
    });

    let bot_check = Arc::new(bot.clone());
    let state_check = state.clone();
    let owner_c = config.owner_id;
    tokio::spawn(async move {
        run_checker_loop(bot_check, state_check, owner_c).await;
    });

    let handler = dptree::entry().branch(Update::filter_message().endpoint(message_handler));
    Dispatcher::builder(bot.clone(), handler)
        .dependencies(dptree::deps![state, config_arc])
        .build()
        .dispatch_with_listener(
            teloxide::update_listeners::polling_default(bot).await,
            Arc::new(|err| async move {
                log::error!("Teloxide dispatch error: {:?}", err);
            }),
        )
        .await;
    Ok(())
}
