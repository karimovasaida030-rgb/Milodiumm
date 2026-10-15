// хз почему это тут, вроде надо
use std::io::IsTerminal;

use dashmap::DashMap;
use tokio::io::{AsyncBufReadExt, BufReader};

fn first_nonempty_line(s: &str) -> String {
    s.strip_prefix('\u{FEFF}')
        .unwrap_or(s)
        .lines()
        .find(|l| !l.trim().is_empty())
        .map(|l| l.trim().to_string())
        .unwrap_or_default()
}

pub fn load_logger_files_account(
    path_clean: &str,
    bot_token: &mut String,
    settings: &DashMap<String, String>,
) {
    let token_dir = format!("{}/TOKEN", path_clean);
    let _ = std::fs::create_dir_all(&token_dir);

    let log_path = format!("{}/log_id.txt", token_dir);
    if let Ok(lid) = std::fs::read_to_string(&log_path) {
        let v = first_nonempty_line(&lid);
        if !v.is_empty() {
            settings.insert("log_id".to_string(), v);
        }
    } else if let Ok(lid) = std::fs::read_to_string("/root/rayzen-bot/TOKEN/log_id.txt") {
        let v = first_nonempty_line(&lid);
        if !v.is_empty() {
            settings.insert("log_id".to_string(), v);
        }
    }

    let tok_path = format!("{}/logger_bot_token.txt", token_dir);
    if let Ok(t) = std::fs::read_to_string(&tok_path) {
        let tok = first_nonempty_line(&t);
        if !tok.is_empty() {
            *bot_token = tok;
        }
    }
}

async fn read_line_stdin(hint: &str) -> String {
    println!("{}", hint);
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut line = String::new();
    let _ = stdin.read_line(&mut line).await;
    line.trim().to_string()
}

pub async fn interactive_fill_account(
    path_clean: &str,
    bot_token: &mut String,
    settings: &DashMap<String, String>,
) {
    let log_id = settings
        .get("log_id")
        .map(|v| v.value().clone())
        .unwrap_or_default();
    let need_id = log_id.trim().is_empty();
    let need_tok = bot_token.trim().is_empty();

    if !need_id && !need_tok {
        return;
    }

    if !std::io::stdin().is_terminal() {
        if need_tok || need_id {
            log::warn!(
                "Логгер не настроен: нет {}. Создайте {}/TOKEN/log_id.txt и logger_bot_token.txt или LOGGER_BOT_TOKEN.",
                if need_tok && need_id {
                    "токена и chat_id"
                } else if need_tok {
                    "токена бота-логгера"
                } else {
                    "chat_id"
                },
                path_clean
            );
        }
        return;
    }

    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Логгер (как в Rayzen)                                       ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Отдельный бот из @BotFather шлёт логи в указанный чат.      ║");
    println!("║  Chat ID — куда слать (свой id, группа, канал с id).         ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    let token_dir = format!("{}/TOKEN", path_clean);
    let _ = std::fs::create_dir_all(&token_dir);

    if need_tok {
        let t =
            read_line_stdin("Токен бота-логгера (формат 123456789:AAH... ; Enter — пропустить):")
                .await;
        if !t.is_empty() {
            *bot_token = t.clone();
            let _ = std::fs::write(
                format!("{}/logger_bot_token.txt", token_dir),
                format!("{}\n", t),
            );
        }
    }
    if need_id {
        let id = read_line_stdin("Chat ID для логов (число; Enter — пропустить):").await;
        if !id.is_empty() {
            settings.insert("log_id".to_string(), id.clone());
            let _ = std::fs::write(format!("{}/log_id.txt", token_dir), format!("{}\n", id));
        }
    }
}
