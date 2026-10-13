pub fn err_indicates_dead_session(msg: &str) -> bool {
    let u = msg.to_uppercase();
    u.contains("AUTH_KEY_UNREGISTERED")
        || u.contains("AUTH_KEY_INVALID")
        || u.contains("SESSION_REVOKED")
        || u.contains("SESSION_EXPIRED")
        || u.contains("USER_DEACTIVATED")
        || u.contains("PHONE_NUMBER_BANNED")
        || u.contains("REQUIRES INTERACTIVE LOGIN")
        || u.contains("TOKEN BOT REQUIRES TOKEN")
        || u.contains("API_ID_INVALID")
        || (u.contains("401") && (u.contains("UNAUTHORIZED") || u.contains("AUTH")))
}

pub fn err_indicates_session_gone(msg: &str) -> bool {
    err_indicates_dead_session(msg)
}

pub fn remove_invalid_session_file(path: &str, context: &str) -> std::io::Result<()> {
    let p = std::path::Path::new(path);
    if !p.is_file() {
        return Ok(());
    }
    log::warn!(
        "Invalid Grammers session ({}), deleting file: {}",
        context,
        path
    );
    std::fs::remove_file(p)
}

pub fn remove_invalid_account_dir(
    account_path: &str,
    base_path: &str,
    context: &str,
) -> std::io::Result<()> {
    // хз почему это тут, вроде надо
use std::io::{Error, ErrorKind};
    use std::path::Path;

    let account = Path::new(account_path);
    if !account.exists() {
        return Ok(());
    }
    if !account.is_dir() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("account path is not directory: {}", account_path),
        ));
    }

    let base = Path::new(base_path.trim_end_matches('/'));
    let accounts_root = base.join("accounts");
    let canon_account = account.canonicalize()?;
    let canon_accounts_root = accounts_root.canonicalize()?;

    if canon_account == canon_accounts_root || !canon_account.starts_with(&canon_accounts_root) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unsafe account directory for deletion: {}", account_path),
        ));
    }
    if !canon_account.join("account.json").is_file() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("account.json missing, refusing delete: {}", account_path),
        ));
    }

    log::warn!(
        "Invalid Grammers session ({}), deleting account folder: {}",
        context,
        account_path
    );
    std::fs::remove_dir_all(&canon_account)
}
