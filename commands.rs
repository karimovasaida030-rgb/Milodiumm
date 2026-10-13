use teloxide::utils::command::BotCommands;

#[derive(BotCommands, Clone, Debug)]
#[command(rename_rule = "lowercase", description = "Rayzen Bot Commands")]
#[allow(non_camel_case_types)]
pub enum Command {
    #[command(description = "Help")]
    help,
    #[command(description = "Settings")]
    settings,
    #[command(description = "Ping")]
    ping,
    #[command(description = "Direct")]
    direct,
    #[command(description = "Id")]
    id,
    #[command(description = "User")]
    user,
    #[command(description = "Clear")]
    clear,
    #[command(description = "Cclean")]
    cclean,
    #[command(description = "Uptime")]
    uptime,
    #[command(description = "Online")]
    online,
    #[command(description = "Offline")]
    offline,
    #[command(description = "Admin")]
    admin(u64),
    #[command(description = "Unadmin")]
    unadmin(u64),
    #[command(description = "Mute")]
    mute(u64),
    #[command(description = "Unmute")]
    unmute(u64),
    #[command(description = "Afk")]
    afk(String),
    #[command(description = "Afk2")]
    afk2,
    #[command(description = "Bot")]
    bot(String),
    #[command(description = "Link")]
    link(String),
    #[command(description = "Ar")]
    ar(String),
    #[command(description = "Arp")]
    arp(String),
    #[command(description = "Arv")]
    arv(String),
    #[command(description = "Tag")]
    tag(String),
    #[command(description = "Tagp")]
    tagp(String),
    #[command(description = "Tagv")]
    tagv(String),
    #[command(description = "Kal")]
    kal(String),
    #[command(description = "Kalp")]
    kalp(String),
    #[command(description = "Kalv")]
    kalv(String),
    #[command(description = "Sp")]
    sp(String),
    off(i64),
    offsr(i64),
    koff(i64),
    tagoff(i64),
    offp(i64),
    kpoff(i64),
    tagpoff(i64),
    offv(i64),
    kvoff(i64),
    tagvoff(i64),
    #[command(description = "Check1")]
    check1(String),
    #[command(description = "Uncheck")]
    uncheck(String),
    #[command(description = "Check")]
    check,
    #[command(description = "Ai")]
    ai(String),
    #[command(description = "X0")]
    x0,
    #[command(description = "Save")]
    save,
    #[command(description = "Unsave")]
    unsave(String),
    #[command(description = "Drop")]
    drop(String),
    #[command(description = "Reg")]
    reg,
    #[command(description = "Unss")]
    uss(u64),
    #[command(description = "Unssm")]
    ussm(u64),
    #[command(description = "All")]
    all(u64),
    #[command(description = "Unall")]
    unall(u64),
    #[command(description = "Us")]
    us(String),
    #[command(description = "Usm")]
    usm(String),
}
