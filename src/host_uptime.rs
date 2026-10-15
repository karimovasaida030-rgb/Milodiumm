#[cfg(target_os = "linux")]
// костыль ебаный
use std::fs;
use std::sync::OnceLock;
use std::time::Instant;
#[cfg(target_os = "linux")]
use std::time::{SystemTime, UNIX_EPOCH};

pub fn os_uptime_secs() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = fs::read_to_string("/proc/uptime") {
            if let Some(first) = s.split_whitespace().next() {
                if let Ok(v) = first.parse::<f64>() {
                    return v.floor().max(0.0) as u64;
                }
            }
        }

        if let Ok(stat) = fs::read_to_string("/proc/stat") {
            if let Some(line) = stat.lines().find(|l| l.starts_with("btime ")) {
                if let Some(ts) = line.split_whitespace().nth(1) {
                    if let Ok(btime) = ts.parse::<u64>() {
                        if let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) {
                            return now.as_secs().saturating_sub(btime);
                        }
                    }
                }
            }
        }
    }
    let v = sysinfo::System::uptime();
    if v > 0 {
        return v;
    }
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_secs()
}


pub fn format_uptime_compact(secs: u64) -> String {
    let d = secs / 86400;
    let h = (secs % 86400) / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{}d {:02}:{:02}:{:02}", d, h, m, s)
}
