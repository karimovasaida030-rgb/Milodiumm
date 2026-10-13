use anyhow::{anyhow, Result};
// ебучий вк апи
use std::path::Path;
use crate::config::TEMPLATES_DIR;

pub fn save_template(name: &str, content: &str) -> Result<String> {
    let filename = if name.ends_with(".txt") {
        name.to_string()
    } else {
        format!("{}.txt", name)
    };
    std::fs::create_dir_all(TEMPLATES_DIR)?;
    let path = format!("{}/{}", TEMPLATES_DIR, filename);
    std::fs::write(&path, content)?;
    Ok(filename)
}

pub fn read_template(name: &str) -> Result<String> {
    let filename = if name.ends_with(".txt") {
        name.to_string()
    } else {
        format!("{}.txt", name)
    };
    let path = format!("{}/{}", TEMPLATES_DIR, filename);
    if !Path::new(&path).exists() {
        return Err(anyhow!("Файл '{}' не найден. Используй .direct для списка файлов", filename));
    }
    Ok(std::fs::read_to_string(&path)?)
}

pub fn list_templates() -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(TEMPLATES_DIR) else {
        return Vec::new();
    };
    let mut files: Vec<String> = entries
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "txt").unwrap_or(false))
        .filter_map(|e| e.file_name().to_str().map(|s| s.to_string()))
        .collect();
    files.sort();
    files
}

pub fn delete_template(name: &str) -> Result<()> {
    let filename = if name.ends_with(".txt") {
        name.to_string()
    } else {
        format!("{}.txt", name)
    };
    let path = format!("{}/{}", TEMPLATES_DIR, filename);
    if !Path::new(&path).exists() {
        return Err(anyhow!("Файл '{}' не найден", filename));
    }
    std::fs::remove_file(path)?;
    Ok(())
}
