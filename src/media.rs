use anyhow::Result;

pub async fn upload_to_catbox(data: Vec<u8>, filename: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let part = reqwest::multipart::Part::bytes(data)
        .file_name(filename.to_string());
    let form = reqwest::multipart::Form::new()
        .text("reqtype", "fileupload")
        .text("userhash", "")
        .part("fileToUpload", part);
    let resp = client
        .post("https://catbox.moe/user/api.php")
        .multipart(form)
        .send()
        .await?;
    let url = resp.text().await?;
    Ok(url.trim().to_string())
}

pub async fn download_bytes(url: &str) -> Result<(Vec<u8>, String)> {
    let client = reqwest::Client::new();
    let resp = client.get(url).send().await?;
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("image/jpeg")
        .to_string();
    let ext = if ct.contains("video") { "mp4" } else { "jpg" };
    let bytes = resp.bytes().await?.to_vec();
    Ok((bytes, format!("file.{}", ext)))
      }
