use crate::classes::{RespDataType::RespDataType, State::State};
use std::sync::Arc;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::io::AsyncWriteExt;

pub async fn handle_multi(
    _commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    in_multi: &mut bool,
) {
    *in_multi = true;
    stream.lock().await.write_all(RespDataType::SimpleString("OK".to_string()).to_string().as_bytes()).await.unwrap();
}

pub async fn handle_discard(
    _commands: &mut Vec<String>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    queued: &mut Vec<Vec<String>>,
    in_multi: &mut bool,
) {
    if *in_multi == false {
        writer
            .lock()
            .await
            .write_all(RespDataType::SimpleError("ERR DISCARD without MULTI".to_string()).to_string().as_bytes())
            .await
            .unwrap();
    } else {
        writer.lock().await.write_all(RespDataType::SimpleString("OK".to_string()).to_string().as_bytes()).await.unwrap();
        queued.clear();
        *in_multi = false;
    }
}

pub async fn handle_exec(
    _commands: &mut Vec<String>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    queued: &mut Vec<Vec<String>>,
    in_multi: &mut bool,
) {
    if *in_multi == false {
        writer
            .lock()
            .await
            .write_all(RespDataType::SimpleError("ERR EXEC without MULTI".to_string()).to_string().as_bytes())
            .await
            .unwrap();
    } else {
        writer
            .lock()
            .await
            .write_all(format!("*{}\r\n", queued.len()).as_bytes())
            .await
            .unwrap();
    }
} 