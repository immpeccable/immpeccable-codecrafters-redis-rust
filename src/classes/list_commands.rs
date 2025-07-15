use crate::classes::{RespDataType::RespDataType, State::State};
use std::sync::Arc;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::io::AsyncWriteExt;

pub async fn handle_rpush(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    if commands.len() < 3 {
        stream.lock().await
            .write_all(RespDataType::SimpleError("ERR wrong number of arguments for RPUSH command".to_string()).to_string().as_bytes())
            .await
            .unwrap();
        return;
    }

    let key = &commands[1];
    let elements: Vec<String> = commands[2..].to_vec();
    
    let list_length = state.lock().await.rpush(key.clone(), elements).await;
    
    stream.lock().await
        .write_all(RespDataType::Integer(list_length as i64).to_string().as_bytes())
        .await
        .unwrap();
}

pub async fn handle_lrange(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    if commands.len() != 4 {
        stream.lock().await
            .write_all(RespDataType::SimpleError("ERR wrong number of arguments for LRANGE command".to_string()).to_string().as_bytes())
            .await
            .unwrap();
        return;
    }

    let key = &commands[1];
    let start = match commands[2].parse::<i64>() {
        Ok(n) => n,
        Err(_) => {
            stream.lock().await
                .write_all(RespDataType::SimpleError("ERR value is not an integer or out of range".to_string()).to_string().as_bytes())
                .await
                .unwrap();
            return;
        }
    };
    let stop = match commands[3].parse::<i64>() {
        Ok(n) => n,
        Err(_) => {
            stream.lock().await
                .write_all(RespDataType::SimpleError("ERR value is not an integer or out of range".to_string()).to_string().as_bytes())
                .await
                .unwrap();
            return;
        }
    };
    
    let elements = state.lock().await.lrange(key, start, stop).await;
    
    // Convert elements to RESP BulkString array
    let resp_elements: Vec<RespDataType> = elements.into_iter()
        .map(|element| RespDataType::BulkString(element))
        .collect();
    
    stream.lock().await
        .write_all(RespDataType::Array(resp_elements).to_string().as_bytes())
        .await
        .unwrap();
}
