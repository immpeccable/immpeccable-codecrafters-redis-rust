use crate::classes::{RespDataType::RespDataType, State::State};
use std::sync::Arc;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::time::{timeout, Duration};
use std::time::Instant;

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

pub async fn handle_lpush(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    if commands.len() < 3 {
        stream.lock().await
            .write_all(RespDataType::SimpleError("ERR wrong number of arguments for LPUSH command".to_string()).to_string().as_bytes())
            .await
            .unwrap();
        return;
    }

    let key = &commands[1];
    let elements: Vec<String> = commands[2..].to_vec();
    
    let list_length = state.lock().await.lpush(key.clone(), elements).await;
    
    stream.lock().await
        .write_all(RespDataType::Integer(list_length as i64).to_string().as_bytes())
        .await
        .unwrap();
}

pub async fn handle_llen(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    if commands.len() != 2 {
        stream.lock().await
            .write_all(RespDataType::SimpleError("ERR wrong number of arguments for LLEN command".to_string()).to_string().as_bytes())
            .await
            .unwrap();
        return;
    }
    let key = &commands[1];
    let len = state.lock().await.llen(key).await;
    stream.lock().await
        .write_all(RespDataType::Integer(len as i64).to_string().as_bytes())
        .await
        .unwrap();
}

pub async fn handle_lpop(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    if commands.len() < 2 || commands.len() > 3 {
        stream.lock().await
            .write_all(RespDataType::SimpleError("ERR wrong number of arguments for LPOP command".to_string()).to_string().as_bytes())
            .await
            .unwrap();
        return;
    }

    let key = &commands[1];
    if commands.len() == 3 {
        // LPOP key count
        let count = match commands[2].parse::<usize>() {
            Ok(n) if n > 0 => n,
            _ => {
                stream.lock().await
                    .write_all(RespDataType::SimpleError("ERR value is not an integer or out of range".to_string()).to_string().as_bytes())
                    .await
                    .unwrap();
                return;
            }
        };
        let popped = state.lock().await.lpop_multiple(key, count).await;
        let resp = RespDataType::Array(popped.into_iter().map(RespDataType::BulkString).collect());
        stream.lock().await
            .write_all(resp.to_string().as_bytes())
            .await
            .unwrap();
    } else {
        // LPOP key
        let popped_element = state.lock().await.lpop(key).await;
        match popped_element {
            Some(element) => {
                stream.lock().await
                    .write_all(RespDataType::BulkString(element).to_string().as_bytes())
                    .await
                    .unwrap();
            }
            None => {
                stream.lock().await
                    .write_all(RespDataType::Nil.to_string().as_bytes())
                    .await
                    .unwrap();
            }
        }
    }
}

pub async fn handle_blpop(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    if commands.len() != 3 {
        stream.lock().await
            .write_all(RespDataType::SimpleError("ERR wrong number of arguments for BLPOP command".to_string()).to_string().as_bytes())
            .await
            .unwrap();
        return;
    }
    let key = &commands[1];
    let timeout_str = &commands[2];
    let timeout_secs: f64 = match timeout_str.parse() {
        Ok(n) => n,
        Err(_) => {
            stream.lock().await
                .write_all(RespDataType::SimpleError("ERR timeout is not a float".to_string()).to_string().as_bytes())
                .await
                .unwrap();
            return;
        }
    };
    // Try to pop immediately
    let popped = state.lock().await.lpop(key).await;
    if let Some(element) = popped {
        let resp = RespDataType::Array(vec![
            RespDataType::BulkString(key.to_string()),
            RespDataType::BulkString(element),
        ]);
        stream.lock().await
            .write_all(resp.to_string().as_bytes())
            .await
            .unwrap();
        return;
    }
    // If timeout is 0, block indefinitely (old behavior)
    if timeout_secs == 0.0 {
        state.lock().await.register_blpop_blocked_client(key, stream.clone()).await;
        return;
    }
    // Otherwise, block for the specified timeout
    let state_clone = state.clone();
    let stream_clone = stream.clone();
    let key_clone = key.to_string();
    tokio::spawn(async move {
        let start = Instant::now();
        loop {
            // Try to pop again
            let popped = state_clone.lock().await.lpop(&key_clone).await;
            if let Some(element) = popped {
                let resp = RespDataType::Array(vec![
                    RespDataType::BulkString(key_clone.clone()),
                    RespDataType::BulkString(element),
                ]);
                let _ = stream_clone.lock().await.write_all(resp.to_string().as_bytes()).await;
                break;
            }
            if start.elapsed().as_secs_f64() >= timeout_secs {
                let _ = stream_clone.lock().await.write_all(RespDataType::Nil.to_string().as_bytes()).await;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
}
