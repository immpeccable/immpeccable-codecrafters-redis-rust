use crate::classes::{RespDataType::RespDataType, State::State};
use std::sync::Arc;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::io::AsyncWriteExt;
use std::time::Instant;

pub async fn handle_set(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    let key = commands[1].clone();
    let value = commands[2].clone();
    let mut expiration: Option<Instant> = None;
    
    let mut options: Option<&[String]> = None;
    if commands.len() > 3 {
        options = Some(&commands[3..]);
    }

    if let Some(op) = options {
        let option_type = &op[0];

        match option_type.to_uppercase().as_str() {
            "PX" => {
                let option_value = &op[1];
                expiration = Some(
                    Instant::now()
                        + std::time::Duration::from_millis(option_value.parse::<u64>().unwrap()),
                );
            }
            _ => {}
        }
    }

    state.lock().await.set_string(key, value, expiration).await;
    let role = state.lock().await.get_role().await;
    if role == "master" {
        stream.lock().await.write_all(RespDataType::SimpleString("OK".to_string()).to_string().as_bytes()).await.unwrap();
        // Note: propagation to replicas should be handled in the dispatcher for now
    }
}

pub async fn handle_get(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    let key = &commands[1];
    let value = state.lock().await.get_string(key).await;

    match value {
        Some(v) => {
            if let Some(expiration_timestamp) = v.expiration_timestamp {
                if Instant::now() > expiration_timestamp {
                    return stream.lock().await.write_all(RespDataType::Nil.to_string().as_bytes()).await.unwrap();
                }
            }
            let bulk_response = RespDataType::BulkString(v.value).to_string();
            return stream
                .lock()
                .await
                .write_all(bulk_response.as_bytes())
                .await
                .unwrap();
        }
        None => stream.lock().await.write_all(RespDataType::Nil.to_string().as_bytes()).await.unwrap(),
    }
}

pub async fn handle_incr(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    let key = commands[1].clone();
    let state_guard = state.lock().await;
    
    let current_value = state_guard.get_string(&key).await;
    
    match current_value {
        Some(v) => {
            match v.value.parse::<u64>() {
                Ok(numeric_value) => {
                    let new_value = (numeric_value + 1).to_string();
                    let new_expiration = v.expiration_timestamp;
                    drop(state_guard);
                    state.lock().await.set_string(key.clone(), new_value, new_expiration).await;
                    stream
                        .lock()
                        .await
                        .write_all(format!(":{}\r\n", numeric_value + 1).as_bytes())
                        .await
                        .unwrap();
                }
                Err(_) => {
                    stream
                        .lock()
                        .await
                        .write_all(RespDataType::SimpleError("ERR value is not an integer or out of range".to_string()).to_string().as_bytes())
                        .await
                        .unwrap();
                }
            }
        }
        None => {
            drop(state_guard);
            state.lock().await.set_string(key.clone(), "1".to_string(), None).await;
            stream.lock().await.write_all(RespDataType::Integer(1).to_string().as_bytes()).await.unwrap();
        }
    }
    let role = state.lock().await.get_role().await;
    if role == "master" {
        // Note: propagation to replicas should be handled in the dispatcher for now
    }
}

pub async fn handle_type(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    let key = &commands[1];
    let value_type = state.lock().await.get_type(key).await;
    
    // Check for expiration
    if let Some(expiring_value) = state.lock().await.get_value(key).await {
        if let Some(expiration_timestamp) = expiring_value.expiration_timestamp {
            if Instant::now() > expiration_timestamp {
                return stream.lock().await.write_all(RespDataType::SimpleString("none".to_string()).to_string().as_bytes()).await.unwrap();
            }
        }
    }
    
    stream.lock().await.write_all(RespDataType::SimpleString(value_type.to_string()).to_string().as_bytes()).await.unwrap();
} 