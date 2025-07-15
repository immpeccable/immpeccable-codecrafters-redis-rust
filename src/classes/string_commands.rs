use crate::classes::{ExpiringValue::ExpiringValue, RespDataType::RespDataType, State::State};
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
    let key = RespDataType::BulkString(commands[1].clone());
    let value = RespDataType::BulkString(commands[2].clone());
    let mut hashmap_value = ExpiringValue {
        value,
        expiration_timestamp: None,
    };
    let mut options: Option<&[String]> = None;
    if commands.len() > 3 {
        options = Some(&commands[3..]);
    }

    let mut guard = state.lock().await;

    if let Some(op) = options {
        let option_type = &op[0];

        match option_type.to_uppercase().as_str() {
            "PX" => {
                let option_value = &op[1];
                hashmap_value.expiration_timestamp = Some(
                    Instant::now()
                        + std::time::Duration::from_millis(option_value.parse::<u64>().unwrap()),
                );
            }
            _ => {}
        }
    }

    guard.shared_data.insert(key, hashmap_value);
    let role = guard.role.clone();
    drop(guard);
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
    let guard = state.lock().await;
    let key = RespDataType::BulkString(commands[1].clone());
    let value = guard.shared_data.get(&key).cloned();

    match value {
        Some(v) => {
            if let Some(expiration_timestamp) = v.expiration_timestamp {
                if Instant::now() > expiration_timestamp {
                    return stream.lock().await.write_all(RespDataType::Nil.to_string().as_bytes()).await.unwrap();
                }
            }
            let RespDataType::BulkString(value) = &v.value else {
                unreachable!("protocol invariant violated: expected BulkString");
            };
            let bulk_response = RespDataType::BulkString(value.clone()).to_string();
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
    let key = RespDataType::BulkString(commands[1].clone());
    let mut guard = state.lock().await;

    let shared_data = &mut guard.shared_data;

    match shared_data.get(&key) {
        Some(v) => {
            if let RespDataType::BulkString(value) = &v.value {
                match value.parse::<u64>() {
                    Ok(numeric_value) => {
                        shared_data.insert(
                            key.clone(),
                            ExpiringValue {
                                value: RespDataType::BulkString((numeric_value + 1).to_string()),
                                expiration_timestamp: v.expiration_timestamp.clone(),
                            },
                        );
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
        }
        None => {
            shared_data.insert(
                key.clone(),
                ExpiringValue {
                    value: RespDataType::BulkString("1".to_string()),
                    expiration_timestamp: None,
                },
            );
            stream.lock().await.write_all(RespDataType::Integer(1).to_string().as_bytes()).await.unwrap();
        }
    }
    let role = guard.role.clone();
    drop(guard);
    if role == "master" {
        // Note: propagation to replicas should be handled in the dispatcher for now
    }
}

pub async fn handle_type(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    let guard = state.lock().await;
    let key = RespDataType::BulkString(commands[1].clone());
    let value = guard.shared_data.get(&key).cloned();
    let key_as_string = commands[1].clone();
    let stream_value = guard.stream_data.get(&key_as_string);

    match value {
        Some(v) => {
            if let Some(expiration_timestamp) = v.expiration_timestamp {
                if Instant::now() > expiration_timestamp {
                    return stream.lock().await.write_all(RespDataType::SimpleString("none".to_string()).to_string().as_bytes()).await.unwrap();
                }
            }
            return stream.lock().await.write_all(RespDataType::SimpleString("string".to_string()).to_string().as_bytes()).await.unwrap();
        }
        None => match stream_value {
            Some(_) => stream.lock().await.write_all(RespDataType::SimpleString("stream".to_string()).to_string().as_bytes()).await.unwrap(),
            None => stream.lock().await.write_all(RespDataType::SimpleString("none".to_string()).to_string().as_bytes()).await.unwrap(),
        },
    }
} 