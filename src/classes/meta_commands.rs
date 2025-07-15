use crate::classes::{RespDataType::RespDataType, State::State};
use std::sync::Arc;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::io::AsyncWriteExt;

pub async fn handle_info(
    _commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    stream.lock().await
        .write_all(
            &RespDataType::BulkString(String::from(format!("role:{}\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", state.lock().await.role))).to_string().as_bytes(),
        )
        .await
        .unwrap();
}

pub async fn handle_config_get(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    if let Some(config_key) = commands.get(2) {
        match config_key.to_uppercase().as_str() {
            "DIR" => {
                if let Some(db_dir) = &mut state.lock().await.db_dir {
                    let response = format!(
                        "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        config_key.len(),
                        config_key,
                        db_dir.len(),
                        db_dir
                    );
                    stream
                        .lock()
                        .await
                        .write_all(response.as_bytes())
                        .await
                        .unwrap();
                }
            }
            "DBFILENAME" => {
                if let Some(db_file_name) = &mut state.lock().await.db_file_name {
                    let response = format!(
                        "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        config_key.len(),
                        config_key,
                        db_file_name.len(),
                        db_file_name
                    );
                    stream
                        .lock()
                        .await
                        .write_all(response.as_bytes())
                        .await
                        .unwrap();
                }
            }
            _ => {
                panic!("unknown config item")
            }
        }
    }
}

pub async fn handle_keys(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    let guard = state.lock().await;
    let matching_keys = {
        let regex_match = &commands[1];
        let mut keys: Vec<String> = Vec::new();
        for key in guard.shared_data.keys() {
            if let RespDataType::BulkString(key) = key {
                let parts: Vec<&str> = regex_match.split("*").collect();
                if key.starts_with(parts[0]) && key.ends_with(parts[1]) {
                    keys.push(key.clone());
                }
            }
        }
        keys
    };

    let mut result = String::from(format!("*{}\r\n", matching_keys.len()));
    for mk in matching_keys {
        result.push_str(&format!("{}", RespDataType::BulkString(mk)));
    }
    stream
        .lock()
        .await
        .write_all(result.as_bytes())
        .await
        .unwrap();
}

pub async fn handle_echo(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
) {
    if let Some(bulk_string) = commands.get(1) {
        stream
            .lock()
            .await
            .write_all(format!("+{}\r\n", bulk_string).as_bytes())
            .await
            .unwrap();
    }
}

pub async fn handle_ping(
    _: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    if state.lock().await.role == "master" {
        stream.lock().await.write_all(RespDataType::SimpleString("PONG".to_string()).to_string().as_bytes()).await.unwrap();
    }
} 