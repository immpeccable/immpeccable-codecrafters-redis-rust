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
    let role = state.lock().await.get_role().await;
    stream.lock().await
        .write_all(
            &RespDataType::BulkString(String::from(format!("role:{}\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", role))).to_string().as_bytes(),
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
        let (db_dir, db_file_name) = state.lock().await.get_db_config().await;
        
        match config_key.to_uppercase().as_str() {
            "DIR" => {
                if let Some(db_dir) = db_dir {
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
                if let Some(db_file_name) = db_file_name {
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
    let all_keys = state.lock().await.get_all_keys().await;
    let regex_match = &commands[1];
    let matching_keys: Vec<String> = all_keys.into_iter()
        .filter(|key| {
            let parts: Vec<&str> = regex_match.split("*").collect();
            key.starts_with(parts[0]) && key.ends_with(parts[1])
        })
        .collect();

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
    let role = state.lock().await.get_role().await;
    if role == "master" {
        stream.lock().await.write_all(RespDataType::SimpleString("PONG".to_string()).to_string().as_bytes()).await.unwrap();
    }
} 