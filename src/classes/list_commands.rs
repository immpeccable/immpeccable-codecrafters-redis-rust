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
    let element = &commands[2];
    
    let list_length = state.lock().await.rpush(key.clone(), element.clone()).await;
    
    stream.lock().await
        .write_all(RespDataType::Integer(list_length as i64).to_string().as_bytes())
        .await
        .unwrap();
}
