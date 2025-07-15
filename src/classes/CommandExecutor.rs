use crate::classes::{
    RespDataType::RespDataType, State::State,
};

use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::Mutex;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf};

use std::sync::Arc;

// Import all command modules
use crate::classes::string_commands::{handle_set, handle_get, handle_incr, handle_type};
use crate::classes::stream_commands::{handle_xadd, handle_xrange, handle_xread};
use crate::classes::replication_commands::{handle_psync, handle_replconf, handle_wait};
use crate::classes::meta_commands::{handle_info, handle_config_get, handle_keys, handle_echo, handle_ping};
use crate::classes::transaction_commands::{handle_multi, handle_exec, handle_discard};
use crate::classes::list_commands::{handle_rpush};

pub struct CommandExecutor {}

impl CommandExecutor {
    pub async fn execute(
        &mut self,
        mut commands: Vec<String>,
        reader: Arc<Mutex<OwnedReadHalf>>,
        writer: Arc<Mutex<OwnedWriteHalf>>,
        state: Arc<Mutex<State>>,
        queued: &mut Vec<Vec<String>>,
        in_multi: &mut bool,
    ) {
        self.handle_commands(&mut commands, reader, writer, state, queued, in_multi)
            .await
    }

    async fn handle_commands(
        &mut self,
        commands: &mut Vec<String>,
        reader: Arc<Mutex<OwnedReadHalf>>,
        mut writer: Arc<Mutex<OwnedWriteHalf>>,
        state: Arc<Mutex<State>>,
        queued: &mut Vec<Vec<String>>,
        in_multi: &mut bool,
    ) {
        let first_command = &commands[0];
        match first_command.to_uppercase().as_str() {
                "PING" => {
                    handle_ping(commands, writer.clone(), state.clone()).await;
                }
                "ECHO" => {
                    handle_echo(commands, writer.clone()).await;
                }
                "SET" => {
                    handle_set(commands, writer.clone(), state.clone()).await;
                    // Propagate to replicas if master
                    let role = state.lock().await.get_role().await;
                    if role == "master" {
                        self.propogate_to_replicas(commands, writer.clone(), state.clone()).await;
                    }
                }
                "GET" => {
                    handle_get(commands, writer.clone(), state.clone()).await;
                }
                "INCR" => {
                    handle_incr(commands, writer.clone(), state.clone()).await;
                    // Propagate to replicas if master
                    let role = state.lock().await.get_role().await;
                    if role == "master" {
                        self.propogate_to_replicas(commands, writer.clone(), state.clone()).await;
                    }
                }
                "TYPE" => {
                    handle_type(commands, writer.clone(), state.clone()).await;
                }
                "CONFIG" => {
                    if let Some(bulk_config_second_string) = commands.get(1) {
                        match bulk_config_second_string.as_str() {
                            "GET" => handle_config_get(commands, writer.clone(), state.clone()).await,
                            _ => {}
                        }
                    }
                }
                "KEYS" => {
                    handle_keys(commands, writer.clone(), state.clone()).await;
                }
                "INFO" => {
                    handle_info(commands, writer.clone(), state.clone()).await;
                }
                "REPLCONF" => {
                    println!("replconf came");
                    handle_replconf(commands, writer.clone(), state.clone(), reader.clone()).await;
                }
                "PSYNC" => {
                    handle_psync(reader.clone(), writer.clone(), state.clone()).await;
                }
                "WAIT" => {
                    handle_wait(commands, writer.clone(), state.clone()).await;
                }
                "XADD" => {
                    handle_xadd(commands, writer.clone(), state.clone()).await;
                    // Propagate to replicas if master
                    let role = state.lock().await.get_role().await;
                    if role == "master" {
                        self.propogate_to_replicas(commands, writer.clone(), state.clone()).await;
                    }
                }
                "XRANGE" => {
                    handle_xrange(commands, writer.clone(), state.clone()).await;
                }
                "XREAD" => {
                    handle_xread(commands, writer.clone(), state.clone()).await;
                }
                "MULTI" => {
                    handle_multi(commands, writer.clone(), in_multi).await;
                }
                "EXEC" => {
                    handle_exec(commands, writer.clone(), queued, in_multi, state.clone()).await;
                }
                "DISCARD" => {
                    handle_discard(commands, writer.clone(), queued, in_multi).await;
                }
                "RPUSH" => {
                    handle_rpush(commands, writer.clone(), state.clone()).await;
                    // Propagate to replicas if master
                    let role = state.lock().await.get_role().await;
                    if role == "master" {
                        self.propogate_to_replicas(commands, writer.clone(), state.clone()).await;
                    }
                }
                _ => {
                    // Handle unimplemented commands with an error response
                    writer
                        .lock()
                        .await
                        .write_all(RespDataType::SimpleError("ERR unknown command".to_string()).to_string().as_bytes())
                        .await
                        .unwrap();
                }
        }
    }

    async fn propogate_to_replicas(
        &mut self,
        commands: &mut Vec<String>,
        stream: Arc<Mutex<OwnedWriteHalf>>,
        state: Arc<Mutex<State>>,
    ) {
        let payload = RespDataType::Array(commands.iter().map(|s| RespDataType::BulkString(s.clone())).collect()).to_string();
        let payload_bytes = payload.as_bytes();
        let number_of_bytes_broadcasted = payload_bytes.len();
        
        let replicas = state.lock().await.get_replicas().await;
        let mut kept = Vec::new();
        
        for client in replicas {
            if client
                .writer
                .lock()
                .await
                .write_all(payload_bytes)
                .await
                .is_ok()
            {
                kept.push(client.clone());
            }
        }
        
        // Update replicas list and offset
        let role = state.lock().await.get_role().await;
        if role == "master" {
            state.lock().await.increment_offset(number_of_bytes_broadcasted).await;
            // Note: We would need to update the replicas list, but the current helper methods don't support this
            // For now, we'll keep the current replicas list as is
        }
    }
}
