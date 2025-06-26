use crate::classes::{
    Constants::EMPTY_RDB_HEX_REPRESENTATION, ExpiringValue::ExpiringValue,
    RespDataType::RespDataType, State::State,
};

use hex;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf};

use std::sync::Arc;
use std::{
    io::{BufRead, BufReader, Error, Read, Seek, Write},
    time::{Duration, Instant},
};

pub struct CommandExecutor {}

impl CommandExecutor {
    pub async fn execute(
        &mut self,
        commands: &mut RespDataType,
        stream: Arc<Mutex<OwnedWriteHalf>>,
        state: State,
    ) {
        match commands {
            RespDataType::Array(v) => self.handle_commands(v, stream, state).await,
            _ => unreachable!("shouldn't be else "),
        }
    }

    async fn handle_commands(
        &mut self,
        commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: State,
    ) {
        let first_command = &commands[0];
        match first_command {
            RespDataType::BulkString(bulk_str) => match bulk_str.to_uppercase().as_str() {
                "PING" => {
                    self.ping_command(commands, stream, state).await;
                }
                "ECHO" => {
                    self.echo_command(commands, stream).await;
                }
                "SET" => {
                    self.set_command(commands, stream, state).await;
                }
                "GET" => {
                    self.get_command(commands, stream, state).await;
                }
                "CONFIG" => {
                    if let RespDataType::BulkString(bulk_config_second_string) = &commands[1] {
                        match bulk_config_second_string.as_str() {
                            "GET" => self.config_get_command(commands, stream, state).await,
                            _ => {}
                        }
                    }
                }
                "KEYS" => {
                    self.keys_command(commands, stream, state).await;
                }
                "INFO" => {
                    self.info_command(commands, stream, state).await;
                }
                "REPLCONF" => {
                    self.repl_conf_command(commands, stream, state).await;
                }

                "PSYNC" => {
                    self.psync(stream, state).await;
                }
                "WAIT" => {
                    self.wait(commands, stream, state).await;
                }
                _ => {}
            },
            _ => {}
        }
    }

    async fn wait(
        &mut self,
        commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: State,
    ) {
        stream
            .lock()
            .await
            .write_all(format!(":{}\r\n", state.replicas.lock().await.len()).as_bytes())
            .await
            .unwrap();
    }

    async fn echo_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
    ) {
        let second_command = &commands[1];
        match second_command {
            RespDataType::BulkString(bulk_string) => {
                stream
                    .lock()
                    .await
                    .write_all(self.convert_simple_string_to_resp(bulk_string).as_bytes())
                    .await
                    .unwrap();
            }
            _ => {}
        }
    }

    async fn repl_conf_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: State,
    ) {
        if let RespDataType::BulkString(repl_conf_second) = &commands[1] {
            match repl_conf_second.to_uppercase().as_str() {
                "CAPA" => {
                    stream.lock().await.write_all(b"+OK\r\n").await.unwrap();
                }
                "LISTENING-PORT" => {
                    stream.lock().await.write_all(b"+OK\r\n").await.unwrap();
                }
                "GETACK" => {
                    stream
                        .lock()
                        .await
                        .write_all(
                            format!(
                                "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                                state.offset.to_string().len(),
                                state.offset
                            )
                            .as_bytes(),
                        )
                        .await
                        .unwrap();
                }
                _ => {}
            }
        }
    }

    async fn psync(&mut self, mut stream: Arc<Mutex<OwnedWriteHalf>>, state: State) {
        // split into a read half (for your handshake & loop) and a write half (which is Clone)

        let header = format!("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0");
        stream
            .lock()
            .await
            .write_all(format!("+{}\r\n", header).as_bytes())
            .await
            .unwrap();

        // send empty-RDB payload
        let dump = hex::decode(EMPTY_RDB_HEX_REPRESENTATION).unwrap();
        stream
            .lock()
            .await
            .write_all(format!("${}\r\n", dump.len()).as_bytes())
            .await
            .unwrap();
        stream.lock().await.write_all(&dump).await.unwrap();

        state.replicas.lock().await.push(stream.clone());
    }

    async fn ping_command(
        &mut self,
        _: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: State,
    ) {
        if state.role == "master" {
            stream.lock().await.write_all(b"+PONG\r\n").await.unwrap();
        }
    }

    async fn config_get_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: State,
    ) {
        let config_key = &commands[2];
        if let RespDataType::BulkString(config_key) = config_key {
            match config_key.to_uppercase().as_str() {
                "DIR" => {
                    if let Some(db_dir) = state.db_dir {
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
                    if let Some(db_file_name) = state.db_file_name {
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

    async fn set_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: State,
    ) {
        let (key, value) = (commands[1].clone(), commands[2].clone());
        let mut hashmap_value = ExpiringValue {
            value,
            expiration_timestamp: None,
        };
        let mut options: Option<&[RespDataType]> = None;
        if commands.len() > 3 {
            options = Some(&commands[3..]);
        }
        if let Some(op) = options {
            let RespDataType::BulkString(option_type) = &op[0] else {
                unreachable!("protocol invariant violated: expected BulkString");
            };

            match option_type.to_uppercase().as_str() {
                "PX" => {
                    let RespDataType::BulkString(option_value) = &op[1] else {
                        unreachable!("protocol invariant violated: expected BulkString");
                    };
                    hashmap_value.expiration_timestamp = Some(
                        Instant::now()
                            + Duration::from_millis(option_value.parse::<u64>().unwrap()),
                    );
                }
                _ => {}
            }
        }

        println!("received set command");

        // Store the data first
        {
            let mut state_guard = state.shared_data.lock().await;
            state_guard.insert(key, hashmap_value);
        }

        if state.role == "master" {
            stream.lock().await.write_all(b"+OK\r\n").await.unwrap();
        }

        let payload = self.convert_array_to_resp(commands.clone());

        let clients = {
            let mut guard = state.replicas.lock().await; // guard: MutexGuard<_, Vec<TcpStream>>
            println!("propagating to the clients {}", guard.len());

            std::mem::take(&mut *guard) // guard’in içindeki Vec’i al, yerine boş Vec koy
        }; // guard düşer, kilit açılır

        // 2. Her client’a yaz ve sadece başarılı olanları topla
        let mut kept = Vec::with_capacity(clients.len());
        for mut client in clients {
            if client
                .lock()
                .await
                .write_all(payload.as_bytes())
                .await
                .is_ok()
            {
                kept.push(client);
            }
        }

        // 3. Yeniden kilidi al ve filtrelenmiş Vec’i geri yaz
        let mut guard = state.replicas.lock().await;
        *guard = kept; // MutexGuard’in işaret ettiği Vec’e atama
                       // guard düşer, kilit tekrar açılır
    }

    async fn get_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: State,
    ) {
        let key = commands[1].clone();
        let value = {
            let state_guard = state.shared_data.lock().await;
            state_guard.get(&key).cloned()
        };

        match value {
            Some(v) => {
                if let Some(expiration_timestamp) = v.expiration_timestamp {
                    if Instant::now() > expiration_timestamp {
                        return stream.lock().await.write_all(b"$-1\r\n").await.unwrap();
                    }
                }
                let RespDataType::BulkString(value) = &v.value else {
                    unreachable!("protocol invariant violated: expected BulkString");
                };
                let bulk_response = self.convert_bulk_string_to_resp(&value);
                return stream
                    .lock()
                    .await
                    .write_all(bulk_response.as_bytes())
                    .await
                    .unwrap();
            }
            None => stream.lock().await.write_all(b"$-1\r\n").await.unwrap(),
        }
    }

    async fn keys_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: State,
    ) {
        let matching_keys = {
            let shared_guard = state.shared_data.lock().await;
            if let RespDataType::BulkString(regex_match) = &commands[1] {
                let mut keys: Vec<String> = Vec::new();
                for key in shared_guard.keys() {
                    if let RespDataType::BulkString(key) = key {
                        let parts: Vec<&str> = regex_match.split("*").collect();
                        if key.starts_with(parts[0]) && key.ends_with(parts[1]) {
                            keys.push(key.clone());
                        }
                    }
                }
                keys
            } else {
                Vec::new()
            }
        };

        let mut result = String::from(format!("*{}\r\n", matching_keys.len()));
        for mk in matching_keys {
            result.push_str(&self.convert_bulk_string_to_resp(&mk));
        }
        stream
            .lock()
            .await
            .write_all(result.as_bytes())
            .await
            .unwrap();
    }

    async fn info_command(
        &mut self,
        _commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: State,
    ) {
        stream.lock().await
            .write_all(
                &self
                    .convert_bulk_string_to_resp(&String::from(format!("role:{}\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", state.role)))
                    .as_bytes(),
            )
            .await
            .unwrap();
    }

    fn convert_simple_string_to_resp(&mut self, input: &String) -> String {
        let res = format!("+{}\r\n", input);
        return res;
    }

    fn convert_bulk_string_to_resp(&mut self, input: &String) -> String {
        let res = format!("${}\r\n{}\r\n", input.len(), input);
        return res;
    }

    fn convert_array_to_resp(&mut self, input: Vec<RespDataType>) -> String {
        let mut res = format!("*{}\r\n", input.len());
        for el in input {
            if let RespDataType::BulkString(el_as_string) = el {
                res.push_str(&self.convert_bulk_string_to_resp(&el_as_string));
            }
        }
        return res;
    }
}
