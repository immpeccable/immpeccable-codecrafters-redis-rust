use crate::classes::{
    Constants::EMPTY_RDB_HEX_REPRESENTATION, ExpiringValue::ExpiringValue,
    RespDataType::RespDataType, State::State,
};

use hex;

use std::{
    io::{BufRead, BufReader, Error, Read, Seek, Write},
    net::TcpStream,
    time::{Duration, Instant},
};

pub struct CommandExecutor {}

impl CommandExecutor {
    pub fn execute(&mut self, commands: &mut RespDataType, stream: &mut TcpStream, state: State) {
        match commands {
            RespDataType::Array(v) => self.handle_commands(v, stream, state),
            _ => unreachable!("shouldn't be else "),
        }
    }

    fn handle_commands(
        &mut self,
        commands: &mut Vec<RespDataType>,
        stream: &mut TcpStream,
        state: State,
    ) {
        let first_command = &commands[0];
        match first_command {
            RespDataType::BulkString(bulk_str) => match bulk_str.to_uppercase().as_str() {
                "PING" => {
                    self.ping_command(commands, stream, state);
                }
                "ECHO" => {
                    self.echo_command(commands, stream);
                }
                "SET" => {
                    self.set_command(commands, stream, state);
                }
                "GET" => {
                    self.get_command(commands, stream, state);
                }
                "CONFIG" => {
                    if let RespDataType::BulkString(bulk_config_second_string) = &commands[1] {
                        match bulk_config_second_string.as_str() {
                            "GET" => self.config_get_command(commands, stream, state),
                            _ => {}
                        }
                    }
                }
                "KEYS" => {
                    self.keys_command(commands, stream, state);
                }
                "INFO" => {
                    self.info_command(commands, stream, state);
                }
                "REPLCONF" => {
                    self.repl_conf_command(commands, stream, state);
                }

                "PSYNC" => {
                    self.psync(stream, state);
                }
                _ => {}
            },
            _ => {}
        }
    }

    fn echo_command(&mut self, commands: &mut Vec<RespDataType>, stream: &mut TcpStream) {
        let second_command = &commands[1];
        match second_command {
            RespDataType::BulkString(bulk_string) => {
                stream
                    .write_all(self.convert_simple_string_to_resp(bulk_string).as_bytes())
                    .unwrap();
            }
            _ => {}
        }
    }

    fn repl_conf_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        stream: &mut TcpStream,
        state: State,
    ) {
        if let RespDataType::BulkString(repl_conf_second) = &commands[1] {
            match repl_conf_second.to_uppercase().as_str() {
                "CAPA" => {
                    stream.write_all(b"+OK\r\n").unwrap();
                }
                "LISTENING-PORT" => {
                    stream.write_all(b"+OK\r\n").unwrap();
                }
                "GETACK" => {
                    stream
                        .write_all(
                            format!(
                                "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                                state.offset.to_string().len(),
                                state.offset
                            )
                            .as_bytes(),
                        )
                        .unwrap();
                }
                _ => {}
            }
        }
    }

    fn psync(&mut self, stream: &mut TcpStream, state: State) {
        // send FULLRESYNC header
        let header = format!("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0");
        stream
            .write_all(format!("+{}\r\n", header).as_bytes())
            .unwrap();

        // send empty-RDB payload
        let dump = hex::decode(EMPTY_RDB_HEX_REPRESENTATION).unwrap();
        stream
            .write_all(format!("${}\r\n", dump.len()).as_bytes())
            .unwrap();
        stream.write_all(&dump).unwrap();

        // clone this connection into our replicas list for downstream
        let mut reps = state.replicas.lock().unwrap();
        reps.push(stream.try_clone().unwrap());
    }

    fn ping_command(&mut self, _: &mut Vec<RespDataType>, stream: &mut TcpStream, state: State) {
        if state.role == "master" {
            stream.write_all(b"+PONG\r\n").unwrap();
        }
    }

    fn config_get_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        stream: &mut TcpStream,
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
                        stream.write_all(response.as_bytes()).unwrap();
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
                        stream.write_all(response.as_bytes()).unwrap();
                    }
                }
                _ => {
                    panic!("unknown config item")
                }
            }
        }
    }

    fn set_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        stream: &mut TcpStream,
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

        let mut state_guard = state.shared_data.lock().unwrap();
        state_guard.insert(key, hashmap_value);

        if state.role == "master" {
            stream.write_all(b"+OK\r\n").unwrap();
        }

        let payload = self.convert_array_to_resp(commands.clone());
        let mut reps = state.replicas.lock().unwrap();
        reps.retain(|mut client| client.write_all(payload.as_bytes()).is_ok());
    }
    fn get_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        stream: &mut TcpStream,
        state: State,
    ) {
        let key = commands[1].clone();
        let state_guard = state.shared_data.lock().unwrap();
        let value = state_guard.get(&key);
        match value {
            Some(v) => {
                if let Some(expiration_timestamp) = v.expiration_timestamp {
                    if Instant::now() > expiration_timestamp {
                        return stream.write_all(b"$-1\r\n").unwrap();
                    }
                }
                let RespDataType::BulkString(value) = &v.value else {
                    unreachable!("protocol invariant violated: expected BulkString");
                };
                let bulk_response = self.convert_bulk_string_to_resp(&value);
                return stream.write_all(bulk_response.as_bytes()).unwrap();
            }
            None => stream.write_all(b"$-1\r\n").unwrap(),
        }
    }

    fn keys_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        stream: &mut TcpStream,
        state: State,
    ) {
        let shared_guard = state.shared_data.lock().unwrap();
        if let RespDataType::BulkString(regex_match) = &commands[1] {
            let mut matching_keys: Vec<String> = Vec::new();
            for key in shared_guard.keys() {
                if let RespDataType::BulkString(key) = key {
                    let parts: Vec<&str> = regex_match.split("*").collect();
                    if key.starts_with(parts[0]) && key.ends_with(parts[1]) {
                        matching_keys.push(key.clone());
                    }
                }
            }
            let mut result = String::from(format!("*{}\r\n", matching_keys.len()));
            for mk in matching_keys {
                result.push_str(&self.convert_bulk_string_to_resp(&mk));
            }
            stream.write_all(result.as_bytes()).unwrap();
        }
    }

    fn info_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        stream: &mut TcpStream,
        state: State,
    ) {
        stream
            .write_all(
                &self
                    .convert_bulk_string_to_resp(&String::from(format!("role:{}\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", state.role)))
                    .as_bytes(),
            )
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
