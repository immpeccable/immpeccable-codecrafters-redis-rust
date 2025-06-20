use crate::classes::{ExpiringValue::ExpiringValue, RespDataType::RespDataType, State::State};

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
                    self.ping_command(commands, stream);
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

    fn ping_command(&mut self, _: &mut Vec<RespDataType>, stream: &mut TcpStream) {
        stream.write_all(b"+PONG\r\n").unwrap();
    }

    fn config_get_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        stream: &mut TcpStream,
        state: State,
    ) {
        let mut config_key = &commands[2];
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
        stream.write_all(b"+OK\r\n").unwrap();
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
                    .convert_bulk_string_to_resp(&String::from("role:master"))
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
}
