#![allow(unused_imports)]
use core::panic;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::iter::Peekable;
use std::net::{TcpListener, TcpStream};
use std::str::{self, Chars};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{env, thread};

#[derive(Eq, Hash, PartialEq, Clone)]
enum RespDataType {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    Array(Vec<RespDataType>),
    BulkString(String),
    Nil,
    Boolean(bool),
    // Double(f32),
}

struct Parser {}

impl Parser {
    fn read_until_next_line(&mut self, chars: &mut Peekable<Chars<'_>>) -> String {
        let mut res = String::new();
        while let Some(&c) = chars.peek() {
            if c == '\r' {
                break;
            }
            res.push(c);
            chars.next();
        }
        return res;
    }

    fn read_n_characters(&mut self, chars: &mut Peekable<Chars<'_>>, n: u32) -> String {
        let mut res = String::new();
        for i in 0..n {
            res.push(chars.next().unwrap());
        }
        return res;
    }

    fn parse(&mut self, input: &str) -> RespDataType {
        let mut chars = input.chars().peekable();
        let mut commands = Vec::new();
        let mut command_size: u32 = 0;
        while let Some(ch) = chars.next() {
            if ch == '*' {
                let command_size_as_string = self.read_until_next_line(&mut chars);
                command_size = command_size_as_string.parse::<u32>().unwrap();
            } else if ch == '$' {
                let bulk_string_size_as_string = self.read_until_next_line(&mut chars);
                let bulk_string_size = bulk_string_size_as_string.parse::<u32>().unwrap();
                chars.next();
                chars.next();
                let simple_string = self.read_n_characters(&mut chars, bulk_string_size);
                chars.next();
                chars.next();
                commands.push(RespDataType::BulkString(simple_string));
            }
        }
        return RespDataType::Array(commands);
    }
}

struct CommandExecutor {}

impl CommandExecutor {
    fn execute(&mut self, commands: &mut RespDataType, stream: &mut TcpStream, state: State) {
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
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            expiration_as_miliseconds: None,
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
                    hashmap_value.expiration_as_miliseconds =
                        Some(option_value.parse::<u128>().unwrap());
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
                if let Some(exp) = v.expiration_as_miliseconds {
                    if v.created_at.elapsed().as_millis() > exp {
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

    fn convert_simple_string_to_resp(&mut self, input: &String) -> String {
        let res = format!("+{}\r\n", input);
        return res;
    }

    fn convert_bulk_string_to_resp(&mut self, input: &String) -> String {
        let res = format!("${}\r\n{}\r\n", input.len(), input);
        return res;
    }
}

struct ExpiringValue {
    value: RespDataType,
    created_at: Instant,
    last_accessed: Instant,
    expiration_as_miliseconds: Option<u128>,
}

#[derive(Clone)]
struct State {
    pub db_file_name: Option<String>,
    pub db_dir: Option<String>,
    pub shared_data: Arc<Mutex<HashMap<RespDataType, ExpiringValue>>>,
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let args: Vec<String> = env::args().collect();
    let shared_state = Arc::new(Mutex::new(HashMap::new()));
    let mut state = State {
        shared_data: shared_state,
        db_dir: None,
        db_file_name: None,
    };

    if args.len() > 1 {
        state.db_dir = Some(args[2].clone());
        state.db_file_name = Some(args[4].clone());
    }

    fn handle_connection(stream: &mut TcpStream, state: State) {
        let mut parser = Parser {};
        let mut executor = CommandExecutor {};
        loop {
            let mut buf = [0; 512];
            let buffer_size = stream.read(&mut buf).unwrap();
            if buffer_size == 0 {
                break;
            }
            match str::from_utf8(&mut buf) {
                Ok(str) => {
                    let mut commands = parser.parse(str);
                    executor.execute(&mut commands, stream, state.clone());
                }
                Err(_) => {
                    panic!("Error converting");
                }
            }
        }
    }

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let state_cloned = state.clone();
                let _ = thread::spawn(move || handle_connection(&mut stream, state_cloned));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
