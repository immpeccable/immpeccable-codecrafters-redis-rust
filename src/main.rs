#![allow(unused_imports)]
use core::{num, panic};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Error, Read, Seek, Write};
use std::iter::Peekable;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::str::{self, Chars};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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

struct Db {}
impl Db {
    fn system_time_to_instant(&mut self, sys_time: SystemTime) -> Instant {
        let now_sys = SystemTime::now();
        let now_inst = Instant::now();

        match sys_time.duration_since(now_sys) {
            // sys_time is in the future ⇒ add the delta
            Ok(delta) => now_inst + delta,
            // sys_time is in the past ⇒ subtract the delta
            Err(err) => now_inst - err.duration(),
        }
    }

    fn size_encoded_bytes(&mut self, reader: &mut BufReader<File>) -> u32 {
        let mut number_byte = [0u8; 1];
        reader.read_exact(&mut number_byte).unwrap();
        let byte = number_byte[0];
        match byte & 0xC0 {
            0 => {
                return byte.try_into().unwrap();
            }
            1 => {
                let mut next_byte = [0u8; 1];
                reader.read_exact(&mut next_byte).unwrap();
                return (2u8.pow(8) * byte & 0x3F + next_byte[0])
                    .try_into()
                    .unwrap();
            }
            2 => {
                let mut next_four_bytes = [0u8; 4];
                reader.read_exact(&mut next_four_bytes).unwrap();
                return u32::from_le_bytes(next_four_bytes);
            }
            3 => match byte {
                0xC0 => {
                    let mut next_one_byte = [0u8; 1];
                    reader.read_exact(&mut next_one_byte).unwrap();
                    return next_one_byte[0].try_into().unwrap();
                }
                0xC1 => {
                    let mut next_two_bytes = [0u8; 2];
                    reader.read_exact(&mut next_two_bytes).unwrap();
                    return u16::from_le_bytes(next_two_bytes).into();
                }
                0xC2 => {
                    let mut next_four_bytes = [0u8; 4];
                    reader.read_exact(&mut next_four_bytes).unwrap();
                    return u32::from_le_bytes(next_four_bytes);
                }
                _ => {}
            },
            _ => {
                unreachable!("should not be more than 3")
            }
        }
        return 1;
    }

    fn string_encoded(&mut self, reader: &mut BufReader<File>) -> String {
        let string_length = self.size_encoded_bytes(reader);
        let mut string_buffer: Vec<u8> = vec![0u8; string_length as usize];
        reader.read_exact(&mut string_buffer).unwrap();
        return String::from_utf8(string_buffer).unwrap();
    }

    fn load(&mut self, state: State) -> Result<(), Error> {
        if let (Some(db_dir), Some(db_file)) = (state.db_dir, state.db_file_name) {
            let file = File::open(&Path::new(format!("{}/{}", db_dir, db_file).as_str()))?;
            let mut reader = BufReader::new(file);
            let mut _discard = Vec::new();
            reader.read_until(0xFE, &mut _discard).unwrap();
            let _db_index = self.size_encoded_bytes(&mut reader);
            reader.consume(1);
            let keys_size = self.size_encoded_bytes(&mut reader);
            let _keys_size_with_expiration_size = self.size_encoded_bytes(&mut reader);
            for _ in 0..keys_size {
                let mut first_byte = [0u8; 1];
                reader.read_exact(&mut first_byte).unwrap();
                let raw_byte = first_byte[0];
                match raw_byte {
                    0x00 => {
                        let hash_key = self.string_encoded(&mut reader);
                        let hash_value = self.string_encoded(&mut reader);
                        println!("{} {}", hash_key, hash_value);
                        let mut state_guard = state.shared_data.lock().unwrap();
                        state_guard.insert(
                            RespDataType::BulkString(hash_key),
                            ExpiringValue {
                                value: RespDataType::BulkString(hash_value),
                                expiration_timestamp: None,
                            },
                        );
                    }
                    0xFC => {
                        let mut expiration_as_miliseconds_bytes = [0u8; 8];
                        reader
                            .read_exact(&mut expiration_as_miliseconds_bytes)
                            .unwrap();
                        let ms = u64::from_le_bytes(expiration_as_miliseconds_bytes);
                        reader.consume(1);
                        let hash_key = self.string_encoded(&mut reader);
                        let hash_value = self.string_encoded(&mut reader);
                        let expiration_time = UNIX_EPOCH + Duration::from_millis(ms);
                        let mut state_guard = state.shared_data.lock().unwrap();
                        state_guard.insert(
                            RespDataType::BulkString(hash_key),
                            ExpiringValue {
                                value: RespDataType::BulkString(hash_value),
                                expiration_timestamp: Some(Instant::from(
                                    self.system_time_to_instant(expiration_time),
                                )),
                            },
                        );
                    }
                    0xFD => {
                        let mut expiration_as_seconds_bytes = [0u8; 4];
                        reader.read_exact(&mut expiration_as_seconds_bytes).unwrap();
                        let seconds = u32::from_le_bytes(expiration_as_seconds_bytes);
                        reader.consume(1);
                        let hash_key = self.string_encoded(&mut reader);
                        let hash_value = self.string_encoded(&mut reader);
                        let expiration_time = UNIX_EPOCH + Duration::from_secs(seconds.into());
                        let mut state_guard = state.shared_data.lock().unwrap();
                        state_guard.insert(
                            RespDataType::BulkString(hash_key),
                            ExpiringValue {
                                value: RespDataType::BulkString(hash_value),
                                expiration_timestamp: Some(Instant::from(
                                    self.system_time_to_instant(expiration_time),
                                )),
                            },
                        );
                    }
                    _ => {}
                }
            }
        }
        return Ok(());
    }
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
                "KEYS" => {
                    self.keys_command(commands, stream, state);
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
    expiration_timestamp: Option<Instant>,
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
        let _ = Db {}.load(state.clone());
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
