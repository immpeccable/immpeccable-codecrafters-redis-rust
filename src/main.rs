#![allow(unused_imports)]
use std::io::{Read, Write};
use std::iter::Peekable;
use std::net::{TcpListener, TcpStream};
use std::str::{self, Chars};
use std::thread;

enum RespDataType {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    Array(Vec<RespDataType>),
    BulkString(String),
    Nil,
    Boolean(bool),
    Double(f32),
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
    fn execute(&mut self, commands: &mut RespDataType, stream: &mut TcpStream) {
        match commands {
            RespDataType::Array(v) => self.handle_commands(v, stream),
            _ => unreachable!("shouldn't be else "),
        }
    }

    fn handle_commands(&mut self, commands: &mut Vec<RespDataType>, stream: &mut TcpStream) {
        let first_command = &commands[0];
        match first_command {
            RespDataType::BulkString(bulk_str) => match bulk_str.as_str() {
                "PING" => {
                    self.ping_command(commands, stream);
                }
                "ECHO" => {
                    self.echo_command(commands, stream);
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

    fn ping_command(&mut self, commands: &mut Vec<RespDataType>, stream: &mut TcpStream) {
        stream.write_all(b"+PONG\r\n").unwrap();
    }

    fn convert_simple_string_to_resp(&mut self, input: &String) -> String {
        let res = format!("+{}\r\n", input);
        return res;
    }
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    fn handle_connection(stream: &mut TcpStream) {
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
                    executor.execute(&mut commands, stream);
                }
                Err(err) => {
                    panic!("Error converting");
                }
            }
        }
    }

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let _ = thread::spawn(move || handle_connection(&mut stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
