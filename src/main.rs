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

mod classes;
use classes::{
    CommandExecutor::CommandExecutor, Db::Db, ExpiringValue::ExpiringValue, Parser::Parser,
    RespDataType::RespDataType, State::State,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let shared_state: Arc<Mutex<HashMap<RespDataType, ExpiringValue>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let mut state = State {
        shared_data: shared_state,
        db_dir: None,
        db_file_name: None,
        role: String::from("master"),
        master_host: None,
        master_port: None,
        master_stream: None,
    };
    let mut i = 1;
    let mut args_map: HashMap<String, String> = HashMap::new();

    let args: Vec<String> = env::args().collect();

    while i < args.len() {
        args_map.insert(args[i].clone(), args[i + 1].clone());
        i += 2;
    }

    state.db_dir = args_map.get(&String::from("--dir")).cloned();
    state.db_file_name = args_map.get(&String::from("--dbfilename")).cloned();
    let mut port = String::from("6379");
    if let Some(port_value) = args_map.get(&String::from("--port")) {
        port = port_value.to_string();
    }
    if let (Some(_), Some(_)) = (&state.db_dir, &state.db_file_name) {
        let _ = Db {}.load(state.clone());
    }

    if let Some(replica_of) = args_map.get(&"--replicaof".to_string()) {
        state.role = String::from("slave");
        let parts: Vec<&str> = replica_of.split(" ").collect();
        state.master_host = Some(parts[0].to_string());
        state.master_port = Some(parts[1].to_string());
        state.master_stream = Some(
            TcpStream::connect(format!("{}:{}", parts[0].to_string(), parts[1].to_string()))
                .unwrap(),
        )
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

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

    if let Some(master_stream) = &mut state.master_stream {
        master_stream.write(b"*1\r\n$4\r\nPING\r\n").unwrap();
        let mut buf = [0u8; 512];
        master_stream.read(&mut buf).unwrap();
        master_stream
            .write(
                format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n",
                    port
                )
                .as_bytes(),
            )
            .unwrap();
        master_stream.read(&mut buf).unwrap();
        master_stream
            .write(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
            .unwrap();
        master_stream.read(&mut buf).unwrap();
        master_stream
            .write(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
            .unwrap();
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
