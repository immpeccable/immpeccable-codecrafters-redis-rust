use core::str;
#[allow(unused_imports)]
use core::{num, panic};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::{env, thread};

mod classes;
use classes::{CommandExecutor::CommandExecutor, Db::Db, Parser::Parser, State::State};

fn main() {
    // bootstrap shared state
    let shared = Arc::new(Mutex::new(HashMap::new()));
    let mut state = State {
        shared_data: shared,
        db_dir: None,
        db_file_name: None,
        role: "master".to_string(),
        master_host: None,
        master_port: None,
        master_stream: None,
        replicas: Arc::new(Mutex::new(Vec::new())),
    };

    // parse args into a map
    let mut args_map = HashMap::new();
    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i + 1 < args.len() {
        args_map.insert(args[i].clone(), args[i + 1].clone());
        i += 2;
    }

    // optional RDB load
    state.db_dir = args_map.get("--dir").cloned();
    state.db_file_name = args_map.get("--dbfilename").cloned();
    if state.db_dir.is_some() && state.db_file_name.is_some() {
        let _ = Db {}.load(state.clone());
    }

    let port = args_map.get("--port").map(|s| s.as_str()).unwrap_or("6379");

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .unwrap_or_else(|e| panic!("couldn't bind to port {}: {}", port, e));

    if let Some(replica_of) = args_map.get("--replicaof") {
        let mut parts = replica_of.split_whitespace();
        let host = parts.next().expect("`--replicaof` requires `host port`");
        let master_port = parts.next().expect("`--replicaof` requires `host port`");
        state.role = "slave".to_string();
        state.master_host = Some(host.to_string());
        state.master_port = Some(master_port.to_string());

        let mut master_stream = TcpStream::connect(format!("{}:{}", host, master_port))
            .expect("couldn't connect to master");

        let remaining_data = do_replication_handshake(&mut master_stream, port);
        println!("{:?}", String::from_utf8_lossy(&remaining_data.to_vec()));

        let state_clone = state.clone();
        thread::spawn(move || {
            handle_replication_loop(master_stream, state_clone, remaining_data);
        });
    }

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let state_clone = state.clone();
                thread::spawn(move || handle_client(stream, state_clone));
            }
            Err(e) => eprintln!("accept error: {}", e),
        }
    }
}

// common client‐handling loop (for master clients only)
fn handle_client(mut stream: TcpStream, state: State) {
    let mut parser = Parser {};
    let mut exec = CommandExecutor {};
    let mut pending: Vec<u8> = Vec::new();

    loop {
        let mut buf = [0u8; 2048];
        let n = match stream.read(&mut buf) {
            Ok(0) | Err(_) => return, // connection closed or error
            Ok(n) => n,
        };

        pending.extend_from_slice(&buf[..n]);

        if let Some(frame_end) = find_complete_frame(&pending) {
            let frame_bytes: Vec<u8> = pending.drain(..frame_end).collect();

            if let Ok(text) = std::str::from_utf8(&frame_bytes) {
                let mut commands = parser.parse(text);
                exec.execute(&mut commands, &mut stream, state.clone());
            }
        }
    }
}

// PSYNC/REPLCONF handshake
fn do_replication_handshake(stream: &mut TcpStream, port: &str) -> Vec<u8> {
    // PING
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut buf = [0u8; 512];
    let n = stream.read(&mut buf).unwrap();
    println!("PING response: {}", String::from_utf8_lossy(&buf[..n]));

    // REPLCONF listening-port
    stream
        .write_all(
            format!(
                "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
                port.len(),
                port
            )
            .as_bytes(),
        )
        .unwrap();
    let n = stream.read(&mut buf).unwrap();
    println!(
        "REPLCONF listening-port response: {}",
        String::from_utf8_lossy(&buf[..n])
    );

    // REPLCONF capa psync2
    stream
        .write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        .unwrap();
    let n = stream.read(&mut buf).unwrap();
    println!(
        "REPLCONF capa response: {}",
        String::from_utf8_lossy(&buf[..n])
    );

    // PSYNC ? -1
    stream
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .unwrap();

    let mut pending: Vec<u8> = Vec::new();
    let mut buf = vec![0; 2048];
    let mut is_full_resync_read = false;
    let mut is_rdb_content_read = false;

    fn try_to_read_rdb(pending: &mut Vec<u8>) -> bool {
        if pending[0] == b'$' {
            println!("received rdb");
            if let Some(end_of_rdb_clrf) = pending.windows(2).position(|w| w == b"\r\n") {
                let size_of_rdb_content =
                    String::from_utf8((&pending[1..end_of_rdb_clrf]).to_vec())
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                pending
                    .drain(..(1 + size_of_rdb_content.to_string().len() + 2 + size_of_rdb_content));
            }
        }
        return true;
    }

    while !is_full_resync_read || !is_rdb_content_read {
        let n = match stream.read(&mut buf) {
            Ok(n) => n,
            Err(err) => 0,
        };
        pending.extend_from_slice(&buf[..n]);
        println!("current status: {}", String::from_utf8_lossy(&pending));

        if try_to_read_rdb(&mut pending) {
            is_rdb_content_read = true;
        }

        if let Some(pos) = find_complete_frame(&pending) {
            let fullresync_resp = String::from_utf8_lossy(&pending[..pos]);
            println!("{} {} {}", fullresync_resp, pos, pending.len());
            if fullresync_resp.starts_with("+FULLRESYNC") {
                pending.drain(..pos);
                is_full_resync_read = true;
            }

            if try_to_read_rdb(&mut pending) {
                is_rdb_content_read = true;
            }
        }
    }
    println!("at the end {}", String::from_utf8_lossy(&pending));
    return pending;
}

fn handle_replication_loop(mut stream: TcpStream, state: State, remaining_data: Vec<u8>) {
    let mut pending = remaining_data;
    let mut parser = Parser {};
    let mut exec = CommandExecutor {};

    loop {
        let mut buf = [0u8; 4096];
        let n = match stream.read(&mut buf) {
            Ok(n) => n,
            Err(err) => 0,
        };
        pending.extend_from_slice(&buf[..n]);

        while let Some(frame_end) = find_complete_frame(&pending) {
            let frame_bytes: Vec<u8> = pending.drain(..frame_end).collect();
            if let Ok(text) = std::str::from_utf8(&frame_bytes) {
                println!("RESP text frame:\n{}", text);
                let mut commands = parser.parse(text);
                exec.execute(&mut commands, &mut stream, state.clone());
            }
        }
    }
}

fn find_complete_frame(buf: &Vec<u8>) -> Option<usize> {
    if buf.is_empty() {
        return None;
    };

    fn find_bulk_size(buf: &Vec<u8>) -> Option<usize> {
        if let Some(length_of_crlf_idx) = buf.windows(2).position(|w| w == b"\r\n") {
            let length_buffer = &buf[1..length_of_crlf_idx];
            let size_as_string = String::from_utf8(length_buffer.to_vec()).unwrap();
            let size = size_as_string.parse::<usize>().unwrap();
            let size_of_size = size_as_string.len();
            return Some(1 + size_of_size + 2 + size + 2);
        }
        return None;
    }

    match buf[0] {
        b'+' => {
            if let Some(crlf_idx) = buf.windows(2).position(|w| w == b"\r\n") {
                let frame_len = crlf_idx + 2;
                return Some(frame_len);
            }
            return None;
        }
        b'$' => {
            return find_bulk_size(buf);
        }
        b'*' => {
            if let Some(array_size_end_crlf) = buf.windows(2).position(|w| w == b"\r\n") {
                let array_size_buffer = &buf[1..array_size_end_crlf];
                let array_size_as_string = String::from_utf8(array_size_buffer.to_vec()).unwrap();
                let array_size = array_size_as_string.parse::<usize>().unwrap();
                let mut total_bulk_string_size = 0;
                let mut start = 1 + array_size_as_string.len() + 2;
                for _ in 0..array_size {
                    if let Some(bulk_size) = find_bulk_size(&buf[start..].to_vec()) {
                        start += bulk_size;
                        total_bulk_string_size += bulk_size;
                    } else {
                        return None;
                    }
                }
                println!(
                    "{}",
                    1 + array_size_as_string.len() + 2 + total_bulk_string_size
                );
                return Some(1 + array_size_as_string.len() + 2 + total_bulk_string_size);
            }
            return None;
        }
        _ => None,
    }
}
