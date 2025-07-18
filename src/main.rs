use core::panic;
use core::str;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};
use tokio::sync::Mutex;
use tokio::{signal, task};

mod classes;
use classes::{CommandExecutor::CommandExecutor, Db::Db, Parser::Parser, State::State};

use crate::classes::RespDataType::RespDataType;

#[tokio::main]
async fn main() {
    // bootstrap shared state using the new State structure
    let mut state = State::new();

    // parse args into a map
    let mut args_map = HashMap::new();
    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i + 1 < args.len() {
        args_map.insert(args[i].clone(), args[i + 1].clone());
        i += 2;
    }

    // optional RDB load
    let db_dir = args_map.get("--dir").cloned();
    let db_file_name = args_map.get("--dbfilename").cloned();
    state.set_db_config(db_dir.clone(), db_file_name.clone()).await;
    
    if db_dir.is_some() && db_file_name.is_some() {
        let _ = Db {}.load(&mut state).await;
    }

    let port = args_map.get("--port").map(|s| s.as_str()).unwrap_or("6379");

    let listener = TokioTcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap_or_else(|e| panic!("couldn't bind to port {}: {}", port, e));

    let shared_state = Arc::new(Mutex::new(state.clone()));
    let shared_state_for_listener = shared_state.clone();

    if let Some(replica_of) = args_map.get("--replicaof") {
        let mut parts = replica_of.split_whitespace();
        let host = parts.next().expect("`--replicaof` requires `host port`");
        let master_port = parts.next().expect("`--replicaof` requires `host port`");
        let state_inside_replica = shared_state.clone();
        state_inside_replica.lock().await.set_role("slave".to_string()).await;
        // Note: master_host and master_port are not used in the current implementation
        // but we could add them to the Config if needed

        let mut master_stream = TokioTcpStream::connect(format!("{}:{}", host, master_port))
            .await
            .expect("couldn't connect to master");

        task::spawn(async move {
            let port = args_map.get("--port").map(|s| s.as_str()).unwrap_or("6379");
            let remaining_data = do_replication_handshake(&mut master_stream, port).await;
            let (reader, writer) = master_stream.into_split();
            let shared_reader = Arc::new(Mutex::new(reader));
            let shared_writer = Arc::new(Mutex::new(writer));

            handle_replication_loop(
                shared_reader,
                shared_writer,
                shared_state.clone(),
                remaining_data,
            )
            .await;
        });
    }
    tokio::spawn(async move {
        listener_loop(listener, shared_state_for_listener).await;
    });

    signal::ctrl_c().await.expect("failed to listen for ctrl_c");
    println!("Shutdown signal received, exiting.");
}

// common client‐handling loop (for master clients only)
async fn handle_client(stream: TokioTcpStream, state: Arc<Mutex<State>>) {
    let pending: Vec<u8> = Vec::new();
    let (raw_reader, raw_writer) = stream.into_split();
    let writer = Arc::new(Mutex::new(raw_writer));
    let reader = Arc::new(Mutex::new(raw_reader));
    handle_command_loop(state, reader, writer, pending).await;
}

async fn handle_replication_loop(
    reader: Arc<Mutex<OwnedReadHalf>>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    mut state: Arc<Mutex<State>>,
    mut remaining_data: Vec<u8>,
) {
    handle_command_loop(state, reader, writer, remaining_data).await;
}

async fn handle_command_loop(
    state: Arc<Mutex<State>>,
    reader: Arc<Mutex<OwnedReadHalf>>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    mut pending: Vec<u8>,
) {
    let mut parser = Parser {};
    let mut exec = CommandExecutor {};
    let mut queued: Vec<Vec<String>> = Vec::new();
    let mut in_multi = false;

    loop {
        while let Some(frame_end) = find_complete_frame(&pending) {
            let frame_bytes: Vec<u8> = pending.drain(..frame_end).collect();

            if let Ok(text) = std::str::from_utf8(&frame_bytes) {
                let commands = parser.parse(text);
                if !commands.is_empty() {
                    let command_type = &commands[0];
                    println!("command type: {:?}", command_type);
                    if in_multi
                        && command_type.to_uppercase() != "EXEC"
                        && command_type.to_uppercase() != "DISCARD"
                    {
                        queued.push(commands);
                        writer.lock().await.write_all(RespDataType::SimpleString("QUEUED".to_string()).to_string().as_bytes()).await.unwrap();
                    } else if in_multi && command_type.to_uppercase() == "EXEC" {
                        let mut queued_clone = queued.clone();

                        exec.execute(
                            commands,
                            reader.clone(),
                            writer.clone(),
                            state.clone(),
                            &mut queued,
                            &mut in_multi,
                        )
                        .await;

                        for command in &mut queued {
                            exec.execute(
                                command.clone(),
                                reader.clone(),
                                writer.clone(),
                                state.clone(),
                                &mut queued_clone,
                                &mut in_multi,
                            )
                            .await;
                        }

                        in_multi = false;
                        queued.clear();
                    } else {
                        exec.execute(
                            commands,
                            reader.clone(),
                            writer.clone(),
                            state.clone(),
                            &mut queued,
                            &mut in_multi,
                        )
                        .await;
                    }
                }
                if state.lock().await.get_role().await == "slave" {
                    state.lock().await.increment_offset(frame_bytes.len()).await;
                }
            }
        }

        let mut buf = [0u8; 2048];

        let n = match reader.lock().await.read(&mut buf).await {
            Ok(0) | Err(_) => return, // connection closed or error
            Ok(n) => n,
        };

        pending.extend_from_slice(&buf[..n]);
    }
}

async fn listener_loop(listener: TokioTcpListener, state: Arc<Mutex<State>>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state_clone = state.clone();
                task::spawn(async move {
                    handle_client(stream, state_clone).await;
                });
            }
            Err(e) => eprintln!("accept error: {}", e),
        }
    }
}

// PSYNC/REPLCONF handshake
async fn do_replication_handshake(stream: &mut TokioTcpStream, port: &str) -> Vec<u8> {
    // PING
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut buf = [0u8; 512];
    let _n = stream.read(&mut buf).await.unwrap();

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
        .await
        .unwrap();
    let __n = stream.read(&mut buf).await.unwrap();

    // REPLCONF capa psync2
    stream
        .write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        .await
        .unwrap();
    let ___n = stream.read(&mut buf).await.unwrap();

    // PSYNC ? -1
    stream
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .await
        .unwrap();

    let mut pending: Vec<u8> = Vec::new();
    let mut buf = vec![0; 2048];
    let mut is_full_resync_read = false;
    let mut is_rdb_content_read = false;

    while !is_full_resync_read || !is_rdb_content_read {
        let n = match stream.read(&mut buf).await {
            Ok(n) => n,
            Err(_err) => 0,
        };
        pending.extend_from_slice(&buf[..n]);

        if !is_full_resync_read {
            if let Some(pos) = find_complete_frame(&pending) {
                let fullresync_resp = String::from_utf8_lossy(&pending[..pos]);
                if fullresync_resp.starts_with("+FULLRESYNC") {
                    pending.drain(..pos);
                    is_full_resync_read = true;
                }
            }
        }
        if is_full_resync_read {
            if pending.len() > 0 && pending[0] == b'$' {
                if let Some(end_of_rdb_clrf) = pending.windows(2).position(|w| w == b"\r\n") {
                    let size_of_rdb_content =
                        String::from_utf8((&pending[1..end_of_rdb_clrf]).to_vec())
                            .unwrap()
                            .parse::<usize>()
                            .unwrap();
                    pending.drain(
                        ..(1 + size_of_rdb_content.to_string().len() + 2 + size_of_rdb_content),
                    );
                    is_rdb_content_read = true;
                }
            }
        }
    }

    return pending;
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
                return Some(1 + array_size_as_string.len() + 2 + total_bulk_string_size);
            }
            return None;
        }
        _ => None,
    }
}
