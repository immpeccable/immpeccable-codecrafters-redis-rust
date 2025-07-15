use crate::classes::{RespDataType::RespDataType, State::State, State::Replica, Constants::EMPTY_RDB_HEX_REPRESENTATION};
use std::sync::Arc;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;
use tokio::io::AsyncWriteExt;
use std::time::{Duration, Instant};
use hex;

pub async fn handle_psync(
    reader: Arc<Mutex<OwnedReadHalf>>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    println!("received psync command");
    state.lock().await.replicas.push(Replica {
        reader: reader.clone(),
        writer: writer.clone(),
        last_ack: 0,
    });
    let header = format!("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0");
    writer
        .lock()
        .await
        .write_all(format!("+{}\r\n", header).as_bytes())
        .await
        .unwrap();

    // send empty-RDB payload
    let dump = hex::decode(EMPTY_RDB_HEX_REPRESENTATION).unwrap();
    writer
        .lock()
        .await
        .write_all(format!("${}\r\n", dump.len()).as_bytes())
        .await
        .unwrap();
    writer.lock().await.write_all(&dump).await.unwrap();
}

pub async fn handle_replconf(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
    reader: Arc<Mutex<OwnedReadHalf>>,
) {
    if let Some(repl_conf_second) = commands.get(1) {
        println!("REPLCONF command received: {}", repl_conf_second);
        match repl_conf_second.to_uppercase().as_str() {
            "CAPA" => {
                stream.lock().await.write_all(RespDataType::SimpleString("OK".to_string()).to_string().as_bytes()).await.unwrap();
            }
            "LISTENING-PORT" => {
                stream.lock().await.write_all(RespDataType::SimpleString("OK".to_string()).to_string().as_bytes()).await.unwrap();
            }
            "GETACK" => {
                let offset = state.lock().await.offset;
                let response = format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                    offset.to_string().len(),
                    offset
                );
                let mut writer = stream.lock().await;
                let result = writer.write_all(response.as_bytes()).await;
                match result {
                    Ok(_) => {
                        println!("ACK response sent successfully");
                        // Try to flush the stream
                        if let Err(e) = writer.flush().await {
                            println!("Failed to flush stream: {}", e);
                        }
                    }
                    Err(e) => println!("Failed to send ACK response: {}", e),
                }
            }
            "ACK" => {
                // This is a response from a replica to a GETACK command
                if let Some(offset_str) = commands.get(2) {
                    if let Ok(offset) = offset_str.parse::<u64>() {
                        println!("Received ACK with offset: {}", offset);
                        // Find the replica that sent this ACK and update its last_ack
                        let mut guard = state.lock().await;
                        for replica in &mut guard.replicas {
                            // Match by reader connection to identify which replica sent this ACK
                            if Arc::ptr_eq(&replica.reader, &reader) {
                                replica.last_ack = offset;
                                println!("Updated replica last_ack to: {}", offset);
                                break;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

pub async fn handle_wait(
    commands: &mut Vec<String>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    let num = commands.get(1).and_then(|bs| bs.parse::<usize>().ok()).unwrap_or(0);
    let timeout_ms = commands.get(2).and_then(|bs| bs.parse::<u64>().ok()).unwrap_or(0);

    let target = state.lock().await.offset;
    if target == 0 {
        // If no commands have been sent, all replicas are considered caught up
        let num_replicas = state.lock().await.replicas.len();
        let mut w = writer.lock().await;
        w.write_all(format!(":{}\r\n", num_replicas).as_bytes())
            .await
            .unwrap();
        return;
    }

    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let mut getack_sent = false; // Track if we've sent GETACK commands

    loop {
        // Send GETACK commands only once
        if !getack_sent {
            let mut replicas_to_check = Vec::new();
            {
                let guard = state.lock().await;
                for replica in &guard.replicas {
                    if replica.last_ack < target.try_into().unwrap() {
                        // Clone the writer to send GETACK outside the lock
                        replicas_to_check.push(replica.writer.clone());
                    }
                }
            }

            // Send GETACK commands outside the state lock
            for replica_writer in &replicas_to_check {
                let cmd = "*3\r\n\
                     $8\r\nREPLCONF\r\n\
                     $6\r\nGETACK\r\n\
                     $1\r\n*\r\n"
                    .as_bytes();
                if let Err(e) = replica_writer.lock().await.write_all(cmd).await {
                    println!("Failed to send GETACK: {}", e);
                }
            }
            getack_sent = true;
        }

        let mut acks = 0;
        {
            let guard = state.lock().await;
            for replica in &guard.replicas {
                if replica.last_ack >= target.try_into().unwrap() {
                    acks += 1;
                }
            }
        }

        if acks >= num || Instant::now() >= deadline {
            let mut w = writer.lock().await;
            w.write_all(format!(":{}\r\n", acks).as_bytes())
                .await
                .unwrap();
            break;
        }

        // back off a bit before polling again
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
} 