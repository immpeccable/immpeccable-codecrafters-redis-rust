use crate::classes::State::Replica;
use crate::classes::{
    Constants::EMPTY_RDB_HEX_REPRESENTATION, ExpiringValue::ExpiringValue,
    RespDataType::RespDataType, State::State,
};

use hex;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf};

use core::num;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

pub struct CommandExecutor {}

impl CommandExecutor {
    pub async fn execute(
        &mut self,
        commands: &mut RespDataType,
        reader: Arc<Mutex<OwnedReadHalf>>,
        writer: Arc<Mutex<OwnedWriteHalf>>,
        state: Arc<Mutex<State>>,
    ) {
        match commands {
            RespDataType::Array(v) => self.handle_commands(v, reader, writer, state).await,
            _ => unreachable!("shouldn't be else "),
        }
    }

    async fn handle_commands(
        &mut self,
        commands: &mut Vec<RespDataType>,
        reader: Arc<Mutex<OwnedReadHalf>>,
        mut writer: Arc<Mutex<OwnedWriteHalf>>,
        state: Arc<Mutex<State>>,
    ) {
        let first_command = &commands[0];
        match first_command {
            RespDataType::BulkString(bulk_str) => match bulk_str.to_uppercase().as_str() {
                "PING" => {
                    self.ping_command(commands, writer, state).await;
                }
                "ECHO" => {
                    self.echo_command(commands, writer).await;
                }
                "SET" => {
                    self.set_command(commands, writer, state).await;
                }
                "GET" => {
                    self.get_command(commands, writer, state).await;
                }
                "TYPE" => {
                    self.type_command(commands, writer, state).await;
                }
                "CONFIG" => {
                    if let RespDataType::BulkString(bulk_config_second_string) = &commands[1] {
                        match bulk_config_second_string.as_str() {
                            "GET" => self.config_get_command(commands, writer, state).await,
                            _ => {}
                        }
                    }
                }
                "KEYS" => {
                    self.keys_command(commands, writer, state).await;
                }
                "INFO" => {
                    self.info_command(commands, writer, state).await;
                }
                "REPLCONF" => {
                    self.repl_conf_command(commands, writer, state, reader)
                        .await;
                }

                "PSYNC" => {
                    self.psync(reader, writer, state).await;
                }
                "WAIT" => {
                    self.wait(commands, writer, state).await;
                }
                "XADD" => {
                    self.xadd(commands, writer, state).await;
                }
                _ => {}
            },
            _ => {}
        }
    }

    async fn wait(
        &mut self,
        commands: &mut Vec<RespDataType>,
        writer: Arc<Mutex<OwnedWriteHalf>>,
        mut state: Arc<Mutex<State>>,
    ) {
        let num = match commands.get(1) {
            Some(RespDataType::BulkString(bs)) => bs.parse::<usize>().unwrap_or(0),
            _ => 0,
        };
        let timeout_ms = match commands.get(2) {
            Some(RespDataType::BulkString(bs)) => bs.parse::<u64>().unwrap_or(0),
            _ => 0,
        };

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
            time::sleep(Duration::from_millis(10)).await;
        }
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
        state: Arc<Mutex<State>>,
        reader: Arc<Mutex<OwnedReadHalf>>,
    ) {
        if let RespDataType::BulkString(repl_conf_second) = &commands[1] {
            println!("REPLCONF command received: {}", repl_conf_second);
            match repl_conf_second.to_uppercase().as_str() {
                "CAPA" => {
                    stream.lock().await.write_all(b"+OK\r\n").await.unwrap();
                }
                "LISTENING-PORT" => {
                    stream.lock().await.write_all(b"+OK\r\n").await.unwrap();
                }
                "GETACK" => {
                    println!("Master received GETACK command, responding with ACK");
                    let offset = state.lock().await.offset;
                    let response = format!(
                        "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                        offset.to_string().len(),
                        offset
                    );
                    println!("Sending ACK response: {}", response);
                    println!("Response bytes: {:?}", response.as_bytes());
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
                    if let RespDataType::BulkString(offset_str) = &commands[2] {
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

    async fn psync(
        &mut self,
        reader: Arc<Mutex<OwnedReadHalf>>,
        mut writer: Arc<Mutex<OwnedWriteHalf>>,
        state: Arc<Mutex<State>>,
    ) {
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
        state.lock().await.replicas.push(Replica {
            reader: reader,
            writer: writer,
            last_ack: 0,
        });
    }

    async fn ping_command(
        &mut self,
        _: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: Arc<Mutex<State>>,
    ) {
        if state.lock().await.role == "master" {
            stream.lock().await.write_all(b"+PONG\r\n").await.unwrap();
        }
    }

    async fn config_get_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: Arc<Mutex<State>>,
    ) {
        let config_key = &commands[2];
        if let RespDataType::BulkString(config_key) = config_key {
            match config_key.to_uppercase().as_str() {
                "DIR" => {
                    if let Some(db_dir) = &mut state.lock().await.db_dir {
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
                    if let Some(db_file_name) = &mut state.lock().await.db_file_name {
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

    fn populate_entries(
        &mut self,
        values: Vec<RespDataType>,
        current_id: &String,
    ) -> HashMap<String, String> {
        let mut res: HashMap<String, String> = HashMap::new();
        res.insert("id".to_string(), current_id.clone());
        let mut i = 0;
        while i < values.len() {
            let (key, value) = (&values[i], &values[i + 1]);

            if let (RespDataType::BulkString(key), RespDataType::BulkString(value)) = (key, value) {
                res.insert(key.clone(), value.clone());
            }
            i += 2;
        }
        res
    }

    fn generate_stream_id(&mut self, latest_id: Option<&String>, current_id: String) -> String {
        let parts: Vec<&str> = current_id.split("-").collect();
        let (miliseconds_time, sequence_number) = (parts[0], parts[1]);
        if miliseconds_time == "*" && sequence_number == "*" {
            return current_id;
        } else if sequence_number == "*" {
            match latest_id {
                Some(latest_id) => {
                    let latest_id_parts: Vec<&str> = latest_id.split("-").collect();
                    let last_miliseconds_time = latest_id_parts[0];
                    let last_sequence_number = latest_id_parts[1];

                    let mut current_sequence_number = 0;
                    if miliseconds_time <= last_miliseconds_time {
                        current_sequence_number =
                            last_sequence_number.parse::<u32>().unwrap() + 1u32
                    }

                    return format!("{}-{}", miliseconds_time, current_sequence_number);
                }
                None => {
                    let mut sequence_number = "0";
                    if miliseconds_time == "0" {
                        sequence_number = "1";
                    }
                    println!("sequence number: {}", sequence_number);
                    return format!("{}-{}", miliseconds_time, sequence_number);
                }
            }
        } else {
            return current_id;
        }
    }

    async fn validate_stream_entry(
        &mut self,
        stream: Arc<Mutex<OwnedWriteHalf>>,
        latest_entry_id: Option<&String>,
        current_entry_id: String,
    ) -> bool {
        if current_entry_id == "0-0" {
            stream
                .lock()
                .await
                .write_all(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                .await
                .unwrap();
            return false;
        } else if let Some(latest_entry_id) = latest_entry_id {
            if current_entry_id <= *latest_entry_id {
                stream
                .lock()
                .await
                .write_all(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                .await
                .unwrap();
                return false;
            }
        }

        return true;
    }

    async fn xadd(
        &mut self,
        commands: &mut Vec<RespDataType>,
        stream: Arc<Mutex<OwnedWriteHalf>>,
        state: Arc<Mutex<State>>,
    ) {
        if let (RespDataType::BulkString(key), RespDataType::BulkString(mut current_id)) =
            (commands[1].clone(), commands[2].clone())
        {
            let mut guard = state.lock().await;
            let stream_data = &mut guard.stream_data;

            let stream_vector = stream_data.get(&key);

            let mut latest_entry_id = None;
            if let Some(stream_vector) = stream_vector {
                latest_entry_id = stream_vector.last().unwrap().get(&"id".to_string())
            }

            current_id = self.generate_stream_id(latest_entry_id, current_id);

            let entry = self.populate_entries(commands[3..].to_vec(), &current_id);

            if self
                .validate_stream_entry(stream.clone(), latest_entry_id, current_id.clone())
                .await
            {
                stream_data.entry(key).or_insert_with(Vec::new).push(entry);
                stream
                    .lock()
                    .await
                    .write_all(self.convert_bulk_string_to_resp(&current_id).as_bytes())
                    .await
                    .unwrap();
            }
        }
    }

    async fn set_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        mut state: Arc<Mutex<State>>,
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

        let mut guard = state.lock().await;

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
        println!("inserted hello wtf");

        guard.shared_data.insert(key, hashmap_value);

        println!("after insert");

        if guard.role == "master" {
            stream.lock().await.write_all(b"+OK\r\n").await.unwrap();
        }

        println!("{} {}", guard.role, guard.replicas.len());

        let payload = self.convert_array_to_resp(commands.clone());

        let clients = &mut guard.replicas;
        println!(" client length: {}", clients.len());
        let mut kept = Vec::new();
        let payload_bytes = payload.as_bytes();
        let number_of_bytes_broadcasted = payload_bytes.len();
        for mut client in clients {
            if client
                .writer
                .lock()
                .await
                .write_all(payload_bytes)
                .await
                .is_ok()
            {
                kept.push(client.clone());
            }
        }
        if guard.role == "master" {
            guard.offset += number_of_bytes_broadcasted;
        }
        guard.replicas = kept;
    }

    async fn type_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: Arc<Mutex<State>>,
    ) {
        let guard = state.lock().await;
        let key = commands[1].clone();
        let value = guard.shared_data.get(&key).cloned();
        let mut stream_value = None;

        if let RespDataType::BulkString(key_as_string) = key {
            stream_value = guard.stream_data.get(&key_as_string);
        }

        match value {
            Some(v) => {
                println!("v :{:?}", v.expiration_timestamp);
                if let Some(expiration_timestamp) = v.expiration_timestamp {
                    if Instant::now() > expiration_timestamp {
                        return stream.lock().await.write_all(b"+none\r\n").await.unwrap();
                    }
                }

                return stream.lock().await.write_all(b"+string\r\n").await.unwrap();
            }
            None => match stream_value {
                Some(_stream_value) => stream.lock().await.write_all(b"+stream\r\n").await.unwrap(),
                None => stream.lock().await.write_all(b"+none\r\n").await.unwrap(),
            },
        }
    }

    async fn get_command(
        &mut self,
        commands: &mut Vec<RespDataType>,
        mut stream: Arc<Mutex<OwnedWriteHalf>>,
        state: Arc<Mutex<State>>,
    ) {
        println!("get command");
        let guard = state.lock().await;
        let key = commands[1].clone();
        let value = guard.shared_data.get(&key).cloned();
        println!("value fetched");

        match value {
            Some(v) => {
                println!("v :{:?}", v.expiration_timestamp);
                if let Some(expiration_timestamp) = v.expiration_timestamp {
                    if Instant::now() > expiration_timestamp {
                        return stream.lock().await.write_all(b"$-1\r\n").await.unwrap();
                    }
                }
                let RespDataType::BulkString(value) = &v.value else {
                    unreachable!("protocol invariant violated: expected BulkString");
                };
                let bulk_response = self.convert_bulk_string_to_resp(&value);
                println!("bulk response: {}", bulk_response);
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
        state: Arc<Mutex<State>>,
    ) {
        let guard = state.lock().await;
        let matching_keys = {
            if let RespDataType::BulkString(regex_match) = &commands[1] {
                let mut keys: Vec<String> = Vec::new();
                for key in guard.shared_data.keys() {
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
        state: Arc<Mutex<State>>,
    ) {
        stream.lock().await
            .write_all(
                &self
                    .convert_bulk_string_to_resp(&String::from(format!("role:{}\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", state.lock().await.role)))
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
