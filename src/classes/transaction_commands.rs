use crate::classes::{RespDataType::RespDataType, State::State};
use std::sync::Arc;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::io::AsyncWriteExt;

pub async fn handle_multi(
    _commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    in_multi: &mut bool,
) {
    *in_multi = true;
    stream.lock().await.write_all(RespDataType::SimpleString("OK".to_string()).to_string().as_bytes()).await.unwrap();
}

pub async fn handle_discard(
    _commands: &mut Vec<String>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    queued: &mut Vec<Vec<String>>,
    in_multi: &mut bool,
) {
    if *in_multi == false {
        writer
            .lock()
            .await
            .write_all(RespDataType::SimpleError("ERR DISCARD without MULTI".to_string()).to_string().as_bytes())
            .await
            .unwrap();
    } else {
        writer.lock().await.write_all(RespDataType::SimpleString("OK".to_string()).to_string().as_bytes()).await.unwrap();
        queued.clear();
        *in_multi = false;
    }
}

pub async fn handle_exec(
    _commands: &mut Vec<String>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    queued: &mut Vec<Vec<String>>,
    in_multi: &mut bool,
    state: Arc<Mutex<State>>,
) {
    if *in_multi == false {
        writer
            .lock()
            .await
            .write_all(RespDataType::SimpleError("ERR EXEC without MULTI".to_string()).to_string().as_bytes())
            .await
            .unwrap();
    } else {
        // Execute all queued commands and collect results
        let mut results = Vec::new();
        
        for command in queued.iter() {
            let mut cmd = command.clone();
            let first_command = &cmd[0];
            
            // Execute the command directly against the state and capture the result
            let result = match first_command.to_uppercase().as_str() {
                "SET" => {
                    if cmd.len() >= 3 {
                        let key = cmd[1].clone();
                        let value = cmd[2].clone();
                        state.lock().await.set_string(key, value, None).await;
                        RespDataType::SimpleString("OK".to_string())
                    } else {
                        RespDataType::SimpleError("ERR wrong number of arguments for SET command".to_string())
                    }
                }
                "GET" => {
                    if cmd.len() >= 2 {
                        let key = &cmd[1];
                        let value = state.lock().await.get_string(key).await;
                        match value {
                            Some(v) => {
                                if let Some(expiration_timestamp) = v.expiration_timestamp {
                                    if std::time::Instant::now() > expiration_timestamp {
                                        RespDataType::Nil
                                    } else {
                                        RespDataType::BulkString(v.value)
                                    }
                                } else {
                                    RespDataType::BulkString(v.value)
                                }
                            }
                            None => RespDataType::Nil,
                        }
                    } else {
                        RespDataType::SimpleError("ERR wrong number of arguments for GET command".to_string())
                    }
                }
                "INCR" => {
                    if cmd.len() >= 2 {
                        let key = cmd[1].clone();
                        let state_guard = state.lock().await;
                        let current_value = state_guard.get_string(&key).await;
                        
                        match current_value {
                            Some(v) => {
                                match v.value.parse::<u64>() {
                                    Ok(numeric_value) => {
                                        let new_value = (numeric_value + 1).to_string();
                                        let new_expiration = v.expiration_timestamp;
                                        drop(state_guard);
                                        state.lock().await.set_string(key.clone(), new_value, new_expiration).await;
                                        RespDataType::Integer((numeric_value + 1) as i64)
                                    }
                                    Err(_) => {
                                        RespDataType::SimpleError("ERR value is not an integer or out of range".to_string())
                                    }
                                }
                            }
                            None => {
                                drop(state_guard);
                                state.lock().await.set_string(key.clone(), "1".to_string(), None).await;
                                RespDataType::Integer(1)
                            }
                        }
                    } else {
                        RespDataType::SimpleError("ERR wrong number of arguments for INCR command".to_string())
                    }
                }
                "TYPE" => {
                    if cmd.len() >= 2 {
                        let key = &cmd[1];
                        let value_type = state.lock().await.get_type(key).await;
                        RespDataType::SimpleString(value_type.to_string())
                    } else {
                        RespDataType::SimpleError("ERR wrong number of arguments for TYPE command".to_string())
                    }
                }
                "PING" => {
                    RespDataType::SimpleString("PONG".to_string())
                }
                "ECHO" => {
                    if cmd.len() >= 2 {
                        RespDataType::BulkString(cmd[1].clone())
                    } else {
                        RespDataType::SimpleError("ERR wrong number of arguments for ECHO command".to_string())
                    }
                }
                "KEYS" => {
                    if cmd.len() >= 2 {
                        let pattern = &cmd[1];
                        let all_keys = state.lock().await.get_all_keys().await;
                        let matching_keys: Vec<String> = all_keys.into_iter()
                            .filter(|key| {
                                let parts: Vec<&str> = pattern.split("*").collect();
                                key.starts_with(parts[0]) && key.ends_with(parts[1])
                            })
                            .collect();
                        RespDataType::Array(matching_keys.into_iter().map(|k| RespDataType::BulkString(k)).collect())
                    } else {
                        RespDataType::SimpleError("ERR wrong number of arguments for KEYS command".to_string())
                    }
                }
                "INFO" => {
                    let role = state.lock().await.get_role().await;
                    let info = format!("role:{}\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", role);
                    RespDataType::BulkString(info)
                }
                "CONFIG" => {
                    if cmd.len() >= 3 && cmd[1] == "GET" {
                        let param = &cmd[2];
                        let (db_dir, db_file_name) = state.lock().await.get_db_config().await;
                        
                        let config_value = match param.to_uppercase().as_str() {
                            "DIR" => db_dir.unwrap_or_default(),
                            "DBFILENAME" => db_file_name.unwrap_or_default(),
                            _ => String::new(),
                        };
                        
                        let mut result = Vec::new();
                        result.push(RespDataType::BulkString(param.clone()));
                        result.push(RespDataType::BulkString(config_value));
                        RespDataType::Array(result)
                    } else {
                        RespDataType::SimpleError("ERR wrong number of arguments for CONFIG command".to_string())
                    }
                }
                "WAIT" => {
                    if cmd.len() >= 2 {
                        let num_replicas = cmd[1].parse::<u64>().unwrap_or(0);
                        RespDataType::Integer(num_replicas as i64)
                    } else {
                        RespDataType::SimpleError("ERR wrong number of arguments for WAIT command".to_string())
                    }
                }
                "XADD" => {
                    if cmd.len() >= 4 {
                        let stream_key = cmd[1].clone();
                        let mut current_id = cmd[2].clone();
                        let field_value_pairs = cmd[3..].to_vec();
                        
                        if field_value_pairs.len() % 2 != 0 {
                            results.push(RespDataType::SimpleError("ERR wrong number of stream fields".to_string()));
                            continue;
                        }
                        
                        let state_guard = state.lock().await;
                        let stream_data = state_guard.get_stream(&stream_key).await;
                        let mut latest_entry_id = None;
                        if let Some(stream_vector) = &stream_data {
                            if let Some(last_entry) = stream_vector.last() {
                                for (k, v) in last_entry {
                                    if k == "id" {
                                        latest_entry_id = Some(v);
                                        break;
                                    }
                                }
                            }
                        }
                        
                        // Simple ID generation for now
                        if current_id == "*" {
                            current_id = format!("{}-0", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis());
                        }
                        
                        let mut entry = Vec::new();
                        entry.push(("id".to_string(), current_id.clone()));
                        for i in (0..field_value_pairs.len()).step_by(2) {
                            entry.push((field_value_pairs[i].clone(), field_value_pairs[i + 1].clone()));
                        }
                        
                        let mut new_stream_data = stream_data.unwrap_or_default();
                        new_stream_data.push(entry);
                        drop(state_guard);
                        state.lock().await.set_stream(stream_key, new_stream_data).await;
                        
                        RespDataType::BulkString(current_id)
                    } else {
                        RespDataType::SimpleError("ERR wrong number of arguments for XADD command".to_string())
                    }
                }
                "XRANGE" => {
                    if cmd.len() >= 4 {
                        let stream_key = cmd[1].clone();
                        let mut start = cmd[2].clone();
                        let mut end = cmd[3].clone();
                        
                        if start == "-" {
                            start = "0-0".to_string();
                        } else if !start.contains("-") {
                            start.push_str("-0");
                        }
                        if end != "+" && !end.contains("-") {
                            end.push_str("-0");
                        }
                        
                        let stream_data = state.lock().await.get_stream(&stream_key).await;
                        match stream_data {
                            Some(vec) => {
                                let mut array_elements = Vec::new();
                                for entry in &vec {
                                    let mut entry_id = None;
                                    for (k, v) in entry {
                                        if k == "id" {
                                            entry_id = Some(v);
                                            break;
                                        }
                                    }
                                    if let Some(entry_id) = entry_id {
                                        if start.as_str() <= entry_id.as_str() {
                                            if entry_id.as_str() <= end.as_str() || end == "+" {
                                                let mut entry_array = Vec::new();
                                                entry_array.push(RespDataType::BulkString(entry_id.clone()));
                                                
                                                let mut fields_array = Vec::new();
                                                for (k, v) in entry {
                                                    if k != "id" {
                                                        fields_array.push(RespDataType::BulkString(k.clone()));
                                                        fields_array.push(RespDataType::BulkString(v.clone()));
                                                    }
                                                }
                                                entry_array.push(RespDataType::Array(fields_array));
                                                array_elements.push(RespDataType::Array(entry_array));
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                }
                                RespDataType::Array(array_elements)
                            }
                            None => RespDataType::Array(vec![]),
                        }
                    } else {
                        RespDataType::SimpleError("ERR wrong number of arguments for XRANGE command".to_string())
                    }
                }
                "XREAD" => {
                    if cmd.len() >= 3 {
                        let mut streams = Vec::new();
                        let mut keys = Vec::new();
                        
                        // Parse the command arguments
                        let mut i = 1;
                        while i < cmd.len() {
                            if cmd[i].to_uppercase() == "BLOCK" {
                                if i + 1 < cmd.len() {
                                    let block_time = cmd[i + 1].parse::<u64>().unwrap_or(0);
                                    if block_time > 0 {
                                        // For now, just return empty result for blocking reads
                                        results.push(RespDataType::Nil);
                                        continue;
                                    }
                                    i += 2;
                                } else {
                                    results.push(RespDataType::SimpleError("ERR wrong number of arguments for XREAD command".to_string()));
                                    continue;
                                }
                            } else if cmd[i].to_uppercase() == "COUNT" {
                                if i + 1 < cmd.len() {
                                    // Skip count for now
                                    i += 2;
                                } else {
                                    results.push(RespDataType::SimpleError("ERR wrong number of arguments for XREAD command".to_string()));
                                    continue;
                                }
                            } else if cmd[i].to_uppercase() == "STREAMS" {
                                i += 1;
                                while i < cmd.len() {
                                    keys.push(cmd[i].clone());
                                    i += 1;
                                }
                            } else {
                                keys.push(cmd[i].clone());
                                i += 1;
                            }
                        }
                        
                        if keys.len() % 2 != 0 {
                            results.push(RespDataType::SimpleError("ERR Unbalanced XREAD list of streams".to_string()));
                            continue;
                        }
                        
                        let num_streams = keys.len() / 2;
                        for j in 0..num_streams {
                            let stream_key = keys[j].clone();
                            let id = keys[j + num_streams].clone();
                            
                            let stream_data = state.lock().await.get_stream(&stream_key).await;
                            match stream_data {
                                Some(vec) => {
                                    let mut matching_entries = Vec::new();
                                    for entry in &vec {
                                        let mut entry_id = None;
                                        for (k, v) in entry {
                                            if k == "id" {
                                                entry_id = Some(v);
                                                break;
                                            }
                                        }
                                        if let Some(entry_id) = entry_id {
                                            if entry_id.as_str() > id.as_str() {
                                                matching_entries.push(entry.clone());
                                            }
                                        }
                                    }
                                    
                                    if !matching_entries.is_empty() {
                                        let mut stream_array = Vec::new();
                                        stream_array.push(RespDataType::BulkString(keys[j].clone()));
                                        
                                        let mut entries_array = Vec::new();
                                        for entry in matching_entries {
                                            let mut entry_array = Vec::new();
                                            let mut entry_id = None;
                                            for (k, v) in &entry {
                                                if k == "id" {
                                                    entry_id = Some(v.clone());
                                                    break;
                                                }
                                            }
                                            if let Some(entry_id) = entry_id {
                                                entry_array.push(RespDataType::BulkString(entry_id));
                                            }
                                            
                                            let mut fields_array = Vec::new();
                                            for (k, v) in entry {
                                                if k != "id" {
                                                    fields_array.push(RespDataType::BulkString(k));
                                                    fields_array.push(RespDataType::BulkString(v));
                                                }
                                            }
                                            entry_array.push(RespDataType::Array(fields_array));
                                            entries_array.push(RespDataType::Array(entry_array));
                                        }
                                        stream_array.push(RespDataType::Array(entries_array));
                                        streams.push(RespDataType::Array(stream_array));
                                    }
                                }
                                None => {
                                    // Skip this stream if it doesn't exist
                                }
                            }
                        }
                        
                        if streams.is_empty() {
                            RespDataType::Nil
                        } else {
                            RespDataType::Array(streams)
                        }
                    } else {
                        RespDataType::SimpleError("ERR wrong number of arguments for XREAD command".to_string())
                    }
                }
                _ => {
                    RespDataType::SimpleError("ERR unknown command".to_string())
                }
            };
            
            results.push(result);
        }
        
        // Send the results as a RESP array
        let response = RespDataType::Array(results);
        writer.lock().await.write_all(response.to_string().as_bytes()).await.unwrap();
        
        // Clear the queue and reset multi state
        queued.clear();
        *in_multi = false;
    }
} 