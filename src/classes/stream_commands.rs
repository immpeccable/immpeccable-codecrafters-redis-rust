use crate::classes::{RespDataType::RespDataType, State::State};
use std::sync::Arc;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::io::AsyncWriteExt;
use std::time::{SystemTime, UNIX_EPOCH, Instant, Duration};
use tokio::time;

fn populate_entries(values: Vec<String>, current_id: &String) -> Vec<(String, String)> {
    let mut res: Vec<(String, String)> = Vec::new();
    res.push(("id".to_string(), current_id.clone()));
    let mut i = 0;
    while i < values.len() {
        let (key, value) = (&values[i], &values[i + 1]);
        res.push((key.clone(), value.clone()));
        i += 2;
    }
    res
}

fn generate_stream_id(latest_id: Option<&String>, current_id: String) -> String {
    if current_id == "*" {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("time should go forward");
        return format!("{}-{}", since_the_epoch.as_millis(), 0);
    }

    let parts: Vec<&str> = current_id.split("-").collect();
    let (miliseconds_time, sequence_number) = (parts[0], parts[1]);

    if sequence_number == "*" {
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
    }

    return current_id;
}

async fn validate_stream_entry(
    stream: Arc<Mutex<OwnedWriteHalf>>,
    latest_entry_id: Option<&String>,
    current_entry_id: String,
) -> bool {
    if current_entry_id == "0-0" {
        stream
            .lock()
            .await
            .write_all(RespDataType::SimpleError("ERR The ID specified in XADD must be greater than 0-0".to_string()).to_string().as_bytes())
            .await
            .unwrap();
        return false;
    } else if let Some(latest_entry_id) = latest_entry_id {
        if current_entry_id <= *latest_entry_id {
            stream
            .lock()
            .await
            .write_all(RespDataType::SimpleError("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()).to_string().as_bytes())
            .await
            .unwrap();
            return false;
        }
    }

    return true;
}

pub async fn handle_xadd(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    if commands.len() >= 3 {
        let key = commands[1].clone();
        let mut current_id = commands[2].clone();
        let state_guard = state.lock().await;
        
        let stream_data = state_guard.get_stream(&key).await;
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
        current_id = generate_stream_id(latest_entry_id, current_id);
        let entry = populate_entries(commands[3..].to_vec(), &current_id);
        
        if validate_stream_entry(stream.clone(), latest_entry_id, current_id.clone()).await {
            let mut new_stream_data = stream_data.unwrap_or_default();
            new_stream_data.push(entry);
            drop(state_guard);
            state.lock().await.set_stream(key, new_stream_data).await;
            
            let role = state.lock().await.get_role().await;
            if role == "master" {
                stream
                    .lock()
                    .await
                    .write_all(RespDataType::BulkString(current_id).to_string().as_bytes())
                    .await
                    .unwrap();
            }
        }
    }
}

pub async fn handle_xrange(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    if commands.len() >= 4 {
        let key = commands[1].clone();
        let mut start = commands[2].clone();
        let mut end = commands[3].clone();
        let state_guard = state.lock().await;
        
        let stream_data = state_guard.get_stream(&key).await;
        
        if start == "-" {
            start = "0-0".to_string();
        } else if !start.contains("-") {
            start.push_str("-0");
        }
        if end != "+" && !end.contains("-") {
            end.push_str("-0");
        }
        
        match stream_data {
            Some(vec) => {
                let mut result: Vec<&Vec<(String, String)>> = Vec::new();
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
                                result.push(entry);
                            } else {
                                break;
                            }
                        }
                    }
                }
                let mut result_resp_string = String::from(format!("*{}\r\n", result.len()));
                for entry in result {
                    result_resp_string.push_str("*2\r\n");
                    let mut entry_id = None;
                    for (k, v) in entry {
                        if k == "id" {
                            entry_id = Some(v);
                            break;
                        }
                    }
                    if let Some(entry_id) = entry_id {
                        result_resp_string.push_str(&format!("{}", RespDataType::BulkString(entry_id.clone())));
                    }
                    let mut field_value_pairs = Vec::new();
                    for (k, v) in entry {
                        if k != "id" {
                            field_value_pairs.push(k.clone());
                            field_value_pairs.push(v.clone());
                        }
                    }
                    result_resp_string.push_str(&format!("{}", RespDataType::Array(field_value_pairs.iter().map(|s| RespDataType::BulkString(s.clone())).collect())));
                }
                stream
                    .lock()
                    .await
                    .write_all(result_resp_string.as_bytes())
                    .await
                    .unwrap();
            }
            None => {
                stream.lock().await.write_all(RespDataType::Array(vec![]).to_string().as_bytes()).await.unwrap();
            }
        }
    }
}

pub async fn handle_xread(
    commands: &mut Vec<String>,
    stream: Arc<Mutex<OwnedWriteHalf>>,
    state: Arc<Mutex<State>>,
) {
    if commands.len() < 3 {
        return stream
            .lock()
            .await
            .write_all(RespDataType::SimpleError("ERR wrong number of arguments for XREAD command".to_string()).to_string().as_bytes())
            .await
            .unwrap();
    }

    let mut i = 1;
    let mut stream_keys_and_starts = Vec::new();
    let mut has_block = false;
    let mut block_time: Option<u64> = None;

    while i < commands.len() {
        let command = &commands[i];
        if command.to_uppercase() == "BLOCK" {
            has_block = true;
            if i + 1 < commands.len() {
                if let Ok(timeout) = commands[i + 1].parse::<u64>() {
                    block_time = Some(timeout);
                }
            }
            i += 2;
        } else if command.to_uppercase() == "STREAMS" {
            i += 1;
            break;
        } else {
            stream_keys_and_starts.push(command.clone());
            i += 1;
        }
    }

    while i < commands.len() {
        stream_keys_and_starts.push(commands[i].clone());
        i += 1;
    }

    if stream_keys_and_starts.len() % 2 != 0 {
        return stream
            .lock()
            .await
            .write_all(RespDataType::SimpleError("ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.".to_string()).to_string().as_bytes())
            .await
            .unwrap();
    }

    let half = stream_keys_and_starts.len() / 2;
    let mut restructured_keys_and_starts = Vec::new();
    let mut j = 0;
    while j < half {
        let stream_key = stream_keys_and_starts[j].clone();
        restructured_keys_and_starts.push(stream_key.clone());
        let exclusive_start_key = stream_keys_and_starts[j + half].clone();
        if exclusive_start_key == "$" {
            let stream_data = state.lock().await.get_stream(&stream_key).await;
            match stream_data {
                Some(v) => {
                    if let Some(last_entry) = v.last() {
                        for (k, v) in last_entry {
                            if k == "id" {
                                restructured_keys_and_starts.push(v.clone());
                                break;
                            }
                        }
                    } else {
                        restructured_keys_and_starts.push(String::from("0-0"));
                    }
                }
                None => {
                    restructured_keys_and_starts.push(String::from("0-0"));
                }
            }
        } else {
            restructured_keys_and_starts.push(exclusive_start_key);
        }
        j += 1;
    }
    
    if has_block {
        let deadline = if let Some(timeout) = block_time {
            if timeout == 0 {
                None // Infinite block
            } else {
                Some(Instant::now() + Duration::from_millis(timeout))
            }
        } else {
            None
        };
        loop {
            let mut results: Vec<(String, Vec<Vec<String>>)> = Vec::new();
            let mut i = 0;
            while i < restructured_keys_and_starts.len() {
                let stream_key = restructured_keys_and_starts[i].clone();
                let exclusive_start = restructured_keys_and_starts[i + 1].clone();
                let mut result_vector_for_stream: Vec<Vec<String>> = Vec::new();
                let stream_vector = state.lock().await.get_stream(&stream_key).await;
                match stream_vector {
                    Some(stream_vector) => {
                        let mut valid_entries: Vec<&Vec<(String, String)>> = Vec::new();
                        for entry in &stream_vector {
                            let mut entry_id = None;
                            for (k, v) in entry {
                                if k == "id" {
                                    entry_id = Some(v);
                                    break;
                                }
                            }
                            if let Some(entry_id) = entry_id {
                                if entry_id.as_str() > exclusive_start.as_str() {
                                    valid_entries.push(entry);
                                }
                            }
                        }
                        for entry in valid_entries {
                            let mut result_vec_for_entry = Vec::new();
                            for (key, value) in entry {
                                if key != "id" {
                                    result_vec_for_entry.push(key.clone());
                                }
                                result_vec_for_entry.push(value.clone());
                            }
                            result_vector_for_stream.push(result_vec_for_entry);
                        }
                    }
                    None => {}
                }
                if !result_vector_for_stream.is_empty() {
                    results.push((stream_key.clone(), result_vector_for_stream));
                }
                i += 2;
            }
            if !results.is_empty() {
                let mut result_resp_string = format!("*{}\r\n", results.len());
                for (rk, rv) in results {
                    result_resp_string.push_str("*2\r\n");
                    result_resp_string.push_str(&format!("{}", RespDataType::BulkString(rk.clone())));
                    result_resp_string.push_str(&format!("*{}\r\n", rv.len()));
                    for entry in rv {
                        result_resp_string.push_str("*2\r\n");
                        result_resp_string.push_str(&format!("{}", RespDataType::BulkString(entry[0].clone())));
                        result_resp_string.push_str(&format!("{}", RespDataType::Array(entry[1..].iter().map(|el| RespDataType::BulkString(el.clone())).collect())));
                    }
                }
                return stream
                    .lock()
                    .await
                    .write_all(result_resp_string.as_bytes())
                    .await
                    .unwrap();
            }
            if let Some(deadline) = deadline {
                if Instant::now() >= deadline {
                    break;
                }
            }
            time::sleep(Duration::from_millis(10)).await;
        }
        // If we get here, it means we timed out (not BLOCK 0)
        return stream.lock().await.write_all(RespDataType::Nil.to_string().as_bytes()).await.unwrap();
    }
    
    let mut results: Vec<(String, Vec<Vec<String>>)> = Vec::new();
    let mut i = 0;
    while i < restructured_keys_and_starts.len() {
        let stream_key = restructured_keys_and_starts[i].clone();
        let exclusive_start = restructured_keys_and_starts[i + 1].clone();
        let mut result_vector_for_stream: Vec<Vec<String>> = Vec::new();
        let stream_vector = state.lock().await.get_stream(&stream_key).await;
        match stream_vector {
            Some(stream_vector) => {
                let mut valid_entries: Vec<&Vec<(String, String)>> = Vec::new();
                for entry in &stream_vector {
                    let mut entry_id = None;
                    for (k, v) in entry {
                        if k == "id" {
                            entry_id = Some(v);
                            break;
                        }
                    }
                    if let Some(entry_id) = entry_id {
                        if entry_id.as_str() > exclusive_start.as_str() {
                            valid_entries.push(entry);
                        }
                    }
                }
                for entry in valid_entries {
                    let mut result_vec_for_entry = Vec::new();
                    for (key, value) in entry {
                        if key != "id" {
                            result_vec_for_entry.push(key.clone());
                        }
                        result_vec_for_entry.push(value.clone());
                    }
                    result_vector_for_stream.push(result_vec_for_entry);
                }
            }
            None => {}
        }
        if !result_vector_for_stream.is_empty() {
            results.push((stream_key.clone(), result_vector_for_stream));
        }
        i += 2;
    }
    
    if results.is_empty() {
        return stream.lock().await.write_all(RespDataType::Nil.to_string().as_bytes()).await.unwrap();
    }
    
    let mut result_resp_string = format!("*{}\r\n", results.len());
    for (rk, rv) in results {
        result_resp_string.push_str("*2\r\n");
        result_resp_string.push_str(&format!("{}", RespDataType::BulkString(rk.clone())));
        result_resp_string.push_str(&format!("*{}\r\n", rv.len()));
        for entry in rv {
            result_resp_string.push_str("*2\r\n");
            result_resp_string.push_str(&format!("{}", RespDataType::BulkString(entry[0].clone())));
            result_resp_string.push_str(&format!("{}", RespDataType::Array(entry[1..].iter().map(|el| RespDataType::BulkString(el.clone())).collect())));
        }
    }
    stream
        .lock()
        .await
        .write_all(result_resp_string.as_bytes())
        .await
        .unwrap();
} 