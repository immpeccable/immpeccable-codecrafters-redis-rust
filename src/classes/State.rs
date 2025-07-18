use std::{collections::HashMap, sync::Arc};
use tokio::{
    io::AsyncWriteExt,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::Mutex,
};
use std::collections::VecDeque;
use std::time::Instant;

use crate::classes::ExpiringValue::ExpiringValue;

#[derive(Clone)]
pub struct Replica {
    pub reader: Arc<Mutex<OwnedReadHalf>>,
    pub writer: Arc<Mutex<OwnedWriteHalf>>,
    pub last_ack: u64,
}

// Unified value representation for all Redis data types
#[derive(Clone)]
pub enum Value {
    String(String),
    Stream(Vec<Vec<(String, String)>>),
    List(Vec<String>),
    // Future data types can be added here:
    // Set(HashSet<String>),
    // Hash(HashMap<String, String>),
    // SortedSet(Vec<(String, f64)>),
}

impl Value {
    pub fn get_type(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::Stream(_) => "stream",
            Value::List(_) => "list",
        }
    }

    pub fn as_string(&self) -> Option<&String> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_stream(&self) -> Option<&Vec<Vec<(String, String)>>> {
        match self {
            Value::Stream(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&Vec<String>> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_list_mut(&mut self) -> Option<&mut Vec<String>> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_stream_mut(&mut self) -> Option<&mut Vec<Vec<(String, String)>>> {
        match self {
            Value::Stream(s) => Some(s),
            _ => None,
        }
    }
}

// Unified data storage with proper typing
pub struct DataStorage {
    pub data: HashMap<String, ExpiringValue<Value>>,
}

impl Clone for DataStorage {
    fn clone(&self) -> DataStorage {
        DataStorage {
            data: self.data.clone(),
        }
    }
}

// Separate replication state with its own lock
pub struct ReplicationState {
    pub replicas: Vec<Replica>,
    pub offset: usize,
}

impl Clone for ReplicationState {
    fn clone(&self) -> ReplicationState {
        ReplicationState {
            replicas: self.replicas.clone(),
            offset: self.offset,
        }
    }
}

// Separate configuration with its own lock
pub struct Config {
    pub db_file_name: Option<String>,
    pub db_dir: Option<String>,
    pub role: String,
    pub master_host: Option<String>,
    pub master_port: Option<String>,
}

impl Clone for Config {
    fn clone(&self) -> Config {
        Config {
            db_file_name: self.db_file_name.clone(),
            db_dir: self.db_dir.clone(),
            role: self.role.clone(),
            master_host: self.master_host.clone(),
            master_port: self.master_port.clone(),
        }
    }
}

pub struct BlockedBlpopClient {
    pub writer: Arc<Mutex<OwnedWriteHalf>>,
    pub start_time: Instant,
    pub timeout: f64, // in seconds
}

// Main state structure that holds separate locked components
pub struct State {
    pub data: Arc<Mutex<DataStorage>>,
    pub replication: Arc<Mutex<ReplicationState>>,
    pub config: Arc<Mutex<Config>>,
    pub blocked_blpop_clients: Arc<Mutex<std::collections::HashMap<String, VecDeque<BlockedBlpopClient>>>>,
}

impl Clone for State {
    fn clone(&self) -> State {
        State {
            data: self.data.clone(),
            replication: self.replication.clone(),
            config: self.config.clone(),
            blocked_blpop_clients: self.blocked_blpop_clients.clone(),
        }
    }
}

impl State {
    pub fn new() -> State {
        State {
            data: Arc::new(Mutex::new(DataStorage { data: std::collections::HashMap::new() })),
            replication: Arc::new(Mutex::new(ReplicationState { replicas: vec![], offset: 0 })),
            config: Arc::new(Mutex::new(Config {
                db_file_name: None,
                db_dir: None,
                role: "master".to_string(),
                master_host: None,
                master_port: None,
            })),
            blocked_blpop_clients: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    // Helper methods to access specific parts of state
    pub async fn get_role(&self) -> String {
        self.config.lock().await.role.clone()
    }

    pub async fn set_role(&self, role: String) {
        self.config.lock().await.role = role;
    }

    pub async fn get_offset(&self) -> usize {
        self.replication.lock().await.offset
    }

    pub async fn increment_offset(&self, amount: usize) {
        self.replication.lock().await.offset += amount;
    }

    pub async fn get_replicas(&self) -> Vec<Replica> {
        self.replication.lock().await.replicas.clone()
    }

    pub async fn add_replica(&self, replica: Replica) {
        self.replication.lock().await.replicas.push(replica);
    }

    pub async fn update_replica_ack(&self, reader: Arc<Mutex<OwnedReadHalf>>, offset: u64) {
        let mut guard = self.replication.lock().await;
        for replica in &mut guard.replicas {
            if Arc::ptr_eq(&replica.reader, &reader) {
                replica.last_ack = offset;
                break;
            }
        }
    }

    // Unified data access methods
    pub async fn get_value(&self, key: &str) -> Option<ExpiringValue<Value>> {
        self.data.lock().await.data.get(key).cloned()
    }

    pub async fn set_value(&self, key: String, value: ExpiringValue<Value>) {
        self.data.lock().await.data.insert(key, value);
    }

    pub async fn get_string(&self, key: &str) -> Option<ExpiringValue<String>> {
        if let Some(expiring_value) = self.get_value(key).await {
            if let Value::String(s) = expiring_value.value {
                return Some(ExpiringValue {
                    value: s,
                    expiration_timestamp: expiring_value.expiration_timestamp,
                });
            }
        }
        None
    }

    pub async fn set_string(&self, key: String, value: String, expiration: Option<std::time::Instant>) {
        let expiring_value = ExpiringValue {
            value: Value::String(value),
            expiration_timestamp: expiration,
        };
        self.set_value(key, expiring_value).await;
    }

    pub async fn get_stream(&self, key: &str) -> Option<Vec<Vec<(String, String)>>> {
        if let Some(expiring_value) = self.get_value(key).await {
            if let Value::Stream(s) = expiring_value.value {
                return Some(s);
            }
        }
        None
    }

    pub async fn set_stream(&self, key: String, value: Vec<Vec<(String, String)>>) {
        let expiring_value = ExpiringValue {
            value: Value::Stream(value),
            expiration_timestamp: None, // Streams don't expire
        };
        self.set_value(key, expiring_value).await;
    }

    pub async fn get_list(&self, key: &str) -> Option<Vec<String>> {
        if let Some(expiring_value) = self.get_value(key).await {
            if let Value::List(l) = expiring_value.value {
                return Some(l);
            }
        }
        None
    }

    pub async fn set_list(&self, key: String, value: Vec<String>) {
        let expiring_value = ExpiringValue {
            value: Value::List(value),
            expiration_timestamp: None, // Lists don't expire for now
        };
        self.set_value(key, expiring_value).await;
    }

    

    pub async fn rpush(&self, key: String, elements: Vec<String>) -> usize {
        let mut data = self.data.lock().await;
        
        if let Some(expiring_value) = data.data.get_mut(&key) {
            if let Value::List(list) = &mut expiring_value.value {
                list.extend(elements);
                return list.len();
            }
        }
        
        // Key doesn't exist or is not a list, create new list
        data.data.insert(key, ExpiringValue {
            value: Value::List(elements.clone()),
            expiration_timestamp: None,
        });
        
        elements.len()
    }

    pub async fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        if let Some(list) = self.get_list(key).await {
            let len = list.len() as i64;
            
            // Convert negative indexes to positive
            let start_idx = if start < 0 {
                let positive_start = len + start;
                if positive_start < 0 { 0 } else { positive_start as usize }
            } else {
                start as usize
            };
            
            let stop_idx = if stop < 0 {
                let positive_stop = len + stop;
                if positive_stop < 0 { 0 } else { positive_stop as usize }
            } else {
                if stop >= len { (len - 1) as usize } else { stop as usize }
            };
            
            // Handle edge cases
            if start_idx >= list.len() || start_idx > stop_idx {
                return Vec::new();
            }
            
            // Extract the range (inclusive)
            list[start_idx..=stop_idx].to_vec()
        } else {
            // List doesn't exist, return empty array
            Vec::new()
        }
    }

    pub async fn lpush(&self, key: String, elements: Vec<String>) -> usize {
        let mut data = self.data.lock().await;
        
        if let Some(expiring_value) = data.data.get_mut(&key) {
            if let Value::List(list) = &mut expiring_value.value {
                // Prepend elements in the order they appear in the command
                for element in elements.iter() {
                    list.insert(0, element.clone());
                }
                return list.len();
            }
        }
        
        // Key doesn't exist or is not a list, create new list
        // For LPUSH, we insert elements in the order they appear in the command
        let mut new_list = Vec::new();
        for element in elements.iter() {
            new_list.insert(0, element.clone());
        }
        
        data.data.insert(key, ExpiringValue {
            value: Value::List(new_list.clone()),
            expiration_timestamp: None,
        });
        
        new_list.len()
    }

    pub async fn llen(&self, key: &str) -> usize {
        if let Some(list) = self.get_list(key).await {
            list.len()
        } else {
            0
        }
    }

    pub async fn lpop(&self, key: &str) -> Option<String> {
        let mut data = self.data.lock().await;
        
        if let Some(expiring_value) = data.data.get_mut(key) {
            if let Value::List(list) = &mut expiring_value.value {
                if !list.is_empty() {
                    return Some(list.remove(0));
                }
            }
        }
        
        None
    }

    pub async fn lpop_multiple(&self, key: &str, count: usize) -> Vec<String> {
        let mut data = self.data.lock().await;
        let mut result = Vec::new();
        if let Some(expiring_value) = data.data.get_mut(key) {
            if let Value::List(list) = &mut expiring_value.value {
                let n = count.min(list.len());
                for _ in 0..n {
                    result.push(list.remove(0));
                }
            }
        }
        result
    }

    pub async fn get_type(&self, key: &str) -> &'static str {
        if let Some(expiring_value) = self.get_value(key).await {
            return expiring_value.value.get_type();
        }
        "none"
    }

    pub async fn get_all_keys(&self) -> Vec<String> {
        self.data.lock().await.data.keys().cloned().collect()
    }

    pub async fn get_db_config(&self) -> (Option<String>, Option<String>) {
        let config = self.config.lock().await;
        (config.db_dir.clone(), config.db_file_name.clone())
    }

    pub async fn set_db_config(&self, db_dir: Option<String>, db_file_name: Option<String>) {
        let mut config = self.config.lock().await;
        config.db_dir = db_dir;
        config.db_file_name = db_file_name;
    }

    pub async fn register_blpop_blocked_client(&self, key: &str, writer: Arc<Mutex<OwnedWriteHalf>>) {
        let mut blocked = self.blocked_blpop_clients.lock().await;
        blocked.entry(key.to_string()).or_default().push_back(BlockedBlpopClient {
            writer,
            start_time: Instant::now(),
            timeout: 30.0, // Default timeout
        });
    }

    pub async fn wake_blpop_blocked_client(&self, key: &str, value: String) -> bool {
        let mut blocked = self.blocked_blpop_clients.lock().await;
        if let Some(queue) = blocked.get_mut(key) {
            if let Some(client) = queue.pop_front() {
                // Check if the client has timed out
                if client.start_time.elapsed().as_secs_f64() > client.timeout {
                    // If timed out, remove the client and return false
                    return false;
                }
                // Compose the response: [key, value]
                let resp = crate::classes::RespDataType::RespDataType::Array(vec![
                    crate::classes::RespDataType::RespDataType::BulkString(key.to_string()),
                    crate::classes::RespDataType::RespDataType::BulkString(value),
                ]);
                let _ = client.writer.lock().await.write_all(resp.to_string().as_bytes()).await;
                return true;
            }
        }
        false
    }
}
