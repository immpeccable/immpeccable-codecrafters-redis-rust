use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::Mutex,
};

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
    // Future data types can be added here:
    // List(Vec<String>),
    // Set(HashSet<String>),
    // Hash(HashMap<String, String>),
    // SortedSet(Vec<(String, f64)>),
}

impl Value {
    pub fn get_type(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::Stream(_) => "stream",
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

// Main state structure that holds separate locked components
pub struct State {
    pub data: Arc<Mutex<DataStorage>>,
    pub replication: Arc<Mutex<ReplicationState>>,
    pub config: Arc<Mutex<Config>>,
}

impl Clone for State {
    fn clone(&self) -> State {
        State {
            data: self.data.clone(),
            replication: self.replication.clone(),
            config: self.config.clone(),
        }
    }
}

impl State {
    pub fn new() -> State {
        State {
            data: Arc::new(Mutex::new(DataStorage {
                data: HashMap::new(),
            })),
            replication: Arc::new(Mutex::new(ReplicationState {
                replicas: Vec::new(),
                offset: 0,
            })),
            config: Arc::new(Mutex::new(Config {
                db_file_name: None,
                db_dir: None,
                role: "master".to_string(),
                master_host: None,
                master_port: None,
            })),
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
}
