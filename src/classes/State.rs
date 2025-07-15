use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::Mutex,
};

use crate::classes::{ExpiringValue::ExpiringValue, RespDataType::RespDataType};

#[derive(Clone)]
pub struct Replica {
    pub reader: Arc<Mutex<OwnedReadHalf>>,
    pub writer: Arc<Mutex<OwnedWriteHalf>>,
    pub last_ack: u64,
}

// Separate data storage with its own lock
pub struct DataStorage {
    pub shared_data: HashMap<RespDataType, ExpiringValue>,
    pub stream_data: HashMap<String, Vec<Vec<(String, String)>>>,
}

impl Clone for DataStorage {
    fn clone(&self) -> DataStorage {
        DataStorage {
            shared_data: self.shared_data.clone(),
            stream_data: self.stream_data.clone(),
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
                shared_data: HashMap::new(),
                stream_data: HashMap::new(),
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

    pub async fn get_shared_data(&self) -> HashMap<RespDataType, ExpiringValue> {
        self.data.lock().await.shared_data.clone()
    }

    pub async fn insert_shared_data(&self, key: RespDataType, value: ExpiringValue) {
        self.data.lock().await.shared_data.insert(key, value);
    }

    pub async fn get_shared_value(&self, key: &RespDataType) -> Option<ExpiringValue> {
        self.data.lock().await.shared_data.get(key).cloned()
    }

    pub async fn get_stream_data(&self) -> HashMap<String, Vec<Vec<(String, String)>>> {
        self.data.lock().await.stream_data.clone()
    }

    pub async fn insert_stream_data(&self, key: String, value: Vec<Vec<(String, String)>>) {
        self.data.lock().await.stream_data.insert(key, value);
    }

    pub async fn get_stream_value(&self, key: &String) -> Option<Vec<Vec<(String, String)>>> {
        self.data.lock().await.stream_data.get(key).cloned()
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
