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

pub struct State {
    pub db_file_name: Option<String>,
    pub db_dir: Option<String>,
    pub shared_data: HashMap<RespDataType, ExpiringValue>,
    pub stream_data: HashMap<String, Vec<Vec<(String, String)>>>,
    pub role: String,
    pub master_host: Option<String>,
    pub master_port: Option<String>,
    pub replicas: Vec<Replica>,
    pub offset: usize,
}

impl Clone for State {
    fn clone(&self) -> State {
        State {
            db_file_name: self.db_file_name.clone(),
            db_dir: self.db_dir.clone(),
            shared_data: self.shared_data.clone(),
            role: self.role.clone(),
            master_host: self.master_host.clone(),
            master_port: self.master_port.clone(),
            replicas: self.replicas.clone(),
            offset: self.offset,
            stream_data: self.stream_data.clone(),
        }
    }
}
