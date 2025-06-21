use std::{
    collections::HashMap,
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
};

use crate::classes::{ExpiringValue::ExpiringValue, RespDataType::RespDataType};

pub struct State {
    pub db_file_name: Option<String>,
    pub db_dir: Option<String>,
    pub shared_data: Arc<Mutex<HashMap<RespDataType, ExpiringValue>>>,
    pub role: String,
    pub master_host: Option<String>,
    pub master_port: Option<String>,
    pub master_stream: Option<TcpStream>,
    pub replicas: Arc<Mutex<Vec<TcpStream>>>,
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
            master_stream: self
                .master_stream
                .as_ref()
                .and_then(|sock| sock.try_clone().ok()),
            replicas: self.replicas.clone(),
        }
    }
}
