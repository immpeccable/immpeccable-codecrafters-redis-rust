use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::classes::{ExpiringValue::ExpiringValue, RespDataType::RespDataType};

#[derive(Clone)]
pub struct State {
    pub db_file_name: Option<String>,
    pub db_dir: Option<String>,
    pub shared_data: Arc<Mutex<HashMap<RespDataType, ExpiringValue>>>,
}
