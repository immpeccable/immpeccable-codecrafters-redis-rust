use std::time::Instant;

use crate::classes::RespDataType::RespDataType;

#[derive(Clone)]
pub struct ExpiringValue {
    pub value: RespDataType,
    pub expiration_timestamp: Option<Instant>,
}
