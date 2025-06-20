use std::time::Instant;

use crate::classes::RespDataType::RespDataType;

pub struct ExpiringValue {
    pub value: RespDataType,
    pub expiration_timestamp: Option<Instant>,
}
