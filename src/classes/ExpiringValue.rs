use std::time::Instant;

#[derive(Clone)]
pub struct ExpiringValue<T> {
    pub value: T,
    pub expiration_timestamp: Option<Instant>,
}
