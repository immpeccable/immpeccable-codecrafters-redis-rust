#[derive(Eq, Hash, PartialEq, Clone)]
pub enum RespDataType {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    Array(Vec<RespDataType>),
    BulkString(String),
    Stream(String),
    Nil,
    Boolean(bool),
    // Double(f32),
}
