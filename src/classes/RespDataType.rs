use std::fmt;

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

impl fmt::Display for RespDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespDataType::SimpleString(s) => write!(f, "+{}\r\n", s),
            RespDataType::SimpleError(s) => write!(f, "-{}\r\n", s),
            RespDataType::Integer(i) => write!(f, ":{}\r\n", i),
            RespDataType::BulkString(s) => write!(f, "${}\r\n{}\r\n", s.len(), s),
            RespDataType::Array(arr) => {
                write!(f, "*{}\r\n", arr.len())?;
                for el in arr {
                    write!(f, "{}", el)?;
                }
                Ok(())
            }
            RespDataType::Nil => write!(f, "$-1\r\n"),
            // Handle other variants as needed
            _ => Err(fmt::Error),
        }
    }
}
