use std::{
    fs::File,
    io::{BufRead, BufReader, Error, Read, Seek, Write},
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::classes::{ExpiringValue::ExpiringValue, RespDataType::RespDataType, State::State};

pub struct Db {}
impl Db {
    fn system_time_to_instant(&mut self, sys_time: SystemTime) -> Instant {
        let now_sys = SystemTime::now();
        let now_inst = Instant::now();

        match sys_time.duration_since(now_sys) {
            // sys_time is in the future ⇒ add the delta
            Ok(delta) => now_inst + delta,
            // sys_time is in the past ⇒ subtract the delta
            Err(err) => now_inst - err.duration(),
        }
    }

    fn size_encoded_bytes(&mut self, reader: &mut BufReader<File>) -> u32 {
        let mut number_byte = [0u8; 1];
        reader.read_exact(&mut number_byte).unwrap();
        let byte = number_byte[0];
        match byte & 0xC0 {
            0 => {
                return byte.try_into().unwrap();
            }
            1 => {
                let mut next_byte = [0u8; 1];
                reader.read_exact(&mut next_byte).unwrap();
                return (2u8.pow(8) * byte & 0x3F + next_byte[0])
                    .try_into()
                    .unwrap();
            }
            2 => {
                let mut next_four_bytes = [0u8; 4];
                reader.read_exact(&mut next_four_bytes).unwrap();
                return u32::from_le_bytes(next_four_bytes);
            }
            3 => match byte {
                0xC0 => {
                    let mut next_one_byte = [0u8; 1];
                    reader.read_exact(&mut next_one_byte).unwrap();
                    return next_one_byte[0].try_into().unwrap();
                }
                0xC1 => {
                    let mut next_two_bytes = [0u8; 2];
                    reader.read_exact(&mut next_two_bytes).unwrap();
                    return u16::from_le_bytes(next_two_bytes).into();
                }
                0xC2 => {
                    let mut next_four_bytes = [0u8; 4];
                    reader.read_exact(&mut next_four_bytes).unwrap();
                    return u32::from_le_bytes(next_four_bytes);
                }
                _ => {}
            },
            _ => {
                unreachable!("should not be more than 3")
            }
        }
        return 1;
    }

    fn string_encoded(&mut self, reader: &mut BufReader<File>) -> String {
        let string_length = self.size_encoded_bytes(reader);
        let mut string_buffer: Vec<u8> = vec![0u8; string_length as usize];
        reader.read_exact(&mut string_buffer).unwrap();
        return String::from_utf8(string_buffer).unwrap();
    }

    pub async fn load(&mut self, state: &mut State) -> Result<(), Error> {
        if let (Some(db_dir), Some(db_file)) = (state.db_dir.clone(), state.db_file_name.clone()) {
            let file = File::open(&Path::new(format!("{}/{}", db_dir, db_file).as_str()))?;
            let mut reader = BufReader::new(file);
            let mut _discard = Vec::new();
            reader.read_until(0xFE, &mut _discard).unwrap();
            let _db_index = self.size_encoded_bytes(&mut reader);
            reader.consume(1);
            let keys_size = self.size_encoded_bytes(&mut reader);
            let _keys_size_with_expiration_size = self.size_encoded_bytes(&mut reader);
            for _ in 0..keys_size {
                let mut first_byte = [0u8; 1];
                reader.read_exact(&mut first_byte).unwrap();
                let raw_byte = first_byte[0];
                match raw_byte {
                    0x00 => {
                        let hash_key = self.string_encoded(&mut reader);
                        let hash_value = self.string_encoded(&mut reader);
                        state.shared_data.insert(
                            RespDataType::BulkString(hash_key),
                            ExpiringValue {
                                value: RespDataType::BulkString(hash_value),
                                expiration_timestamp: None,
                            },
                        );
                    }
                    0xFC => {
                        let mut expiration_as_miliseconds_bytes = [0u8; 8];
                        reader
                            .read_exact(&mut expiration_as_miliseconds_bytes)
                            .unwrap();
                        let ms = u64::from_le_bytes(expiration_as_miliseconds_bytes);
                        reader.consume(1);
                        let hash_key = self.string_encoded(&mut reader);
                        let hash_value = self.string_encoded(&mut reader);
                        let expiration_time = UNIX_EPOCH + Duration::from_millis(ms);
                        let state_guard = &mut state.shared_data;
                        state_guard.insert(
                            RespDataType::BulkString(hash_key),
                            ExpiringValue {
                                value: RespDataType::BulkString(hash_value),
                                expiration_timestamp: Some(Instant::from(
                                    self.system_time_to_instant(expiration_time),
                                )),
                            },
                        );
                    }
                    0xFD => {
                        let mut expiration_as_seconds_bytes = [0u8; 4];
                        reader.read_exact(&mut expiration_as_seconds_bytes).unwrap();
                        let seconds = u32::from_le_bytes(expiration_as_seconds_bytes);
                        reader.consume(1);
                        let hash_key = self.string_encoded(&mut reader);
                        let hash_value = self.string_encoded(&mut reader);
                        let expiration_time = UNIX_EPOCH + Duration::from_secs(seconds.into());

                        state.shared_data.insert(
                            RespDataType::BulkString(hash_key),
                            ExpiringValue {
                                value: RespDataType::BulkString(hash_value),
                                expiration_timestamp: Some(Instant::from(
                                    self.system_time_to_instant(expiration_time),
                                )),
                            },
                        );
                    }
                    _ => {}
                }
            }
        }
        return Ok(());
    }
}
