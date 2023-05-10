use crate::error::{Error, Result};

pub fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    return (bytes.len() as u32)
        .to_be_bytes()
        .into_iter()
        .chain(bytes.iter().cloned())
        .collect();
}

#[derive(Debug, PartialEq)]
pub enum KVOperation {
    Get,
    Delete(Vec<u8>),
    Set(Vec<u8>, Vec<u8>),
}

impl KVOperation {
    pub fn get() -> KVOperation {
        return KVOperation::Get;
    }
    pub fn delete(key: &[u8]) -> KVOperation {
        return KVOperation::Delete(encode_bytes(key));
    }
    pub fn set(key: &[u8], value: &[u8]) -> KVOperation {
        return KVOperation::Set(encode_bytes(key), encode_bytes(value));
    }
    /// KVOperation is encoded as:
    /// 1 byte for operation
    /// + 4 bytes for the key length + key bytes (if set | delete)
    /// + 4 bytes for the value length + value bytes (if set)
    pub fn encode(self) -> Vec<u8> {
        return match self {
            KVOperation::Get => vec![0x00],
            KVOperation::Delete(key) => vec![0x01].into_iter().chain(key.into_iter()).collect(),
            KVOperation::Set(key, value) => vec![0x02]
                .into_iter()
                .chain(key.into_iter())
                .chain(value.into_iter())
                .collect(),
        };
    }
    // 1 byte for operation + 4 bytes for sizeof key + 4 bytes for sizeof
    // value
    pub fn decode(bytes: Vec<u8>) -> Result<KVOperation> {
        let op = bytes.get(0).ok_or(Error::Parse(
            "Invalid operation: not enough bytes for operation".into(),
        ))?;
        match op {
            0x00 => {
                if bytes.len() > 1 {
                    return Err(Error::Parse(
                        "Invalid operation: too many bytes for get operation".into(),
                    ));
                }
                return Ok(KVOperation::Get);
            }
            0x01 | 0x02 => {
                let key_len_last_index = 1 + 4 as usize;
                let key_len = u32::from_be_bytes(
                    bytes
                        .get(1..key_len_last_index)
                        .ok_or(Error::Parse(
                            "Invalid operation: not enough bytes for key length".into(),
                        ))?
                        .try_into()
                        .unwrap(),
                );
                let key_last_index = key_len_last_index + key_len as usize;
                let key = bytes
                    .get(key_len_last_index..key_last_index)
                    .ok_or(Error::Parse(
                        "Invalid operation: not enough bytes for key".into(),
                    ))?;
                if *op == 0x01 {
                    if bytes.len() > key_last_index + 1 {
                        return Err(Error::Parse(
                            "Invalid operation: too many bytes for delete operation".into(),
                        ));
                    }
                    return Ok(KVOperation::delete(key));
                }
                let value_len_last_index = key_last_index + 4 as usize;
                let value_len = u32::from_be_bytes(
                    bytes
                        .get(key_last_index..value_len_last_index)
                        .ok_or(Error::Parse(
                            "Invalid operation: not enough bytes for value length".into(),
                        ))?
                        .try_into()
                        .unwrap(),
                );
                let value_last_index = value_len_last_index + value_len as usize;
                let value =
                    bytes
                        .get(value_len_last_index..value_last_index)
                        .ok_or(Error::Parse(
                            "Invalid operation: not enough bytes for value".into(),
                        ))?;
                if bytes.len() > value_last_index + 1 {
                    return Err(Error::Parse(
                        "Invalid operation: too many bytes for set operation".into(),
                    ));
                }
                return Ok(KVOperation::set(key, value));
            }
            _ => return Err(Error::Parse(format!("Invalid operation encoding: {}", *op))),
        }
    }
}

#[test]
fn test_encode_bytes() -> Result<()> {
    let bytes: Vec<u8> = vec![0x00, 0x01, 0x02, 0x03];
    assert_eq!(encode_bytes(&bytes[..]), vec![0, 0, 0, 4, 0, 1, 2, 3]);
    Ok(())
}

#[test]
fn test_encode_operation() -> Result<()> {
    let get = KVOperation::get().encode();
    assert_eq!(get, vec![0]);
    let key = vec![0x01, 0x02];
    let delete = KVOperation::delete(&key).encode();
    assert_eq!(delete, vec![1, 0, 0, 0, 2, 1, 2]);
    let value = vec![0x03, 0x04, 0x05];
    let set = KVOperation::set(&key, &value).encode();
    assert_eq!(set, vec![2, 0, 0, 0, 2, 1, 2, 0, 0, 0, 3, 3, 4, 5]);
    Ok(())
}

#[test]
fn test_decode_operation() -> Result<()> {
    let get_decoded = KVOperation::decode(vec![0])?;
    assert_eq!(get_decoded, KVOperation::get());
    let key = vec![0x01, 0x02];
    let delete_decoded = KVOperation::decode(vec![1, 0, 0, 0, 2, 1, 2])?;
    assert_eq!(delete_decoded, KVOperation::delete(&key));
    let value = vec![0x03, 0x04, 0x05];
    let set_decoded = KVOperation::decode(vec![2, 0, 0, 0, 2, 1, 2, 0, 0, 0, 3, 3, 4, 5])?;
    assert_eq!(set_decoded, KVOperation::set(&key, &value));
    Ok(())
}
