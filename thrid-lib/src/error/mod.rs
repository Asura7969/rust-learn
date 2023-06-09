use thiserror::Error;
// https://juejin.cn/post/7097023377864392718
#[derive(Error, Debug)]
pub enum MyError {
    #[error("data store disconnected")]
    Disconnect(#[from] std::io::Error),
    #[error("the data for key `{0}` is not available")]
    Redaction(String),
    #[error("invalid header (expected {expected:?}, found {found:?})")]
    InvalidHeader { expected: String, found: String },
    #[error("Value too large, valuse: `{0}`")]
    BigError(i32),
    #[error("unknown data store error")]
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn throw_error(a: i32) -> Result<i32, MyError> {
        if a > 100 {
            Err(MyError::BigError(a))
        } else {
            Ok(a * a)
        }
    }

    #[test]
    fn custom_error() {
        let r = throw_error(101);
        assert_eq!(
            r.err().map(|err| err.to_string()),
            Some("Value too large, valuse: `101`".to_string())
        )
    }
}
