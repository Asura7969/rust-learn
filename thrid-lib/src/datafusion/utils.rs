use anyhow::{Ok, Result};
use std::fs;

pub(crate) fn read_to_string(path: &str) -> Result<String> {
    let content = fs::read_to_string(path)?;
    Ok(content)
}
