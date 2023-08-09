use parquet::errors::ParquetError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PaimonError {
    #[error("parquet error")]
    ParquetError(#[from] ParquetError),

    // #[error("unknown data store error")]
    // Unknown,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
