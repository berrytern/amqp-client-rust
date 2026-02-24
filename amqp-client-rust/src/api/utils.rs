use std::fmt::{Display, write};

use crate::errors::{AppError, AppErrorType};
use tracing::error;


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirmations{
    Disables,
    PublisherConfirms,
    RPCClientPublisherConfirms,
    RPCServerPublisherConfirms,
}

pub enum DeliveryMode {
    Transient = 1,
    Persistent = 2,
}

pub enum PendingCmd {
    Ack((u64, bool)),
    Nack((u64, bool)),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentEncoding {
    #[cfg(feature = "zstd")]
    Zstd,
    #[cfg(feature = "lz4_flex")]
    Lz4,
    #[cfg(feature = "flate2")]
    Zlib,
    None,
}
impl ContentEncoding {
    pub fn from_str(s: &str) -> Option<ContentEncoding> {
        match s {
            #[cfg(feature = "zstd")]
            "application/zstd" | "application/zstandard" => Some(ContentEncoding::Zstd),
            #[cfg(feature = "lz4_flex")]
            "application/lz4" => Some(ContentEncoding::Lz4),
            #[cfg(feature = "flate2")]
            "application/x-gzip" | "application/gzip" | "application/zlib" => Some(ContentEncoding::Zlib),
            "none" => Some(ContentEncoding::None),
            _ => None,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            #[cfg(feature = "zstd")]
            ContentEncoding::Zstd => "application/zstd",
            #[cfg(feature = "lz4_flex")]
            ContentEncoding::Lz4 => "application/lz4",
            #[cfg(feature = "flate2")]
            ContentEncoding::Zlib => "application/x-gzip",
            ContentEncoding::None => "none",
        }
    }
}

impl Display for ContentEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(feature = "zstd")]
fn compress_zstd(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    zstd::encode_all(data, 1) 
}


#[cfg(feature = "zstd")]
fn decompress_zstd(compressed_data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    zstd::decode_all(compressed_data)
}

#[cfg(feature = "lz4_flex")]
fn compress_lz4(data: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(data)
}
#[cfg(feature = "lz4_flex")]
fn decompress_lz4(compressed_data: &[u8]) -> Result<Vec<u8>, AppError> {
    Ok(lz4_flex::decompress_size_prepended(compressed_data)?)
}

#[cfg(feature = "flate2")]
fn compress_zlib(data: &[u8]) -> Result<Vec<u8>, AppError> {
    use std::io::Read;
    let mut encoder = flate2::read::ZlibEncoder::new(data, flate2::Compression::default());
    let mut compressed = Vec::new();
    
    match encoder.read_to_end(&mut compressed) {
        Ok(_) => Ok(compressed),
        Err(e) => Err(AppError::new(
            Some(format!("Zlib compression failed: {}", e)), 
            None, 
            AppErrorType::InternalError
        )),
    }
}
#[cfg(feature = "flate2")]
fn decompress_zlib(compressed_data: &[u8]) -> Result<Vec<u8>, AppError> {
    use std::io::Read;

    let mut decoder = flate2::read::ZlibDecoder::new(compressed_data);
    let mut decompressed = Vec::new();
    
    match decoder.read_to_end(&mut decompressed) {
        Ok(_) => Ok(decompressed),
        Err(e) => Err(AppError::new(
            Some(format!("Zlib decompression failed: {}", e)), 
            None, 
            AppErrorType::InternalError
        )),
    }
}


pub fn decompress(content: Vec<u8>, content_encoding: Option<&str>) -> Result<Vec<u8>, AppError> {
    if let Some(ct) = content_encoding {
        match ct {
            #[cfg(feature = "zstd")]
            "application/zstd" | "application/zstandard" => {
                match decompress_zstd(&content[..]) {
                    Ok(decompressed) => {
                        Ok(decompressed)
                    }
                    Err(e) => {
                        error!("Failed to create gzip decoder: {}", e);
                        Err(AppError::new(Some("Failed to create gzip decoder".to_string()), None, AppErrorType::InternalError).into())
                    }
                }
            },
            #[cfg(feature = "lz4_flex")]
            "application/lz4" => {
                match decompress_lz4(&content[..]) {
                    Ok(decompressed) => Ok(decompressed),
                    Err(e) => {
                        error!("Failed to decompress LZ4 content: {}", e);
                        Err(AppError::new(Some("Failed to decompress LZ4 content".to_string()), None, AppErrorType::InternalError))
                    }
                }
            },
            #[cfg(feature = "flate2")]
            "application/x-gzip" | "application/gzip" | "application/zlib" => {
                match decompress_zlib(&content[..]) {
                    Ok(decompressed) => Ok(decompressed),
                    Err(e) => {
                        error!("Failed to create gzip decoder: {}", e);
                        Err(AppError::new(Some("Failed to create gzip decoder".to_string()), None, AppErrorType::InternalError).into())
                    }
                }
            },
            _ => Err(AppError::new(Some(format!("Unsupported content encoding: {}", ct)), None, AppErrorType::InternalError))
        }
    } else {
        Ok(content)
    }
}

pub fn compress(content: impl Into<Vec<u8>>, content_type: ContentEncoding) -> Result<Vec<u8>, AppError> {
    match content_type {
        #[cfg(feature = "zstd")]
        ContentEncoding::Zstd => {
            match compress_zstd(&content.into()) {
                Ok(compressed) => Ok(compressed),
                Err(e) => {
                    error!("Failed to compress with zstd: {}", e);
                    Err(AppError::new(Some("Failed to compress with zstd".to_string()), None, AppErrorType::InternalError))
                }
            }
        },
        #[cfg(feature = "lz4_flex")]
        ContentEncoding::Lz4 => Ok(compress_lz4(&content.into())),
        #[cfg(feature = "flate2")]
        ContentEncoding::Zlib => Ok(compress_zlib(&mut content.into())?),
        ContentEncoding::None => Ok(content.into()),
    }
}