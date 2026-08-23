use std::{
    collections::HashMap,
    fmt::Display,
    pin::Pin, sync::Arc,
    hash::Hash
};
use std::error::Error as StdError;
use crate::errors::{AppError, AppErrorType};
use amqprs::{FieldTable, ShortStr};
#[cfg(any(feature = "zstd", feature = "lz4_flex", feature = "flate2"))]
use tracing::error;

#[derive(Debug, Clone)]
pub struct Message {
    pub body: Arc<[u8]>,
    pub content_type: Option<String>,
}

pub type Handler = Arc<
    dyn Fn(
            Message,
        )
            -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>>
        + Send
        + Sync,
>;
pub type RPCHandler = Arc<
    dyn Fn(
            Message,
        )
            -> Pin<Box<dyn Future<Output = Result<Message, Box<dyn StdError + Send + Sync>>> + Send>>
        + Send
        + Sync,
>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirmations{
    Disables,
    PublisherConfirms,
    RPCClientPublisherConfirms,
    RPCServerPublisherConfirms,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryMode {
    Transient = 1,
    Persistent = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeType {
    Direct,
    Fanout,
    Topic,
}

pub enum ChannelCmd {
    PublishAck((u64, bool)),
    PublishNack((u64, bool)),
    ReOpen(u16),
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
            "application/zstd" | "application/zstandard" | "zstd" => Some(ContentEncoding::Zstd),
            #[cfg(feature = "lz4_flex")]
            "application/lz4" | "lz4" => Some(ContentEncoding::Lz4),
            #[cfg(feature = "flate2")]
            "application/zlib" | "application/gzip" | "application/x-gzip" | "zlib" | "deflate" | "gzip" => Some(ContentEncoding::Zlib),
            "none" | "" => Some(ContentEncoding::None),
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
            ContentEncoding::Zlib => "application/zlib",
            ContentEncoding::None => "none",
        }
    }
}

impl Display for ContentEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Clone,Debug)]
pub struct TopicNode<T> {
    children: HashMap<String, TopicNode<T>>,
    values: Vec<T>,
}

impl<T> Default for TopicNode<T> {
    fn default() -> Self {
        Self {
            children: HashMap::new(),
            values: Vec::new(),
        }
    }
}

#[derive(Clone,Debug, Default)]
pub struct TopicTrie<T> {
    root: TopicNode<T>,
}

impl<T: Clone> TopicTrie<T> {
    pub fn new() -> Self {
        Self {
            root: TopicNode::default(),
        }
    }

    /// Inserts a new subscription pattern (binding key) and its associated handler.
    pub fn insert(&mut self, pattern: &str, value: T) {
        let segments: Vec<&str> = if pattern.is_empty() {
            vec![]
        } else {
            pattern.split('.').collect()
        };

        let mut current = &mut self.root;
        for segment in segments {
            // Move to the child node, creating it if it doesn't exist
            current = current.children.entry(segment.to_string()).or_default();
        }
        // Add the handler at the terminal node
        current.values.push(value);
    }

    /// Searches for all handlers that match the incoming message's routing key.
    pub fn search(&self, routing_key: &str) -> Vec<T> {
        let mut results = Vec::new();
        let segments: Vec<&str> = if routing_key.is_empty() {
            vec![]
        } else {
            routing_key.split('.').collect()
        };
        
        self.search_node(&self.root, &segments, &mut results);
        results
    }

    /// Recursive search to handle branches created by '*' and '#'
    fn search_node(&self, node: &TopicNode<T>, segments: &[&str], results: &mut Vec<T>) {
        if segments.is_empty() {
            // 1. If we've exhausted the routing key, any values at this node are a match.
            results.extend(node.values.iter().cloned());

            // 2. Edge Case: A '#' can match ZERO segments. 
            // If we are out of segments, but the pattern ends in '#', it still matches.
            // Example: Pattern "stock.#" matches routing key "stock"
            if let Some(hash_child) = node.children.get("#") {
                self.search_node(hash_child, segments, results);
            }
            return;
        }

        let head = segments[0];
        let tail = &segments[1..];

        // Path A: Exact Match
        if let Some(child) = node.children.get(head) {
            self.search_node(child, tail, results);
        }

        // Path B: Star '*' Match (substitutes exactly one word)
        if let Some(star_child) = node.children.get("*") {
            self.search_node(star_child, tail, results);
        }

        // Path C: Hash '#' Match (substitutes zero or more words)
        if let Some(hash_child) = node.children.get("#") {
            // Because '#' can consume any number of words, we branch out and test 
            // consuming 0 segments, 1 segment, 2 segments... all the way to the end.
            for i in 0..=segments.len() {
                self.search_node(hash_child, &segments[i..], results);
            }
        }
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
                    Ok(decompressed) => Ok(decompressed),
                    Err(e) => {
                        error!("Failed to decompress Zstd content: {}", e);
                        Err(AppError::new(Some("Failed to decompress Zstd content".to_string()), None, AppErrorType::InternalError))
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
                        error!("Failed to decompress Zlib content: {}", e);
                        Err(AppError::new(Some("Failed to decompress Zlib content".to_string()), None, AppErrorType::InternalError))
                    }
                }
            },
            "none" | "" => Ok(content),
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueueOptions {
    pub auto_delete: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub no_create: bool,
    arguments: HashMap<String, String>,
}

impl Hash for QueueOptions {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.auto_delete.hash(state);
        self.durable.hash(state);
        self.exclusive.hash(state);
        self.no_create.hash(state);
        let mut sorted_args: Vec<(&String, &String)> = self.arguments.iter().collect();
        sorted_args.sort_by(|a, b| a.0.cmp(b.0));
        for (key, value) in sorted_args {
            key.hash(state);
            value.hash(state);
        }
    }
}
impl QueueOptions {
    pub fn new() -> Self {
        Self {
            auto_delete: false,
            durable: false,
            exclusive: false,
            no_create: false,
            arguments: HashMap::new(),
        }
    }

    pub fn build() -> Self {
        Self::new()
    }

    pub fn auto_delete(mut self, auto_delete: bool) -> Self {
        self.auto_delete = auto_delete;
        self
    }
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }
    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = exclusive;
        self
    }
    pub fn no_create(mut self, no_create: bool) -> Self {
        self.no_create = no_create;
        self
    }
    pub fn dead_letter_exchange(mut self, exchange: impl Into<String>) -> Self {
        self.arguments.insert("x-dead-letter-exchange".to_string(), exchange.into());
        self
    }
    pub fn dead_letter_routing_key(mut self, routing_key: impl Into<String>) -> Self {
        self.arguments.insert("x-dead-letter-routing-key".to_string(), routing_key.into());
        self
    }
    pub fn argument(mut self, key: String, value: String) -> Result<Self, AppError> {
        self.arguments.insert(key.try_into().map_err(|_| AppError::new(Some("key must be short".to_owned()), None, AppErrorType::InternalError))?, value);
        Ok(self)
    }
    pub fn arguments(mut self, arguments: &HashMap<String, String>) -> Result<Self, AppError> {
        for (key, value) in arguments.iter() {
            let key_2 = key.to_owned();
            let _: ShortStr = key_2.try_into().map_err(|_| AppError::new(Some(format!("key '{}' must be short", key)), None, AppErrorType::InternalError))?;
            let value = value.to_owned();
            self.arguments.insert(key.to_owned(), value);
        }
        Ok(self)
    }
}

impl Into<FieldTable> for QueueOptions {
    fn into(self) -> FieldTable {
        let mut table = FieldTable::new();
        for (key, value) in self.arguments.into_iter() {
            table.insert(key.try_into().unwrap(), value.into());
        }
        table
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_encoding_none() {
        let raw_data = b"Hello AMQP compression world!".to_vec();
        let compressed = compress(raw_data.clone(), ContentEncoding::None).expect("compression failed");
        assert_eq!(compressed, raw_data);

        let decompressed = decompress(compressed, None).expect("decompression failed");
        assert_eq!(decompressed, raw_data);

        let decompressed_explicit_none = decompress(raw_data.clone(), Some("none")).expect("decompression failed");
        assert_eq!(decompressed_explicit_none, raw_data);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_compress_decompress_zstd() {
        let raw_data = b"Repeated data for ZSTD compression testing. Repeated data for ZSTD compression testing.".repeat(10);
        let compressed = compress(raw_data.clone(), ContentEncoding::Zstd).expect("ZSTD compression failed");
        assert!(compressed.len() < raw_data.len(), "Compressed size should be smaller for repeated data");

        let decompressed = decompress(compressed, Some("application/zstd")).expect("ZSTD decompression failed");
        assert_eq!(decompressed, raw_data);

        let decompressed_alias = decompress(
            compress(raw_data.clone(), ContentEncoding::Zstd).unwrap(),
            Some("application/zstandard")
        ).expect("ZSTD alias decompression failed");
        assert_eq!(decompressed_alias, raw_data);
    }

    #[cfg(feature = "lz4_flex")]
    #[test]
    fn test_compress_decompress_lz4() {
        let raw_data = b"Repeated data for LZ4 compression testing. Repeated data for LZ4 compression testing.".repeat(10);
        let compressed = compress(raw_data.clone(), ContentEncoding::Lz4).expect("LZ4 compression failed");

        let decompressed = decompress(compressed, Some("application/lz4")).expect("LZ4 decompression failed");
        assert_eq!(decompressed, raw_data);
    }

    #[cfg(feature = "flate2")]
    #[test]
    fn test_compress_decompress_zlib() {
        let raw_data = b"Repeated data for ZLIB compression testing. Repeated data for ZLIB compression testing.".repeat(10);
        let compressed = compress(raw_data.clone(), ContentEncoding::Zlib).expect("ZLIB compression failed");
        assert!(compressed.len() < raw_data.len(), "Compressed size should be smaller for repeated data");

        let decompressed = decompress(compressed, Some("application/x-gzip")).expect("ZLIB decompression failed");
        assert_eq!(decompressed, raw_data);

        let decompressed_zlib = decompress(
            compress(raw_data.clone(), ContentEncoding::Zlib).unwrap(),
            Some("application/zlib")
        ).expect("ZLIB decompression failed");
        assert_eq!(decompressed_zlib, raw_data);
    }

    #[test]
    fn test_decompress_unsupported() {
        let data = b"some data".to_vec();
        let result = decompress(data, Some("application/unsupported-format-xyz"));
        assert!(result.is_err());
    }

    #[test]
    fn test_content_encoding_conversions_and_aliases() {
        assert_eq!(ContentEncoding::from_str("none"), Some(ContentEncoding::None));
        assert_eq!(ContentEncoding::from_str(""), Some(ContentEncoding::None));
        assert_eq!(ContentEncoding::None.as_str(), "none");

        #[cfg(feature = "zstd")]
        {
            assert_eq!(ContentEncoding::from_str("zstd"), Some(ContentEncoding::Zstd));
            assert_eq!(ContentEncoding::from_str("application/zstd"), Some(ContentEncoding::Zstd));
            assert_eq!(ContentEncoding::from_str("application/zstandard"), Some(ContentEncoding::Zstd));
            assert_eq!(ContentEncoding::Zstd.as_str(), "application/zstd");
        }

        #[cfg(feature = "lz4_flex")]
        {
            assert_eq!(ContentEncoding::from_str("lz4"), Some(ContentEncoding::Lz4));
            assert_eq!(ContentEncoding::from_str("application/lz4"), Some(ContentEncoding::Lz4));
            assert_eq!(ContentEncoding::Lz4.as_str(), "application/lz4");
        }

        #[cfg(feature = "flate2")]
        {
            assert_eq!(ContentEncoding::from_str("zlib"), Some(ContentEncoding::Zlib));
            assert_eq!(ContentEncoding::from_str("deflate"), Some(ContentEncoding::Zlib));
            assert_eq!(ContentEncoding::from_str("gzip"), Some(ContentEncoding::Zlib));
            assert_eq!(ContentEncoding::from_str("application/zlib"), Some(ContentEncoding::Zlib));
            assert_eq!(ContentEncoding::from_str("application/gzip"), Some(ContentEncoding::Zlib));
            assert_eq!(ContentEncoding::from_str("application/x-gzip"), Some(ContentEncoding::Zlib));
            assert_eq!(ContentEncoding::Zlib.as_str(), "application/zlib");
        }

        assert_eq!(ContentEncoding::from_str("unknown_format_xyz"), None);
    }

    #[test]
    fn test_queue_options_builder_and_field_table() {
        let mut custom_args = HashMap::new();
        custom_args.insert("x-message-ttl".to_string(), "60000".to_string());
        custom_args.insert("x-max-length".to_string(), "1000".to_string());

        let options = QueueOptions::new()
            .durable(true)
            .auto_delete(false)
            .exclusive(true)
            .arguments(&custom_args)
            .expect("valid arguments");

        assert!(options.durable);
        assert!(!options.auto_delete);
        assert!(options.exclusive);
        assert_eq!(options.arguments.get("x-message-ttl").unwrap(), "60000");
        assert_eq!(options.arguments.get("x-max-length").unwrap(), "1000");

        let _table: FieldTable = options.into();
    }

    #[test]
    fn test_queue_options_dead_letter_helpers() {
        let options = QueueOptions::new()
            .durable(true)
            .dead_letter_exchange("events.dlx")
            .dead_letter_routing_key("events.dead");

        assert_eq!(options.arguments.get("x-dead-letter-exchange").unwrap(), "events.dlx");
        assert_eq!(options.arguments.get("x-dead-letter-routing-key").unwrap(), "events.dead");

        let _table: FieldTable = options.into();
    }

    #[test]
    fn test_topic_trie_matching() {
        let mut trie = TopicTrie::new();
        trie.insert("orders.created", 1);
        trie.insert("orders.*", 2);
        trie.insert("orders.#", 3);
        trie.insert("*.updated", 4);
        trie.insert("stock.#", 5);
        trie.insert("#", 6);

        let res = trie.search("orders.created");
        assert!(res.contains(&1));
        assert!(res.contains(&2));
        assert!(res.contains(&3));
        assert!(!res.contains(&4));
        assert!(res.contains(&6));

        let res2 = trie.search("orders.cancelled");
        assert!(!res2.contains(&1));
        assert!(res2.contains(&2));
        assert!(res2.contains(&3));
        assert!(res2.contains(&6));

        let res3 = trie.search("users.updated");
        assert!(res3.contains(&4));
        assert!(!res3.contains(&1));
        assert!(res3.contains(&6));

        // Test "#" matching zero segments ("stock.#" matching "stock")
        let res4 = trie.search("stock");
        assert!(res4.contains(&5));
        assert!(res4.contains(&6));

        // Test multi-level segments with "stock.#"
        let res5 = trie.search("stock.warehouse.shelf.item");
        assert!(res5.contains(&5));
        assert!(res5.contains(&6));
    }
}
