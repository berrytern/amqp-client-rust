use amqp_client_rust::api::utils::{compress, decompress, ContentEncoding, TopicTrie};
use proptest::prelude::*;

#[test]
fn test_trie_hash_in_middle_zero_words() {
    let mut trie = TopicTrie::new();
    trie.insert("a.#.c", 1);
    
    // '#' in the middle matching 0 words ("a.c")
    let res = trie.search("a.c");
    assert!(res.contains(&1), "a.#.c should match a.c (0 segments for #)");

    // '#' in the middle matching 1 word ("a.b.c")
    let res2 = trie.search("a.b.c");
    assert!(res2.contains(&1), "a.#.c should match a.b.c (1 segment for #)");

    // '#' in the middle matching multiple words ("a.x.y.z.c")
    let res3 = trie.search("a.x.y.z.c");
    assert!(res3.contains(&1), "a.#.c should match a.x.y.z.c (multiple segments for #)");

    // Should NOT match if ending segment doesn't match
    let res4 = trie.search("a.b.d");
    assert!(res4.is_empty(), "a.#.c should not match a.b.d");
}

#[test]
fn test_trie_hash_at_beginning() {
    let mut trie = TopicTrie::new();
    trie.insert("#.completed", 42);

    // Matching 0 segments ("completed")
    let res = trie.search("completed");
    assert!(res.contains(&42), "#.completed should match completed");

    // Matching 1 segment ("task.completed")
    let res2 = trie.search("task.completed");
    assert!(res2.contains(&42), "#.completed should match task.completed");

    // Matching 3 segments ("org.dept.task.completed")
    let res3 = trie.search("org.dept.task.completed");
    assert!(res3.contains(&42), "#.completed should match org.dept.task.completed");

    // Negative match
    let res4 = trie.search("task.failed");
    assert!(res4.is_empty(), "#.completed should not match task.failed");
}

#[test]
fn test_trie_empty_and_root_patterns() {
    let mut trie = TopicTrie::new();
    trie.insert("", 10);
    trie.insert("#", 20);

    // Empty routing key matches empty pattern and hash
    let res_empty = trie.search("");
    assert!(res_empty.contains(&10));
    assert!(res_empty.contains(&20));

    // Non-empty routing key matches hash but not empty pattern
    let res_any = trie.search("some.key");
    assert!(!res_any.contains(&10));
    assert!(res_any.contains(&20));
}

#[test]
fn test_trie_consecutive_stars() {
    let mut trie = TopicTrie::new();
    trie.insert("*.*.*", 99);

    // Exactly 3 segments
    assert_eq!(trie.search("a.b.c"), vec![99]);
    assert_eq!(trie.search("1.2.3"), vec![99]);

    // Less or more than 3 segments should fail
    assert!(trie.search("a.b").is_empty());
    assert!(trie.search("a.b.c.d").is_empty());
    assert!(trie.search("").is_empty());
}

// ---------------------------------------------------------
// Property-Based Tests with proptest
// ---------------------------------------------------------

proptest! {
    // 1. Exact pattern matching: an exact routing key must always be matched by itself
    #[test]
    fn prop_exact_matching(words in prop::collection::vec("[a-z0-9]{1,10}", 1..6)) {
        let pattern = words.join(".");
        let mut trie = TopicTrie::new();
        trie.insert(&pattern, 1);

        let matches = trie.search(&pattern);
        prop_assert!(matches.contains(&1), "Exact routing key '{}' was not found", pattern);
    }

    // 2. Hash (#) alone must match ANY valid routing key
    #[test]
    fn prop_hash_matches_all(key in prop::collection::vec("[a-z0-9]{1,10}", 0..8)) {
        let routing_key = key.join(".");
        let mut trie = TopicTrie::new();
        trie.insert("#", 100);

        let matches = trie.search(&routing_key);
        prop_assert!(matches.contains(&100), "'#' failed to match routing key '{}'", routing_key);
    }

    // 3. Prefix with '#': "prefix.#" must match any key starting with "prefix"
    #[test]
    fn prop_prefix_hash_matches(prefix in "[a-z0-9]{1,8}", suffix_words in prop::collection::vec("[a-z0-9]{1,8}", 0..5)) {
        let pattern = format!("{}.#", prefix);
        let mut trie = TopicTrie::new();
        trie.insert(&pattern, 77);

        let routing_key = if suffix_words.is_empty() {
            prefix.clone()
        } else {
            format!("{}.{}", prefix, suffix_words.join("."))
        };

        let matches = trie.search(&routing_key);
        prop_assert!(matches.contains(&77), "'{}' failed to match '{}'", pattern, routing_key);
    }

    // 4. Robustness / No panics: searching or inserting completely arbitrary strings must never panic
    #[test]
    fn prop_no_panics_on_arbitrary_inputs(
        pat in "\\PC*",
        key in "\\PC*"
    ) {
        let mut trie = TopicTrie::new();
        trie.insert(&pat, 1);
        let _ = trie.search(&key);
    }

    // 5. Compression roundtrip with arbitrary byte payloads (None)
    #[test]
    fn prop_compress_decompress_none(data in prop::collection::vec(any::<u8>(), 0..2048)) {
        let compressed = compress(data.clone(), ContentEncoding::None).expect("compression failed");
        prop_assert_eq!(&compressed, &data);

        let decompressed = decompress(compressed, None).expect("decompression failed");
        prop_assert_eq!(decompressed, data);
    }
}

#[cfg(feature = "zstd")]
proptest! {
    #[test]
    fn prop_compress_decompress_zstd(data in prop::collection::vec(any::<u8>(), 0..4096)) {
        let compressed = compress(data.clone(), ContentEncoding::Zstd).expect("zstd compression failed");
        let decompressed = decompress(compressed, Some("application/zstd")).expect("zstd decompression failed");
        prop_assert_eq!(decompressed, data);
    }
}

#[cfg(feature = "lz4_flex")]
proptest! {
    #[test]
    fn prop_compress_decompress_lz4(data in prop::collection::vec(any::<u8>(), 0..4096)) {
        let compressed = compress(data.clone(), ContentEncoding::Lz4).expect("lz4 compression failed");
        let decompressed = decompress(compressed, Some("application/lz4")).expect("lz4 decompression failed");
        prop_assert_eq!(decompressed, data);
    }
}

#[cfg(feature = "flate2")]
proptest! {
    #[test]
    fn prop_compress_decompress_zlib(data in prop::collection::vec(any::<u8>(), 0..4096)) {
        let compressed = compress(data.clone(), ContentEncoding::Zlib).expect("zlib compression failed");
        let decompressed = decompress(compressed, Some("application/zlib")).expect("zlib decompression failed");
        prop_assert_eq!(decompressed, data);
    }
}

#[test]
fn test_decompress_corrupted_data_returns_error() {
    #[allow(unused_variables)]
    let corrupted = b"this is definitely not valid compressed data".to_vec();

    #[cfg(feature = "zstd")]
    {
        let res = decompress(corrupted.clone(), Some("application/zstd"));
        assert!(res.is_err(), "Zstd decompression of corrupt data should fail gracefully");
    }

    #[cfg(feature = "lz4_flex")]
    {
        let res = decompress(corrupted.clone(), Some("application/lz4"));
        assert!(res.is_err(), "LZ4 decompression of corrupt data should fail gracefully");
    }

    #[cfg(feature = "flate2")]
    {
        let res = decompress(corrupted.clone(), Some("application/zlib"));
        assert!(res.is_err(), "Zlib decompression of corrupt data should fail gracefully");
    }
}
