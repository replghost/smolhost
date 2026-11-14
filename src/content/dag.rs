// SPDX-License-Identifier: GPL-3.0-or-later

//! Bounded, CID-verified UnixFS file traversal. Partial output is never a receipt:
//! callers must discard it if any later block or subtree validation fails.

use anyhow::{Context, Result, bail, ensure};
use prost::Message;
use smoldot::libp2p::cid::Cid;

pub const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_BLOCK_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_BLOCKS: usize = 4096;
/// Maximum encoded DAG overhead beyond the caller's reconstructed file limit.
pub const MAX_DAG_OVERHEAD_BYTES: u64 = 16 * 1024 * 1024;
/// Maximum number of nodes on a root-to-leaf path, including the root.
pub const MAX_DEPTH: usize = 32;

// CIDv1, the two supported codecs, and a supported 256-bit multihash.
const MAX_CID_BYTES: usize = 38;
const MAX_CID_TEXT: usize = 62;
const RAW: u64 = 0x55;
const DAG_PB: u64 = 0x70;

struct Request {
    cid: Cid,
    text: String,
    codec: u64,
    size: Option<u64>,
    wire_size: Option<u64>,
}

impl Request {
    fn new(cid: Cid, size: Option<u64>, wire_size: Option<u64>) -> Result<Self> {
        let codec = cid_codec(cid.as_ref())?;
        Ok(Self {
            text: cid.to_string(),
            cid,
            codec,
            size,
            wire_size,
        })
    }
}

struct Frame {
    cid: Cid,
    children: std::vec::IntoIter<Request>,
    end_bytes: u64,
    start_wire_bytes: u64,
    wire_size: Option<u64>,
}

pub(crate) struct Walker {
    current: Request,
    ancestors: Vec<Frame>,
    max_bytes: u64,
    bytes: u64,
    wire_bytes: u64,
    blocks: u64,
    // Counts occurrences, not distinct CIDs. Repeated chunks are legitimate.
    scheduled: usize,
    complete: bool,
    failed: bool,
}

impl Walker {
    pub(crate) fn new(cid: &str, max_bytes: u64) -> Result<Self> {
        ensure!(
            max_bytes <= MAX_FILE_BYTES,
            "file byte limit exceeds hard maximum"
        );
        ensure!(cid.len() <= MAX_CID_TEXT, "CID is too long");
        let parsed: Cid = cid.parse().context("unsupported CID encoding or hash")?;
        let current = Request::new(parsed, None, None)?;
        ensure!(
            current.text == cid,
            "CID must use canonical lowercase base32"
        );
        Ok(Self {
            current,
            ancestors: Vec::new(),
            max_bytes,
            bytes: 0,
            wire_bytes: 0,
            blocks: 0,
            scheduled: 1,
            complete: false,
            failed: false,
        })
    }

    pub(crate) fn next_cid(&self) -> Option<&str> {
        // A failed walker must never look like a successfully completed file.
        (!self.complete).then_some(self.current.text.as_str())
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn blocks(&self) -> u64 {
        self.blocks
    }

    pub(crate) fn accept(&mut self, block: Vec<u8>) -> Result<Option<Vec<u8>>> {
        ensure!(!self.failed, "file walker has already failed");
        ensure!(!self.complete, "file walker is already complete");
        let result = self.accept_block(block);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn accept_block(&mut self, block: Vec<u8>) -> Result<Option<Vec<u8>>> {
        ensure!(block.len() <= MAX_BLOCK_BYTES, "block byte limit exceeded");
        ensure!(
            self.blocks < MAX_BLOCKS as u64,
            "block count limit exceeded"
        );
        let wire_length = block.len() as u64;
        let wire_bytes = self
            .wire_bytes
            .checked_add(wire_length)
            .context("DAG size overflow")?;
        ensure!(
            wire_bytes <= self.max_bytes + MAX_DAG_OVERHEAD_BYTES,
            "encoded DAG byte limit exceeded"
        );
        ensure!(
            self.current.cid.prefix().with_digest_of(&block) == self.current.cid,
            "block CID digest mismatch"
        );
        let (output, children, size) = match self.current.codec {
            RAW => {
                let size = block.len() as u64;
                (block, Vec::new(), size)
            }
            DAG_PB => self.decode_file(&block)?,
            _ => unreachable!("request codec was validated"),
        };
        ensure!(
            self.current.size.is_none_or(|expected| expected == size),
            "child size does not match UnixFS blockSizes"
        );
        let end_bytes = self.bytes.checked_add(size).context("file size overflow")?;
        ensure!(end_bytes <= self.max_bytes, "file byte limit exceeded");
        let bytes = self
            .bytes
            .checked_add(output.len() as u64)
            .context("file size overflow")?;
        let scheduled = self
            .scheduled
            .checked_add(children.len())
            .context("link count overflow")?;
        ensure!(
            scheduled <= MAX_BLOCKS,
            "cumulative link/block budget exceeded"
        );
        ensure!(
            children.is_empty() || self.ancestors.len() + 1 < MAX_DEPTH,
            "DAG depth limit exceeded"
        );

        // Tsize is encoded subtree size, not logical UnixFS file size. Even if
        // some child Tsize fields are absent, reject an impossible lower bound.
        let mut minimum_wire_size = wire_length;
        let mut all_wire_sizes = true;
        for child in &children {
            ensure!(
                child.cid != self.current.cid
                    && !self.ancestors.iter().any(|frame| frame.cid == child.cid),
                "DAG cycle detected"
            );
            if let Some(size) = child.wire_size {
                minimum_wire_size = minimum_wire_size
                    .checked_add(size)
                    .context("DAG size overflow")?;
            } else {
                all_wire_sizes = false;
            }
        }
        if let Some(expected) = self.current.wire_size {
            ensure!(
                minimum_wire_size <= expected && (!all_wire_sizes || minimum_wire_size == expected),
                "DAG link Tsize mismatch"
            );
        }

        if children.is_empty() {
            // Do all final subtree checks before returning even this last chunk.
            while let Some(frame) = self.ancestors.last() {
                if frame.children.len() != 0 {
                    break;
                }
                ensure!(bytes == frame.end_bytes, "UnixFS subtree filesize mismatch");
                ensure!(
                    frame
                        .wire_size
                        .is_none_or(|expected| wire_bytes - frame.start_wire_bytes == expected),
                    "DAG subtree Tsize mismatch"
                );
                self.ancestors.pop();
            }
            if let Some(frame) = self.ancestors.last_mut() {
                self.current = frame
                    .children
                    .next()
                    .expect("nonempty children checked above");
            } else {
                self.complete = true;
            }
        } else {
            let mut children = children.into_iter();
            let next = children.next().expect("nonempty children checked above");
            let previous = std::mem::replace(&mut self.current, next);
            self.ancestors.push(Frame {
                cid: previous.cid,
                children,
                end_bytes,
                start_wire_bytes: self.wire_bytes,
                wire_size: previous.wire_size,
            });
        }
        self.bytes = bytes;
        self.wire_bytes = wire_bytes;
        self.blocks += 1;
        self.scheduled = scheduled;
        Ok((!output.is_empty()).then_some(output))
    }

    fn decode_file(&self, block: &[u8]) -> Result<(Vec<u8>, Vec<Request>, u64)> {
        // Scan borrowed wire data before prost allocates repeated-message or
        // packed-integer vectors. The remaining *global* budget bounds both.
        let data = preflight_node(block, MAX_BLOCKS - self.scheduled)?;
        let file = UnixFs::decode(data).context("invalid UnixFS protobuf")?;
        ensure!(matches!(file.kind, Some(0 | 2)), "unsupported UnixFS type");
        // PBNode intentionally omits Data: prost skips it without copying the
        // entire UnixFS message a second time. Only the inline payload is owned.
        let node = PbNode::decode(block).context("invalid DAG-PB protobuf")?;
        ensure!(
            node.links.len() == file.block_sizes.len(),
            "UnixFS link/blockSizes count mismatch"
        );
        ensure!(
            file.kind != Some(0) || node.links.is_empty(),
            "UnixFS Raw cannot have links"
        );
        let size = file
            .block_sizes
            .iter()
            .try_fold(file.data.len() as u64, |sum, size| {
                sum.checked_add(*size).context("UnixFS filesize overflow")
            })?;
        ensure!(
            file.file_size.is_none_or(|expected| expected == size),
            "UnixFS filesize mismatch"
        );
        ensure!(
            size <= self.max_bytes.saturating_sub(self.bytes),
            "file byte limit exceeded"
        );
        let mut children = Vec::with_capacity(node.links.len());
        for (link, size) in node.links.into_iter().zip(file.block_sizes) {
            let cid = Cid::from_bytes(link.hash).context("invalid linked CID")?;
            children.push(Request::new(cid, Some(size), link.tsize)?);
        }
        Ok((file.data, children, size))
    }
}

#[derive(Clone, PartialEq, Message)]
struct PbNode {
    #[prost(message, repeated, tag = "2")]
    links: Vec<PbLink>,
}

#[derive(Clone, PartialEq, Message)]
struct PbLink {
    #[prost(bytes = "vec", tag = "1")]
    hash: Vec<u8>,
    #[prost(uint64, optional, tag = "3")]
    tsize: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
struct UnixFs {
    #[prost(uint64, optional, tag = "1")]
    kind: Option<u64>,
    #[prost(bytes = "vec", tag = "2")]
    data: Vec<u8>,
    #[prost(uint64, optional, tag = "3")]
    file_size: Option<u64>,
    #[prost(uint64, repeated, tag = "4")]
    block_sizes: Vec<u64>,
}

// This is a nonallocating protobuf bounds check, not a second message decoder.
// Prost remains responsible for decoding owned messages. Link names are checked
// in place and skipped by prost because they do not contribute file bytes.
enum Field<'a> {
    Varint,
    Bytes(&'a [u8]),
    Fixed,
}

fn varint(input: &mut &[u8]) -> Result<u64> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let (&byte, rest) = input.split_first().context("truncated protobuf varint")?;
        *input = rest;
        ensure!(shift != 63 || byte <= 1, "protobuf varint overflow");
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("protobuf varint overflow")
}

fn take<'a>(input: &mut &'a [u8], length: usize) -> Result<&'a [u8]> {
    ensure!(length <= input.len(), "truncated protobuf field");
    let (value, rest) = input.split_at(length);
    *input = rest;
    Ok(value)
}

fn field<'a>(input: &mut &'a [u8]) -> Result<(u64, Field<'a>)> {
    let key = varint(input)?;
    let tag = key >> 3;
    ensure!(
        (1..=0x1fff_ffff).contains(&tag),
        "invalid protobuf field number"
    );
    let value = match key & 7 {
        0 => {
            varint(input)?;
            Field::Varint
        }
        1 => {
            take(input, 8)?;
            Field::Fixed
        }
        2 => {
            let length = usize::try_from(varint(input)?).context("protobuf length overflow")?;
            Field::Bytes(take(input, length)?)
        }
        5 => {
            take(input, 4)?;
            Field::Fixed
        }
        _ => bail!("unsupported protobuf wire type"),
    };
    Ok((tag, value))
}

fn singular(seen: &mut u8, tag: u64) -> Result<()> {
    let mask = 1 << tag;
    ensure!(*seen & mask == 0, "duplicate singular protobuf field");
    *seen |= mask;
    Ok(())
}

fn preflight_node(mut input: &[u8], budget: usize) -> Result<&[u8]> {
    let mut data = None;
    let mut links = 0;
    while !input.is_empty() {
        match field(&mut input)? {
            (1, Field::Bytes(value)) => {
                ensure!(data.is_none(), "duplicate DAG-PB Data");
                preflight_file(value, budget)?;
                data = Some(value);
            }
            (2, Field::Bytes(value)) => {
                links += 1;
                ensure!(links <= budget, "cumulative link/block budget exceeded");
                preflight_link(value)?;
            }
            _ => bail!("unsupported DAG-PB field or wire type"),
        }
    }
    data.context("DAG-PB node has no UnixFS Data")
}

fn preflight_link(mut input: &[u8]) -> Result<()> {
    let mut seen = 0;
    while !input.is_empty() {
        let (tag, value) = field(&mut input)?;
        match (tag, value) {
            (1, Field::Bytes(hash)) => {
                singular(&mut seen, tag)?;
                ensure!(hash.len() <= MAX_CID_BYTES, "CID is too long");
            }
            (2, Field::Bytes(name)) => {
                singular(&mut seen, tag)?;
                std::str::from_utf8(name).context("invalid DAG-PB link name")?;
            }
            (3, Field::Varint) => singular(&mut seen, tag)?,
            _ => bail!("unsupported DAG-PB link field or wire type"),
        }
    }
    ensure!(seen & (1 << 1) != 0, "DAG-PB link has no CID");
    Ok(())
}

fn preflight_file(mut input: &[u8], budget: usize) -> Result<()> {
    let mut seen = 0;
    let mut sizes = 0;
    while !input.is_empty() {
        let (tag, value) = field(&mut input)?;
        match (tag, value) {
            (1 | 3, Field::Varint) | (2, Field::Bytes(_)) => singular(&mut seen, tag)?,
            (4, Field::Varint) => {
                sizes += 1;
                ensure!(sizes <= budget, "UnixFS blockSizes budget exceeded");
            }
            (4, Field::Bytes(mut packed)) => {
                while !packed.is_empty() {
                    varint(&mut packed)?;
                    sizes += 1;
                    ensure!(sizes <= budget, "UnixFS blockSizes budget exceeded");
                }
            }
            (1..=4, _) => bail!("invalid UnixFS wire type"),
            // Optional UnixFS metadata does not affect file content. Unknown
            // fields were already bounded/skipped without recursive allocation.
            _ => {}
        }
    }
    ensure!(seen & (1 << 1) != 0, "UnixFS type is missing");
    Ok(())
}

fn cid_codec(mut input: &[u8]) -> Result<u64> {
    ensure!(input.len() <= MAX_CID_BYTES, "CID is too long");
    let mut parts = [0; 4];
    for part in &mut parts {
        let before = input;
        *part = varint(&mut input).context("invalid CID varint")?;
        let length = before.len() - input.len();
        ensure!(
            length == 1 || before[length - 1] != 0,
            "noncanonical CID varint"
        );
    }
    ensure!(parts[0] == 1, "only CIDv1 is supported");
    ensure!(matches!(parts[1], RAW | DAG_PB), "unsupported CID codec");
    ensure!(
        matches!(parts[2], 0x12 | 0x1b | 0xb220),
        "unsupported CID hash"
    );
    ensure!(
        parts[3] == 32 && input.len() == 32,
        "invalid CID digest length"
    );
    Ok(parts[1])
}

#[cfg(test)]
mod tests {
    use super::*;
    use smoldot::libp2p::cid::CidPrefix;

    struct Block {
        cid: Cid,
        bytes: Vec<u8>,
    }

    fn block(codec: u8, bytes: Vec<u8>) -> Block {
        let cid = CidPrefix::from_bytes(vec![1, codec, 0x12, 32])
            .unwrap()
            .with_digest_of(&bytes);
        Block { cid, bytes }
    }

    fn raw(bytes: &[u8]) -> Block {
        block(RAW as u8, bytes.to_vec())
    }

    fn link(block: &Block) -> PbLink {
        PbLink {
            hash: block.cid.as_ref().to_vec(),
            tsize: None,
        }
    }

    fn metadata(data: &[u8], sizes: &[u64], total: Option<u64>) -> UnixFs {
        UnixFs {
            kind: Some(2),
            data: data.to_vec(),
            file_size: total,
            block_sizes: sizes.to_vec(),
        }
    }

    fn put_varint(mut value: u64, output: &mut Vec<u8>) {
        while value >= 128 {
            output.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }

    fn node_bytes(links: Vec<PbLink>, data: &[u8]) -> Vec<u8> {
        // Bulletin's historical DAG-PB order: Links first, then Data.
        let mut bytes = PbNode { links }.encode_to_vec();
        bytes.push(0x0a);
        put_varint(data.len() as u64, &mut bytes);
        bytes.extend_from_slice(data);
        bytes
    }

    fn file(links: Vec<PbLink>, data: UnixFs) -> Block {
        block(DAG_PB as u8, node_bytes(links, &data.encode_to_vec()))
    }

    fn walker(block: &Block, max_bytes: u64) -> Walker {
        Walker::new(&block.cid.to_string(), max_bytes).unwrap()
    }

    fn rejects(block: &Block, max_bytes: u64, message: &str) {
        let mut walker = walker(block, max_bytes);
        let error = walker.accept(block.bytes.clone()).unwrap_err();
        assert!(format!("{error:#}").contains(message), "{error:#}");
        assert_eq!(walker.bytes(), 0);
        assert_eq!(walker.blocks(), 0);
        assert!(
            walker.next_cid().is_some(),
            "failure must not look complete"
        );
        assert!(
            walker.accept(block.bytes.clone()).is_err(),
            "failure is terminal"
        );
    }

    #[test]
    fn raw_hash_formats_and_zero_copy_output() {
        for hash in [vec![0x12], vec![0x1b], vec![0xa0, 0xe4, 0x02]] {
            let bytes = b"verified raw bytes".to_vec();
            let address = bytes.as_ptr();
            let mut prefix = vec![1, RAW as u8];
            prefix.extend(hash);
            prefix.push(32);
            let cid = CidPrefix::from_bytes(prefix)
                .unwrap()
                .with_digest_of(&bytes);
            let mut walker = Walker::new(&cid.to_string(), bytes.len() as u64).unwrap();
            let output = walker.accept(bytes).unwrap().unwrap();
            assert_eq!(output.as_ptr(), address);
            assert_eq!(output, b"verified raw bytes");
            assert_eq!(walker.bytes(), output.len() as u64);
            assert_eq!(walker.blocks(), 1);
            assert_eq!(walker.next_cid(), None);
            assert!(walker.accept(Vec::new()).is_err());
        }
    }

    #[test]
    fn empty_raw_and_unixfs_files_allow_zero_limit() {
        for block in [raw(b""), file(vec![], metadata(b"", &[], Some(0)))] {
            let mut walker = walker(&block, 0);
            assert!(walker.accept(block.bytes).unwrap().is_none());
            assert_eq!(walker.next_cid(), None);
            assert_eq!(walker.bytes(), 0);
            assert_eq!(walker.blocks(), 1);
        }
    }

    #[test]
    fn rejects_wrong_cid_and_keeps_partial_output_uncommitted() {
        let a = raw(b"a");
        let b = raw(b"b");
        let manifest = file(vec![], metadata(b"a", &[], Some(1)));
        for expected in [&a, &manifest] {
            let mut walker = walker(expected, 10);
            assert!(
                walker
                    .accept(b.bytes.clone())
                    .unwrap_err()
                    .to_string()
                    .contains("digest")
            );
            assert_eq!(walker.bytes(), 0);
            assert!(walker.next_cid().is_some());
        }
        let root = file(vec![link(&a), link(&b)], metadata(b"", &[1, 1], Some(2)));
        let mut walker = walker(&root, 2);
        assert!(walker.accept(root.bytes).unwrap().is_none());
        assert_eq!(walker.accept(a.bytes.clone()).unwrap().unwrap(), b"a");
        assert!(walker.accept(a.bytes).is_err());
        assert_eq!(walker.bytes(), 1);
        assert_eq!(walker.blocks(), 2);
        assert!(walker.next_cid().is_some());
        assert!(walker.accept(b.bytes).is_err());
    }

    #[test]
    fn inline_nested_raw_and_repeated_subtrees_keep_file_order() {
        let a = raw(b"a");
        let b = raw(b"b");
        let nested = file(vec![link(&a), link(&b)], metadata(b"N", &[1, 1], Some(3)));
        let mut raw_metadata = metadata(b"C", &[], None);
        raw_metadata.kind = Some(0);
        let c = file(vec![], raw_metadata);
        let root = file(
            vec![link(&nested), link(&c), link(&nested)],
            metadata(b"R", &[3, 1, 3], Some(8)),
        );
        let mut walker = walker(&root, 8);
        let mut output = Vec::new();
        for block in [&root, &nested, &a, &b, &c, &nested, &a, &b] {
            assert_eq!(walker.next_cid(), Some(block.cid.to_string().as_str()));
            if let Some(chunk) = walker.accept(block.bytes.clone()).unwrap() {
                output.extend(chunk);
            }
        }
        assert_eq!(output, b"RNabCNab");
        assert_eq!(walker.bytes(), 8);
        assert_eq!(walker.blocks(), 8);
        assert_eq!(walker.next_cid(), None);
    }

    #[test]
    fn validates_unpacked_bulletin_block_sizes() {
        let a = raw(b"ab");
        let b = raw(b"c");
        // type File, filesize 3, unpacked blockSizes [2, 1].
        let root = block(
            DAG_PB as u8,
            node_bytes(vec![link(&a), link(&b)], &[8, 2, 24, 3, 32, 2, 32, 1]),
        );
        let mut walker = walker(&root, 3);
        assert!(walker.accept(root.bytes).unwrap().is_none());
        assert_eq!(walker.accept(a.bytes).unwrap().unwrap(), b"ab");
        assert_eq!(walker.accept(b.bytes).unwrap().unwrap(), b"c");
        assert!(walker.next_cid().is_none());
    }

    #[test]
    fn rejects_size_mismatch_overflow_and_missing_link_sizes() {
        let leaf = raw(b"x");
        rejects(
            &file(vec![], metadata(b"abc", &[], Some(2))),
            10,
            "filesize mismatch",
        );
        rejects(
            &file(vec![link(&leaf)], metadata(b"", &[], Some(0))),
            10,
            "count mismatch",
        );
        rejects(
            &file(vec![], metadata(b"", &[1], Some(1))),
            10,
            "count mismatch",
        );
        rejects(
            &file(
                vec![link(&leaf), link(&leaf)],
                metadata(b"", &[u64::MAX, 1], None),
            ),
            MAX_FILE_BYTES,
            "overflow",
        );
        let root = file(vec![link(&leaf)], metadata(b"", &[2], Some(2)));
        let mut walker = walker(&root, 2);
        walker.accept(root.bytes).unwrap();
        assert!(
            walker
                .accept(leaf.bytes)
                .unwrap_err()
                .to_string()
                .contains("blockSizes")
        );
        assert_eq!(walker.bytes(), 0);
        assert!(walker.next_cid().is_some());
    }

    #[test]
    fn nested_declared_size_is_checked_before_inline_bytes_escape() {
        let child = file(vec![], metadata(b"abc", &[], Some(3)));
        let root = file(vec![link(&child)], metadata(b"R", &[2], Some(3)));
        let mut walker = walker(&root, 3);
        assert_eq!(walker.accept(root.bytes).unwrap().unwrap(), b"R");
        assert!(walker.accept(child.bytes).is_err());
        assert_eq!(walker.bytes(), 1);
        assert!(walker.next_cid().is_some());
    }

    #[test]
    fn byte_limits_apply_to_raw_blocks_and_declared_files_before_output() {
        let leaf = raw(b"123");
        rejects(&leaf, 2, "byte limit");
        rejects(
            &file(vec![link(&leaf)], metadata(b"x", &[3], Some(4))),
            3,
            "byte limit",
        );
        rejects(
            &raw(&vec![0; MAX_BLOCK_BYTES + 1]),
            MAX_FILE_BYTES,
            "block byte limit",
        );
        assert!(Walker::new(&leaf.cid.to_string(), MAX_FILE_BYTES + 1).is_err());
        let at_limit = raw(&vec![0; MAX_BLOCK_BYTES]);
        let mut walker = walker(&at_limit, MAX_BLOCK_BYTES as u64);
        assert_eq!(
            walker.accept(at_limit.bytes).unwrap().unwrap().len(),
            MAX_BLOCK_BYTES
        );
        assert!(walker.next_cid().is_none());
    }

    #[test]
    fn rejects_malformed_truncated_duplicate_and_overflowing_protobuf() {
        for bytes in [
            vec![0x0a, 3, 8],
            vec![0x0a, 0x80],
            vec![0x0b],
            vec![0],
            node_bytes(vec![], &[8, 2, 24, 0x80]),
            node_bytes(vec![], &[8, 2, 34, 1, 0x80]),
            node_bytes(vec![], &[8, 2, 8, 2]),
            node_bytes(vec![], &[24, 0]),
            node_bytes(vec![], &[10, 0]),
            node_bytes(
                vec![],
                &[
                    8, 2, 24, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2,
                ],
            ),
            node_bytes(
                vec![PbLink {
                    hash: vec![],
                    tsize: None,
                }],
                &[8, 2],
            ),
        ] {
            let malformed = block(DAG_PB as u8, bytes);
            let mut walker = walker(&malformed, MAX_FILE_BYTES);
            assert!(walker.accept(malformed.bytes).is_err());
            assert_eq!(walker.bytes(), 0);
            assert!(walker.next_cid().is_some());
        }
        let mut duplicate = node_bytes(vec![], &[8, 2]);
        duplicate.extend_from_slice(&[10, 2, 8, 2]);
        rejects(&block(DAG_PB as u8, duplicate), 0, "duplicate");
    }

    #[test]
    fn rejects_unsupported_types_encodings_and_link_codecs() {
        for kind in [1, 3, 4, 5, 6, u64::MAX, (1u64 << 32) + 2] {
            let mut data = metadata(b"inline", &[], None);
            data.kind = Some(kind);
            rejects(&file(vec![], data), 10, "unsupported UnixFS type");
        }
        let leaf = raw(b"x");
        let mut data = metadata(b"", &[1], Some(1));
        data.kind = Some(0);
        rejects(&file(vec![link(&leaf)], data), 1, "Raw cannot have links");
        let cid = leaf.cid.to_string();
        for text in [
            cid.to_uppercase(),
            format!("z{}", &cid[1..]),
            "QmInvalidCidV0".into(),
            format!("{cid}a"),
            "b".repeat(MAX_CID_TEXT + 1),
        ] {
            assert!(Walker::new(&text, 1).is_err());
        }
        let unsupported = block(0x71, b"not a supported codec".to_vec());
        assert!(Walker::new(&unsupported.cid.to_string(), 100).is_err());
        rejects(
            &file(vec![link(&unsupported)], metadata(b"", &[1], Some(1))),
            1,
            "unsupported CID codec",
        );
        for bytes in [
            {
                let mut b = leaf.cid.as_ref().to_vec();
                b[0] = 0;
                b
            },
            {
                let mut b = leaf.cid.as_ref().to_vec();
                b[2] = 0x13;
                b
            },
            {
                let mut b = leaf.cid.as_ref().to_vec();
                b[3] = 31;
                b
            },
            {
                let mut b = vec![0x81, 0];
                b.extend_from_slice(&leaf.cid.as_ref()[1..]);
                b
            },
        ] {
            let bad_link = PbLink {
                hash: bytes,
                tsize: None,
            };
            let bad = file(vec![bad_link], metadata(b"", &[1], Some(1)));
            let mut walker = walker(&bad, 1);
            assert!(walker.accept(bad.bytes).is_err());
        }
    }

    #[test]
    fn bounds_protobuf_expansion_before_prost_decoding() {
        let leaf = raw(b"");
        rejects(
            &file(
                (0..MAX_BLOCKS).map(|_| link(&leaf)).collect(),
                metadata(b"", &[], Some(0)),
            ),
            0,
            "link/block budget",
        );
        // A tiny packed integer expands into a u64; count before allocation.
        let mut packed = vec![8, 2, 34];
        put_varint(MAX_BLOCKS as u64, &mut packed);
        packed.extend(std::iter::repeat_n(0, MAX_BLOCKS));
        rejects(
            &block(DAG_PB as u8, node_bytes(vec![], &packed)),
            0,
            "blockSizes budget",
        );
        let mut unpacked = vec![8, 2];
        for _ in 0..MAX_BLOCKS {
            unpacked.extend_from_slice(&[32, 0]);
        }
        rejects(
            &block(DAG_PB as u8, node_bytes(vec![], &unpacked)),
            0,
            "blockSizes budget",
        );
    }

    #[test]
    fn repeated_padded_empty_files_cannot_amplify_the_download_budget() {
        let mut data = metadata(b"", &[], Some(0)).encode_to_vec();
        // Unknown optional UnixFS metadata is valid but is not file payload.
        put_varint((100 << 3) | 2, &mut data);
        let padding = MAX_BLOCK_BYTES - 64;
        put_varint(padding as u64, &mut data);
        data.resize(data.len() + padding, 0);
        let leaf = block(DAG_PB as u8, node_bytes(vec![], &data));
        let root = file(
            (0..10).map(|_| link(&leaf)).collect(),
            metadata(b"", &[0; 10], Some(0)),
        );
        let mut walk = walker(&root, 0);
        let mut encoded = root.bytes.len() as u64;
        walk.accept(root.bytes).unwrap();
        while encoded + leaf.bytes.len() as u64 <= MAX_DAG_OVERHEAD_BYTES {
            assert!(walk.accept(leaf.bytes.clone()).unwrap().is_none());
            encoded += leaf.bytes.len() as u64;
        }
        assert!(
            walk.accept(leaf.bytes.clone())
                .unwrap_err()
                .to_string()
                .contains("encoded DAG byte limit")
        );
        assert_eq!(walk.bytes(), 0);
        assert!(walk.next_cid().is_some(), "rejected DAG must not complete");
        assert!(walk.accept(leaf.bytes).is_err(), "failure must be terminal");
    }

    #[test]
    fn repeated_chunks_count_per_occurrence_and_cumulative_budget_never_resets() {
        let leaf = raw(b"");
        let root = file(
            (0..MAX_BLOCKS - 1).map(|_| link(&leaf)).collect(),
            metadata(b"", &vec![0; MAX_BLOCKS - 1], Some(0)),
        );
        let mut exact = walker(&root, 0);
        exact.accept(root.bytes).unwrap();
        for _ in 0..MAX_BLOCKS - 1 {
            assert!(exact.accept(leaf.bytes.clone()).unwrap().is_none());
        }
        assert_eq!(exact.blocks(), MAX_BLOCKS as u64);
        assert!(exact.next_cid().is_none());

        let nested = file(vec![link(&leaf)], metadata(b"", &[0], Some(0)));
        let mut links: Vec<_> = (0..MAX_BLOCKS - 2).map(|_| link(&leaf)).collect();
        links.push(link(&nested));
        let root = file(links, metadata(b"", &vec![0; MAX_BLOCKS - 1], Some(0)));
        let mut cumulative = walker(&root, 0);
        cumulative.accept(root.bytes).unwrap();
        for _ in 0..MAX_BLOCKS - 2 {
            cumulative.accept(leaf.bytes.clone()).unwrap();
        }
        assert!(
            cumulative
                .accept(nested.bytes)
                .unwrap_err()
                .to_string()
                .contains("budget")
        );
        assert_eq!(cumulative.blocks(), (MAX_BLOCKS - 1) as u64);
        assert!(cumulative.next_cid().is_some());
    }

    #[test]
    fn depth_limit_includes_root_and_rejects_before_requesting_excess_child() {
        for depth in [MAX_DEPTH, MAX_DEPTH + 1] {
            let mut blocks = vec![raw(b"")];
            for _ in 1..depth {
                blocks.push(file(
                    vec![link(blocks.last().unwrap())],
                    metadata(b"", &[0], Some(0)),
                ));
            }
            let mut walker = walker(blocks.last().unwrap(), 0);
            let mut rejected = false;
            for block in blocks.iter().rev() {
                match walker.accept(block.bytes.clone()) {
                    Ok(output) => assert!(output.is_none()),
                    Err(error) => {
                        assert!(error.to_string().contains("depth"));
                        rejected = true;
                        break;
                    }
                }
            }
            assert_eq!(rejected, depth > MAX_DEPTH);
            assert_eq!(walker.next_cid().is_none(), depth == MAX_DEPTH);
            assert_eq!(
                walker.blocks(),
                if rejected {
                    (MAX_DEPTH - 1) as u64
                } else {
                    MAX_DEPTH as u64
                }
            );
        }
    }

    #[test]
    fn validates_encoded_subtree_tsize_separately_from_logical_size() {
        let leaf = raw(b"abc");
        let child = file(vec![link(&leaf)], metadata(b"I", &[3], Some(4)));
        for adjustment in [0, 1] {
            let mut child_link = link(&child);
            child_link.tsize =
                Some(child.bytes.len() as u64 + leaf.bytes.len() as u64 + adjustment);
            let root = file(vec![child_link], metadata(b"", &[4], Some(4)));
            let mut walker = walker(&root, 4);
            walker.accept(root.bytes).unwrap();
            assert_eq!(walker.accept(child.bytes.clone()).unwrap().unwrap(), b"I");
            if adjustment == 0 {
                assert_eq!(walker.accept(leaf.bytes.clone()).unwrap().unwrap(), b"abc");
                assert!(walker.next_cid().is_none());
            } else {
                // Last chunk cannot escape before its enclosing frame is checked.
                assert!(
                    walker
                        .accept(leaf.bytes.clone())
                        .unwrap_err()
                        .to_string()
                        .contains("Tsize")
                );
                assert_eq!(walker.bytes(), 1);
                assert!(walker.next_cid().is_some());
            }
        }
        let mut bad_link = link(&leaf);
        bad_link.tsize = Some(2);
        let root = file(vec![bad_link], metadata(b"", &[3], Some(3)));
        let mut walker = walker(&root, 3);
        walker.accept(root.bytes).unwrap();
        assert!(
            walker
                .accept(leaf.bytes)
                .unwrap_err()
                .to_string()
                .contains("Tsize")
        );
        assert_eq!(walker.bytes(), 0);
    }
}
