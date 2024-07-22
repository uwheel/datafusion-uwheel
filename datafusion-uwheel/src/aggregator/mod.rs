use bitpacking::{BitPacker, BitPacker4x};
use uwheel::{aggregator::Compression, Aggregator};

// A U32 Sum Aggregator with Bitpacking encoding support
#[derive(Clone, Copy, Debug, Default)]
pub struct BitPackingSumAggregator;

impl Aggregator for BitPackingSumAggregator {
    const IDENTITY: Self::PartialAggregate = 0;

    type Input = u32;
    type PartialAggregate = u32;
    type MutablePartialAggregate = u32;
    type Aggregate = u32;

    fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
        input
    }
    fn combine_mutable(mutable: &mut Self::MutablePartialAggregate, input: Self::Input) {
        *mutable += input
    }
    fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate {
        a
    }

    fn combine(a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
        a + b
    }
    fn combine_inverse() -> Option<uwheel::aggregator::InverseFn<Self::PartialAggregate>> {
        Some(|a, b| a.saturating_sub(b))
    }
    fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
        a
    }

    fn compression() -> Option<Compression<Self::PartialAggregate>> {
        let compressor = |slice: &[u32]| {
            let bitpacker = BitPacker4x::new();
            let num_bits = bitpacker.num_bits(slice);
            let mut compressed = vec![0u8; BitPacker4x::BLOCK_LEN * 4];
            let compressed_len = bitpacker.compress(slice, &mut compressed[..], num_bits);

            // 1 bit for metadata + compressed data
            let mut result = Vec::with_capacity(1 + compressed_len);
            // Prepend metadata
            result.push(num_bits);
            // Append compressed data
            result.extend_from_slice(&compressed[..compressed_len]);

            result
        };
        let decompressor = |bytes: &[u8]| {
            let bit_packer = BitPacker4x::new();
            // Extract num bits metadata
            let bits = bytes[0];
            // Decompress data
            let mut decompressed = vec![0u32; BitPacker4x::BLOCK_LEN];
            bit_packer.decompress(&bytes[1..], &mut decompressed, bits);

            decompressed
        };
        Some(Compression::new(compressor, decompressor))
    }
}
