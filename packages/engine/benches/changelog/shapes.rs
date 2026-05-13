use lix_engine::changelog::bench as changelog_bench;
use lix_engine::changelog::bench::{BenchCorpus, BenchSegment};

use crate::config::{
    BenchShape, CorpusShape, HeavyCorpusShape, KeyLayoutShape, MembershipFanoutShape, PayloadShape,
};

pub(crate) fn segment(shape: BenchShape) -> BenchSegment {
    match shape {
        BenchShape::Tiny => changelog_bench::segment_1c_1ch(),
        BenchShape::Small => changelog_bench::segment_1c_100ch(),
        BenchShape::Medium => changelog_bench::segment_1c_1000ch(),
        BenchShape::Mixed => changelog_bench::segment_100c_1000ch(),
    }
    .expect("build changelog benchmark segment")
}

pub(crate) fn corpus(shape: CorpusShape) -> BenchCorpus {
    match shape {
        CorpusShape::TenSegments => changelog_bench::corpus_10seg_10c_100ch(),
        CorpusShape::HundredSegments => changelog_bench::corpus_100seg_100c_1000ch(),
    }
    .expect("build changelog benchmark corpus")
}

pub(crate) fn heavy_corpus(shape: HeavyCorpusShape) -> BenchCorpus {
    match shape {
        HeavyCorpusShape::ThousandSegments => changelog_bench::corpus_1000seg_1000c_10000ch(),
    }
    .expect("build heavy changelog benchmark corpus")
}

pub(crate) fn payload_segment(shape: PayloadShape) -> BenchSegment {
    match shape {
        PayloadShape::NoPayload => changelog_bench::segment_1c_1000ch(),
        PayloadShape::SmallInline => changelog_bench::segment_1c_1000ch_small_inline_payloads(),
        PayloadShape::LargeInline => changelog_bench::segment_1c_1000ch_large_inline_payloads(),
        PayloadShape::ExternalRefsOnly => {
            changelog_bench::segment_1c_1000ch_external_payload_refs()
        }
    }
    .expect("build changelog payload benchmark segment")
}

pub(crate) fn key_layout_segment(shape: KeyLayoutShape) -> BenchSegment {
    match shape {
        KeyLayoutShape::Clustered => changelog_bench::segment_1c_1000ch_clustered_keys(),
        KeyLayoutShape::Random => changelog_bench::segment_1c_1000ch_random_keys(),
        KeyLayoutShape::ReuseAcrossCommits => {
            changelog_bench::segment_100c_1000ch_reused_keys_across_commits()
        }
    }
    .expect("build changelog key-layout benchmark segment")
}

pub(crate) fn membership_fanout(shape: MembershipFanoutShape) -> BenchSegment {
    changelog_bench::segment_change_membership_fanout(shape.fanout())
        .expect("build changelog membership fanout segment")
}
