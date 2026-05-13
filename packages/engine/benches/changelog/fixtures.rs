use lix_engine::changelog::bench::{BenchCorpus, BenchSegment};

use crate::config::{
    BenchShape, CorpusShape, HeavyCorpusShape, KeyLayoutShape, MembershipFanoutShape, PayloadShape,
};
use crate::shapes;

#[derive(Clone)]
pub(crate) struct SegmentFixture {
    pub(crate) segment: BenchSegment,
    pub(crate) commit_ids: Vec<String>,
    pub(crate) change_ids: Vec<String>,
}

impl SegmentFixture {
    pub(crate) fn new(shape: BenchShape) -> Self {
        let segment = shapes::segment(shape);
        let commit_ids = segment.commit_ids();
        let change_ids = segment.change_ids();
        debug_assert_eq!(commit_ids.len(), shape.commit_count());
        debug_assert_eq!(change_ids.len(), shape.change_count());
        Self {
            segment,
            commit_ids,
            change_ids,
        }
    }

    pub(crate) fn first_commit_id(&self) -> &str {
        self.commit_ids
            .first()
            .expect("changelog bench fixture has at least one commit")
    }
}

#[derive(Clone)]
pub(crate) struct PayloadFixture {
    pub(crate) segment: BenchSegment,
    pub(crate) change_ids: Vec<String>,
}

impl PayloadFixture {
    pub(crate) fn new(shape: PayloadShape) -> Self {
        let segment = shapes::payload_segment(shape);
        let change_ids = segment.change_ids();
        Self {
            segment,
            change_ids,
        }
    }
}

#[derive(Clone)]
pub(crate) struct KeyLayoutFixture {
    pub(crate) segment: BenchSegment,
}

impl KeyLayoutFixture {
    pub(crate) fn new(shape: KeyLayoutShape) -> Self {
        Self {
            segment: shapes::key_layout_segment(shape),
        }
    }
}

#[derive(Clone)]
pub(crate) struct CorpusFixture {
    pub(crate) corpus: BenchCorpus,
    pub(crate) commit_ids: Vec<String>,
    pub(crate) change_ids: Vec<String>,
    pub(crate) first_segment_commit_ids: Vec<String>,
    pub(crate) first_segment_change_ids: Vec<String>,
}

impl CorpusFixture {
    pub(crate) fn new(shape: CorpusShape) -> Self {
        let corpus = shapes::corpus(shape);
        let commit_ids = corpus.commit_ids().to_vec();
        let change_ids = corpus.change_ids().to_vec();
        let first_segment_commit_ids = corpus.first_segment_commit_ids();
        let first_segment_change_ids = corpus.first_segment_change_ids();
        Self {
            corpus,
            commit_ids,
            change_ids,
            first_segment_commit_ids,
            first_segment_change_ids,
        }
    }
}

#[derive(Clone)]
pub(crate) struct HeavyCorpusFixture {
    pub(crate) corpus: BenchCorpus,
    pub(crate) change_ids: Vec<String>,
}

impl HeavyCorpusFixture {
    pub(crate) fn new(shape: HeavyCorpusShape) -> Self {
        let corpus = shapes::heavy_corpus(shape);
        let change_ids = corpus.change_ids().to_vec();
        Self { corpus, change_ids }
    }
}

#[derive(Clone)]
pub(crate) struct MembershipFanoutFixture {
    pub(crate) segment: BenchSegment,
    pub(crate) change_id: String,
}

impl MembershipFanoutFixture {
    pub(crate) fn new(shape: MembershipFanoutShape) -> Self {
        let segment = shapes::membership_fanout(shape);
        let change_id = segment
            .change_ids()
            .into_iter()
            .next()
            .expect("membership fanout fixture has one change");
        Self { segment, change_id }
    }
}
