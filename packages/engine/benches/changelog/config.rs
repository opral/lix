use std::time::Duration;

use criterion::{measurement::WallTime, BenchmarkGroup};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BenchShape {
    Tiny,
    Small,
    Medium,
    Mixed,
}

impl BenchShape {
    pub(crate) const ALL: [Self; 4] = [Self::Tiny, Self::Small, Self::Medium, Self::Mixed];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Tiny => "1c_1ch",
            Self::Small => "1c_100ch",
            Self::Medium => "1c_1000ch",
            Self::Mixed => "100c_1000ch",
        }
    }

    pub(crate) fn commit_count(self) -> usize {
        match self {
            Self::Tiny | Self::Small | Self::Medium => 1,
            Self::Mixed => 100,
        }
    }

    pub(crate) fn change_count(self) -> usize {
        match self {
            Self::Tiny => 1,
            Self::Small => 100,
            Self::Medium | Self::Mixed => 1_000,
        }
    }

    pub(crate) fn sample_size(self) -> usize {
        match self {
            Self::Tiny => 50,
            Self::Small => 30,
            Self::Medium => 20,
            Self::Mixed => 15,
        }
    }
}

pub(crate) fn configure_group(group: &mut BenchmarkGroup<'_, WallTime>, shape: BenchShape) {
    group.sample_size(shape.sample_size());
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(match shape {
        BenchShape::Tiny => 500,
        BenchShape::Small => 700,
        BenchShape::Medium | BenchShape::Mixed => 1_000,
    }));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CorpusShape {
    TenSegments,
    HundredSegments,
}

impl CorpusShape {
    pub(crate) const CI: [Self; 2] = [Self::TenSegments, Self::HundredSegments];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::TenSegments => "10seg_10c_100ch",
            Self::HundredSegments => "100seg_100c_1000ch",
        }
    }

    pub(crate) fn sample_size(self) -> usize {
        match self {
            Self::TenSegments => 15,
            Self::HundredSegments => 10,
        }
    }
}

pub(crate) fn configure_corpus_group(group: &mut BenchmarkGroup<'_, WallTime>, shape: CorpusShape) {
    group.sample_size(shape.sample_size());
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(match shape {
        CorpusShape::TenSegments => 800,
        CorpusShape::HundredSegments => 1_000,
    }));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MembershipFanoutShape {
    One,
    Ten,
    Hundred,
}

impl MembershipFanoutShape {
    pub(crate) const CI: [Self; 3] = [Self::One, Self::Ten, Self::Hundred];

    pub(crate) fn fanout(self) -> usize {
        match self {
            Self::One => 1,
            Self::Ten => 10,
            Self::Hundred => 100,
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::One => "fanout_1",
            Self::Ten => "fanout_10",
            Self::Hundred => "fanout_100",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GcShape {
    AllLive,
    HalfLive,
    MostlyDead,
}

impl GcShape {
    pub(crate) const CI: [Self; 3] = [Self::AllLive, Self::HalfLive, Self::MostlyDead];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::AllLive => "live_100pct",
            Self::HalfLive => "live_50pct_mixed_segments",
            Self::MostlyDead => "live_1pct_many_dead",
        }
    }

    pub(crate) fn live_dead_segments(self) -> (usize, usize) {
        match self {
            Self::AllLive => (100, 0),
            Self::HalfLive => (50, 50),
            Self::MostlyDead => (1, 99),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PayloadShape {
    NoPayload,
    SmallInline,
    LargeInline,
    ExternalRefsOnly,
}

impl PayloadShape {
    pub(crate) const CI: [Self; 4] = [
        Self::NoPayload,
        Self::SmallInline,
        Self::LargeInline,
        Self::ExternalRefsOnly,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::NoPayload => "no_payload",
            Self::SmallInline => "small_inline",
            Self::LargeInline => "large_inline",
            Self::ExternalRefsOnly => "external_refs_only",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KeyLayoutShape {
    Clustered,
    Random,
    ReuseAcrossCommits,
}

impl KeyLayoutShape {
    pub(crate) const CI: [Self; 3] = [Self::Clustered, Self::Random, Self::ReuseAcrossCommits];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Clustered => "clustered_keys",
            Self::Random => "random_keys",
            Self::ReuseAcrossCommits => "reused_keys_across_commits",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HeavyCorpusShape {
    ThousandSegments,
}

impl HeavyCorpusShape {
    pub(crate) const ALL: [Self; 1] = [Self::ThousandSegments];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::ThousandSegments => "1000seg_1000c_10000ch",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitProjectionShape {
    Header,
    Body,
    Full,
}

impl CommitProjectionShape {
    pub(crate) const CI: [Self; 3] = [Self::Header, Self::Body, Self::Full];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::Body => "body",
            Self::Full => "full",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChangeProjectionShape {
    PhysicalLocation,
    Logical,
    Segment,
}

impl ChangeProjectionShape {
    pub(crate) const CI: [Self; 3] = [Self::PhysicalLocation, Self::Logical, Self::Segment];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::PhysicalLocation => "physical_location",
            Self::Logical => "logical",
            Self::Segment => "segment",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LookupBatchShape {
    One,
    Ten,
    Hundred,
    Thousand,
}

impl LookupBatchShape {
    pub(crate) const CI: [Self; 4] = [Self::One, Self::Ten, Self::Hundred, Self::Thousand];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::One => "m_1",
            Self::Ten => "m_10",
            Self::Hundred => "m_100",
            Self::Thousand => "m_1000",
        }
    }

    pub(crate) fn len(self) -> usize {
        match self {
            Self::One => 1,
            Self::Ten => 10,
            Self::Hundred => 100,
            Self::Thousand => 1_000,
        }
    }
}
