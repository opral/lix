use crate::backends::BackendProfile;
use crate::kv_layout;
use crate::workload::WorkloadRow;

pub(crate) type PhysicalFixture = kv_layout::KvFixture;

pub(crate) fn empty_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> PhysicalFixture {
    kv_layout::empty_fixture(profile, rows)
}

pub(crate) fn seeded_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> PhysicalFixture {
    kv_layout::seeded_fixture(profile, rows)
}
