use crate::backends::BackendProfile;
use crate::kv_layout;
use crate::workload::WorkloadRow;

pub(crate) fn insert_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
    kv_layout::insert_all(profile, rows)
}

pub(crate) fn read_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
    kv_layout::read_all(profile, rows)
}

pub(crate) fn read_all_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
    kv_layout::read_all_by_pk(profile, rows)
}

pub(crate) fn read_one_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
    kv_layout::read_one_by_pk(profile, rows)
}

pub(crate) fn update_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
    kv_layout::update_all(profile, rows)
}

pub(crate) fn update_one_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
    kv_layout::update_one_by_pk(profile, rows)
}

pub(crate) fn delete_all(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
    kv_layout::delete_all(profile, rows)
}

pub(crate) fn delete_one_by_pk(profile: BackendProfile, rows: &[WorkloadRow]) -> usize {
    kv_layout::delete_one_by_pk(profile, rows)
}
