use criterion::{black_box, BatchSize, Criterion};
use lix_engine::changelog::bench as changelog_bench;

use crate::config::{configure_group, BenchShape, KeyLayoutShape, PayloadShape};
use crate::fixtures::{KeyLayoutFixture, PayloadFixture, SegmentFixture};

pub(crate) fn bench(c: &mut Criterion) {
    bench_single_segment_cpu(c);
    bench_payload_cpu(c);
    bench_key_layout_cpu(c);
}

fn bench_single_segment_cpu(c: &mut Criterion) {
    for shape in BenchShape::ALL {
        let fixture = SegmentFixture::new(shape);
        let encoded =
            changelog_bench::encode_bench_segment(&fixture.segment).expect("encode segment");
        let mut group = c.benchmark_group(format!("changelog/cpu/{}", shape.label()));
        configure_group(&mut group, shape);

        group.bench_function("encode_segment", |b| {
            b.iter_batched(
                || fixture.segment.clone(),
                |segment| {
                    black_box(
                        changelog_bench::encode_bench_segment(&segment)
                            .expect("encode changelog segment"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function("decode_segment", |b| {
            b.iter_batched(
                || encoded.clone(),
                |bytes| {
                    black_box(
                        changelog_bench::decode_bench_segment(&bytes)
                            .expect("decode changelog segment"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function("view_segment", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::view_bench_segment(&encoded).expect("view changelog segment"),
                )
            })
        });

        group.bench_function("canonicalize_segment", |b| {
            b.iter_batched(
                || fixture.segment.clone(),
                |segment| {
                    black_box(
                        changelog_bench::canonicalize_bench_segment(segment)
                            .expect("canonicalize changelog segment"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function("validate_segment_shape", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::validate_bench_segment_shape(&fixture.segment)
                        .expect("validate changelog segment shape"),
                )
            })
        });

        group.bench_function("build_decoded_segment_index", |b| {
            b.iter_batched(
                || encoded.clone(),
                |bytes| {
                    black_box(
                        changelog_bench::decode_bench_segment_index(&bytes)
                            .expect("decode segment and build decoded segment index"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function("build_decoded_segment_index_from_value", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::build_decoded_segment_index(&fixture.segment)
                        .expect("build decoded segment index"),
                )
            })
        });

        group.bench_function("segment_directory_commit_lookup_all", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::lookup_segment_directory_commits(
                        &fixture.segment,
                        &fixture.commit_ids,
                    )
                    .expect("lookup segment commit directory"),
                )
            })
        });

        group.bench_function("segment_directory_change_lookup_all", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::lookup_segment_directory_changes(
                        &fixture.segment,
                        &fixture.change_ids,
                    )
                    .expect("lookup segment change directory"),
                )
            })
        });

        let decoded_index = changelog_bench::build_decoded_segment_index(&fixture.segment)
            .expect("build decoded segment index for lookup bench");
        group.bench_function("decoded_segment_index_commit_lookup_all", |b| {
            b.iter(|| {
                black_box(changelog_bench::lookup_decoded_segment_index_commits(
                    &decoded_index,
                    &fixture.commit_ids,
                ))
            })
        });

        group.bench_function("decoded_segment_index_change_lookup_all", |b| {
            b.iter(|| {
                black_box(changelog_bench::lookup_decoded_segment_index_changes(
                    &decoded_index,
                    &fixture.change_ids,
                ))
            })
        });

        group.bench_function("build_by_commit", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::build_by_commit_entries(&fixture.segment)
                        .expect("build by_commit entries"),
                )
            })
        });

        group.bench_function("build_by_change", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::build_by_change_entries(&fixture.segment)
                        .expect("build by_change entries"),
                )
            })
        });

        group.bench_function("build_by_change_membership", |b| {
            b.iter(|| {
                black_box(changelog_bench::build_by_change_membership_entries(
                    &fixture.segment,
                ))
            })
        });

        group.bench_function("project_segment_change_to_logical", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::project_first_change_to_logical(&fixture.segment)
                        .expect("project first change"),
                )
            })
        });

        group.bench_function("validate_commit_checksum", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::validate_first_commit_checksum(&fixture.segment)
                        .expect("validate first commit checksum"),
                )
            })
        });

        group.bench_function("validate_change_checksum", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::validate_first_change_checksum(&fixture.segment)
                        .expect("validate first change checksum"),
                )
            })
        });

        group.bench_function("validate_publication_closure", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::validate_publication_closure(&fixture.segment)
                        .expect("validate publication closure"),
                )
            })
        });

        group.finish();
    }
}

fn bench_payload_cpu(c: &mut Criterion) {
    for shape in PayloadShape::CI {
        let fixture = PayloadFixture::new(shape);
        let encoded =
            changelog_bench::encode_bench_segment(&fixture.segment).expect("encode segment");
        let mut group = c.benchmark_group(format!("changelog/cpu/payload/{}", shape.label()));
        group.sample_size(match shape {
            PayloadShape::LargeInline => 10,
            _ => 20,
        });

        group.bench_function("encode_segment", |b| {
            b.iter_batched(
                || fixture.segment.clone(),
                |segment| {
                    black_box(
                        changelog_bench::encode_bench_segment(&segment)
                            .expect("encode payload segment"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function("decode_segment", |b| {
            b.iter_batched(
                || encoded.clone(),
                |bytes| {
                    black_box(
                        changelog_bench::decode_bench_segment(&bytes)
                            .expect("decode payload segment"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function("size_stats", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::segment_size_stats(&fixture.segment)
                        .expect("measure segment size stats"),
                )
            })
        });

        group.bench_function("resolve_inline_payloads", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::resolve_inline_payloads(&fixture.segment)
                        .expect("resolve inline payloads"),
                )
            })
        });

        group.finish();
    }
}

fn bench_key_layout_cpu(c: &mut Criterion) {
    for shape in KeyLayoutShape::CI {
        let fixture = KeyLayoutFixture::new(shape);
        let mut group = c.benchmark_group(format!("changelog/cpu/key_layout/{}", shape.label()));
        group.sample_size(20);

        group.bench_function("encode_segment", |b| {
            b.iter_batched(
                || fixture.segment.clone(),
                |segment| {
                    black_box(
                        changelog_bench::encode_bench_segment(&segment)
                            .expect("encode key-layout segment"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function("canonicalize_segment", |b| {
            b.iter_batched(
                || fixture.segment.clone(),
                |segment| {
                    black_box(
                        changelog_bench::canonicalize_bench_segment(segment)
                            .expect("canonicalize key-layout segment"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function("validate_publication_closure", |b| {
            b.iter(|| {
                black_box(
                    changelog_bench::validate_publication_closure(&fixture.segment)
                        .expect("validate key-layout publication closure"),
                )
            })
        });

        group.finish();
    }
}
