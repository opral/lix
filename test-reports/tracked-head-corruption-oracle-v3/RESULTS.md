# Focused v3 results

Source gate: PASS.

Standalone source compile:

    rustc --edition=2021 -D warnings selector_domain_contract_model.rs

Binary output:

    v3_cases=24 passed=24 failures=0 stale_view_cases=6 stale_view_failures=0 retained_read_per_case=1 durable_work_per_failure=0 digest=55329c3b11586602

Standalone test compile/run: 3/3 tests passed.

Binary SHA-256: c690d4aad11e7423d30d64ca93cf334b28a2068dc7b0cbee1136cd94718c0497
Binary output SHA-256: 99d37a26d21d8ee05abf9f6683768dcb9da32c7870304218f205d2aa4e531635
Test binary SHA-256: 7689e072cab5fff5aa24c5b2950efe66cc4ad82ac66136540d5563a1982ec4a9
Test output SHA-256: 9c9619d97053b998dd916ac0c370e34c919ce8543d7c53d135554f8c6c3ce850

No Lix, Cargo, adapter, runtime, benchmark, or production build was run.
