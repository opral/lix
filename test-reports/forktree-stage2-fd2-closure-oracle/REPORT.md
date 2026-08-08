# FD2 closure oracle handoff

Status: frozen additive TEST/REPORT-only package. The b484 source is an
intentional RED control; the pure model is GREEN and the working-diff path is
the positive source control.

Bound source:

- head `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- tree `4477c83b246bddac09cd972564bd4ccd67f90f7b`
- parent `fd2be256d763f17e9f127d4c984e36fba191cb82`
- full-index diff `d36495fc406cc213bb5729babae761916f97bd515221de14c1f3ae114ec22610`
- patch `e90c9dd93db7c343f67887218049406640a77631`
- prior blocker report `83871d2d7c1e8faa0231f77aae75a3f2811debfaeaebd5fb6c18aa83d74d5e96`

The oracle is deliberately separate from the existing FD2 A/B production
oracle paths. It covers all nine closure seams, valid explicit-empty/live/
tombstone controls, and the existing working-diff identity/tombstone positive
control. `SHA256SUMS` covers every package artifact except itself.

No production edit, build, adapter runtime, PR mutation, or main change is
authorized by this package.
