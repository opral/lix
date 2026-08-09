# a33 unchanged-child authentication oracle

This is a test/report-only correction oracle for the blocked immutable a33
splice candidate:

- head `a33b7b9e12d84bbb95d64a29561a0b7572072ab2`
- tree `e32e6be39b627c92fb9f2fd8e5ea273b7589157b`
- parent/base `0499bcf9ab5d21a42da308509bb3b257ebc9d0ce`
- parent..head full-index binary diff SHA-256
  `2887c4ef0084b7ecf236a1fd867bd9191eb71c5f6035c67eea25fbc5278c7a24`
- stable patch-id `5a43d62e9001b1a6a41db0a945f3e48d4774bcfb`

The executable model is
`packages/rs-sdk-tests/examples/authenticated_splice_corruption_oracle.rs`.
It is deliberately dependency-free and uses a deterministic model digest; it
does not replace production ObjectId/domain/content authentication.

## Required gate

The fixture has one valid authenticated `lix_binary_blob_ref` StateKey and a
64-chunk fixed-width base manifest. One changed chunk is regenerated, while all
63 unchanged child IDs and content digests must be checked on the same retained
view before any plan is returned. The following corruption cases target one
unchanged child:

- missing object;
- malformed bytes/digest;
- wrong object domain;
- same-size substituted bytes under the referenced identity.

The old a33 behavior is modeled as the negative control: it accepts each case
because it only validates the StateKey/base-manifest envelope and copies the
unchanged child edge. The required gate rejects each case before a write,
selector, receipt, or successor manifest can be committed. The model also
checks a valid publication, exact `1 changed + 63 reused` identity counts,
same retained-view identity, and cold reopen for both named RocksDB and SlateDB
controls. Failed publications preserve all counters and durable state.

## Production successor contract

The production correction must, on the existing operation-owned `CoherentView`:

1. bind the exact StateKey and base manifest as a single authenticated owner;
2. load/decode every base child object referenced by the manifest;
3. reject missing, malformed, wrong-domain, wrong-size, same-size-substituted,
   digest-mismatched, or BlobId-mismatched children before staging or writes;
4. verify all 63 reused identities/digests and the changed child identity;
5. create one `PreparedPublication`/plan only after the complete closure passes;
6. preserve rollback, cold-reopen, selector, receipt, and no-partial-write
   behavior.

No raw object-space authority, second read, cache, fallback, compatibility
format, or second writer is permitted.
