# Plan Validation Notes

## Lix Owned State Has Plugin Key `lix`

- `plugin_key` should be `lix` when written by the engine.

```diff
-lix_sdk
+lix
```

Reasoning: we now have a native engine that writes these rows; the SDK is just a layer on top.
