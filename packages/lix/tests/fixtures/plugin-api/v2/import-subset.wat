(component
  (type $operation (func (result u32)))
  (import "lix:plugin-v2/host"
    (instance $host
      (export "existing-operation" (func (type $operation)))))
  (alias export $host "existing-operation" (func $existing))
  (core func $lowered (canon lower (func $existing)))
  (core module $wrapper
    (import "host" "existing" (func $existing (result i32)))
    (func (export "run") (result i32) call $existing))
  (core instance $imports (export "existing" (func $lowered)))
  (core instance $instance
    (instantiate $wrapper (with "host" (instance $imports))))
  (func (export "run") (result u32)
    (canon lift (core func $instance "run")))
)
