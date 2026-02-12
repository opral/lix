pub mod simulation_test;
pub mod simulations;
pub mod wasmtime_runtime;

#[macro_export]
macro_rules! simulation_test {
    ($name:ident, |$sim:ident| $body:expr) => {
        $crate::simulation_test!(
            $name,
            simulations = [sqlite, postgres, materialization],
            |$sim| $body
        );
    };
    ($name:ident, simulations = [$($simulation:ident),+ $(,)?], |$sim:ident| $body:expr) => {
        paste::paste! {
            $(
                #[test]
                fn [<$name _ $simulation>]() {
                    std::thread::Builder::new()
                        .name(concat!(stringify!($name), "_", stringify!($simulation)).to_string())
                        .stack_size(8 * 1024 * 1024)
                        .spawn(|| {
                            let runtime = tokio::runtime::Builder::new_current_thread()
                                .enable_all()
                                .build()
                                .expect("failed to build tokio runtime");
                            runtime.block_on(async {
                                $crate::support::simulation_test::run_single_simulation_test(
                                    stringify!($simulation),
                                    concat!(module_path!(), "::", stringify!($name)),
                                    |$sim| $body,
                                )
                                .await;
                            });
                        })
                        .expect(concat!(
                            "failed to spawn ",
                            stringify!($simulation),
                            " test thread"
                        ))
                        .join()
                        .expect(concat!(
                            stringify!($simulation),
                            " simulation test thread panicked"
                        ));
                }
            )+
        }
    };
}
