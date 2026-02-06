pub mod simulation_test;
pub mod simulations;

#[macro_export]
macro_rules! simulation_test {
    ($name:ident, |$sim:ident| $body:expr) => {
        paste::paste! {
            #[test]
            fn [<$name _sqlite>]() {
                std::thread::Builder::new()
                    .name(concat!(stringify!($name), "_sqlite").to_string())
                    .stack_size(8 * 1024 * 1024)
                    .spawn(|| {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("failed to build tokio runtime");
                        runtime.block_on(async {
                            $crate::support::simulation_test::run_single_simulation_test(
                                "sqlite",
                                concat!(module_path!(), "::", stringify!($name)),
                                |$sim| $body,
                            )
                            .await;
                        });
                    })
                    .expect("failed to spawn sqlite test thread")
                    .join()
                    .expect("sqlite simulation test thread panicked");
            }

            #[test]
            fn [<$name _postgres>]() {
                std::thread::Builder::new()
                    .name(concat!(stringify!($name), "_postgres").to_string())
                    .stack_size(8 * 1024 * 1024)
                    .spawn(|| {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("failed to build tokio runtime");
                        runtime.block_on(async {
                            $crate::support::simulation_test::run_single_simulation_test(
                                "postgres",
                                concat!(module_path!(), "::", stringify!($name)),
                                |$sim| $body,
                            )
                            .await;
                        });
                    })
                    .expect("failed to spawn postgres test thread")
                    .join()
                    .expect("postgres simulation test thread panicked");
            }
        }
    };
}
