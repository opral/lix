pub mod simulation_test;
pub mod simulations;

#[macro_export]
macro_rules! simulation_test {
	($name:ident, |$sim:ident| $body:expr) => {
		paste::paste! {
			#[tokio::test]
			async fn [<$name _sqlite>]() {
				let $sim = $crate::support::simulation_test::default_simulations()
					.into_iter()
					.find(|sim| sim.name == "sqlite")
					.expect("sqlite simulation missing");
				$crate::support::simulation_test::run_simulation_test(vec![$sim], |$sim| $body).await;
			}

			#[tokio::test]
			async fn [<$name _postgres>]() {
				let $sim = $crate::support::simulation_test::default_simulations()
					.into_iter()
					.find(|sim| sim.name == "postgres")
					.expect("postgres simulation missing");
				$crate::support::simulation_test::run_simulation_test(vec![$sim], |$sim| $body).await;
			}
		}
	};
}
