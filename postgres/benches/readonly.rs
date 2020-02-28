// Benchmarking of benefit of readonly pool

use criterion::{criterion_main};
use futures::future::join_all;
use deadpool_postgres::Config;
use tokio::runtime::Runtime;

macro_rules! bench_pool {
	($criterion:ident, $pool:ident, $name:expr) => {
		let pool = $pool.clone();
		// start benchmark loops
		$criterion.bench_function($name, move |b| {
			b.iter_custom(|iters| {
				let mut pools: Vec<_> = (1..iters).map(|_| pool.clone()).collect();
				let mut rt = Runtime::new().unwrap();
				
				let elapsed = rt.block_on(async {
					let start = std::time::Instant::now();
					// benchmark body
					join_all(
						pools
						.iter_mut()
						.map(|pool| async move {
							if let Ok(client) = pool.get().await {
//								println!("got client");
								let stmt = client.prepare("SELECT 1 + 2").await.unwrap();
								let rows = client.query(&stmt, &[]).await.unwrap();
								let value: i32 = rows[0].get(0);
								assert_eq!(value, 3i32);
							} else {
								tokio::time::delay_for(tokio::time::Duration::from_millis(10)).await;
							}
						})
					).await;
					start.elapsed()
				});
				// check that at least first request succeeded
				elapsed
			})
		});
	}
}

pub fn pool_benches() {
    let mut criterion: ::criterion::Criterion<_> =
		::criterion::Criterion::default().configure_from_args();

	let cfg = Config::from_env("PG").unwrap();
	dbg!(&cfg);
	let pool = cfg.create_pool(tokio_postgres::NoTls).unwrap();
	let readonly_pool = cfg.create_readonly_pool(tokio_postgres::NoTls).unwrap();

	bench_pool!(criterion, readonly_pool, "PostgreSQL ReadonlyPool");
	bench_pool!(criterion, pool, "PostgreSQL Pool");
	bench_pool!(criterion, readonly_pool, "PostgreSQL ReadonlyPool");
	bench_pool!(criterion, pool, "PostgreSQL Pool");
}

criterion_main!(pool_benches);

