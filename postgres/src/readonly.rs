//! Readonly pool provides performance gains for individual database operations
//! It does not provide transactions handling

#![warn(missing_docs, unreachable_pub)]

use std::ops::{Deref};
use log::{info, warn};
use tokio_postgres::{ types::Type, Client as PgClient, Error, Statement };

/// A type alias for using `deadpool::Pool` with `tokio_postgres` for readonly queries
pub type ReadonlyPool = readonly_pool::ReadonlyPool<ReadonlyClientWrapper, tokio_postgres::Error>;

/// A type alias for using `deadpool::Object` with `tokio_postgres` for readonly queries
pub type ReadonlyClient = deadpool::managed::Object<ReadonlyClientWrapper, tokio_postgres::Error>;

mod readonly_pool {

    use deadpool::managed::{Pool, Status, PoolConfig, Manager, Object, PoolError};
    use std::sync::Arc;
    use crossbeam_queue::ArrayQueue;

    pub trait IsValid {
        fn is_valid(&self) -> bool;
    } 

    struct ReadonlyPoolInner<T: IsValid, E> {
        pool: Pool<T, E>,
        objects: ArrayQueue<Arc<Object<T, E>>>,
    }

    /// ReadonlyPool is a managed pool around shared not mutable references.
    ///
    /// This struct can be cloned and transferred across thread boundaries
    /// and uses reference counting for its internal state.
    pub struct ReadonlyPool<T: IsValid, E> {
        inner: Arc<ReadonlyPoolInner<T, E>>,
    }

    impl<T: IsValid, E> Clone for ReadonlyPool<T, E> {
        fn clone(&self) -> ReadonlyPool<T, E> {
            ReadonlyPool {
                inner: self.inner.clone(),
            }
        }
    }

    impl<T: IsValid, E> ReadonlyPool<T, E> {
        /// Create new connection pool with a given `manager` and `max_size`.
        /// The `manager` is used to create and recycle objects and `max_size`
        /// is the maximum number of objects ever created.
        pub fn new(manager: impl Manager<T, E> + Send + Sync + 'static, max_size: usize) -> ReadonlyPool<T, E> {
            Self::from_config(manager, PoolConfig::new(max_size))
        }
        /// Create new connection pool with a given `manager` and `config`.
        /// The `manager` is used to create and recycle objects and `max_size`
        /// is the maximum number of objects ever created.
        pub fn from_config(
            manager: impl Manager<T, E> + Send + Sync + 'static,
            config: PoolConfig,
        ) -> ReadonlyPool<T, E> {
            ReadonlyPool {
                inner: Arc::new(
                    ReadonlyPoolInner {
                        objects: ArrayQueue::new(config.max_size),
                        pool: Pool::from_config(manager, config),
                    }
                ),
            }
        }
        /// Retrieve object from pool or wait for one to become available.
        pub async fn get(&self) -> Result<Arc<Object<T, E>>, PoolError<E>> {
            if self.inner.objects.is_full() {
                let obj = self.inner.objects.pop().unwrap();
                if obj.is_valid() {
                    self.inner.objects.push(obj.clone()).unwrap();
                    return Ok(obj)
                }
            }
            let obj = Arc::new(self.inner.pool.get().await?);
            self.inner.objects.push(obj.clone()).unwrap();
            Ok(obj)
        }
        /// Retrieve status of the pool
        #[inline]
        pub fn status(&self) -> Status {
            self.inner.pool.status()
        }
    }
}

use async_trait::async_trait;
use super::{Manager, RecycleResult};
use tokio_postgres::{tls::MakeTlsConnect, tls::TlsConnect, Socket};
use futures::FutureExt;
use tokio::spawn;


#[async_trait]
impl<T> deadpool::managed::Manager<ReadonlyClientWrapper, Error> for Manager<T>
where
    T: MakeTlsConnect<Socket> + Clone + Sync + Send + 'static,
    T::Stream: Sync + Send,
    T::TlsConnect: Sync + Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn create(&self) -> Result<ReadonlyClientWrapper, Error> {
        let (client, connection) = self.config.connect(self.tls.clone()).await?;
        let connection = connection.map(|r| {
            if let Err(e) = r {
                warn!(target: "deadpool.postgres", "Connection error: {}", e);
            }
        });
        spawn(connection);
        Ok(ReadonlyClientWrapper::new(client))
    }
    async fn recycle(&self, client: &mut ReadonlyClientWrapper) -> RecycleResult {
        match client.simple_query("").await {
            Ok(_) => Ok(()),
            Err(e) => {
                info!(target: "deadpool.postgres", "Connection could not be recycled: {}", e);
                Err(e.into())
            }
        }
    }
}

use super::StatementCache;

/// A wrapper for `tokio_postgres::Client` which includes a statement cache.
/// ReadonlyClientWrapper does not allow to handle transactions and does not implement DerefMut.
pub struct ReadonlyClientWrapper {
    client: PgClient,
    /// The statement cache
    pub statement_cache: StatementCache,
}

impl ReadonlyClientWrapper {
    /// Create new wrapper instance using an existing `tokio_postgres::Client`
    pub fn new(client: PgClient) -> Self {
        Self {
            client: client,
            statement_cache: StatementCache::new(),
        }
    }
    /// Creates a new prepared statement using the statement cache if possible.
    ///
    /// See [`tokio_postgres::Client::prepare`](#method.prepare-1)
    pub async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.prepare_typed(query, &[]).await
    }
    /// Creates a new prepared statement using the statement cache if possible.
    ///
    /// See [`tokio_postgres::Client::prepare_typed`](#method.prepare_typed-1)
    pub async fn prepare_typed(&self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        match self.statement_cache.get(query, types).await {
            Some(statement) => Ok(statement),
            None => {
                let stmt = self.client.prepare_typed(query, types).await?;
                self.statement_cache
                    .insert(query, types, stmt.clone())
                    .await;
                Ok(stmt)
            }
        }
    }
}

impl Deref for ReadonlyClientWrapper {
    type Target = PgClient;
    fn deref(&self) -> &PgClient {
        &self.client
    }
}

impl readonly_pool::IsValid for ReadonlyClientWrapper {
    fn is_valid(&self) -> bool {
        !self.client.is_closed()
    }
}