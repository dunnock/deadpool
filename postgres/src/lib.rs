//! # Deadpool for PostgreSQL [![Latest Version](https://img.shields.io/crates/v/deadpool-postgres.svg)](https://crates.io/crates/deadpool-postgres)
//!
//! Deadpool is a dead simple async pool for connections and objects
//! of any type.
//!
//! This crate implements a [`deadpool`](https://crates.io/crates/deadpool)
//! manager for [`tokio-postgres`](https://crates.io/crates/tokio-postgres)
//! and also provides a `statement` cache by wrapping `tokio_postgres::Client`
//! and `tokio_postgres::Transaction`.
//!
//! ## Features
//!
//! | Feature | Description | Extra dependencies | Default |
//! | ------- | ----------- | ------------------ | ------- |
//! | `config` | Enable support for [config](https://crates.io/crates/config) crate | `config`, `serde/derive` | yes |
//!
//! ## Example
//!
//! ```rust
//! use deadpool_postgres::{Config, Manager, Pool};
//! use tokio_postgres::{NoTls};
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut cfg = Config::from_env("PG").unwrap();
//!     let pool = cfg.create_pool(NoTls).unwrap();
//!     for i in 1..10 {
//!         let mut client = pool.get().await.unwrap();
//!         let stmt = client.prepare("SELECT 1 + $1").await.unwrap();
//!         let rows = client.query(&stmt, &[&i]).await.unwrap();
//!         let value: i32 = rows[0].get(0);
//!         assert_eq!(value, i + 1);
//!     }
//! }
//! ```
//!
//! ## License
//!
//! Licensed under either of
//!
//! - Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
//! - MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
//!
//! at your option.

#![warn(missing_docs, unreachable_pub)]

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use futures::FutureExt;
use log::{info, warn};
use tokio::spawn;
use tokio::sync::RwLock;
use tokio_postgres::{
    tls::MakeTlsConnect, tls::TlsConnect, types::Type, Client as PgClient, Config as PgConfig,
    Error, Socket, Statement, Transaction as PgTransaction,
};

pub mod config;
pub use crate::config::Config;

pub mod readonly;

/// A type alias for using `deadpool::Pool` with `tokio_postgres`
pub type Pool = deadpool::managed::Pool<ClientWrapper, tokio_postgres::Error>;

/// A type alias for using `deadpool::PoolError` with `tokio_postgres`
pub type PoolError = deadpool::managed::PoolError<tokio_postgres::Error>;

/// A type alias for using `deadpool::Object` with `tokio_postgres`
pub type Client = deadpool::managed::Object<ClientWrapper, tokio_postgres::Error>;

pub(crate) type RecycleResult = deadpool::managed::RecycleResult<Error>;

/// The manager for creating and recyling postgresql connections
pub struct Manager<T: MakeTlsConnect<Socket>> {
    config: PgConfig,
    tls: T,
}

impl<T: MakeTlsConnect<Socket>> Manager<T> {
    /// Create manager using `PgConfig` and a `TlsConnector`
    pub fn new(config: PgConfig, tls: T) -> Manager<T> {
        Manager {
            config: config,
            tls: tls,
        }
    }
}

#[async_trait]
impl<T> deadpool::managed::Manager<ClientWrapper, Error> for Manager<T>
where
    T: MakeTlsConnect<Socket> + Clone + Sync + Send + 'static,
    T::Stream: Sync + Send,
    T::TlsConnect: Sync + Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn create(&self) -> Result<ClientWrapper, Error> {
        let (client, connection) = self.config.connect(self.tls.clone()).await?;
        let connection = connection.map(|r| {
            if let Err(e) = r {
                warn!(target: "deadpool.postgres", "Connection error: {}", e);
            }
        });
        spawn(connection);
        Ok(ClientWrapper::new(client))
    }
    async fn recycle(&self, client: &mut ClientWrapper) -> RecycleResult {
        match client.simple_query("").await {
            Ok(_) => Ok(()),
            Err(e) => {
                info!(target: "deadpool.postgres", "Connection could not be recycled: {}", e);
                Err(e.into())
            }
        }
    }
}

/// This structure holds the cached statements and provides access to
/// functions for retrieving the current size and clearing the cache.
pub struct StatementCache {
    map: RwLock<HashMap<StatementCacheKey<'static>, Statement>>,
    size: AtomicUsize,
}

// Allows us to use owned keys in the `HashMap`, but still be able
// to call `get` with borrowed keys instead of allocating them each time.
#[derive(Hash, Eq, PartialEq)]
struct StatementCacheKey<'a> {
    query: Cow<'a, str>,
    types: Cow<'a, [Type]>,
}

impl StatementCache {
    fn new() -> StatementCache {
        StatementCache {
            map: RwLock::new(HashMap::new()),
            size: AtomicUsize::new(0),
        }
    }
    /// Retrieve current size of the cache
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
    /// Clear cache
    pub async fn clear(&self) {
        let mut map = self.map.write().await;
        map.clear();
        self.size.store(0, Ordering::Relaxed);
    }
    /// Get statement from cache
    async fn get<'a>(&self, query: &str, types: &[Type]) -> Option<Statement> {
        let key = StatementCacheKey {
            query: Cow::Borrowed(query),
            types: Cow::Borrowed(types),
        };
        self.map.read().await.get(&key).map(|stmt| stmt.to_owned())
    }
    /// Insert statement into cache
    async fn insert(&self, query: &str, types: &[Type], stmt: Statement) {
        let key = StatementCacheKey {
            query: Cow::Owned(query.to_owned()),
            types: Cow::Owned(types.to_owned()),
        };
        let mut map = self.map.write().await;
        map.insert(key, stmt);
        self.size.fetch_add(1, Ordering::Relaxed);
    }
}

/// A wrapper for `tokio_postgres::Client` which includes a statement cache.
pub struct ClientWrapper {
    client: PgClient,
    /// The statement cache
    pub statement_cache: StatementCache,
}

impl ClientWrapper {
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
    /// Begins a new database transaction which supports the statement cache.
    ///
    /// See [`tokio_postgres::Client::transaction`](#method.transaction-1)
    pub async fn transaction<'a>(&'a mut self) -> Result<Transaction<'a>, Error> {
        Ok(Transaction {
            txn: PgClient::transaction(&mut self.client).await?,
            statement_cache: &mut self.statement_cache,
        })
    }
}

impl Deref for ClientWrapper {
    type Target = PgClient;
    fn deref(&self) -> &PgClient {
        &self.client
    }
}

impl DerefMut for ClientWrapper {
    fn deref_mut(&mut self) -> &mut PgClient {
        &mut self.client
    }
}

/// A wrapper for `tokio_postgres::Transaction` which uses the statement cache
/// from the client object it was created by.
pub struct Transaction<'a> {
    txn: PgTransaction<'a>,
    /// The statement cache
    pub statement_cache: &'a mut StatementCache,
}

impl<'a> Transaction<'a> {
    /// Creates a new prepared statement using the statement cache if possible.
    ///
    /// See [`tokio_postgres::Client::prepare`](#method.prepare_typed-1)
    pub async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.prepare_typed(query, &[]).await
    }
    /// Creates a new prepared statement using the statement cache if possible.
    ///
    /// See [`tokio_postgres::Transaction::prepare_typed`](#method.prepare_typed-1)
    pub async fn prepare_typed(&self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        match self.statement_cache.get(query, types).await {
            Some(statement) => Ok(statement),
            None => {
                let stmt = self.txn.prepare_typed(query, types).await?;
                self.statement_cache
                    .insert(query, types, stmt.clone())
                    .await;
                Ok(stmt)
            }
        }
    }
    /// Like `tokio_postgres::Transaction::commit`
    pub async fn commit(self) -> Result<(), Error> {
        self.txn.commit().await
    }
    /// Like `tokio_postgres::Transaction::rollback`
    pub async fn rollback(self) -> Result<(), Error> {
        self.txn.rollback().await
    }
    /// Like `tokio_postgres::Transaction::transaction`
    pub async fn transaction<'b>(&'b mut self) -> Result<Transaction<'b>, Error> {
        Ok(Transaction {
            txn: PgTransaction::transaction(&mut self.txn).await?,
            statement_cache: &mut self.statement_cache,
        })
    }
}

impl<'a> Deref for Transaction<'a> {
    type Target = PgTransaction<'a>;
    fn deref(&self) -> &PgTransaction<'a> {
        &self.txn
    }
}

impl<'a> DerefMut for Transaction<'a> {
    fn deref_mut(&mut self) -> &mut PgTransaction<'a> {
        &mut self.txn
    }
}
