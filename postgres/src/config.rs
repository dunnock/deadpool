//! This module describes configuration used for [`Pool`] creation.

use std::env;
use std::fmt;
use std::path::Path;
use std::time::Duration;

use deadpool::managed::PoolConfig;
use tokio_postgres::config::{
    ChannelBinding as PgChannelBinding, SslMode as PgSslMode,
    TargetSessionAttrs as PgTargetSessionAttrs,
};
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::Socket;

use crate::{Pool, readonly::ReadonlyPool};

/// An error which is returned by `Config::create_pool` if something is
/// wrong with the configuration.
#[derive(Debug)]
pub enum ConfigError {
    /// Message of the error.
    Message(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(message) => write!(f, "{}", message),
        }
    }
}

#[cfg(feature = "config")]
impl Into<::config_crate::ConfigError> for ConfigError {
    fn into(self) -> ::config_crate::ConfigError {
        match self {
            Self::Message(message) => ::config_crate::ConfigError::Message(message),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Properties required of a session.
#[derive(Debug, Copy, Clone, PartialEq)]
#[cfg_attr(feature = "config", derive(serde::Deserialize))]
#[non_exhaustive]
pub enum TargetSessionAttrs {
    /// No special properties are required.
    Any,
    /// The session must allow writes.
    ReadWrite,
}

impl Into<PgTargetSessionAttrs> for TargetSessionAttrs {
    fn into(self) -> PgTargetSessionAttrs {
        match self {
            Self::Any => PgTargetSessionAttrs::Any,
            Self::ReadWrite => PgTargetSessionAttrs::ReadWrite,
        }
    }
}

/// TLS configuration.
#[derive(Debug, Copy, Clone, PartialEq)]
#[cfg_attr(feature = "config", derive(serde::Deserialize))]
#[non_exhaustive]
pub enum SslMode {
    /// Do not use TLS.
    Disable,
    /// Attempt to connect with TLS but allow sessions without.
    Prefer,
    /// Require the use of TLS.
    Require,
}

impl Into<PgSslMode> for SslMode {
    fn into(self) -> PgSslMode {
        match self {
            Self::Disable => PgSslMode::Disable,
            Self::Prefer => PgSslMode::Prefer,
            Self::Require => PgSslMode::Require,
        }
    }
}

/// Channel binding configuration.
#[derive(Debug, Copy, Clone, PartialEq)]
#[cfg_attr(feature = "config", derive(serde::Deserialize))]
#[non_exhaustive]
pub enum ChannelBinding {
    /// Do not use channel binding.
    Disable,
    /// Attempt to use channel binding but allow sessions without.
    Prefer,
    /// Require the use of channel binding.
    Require,
}

impl Into<PgChannelBinding> for ChannelBinding {
    fn into(self) -> PgChannelBinding {
        match self {
            Self::Disable => PgChannelBinding::Disable,
            Self::Prefer => PgChannelBinding::Prefer,
            Self::Require => PgChannelBinding::Require,
        }
    }
}

/// Configuration object. By enabling the `config` feature you can
/// read the configuration using the [`config`](https://crates.io/crates/config)
/// crate.
/// ## Example environment
/// ```env
/// PG_HOST=pg.example.com
/// PG_USER=john_doe
/// PG_PASSWORD=topsecret
/// PG_DBNAME=example
/// PG_POOL.MAX_SIZE=16
/// PG_POOL.TIMEOUTS.WAIT.SECS=5
/// PG_POOL.TIMEOUTS.WAIT.NANOS=0
/// ```
/// ## Example usage
/// ```rust,ignore
/// Config::from_env("PG");
/// ```
#[derive(Clone, Debug)]
#[cfg_attr(feature = "config", derive(serde::Deserialize))]
pub struct Config {
    /// See `tokio_postgres::Config::user`
    pub user: Option<String>,
    /// See `tokio_postgres::Config::password`
    pub password: Option<String>,
    /// See `tokio_postgres::Config::dbname`
    pub dbname: Option<String>,
    /// See `tokio_postgres::Config::options`
    pub options: Option<String>,
    /// See `tokio_postgres::Config::application_name`
    pub application_name: Option<String>,
    /// See `tokio_postgres::Config::ssl_mode`
    pub ssl_mode: Option<SslMode>,
    /// This is similar to `hosts` but only allows one host to be specified.
    /// Unlike `tokio-postgres::Config` this structure differenciates between
    /// one host and more than one host. This makes it possible to store this
    /// configuration in an envorinment variable.
    /// See `tokio_postgres::Config::host`
    pub host: Option<String>,
    /// See `tokio_postgres::Config::hosts`
    pub hosts: Option<Vec<String>>,
    /// This is similar to `ports` but only allows one port to be specified.
    /// Unlike `tokio-postgres::Config` this structure differenciates between
    /// one port and more than one port. This makes it possible to store this
    /// configuration in an environment variable.
    /// See `tokio_postgres::Config::port`
    pub port: Option<u16>,
    /// See `tokio_postgres::Config::port`
    pub ports: Option<Vec<u16>>,
    /// See `tokio_postgres::Config::connect_timeout`
    pub connect_timeout: Option<Duration>,
    /// See `tokio_postgres::Config::keepalives`
    pub keepalives: Option<bool>,
    /// See `tokio_postgres::Config::keepalives_idle`
    pub keepalives_idle: Option<Duration>,
    /// See `tokio_postgres::Config::target_session_attrs`
    pub target_session_attrs: Option<TargetSessionAttrs>,
    /// See `tokio_postgres::Config::channel_binding`
    pub channel_binding: Option<ChannelBinding>,
    /// Pool configuration
    pub pool: Option<PoolConfig>,
}

impl Config {
    /// Create configuration from environment variables.
    #[cfg(feature = "config")]
    pub fn from_env(prefix: &str) -> Result<Self, ::config_crate::ConfigError> {
        use ::config_crate::Environment;
        let mut cfg = ::config_crate::Config::new();
        cfg.merge(Environment::with_prefix(prefix))?;
        cfg.try_into()
    }
    /// Create pool using the current configuration
    pub fn create_pool<T>(&self, tls: T) -> Result<Pool, ConfigError>
    where
        T: MakeTlsConnect<Socket> + Clone + Sync + Send + 'static,
        T::Stream: Sync + Send,
        T::TlsConnect: Sync + Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let pg_config = self.get_pg_config()?;
        let manager = crate::Manager::new(pg_config, tls);
        let pool_config = self.get_pool_config();
        Ok(Pool::from_config(manager, pool_config))
    }
    /// Create readonly pool using the current configuration
    pub fn create_readonly_pool<T>(&self, tls: T) -> Result<ReadonlyPool, ConfigError>
    where
        T: MakeTlsConnect<Socket> + Clone + Sync + Send + 'static,
        T::Stream: Sync + Send,
        T::TlsConnect: Sync + Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let pg_config = self.get_pg_config()?;
        let manager = crate::Manager::new(pg_config, tls);
        let pool_config = self.get_pool_config();
        Ok(ReadonlyPool::from_config(manager, pool_config))
    }
    /// Get `tokio_postgres::Config` which can be used to connect to
    /// the database.
    pub fn get_pg_config(&self) -> Result<tokio_postgres::Config, ConfigError> {
        let mut cfg = tokio_postgres::Config::new();
        if let Some(user) = &self.user {
            cfg.user(user.as_str());
        } else if let Ok(user) = env::var("USER") {
            cfg.user(user.as_str());
        }
        if let Some(password) = &self.password {
            cfg.password(password);
        }
        match &self.dbname {
            Some(dbname) => match dbname.as_str() {
                "" => {
                    return Err(ConfigError::Message(
                        "configuration property \"dbname\" not found".to_string(),
                    ))
                }
                dbname => cfg.dbname(dbname),
            },
            None => {
                return Err(ConfigError::Message(
                    "configuration property \"dbname\" contains an empty string".to_string(),
                ))
            }
        };
        if let Some(options) = &self.options {
            cfg.options(options.as_str());
        }
        if let Some(application_name) = &self.application_name {
            cfg.application_name(application_name.as_str());
        }
        if let Some(host) = &self.host {
            cfg.host(host.as_str());
        }
        if let Some(hosts) = &self.hosts {
            for host in hosts.iter() {
                cfg.host(host.as_str());
            }
        } else {
            // Systems that support it default to unix domain sockets
            #[cfg(unix)]
            {
                if Path::new("/run/postgresql").exists() {
                    cfg.host_path("/run/postgresql");
                } else if Path::new("/var/run/postgresql").exists() {
                    cfg.host_path("/var/run/postgresql");
                } else {
                    cfg.host_path("/tmp");
                }
            }
            // Windows and other systems use 127.0.0.1 instead
            #[cfg(not(unix))]
            cfg.host("127.0.0.1");
        }
        if let Some(port) = &self.port {
            cfg.port(*port);
        }
        if let Some(ports) = &self.ports {
            for port in ports.iter() {
                cfg.port(*port);
            }
        }
        if let Some(connect_timeout) = &self.connect_timeout {
            cfg.connect_timeout(*connect_timeout);
        }
        if let Some(keepalives) = &self.keepalives {
            cfg.keepalives(*keepalives);
        }
        if let Some(keepalives_idle) = &self.keepalives_idle {
            cfg.keepalives_idle(*keepalives_idle);
        }
        Ok(cfg)
    }
    /// Get `deadpool::PoolConfig` which can be used to construct a
    /// `deadpool::managed::Pool` instance.
    pub fn get_pool_config(&self) -> PoolConfig {
        self.pool.clone().unwrap_or_default()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            user: None,
            password: None,
            dbname: None,
            options: None,
            application_name: None,
            ssl_mode: None,
            host: None,
            hosts: None,
            port: None,
            ports: None,
            connect_timeout: None,
            keepalives: None,
            keepalives_idle: None,
            target_session_attrs: None,
            channel_binding: None,
            pool: None,
        }
    }
}
