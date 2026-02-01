/*
 *
 *  *
 *  *      Copyright (c) 2018-2025, SnackCloud All rights reserved.
 *  *
 *  *   Redistribution and use in source and binary forms, with or without
 *  *   modification, are permitted provided that the following conditions are met:
 *  *
 *  *   Redistributions of source code must retain the above copyright notice,
 *  *   this list of conditions and the following disclaimer.
 *  *   Redistributions in binary form must reproduce the above copyright
 *  *   notice, this list of conditions and the following disclaimer in the
 *  *   documentation and/or other materials provided with the distribution.
 *  *   Neither the name of the www.snackcloud.cn developer nor the names of its
 *  *   contributors may be used to endorse or promote products derived from
 *  *   this software without specific prior written permission.
 *  *   Author: SnackCloud
 *  *
 *  
 */
use deadpool::managed::Metrics;
use deadpool::Runtime;
use lru::LruCache;
use redis::aio::ConnectionLike as AsyncConnectionLike;
use redis::cluster::{ClusterClient, ClusterClientBuilder};
use redis::cluster_async::ClusterConnection;
use redis::sentinel::{SentinelClient, SentinelNodeConnectionInfo, SentinelServerType};
use redis::{Client, ConnectionAddr, ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo, TlsMode};
use std::num::NonZeroUsize;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as TokioMutex, RwLock};
use tokio::time::timeout;

use crate::config::RedissonConfig;
use crate::errors::{RedissonError, RedissonResult};
use crate::{thread_id_to_u64, CachedConnection, CircuitBreaker, ConnectionMode, ConnectionStats, ConnectionType, DetailedConnectionStats};

// Asynchronous connection pool type alias
type AsyncRedisPool = deadpool::managed::Pool<AsyncSingleRedisConnectionManager>;
type AsyncClusterPool = deadpool::managed::Pool<AsyncClusterConnectionManager>;
type AsyncSentinelPool = deadpool::managed::Pool<AsyncSentinelConnectionManager>;

// Asynchronous connection enumeration
pub enum AsyncRedisConnection {
    Single(deadpool::managed::Object<AsyncSingleRedisConnectionManager>),
    Cluster(deadpool::managed::Object<AsyncClusterConnectionManager>),
    Sentinel(deadpool::managed::Object<AsyncSentinelConnectionManager>),
}

// Implement AsyncConnectionLike for asynchronous connections
impl AsyncConnectionLike for AsyncRedisConnection {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        match self {
            AsyncRedisConnection::Single(conn) => conn.req_packed_command(cmd),
            AsyncRedisConnection::Cluster(conn) => conn.req_packed_command(cmd),
            AsyncRedisConnection::Sentinel(conn) => conn.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        match self {
            AsyncRedisConnection::Single(conn) => conn.req_packed_commands(cmd, offset, count),
            AsyncRedisConnection::Cluster(conn) => conn.req_packed_commands(cmd, offset, count),
            AsyncRedisConnection::Sentinel(conn) => conn.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            AsyncRedisConnection::Single(conn) => conn.get_db(),
            AsyncRedisConnection::Cluster(conn) => conn.get_db(),
            AsyncRedisConnection::Sentinel(conn) => conn.get_db(),
        }
    }
}

// Asynchronous connection manager
pub struct AsyncSingleRedisConnectionManager {
    client: Client,
    config: RedissonConfig,
}

#[async_trait::async_trait]
impl deadpool::managed::Manager for AsyncSingleRedisConnectionManager {
    type Type = redis::aio::MultiplexedConnection;
    type Error = redis::RedisError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        // Setting connection parameters
        if let Some(db) = self.config.database {
            redis::cmd("SELECT")
                .arg(db)
                .query_async::<()>(&mut conn)
                .await?;
        }

        Ok(conn)
    }

    async fn recycle(&self, conn: &mut Self::Type, _metrics: &Metrics) -> deadpool::managed::RecycleResult<Self::Error> {
        // Check that the connection is still valid
        match redis::cmd("PING").query_async::<String>(conn).await {
            Ok(pong) if pong == "PONG" => Ok(()),
            Ok(_) => Err(deadpool::managed::RecycleError::Message("Invalid PONG response".into())),
            Err(e) => Err(deadpool::managed::RecycleError::Backend(e)),
        }
    }
}

// Asynchronous cluster connection manager
pub struct AsyncClusterConnectionManager {
    client: ClusterClient,
}

#[async_trait::async_trait]
impl deadpool::managed::Manager for AsyncClusterConnectionManager {
    type Type = ClusterConnection;
    type Error = redis::RedisError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        self.client.get_async_connection().await
    }

    async fn recycle(&self, conn: &mut Self::Type, _metrics: &Metrics) -> deadpool::managed::RecycleResult<Self::Error> {
        match redis::cmd("PING").query_async::<String>(conn).await {
            Ok(pong) if pong == "PONG" => Ok(()),
            Ok(_) => Err(deadpool::managed::RecycleError::Message("Invalid PONG response".into())),
            Err(e) => Err(deadpool::managed::RecycleError::Backend(e)),
        }
    }
}

// Asynchronous Sentinel connection manager
pub struct AsyncSentinelConnectionManager {
    client: TokioMutex<SentinelClient>,
}

#[async_trait::async_trait]
impl deadpool::managed::Manager for AsyncSentinelConnectionManager {
    type Type = redis::aio::MultiplexedConnection;
    type Error = redis::RedisError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let mut client = self.client.lock().await;
        client.get_async_connection().await
    }

    async fn recycle(&self, conn: &mut Self::Type, _metrics: &Metrics) -> deadpool::managed::RecycleResult<Self::Error> {
        match redis::cmd("PING").query_async::<String>(conn).await {
            Ok(pong) if pong == "PONG" => Ok(()),
            Ok(_) => Err(deadpool::managed::RecycleError::Message("Invalid PONG response".into())),
            Err(e) => Err(deadpool::managed::RecycleError::Backend(e)),
        }
    }
}

// The main asynchronous connection manager
pub struct AsyncRedisConnectionManager {
    config: RedissonConfig,
    // Single machine mode
    single_pool: Option<Arc<AsyncRedisPool>>,
    // Cluster mode
    cluster_pool: Option<Arc<AsyncClusterPool>>,
    // Sentinel mode
    sentinel_pool: Option<Arc<AsyncSentinelPool>>,
    // Connection caching
    connection_cache: Arc<TokioMutex<LruCache<String, CachedConnection>>>,
    // Statistical information
    stats: Arc<RwLock<ConnectionStats>>,
    // Circuit breaker
    circuit_breaker: Arc<RwLock<CircuitBreaker>>,
    // Alternate connection pool
    backup_pools: Arc<TokioMutex<Vec<AsyncBackupPool>>>,
    // Is it closed?
    is_closed: Arc<AtomicBool>,
}

struct AsyncBackupPool {
    pool: Arc<AsyncRedisPool>,
    created_at: Instant,
    is_healthy: bool,
}

impl AsyncRedisConnectionManager {
    pub async fn new(config: &RedissonConfig) -> RedissonResult<Self> {
        // Validate configuration
        Self::validate_config(config)?;

        match &config.connection_mode {
            ConnectionMode::SingleServer { .. } => {
                let pool = Self::create_single_pool(config).await?;

                Ok(Self {
                    config: config.clone(),
                    single_pool: Some(Arc::new(pool)),
                    cluster_pool: None,
                    sentinel_pool: None,
                    connection_cache: Arc::new(TokioMutex::new(LruCache::new(
                        NonZeroUsize::new(config.pool_size as usize * 2).unwrap()
                    ))),
                    stats: Arc::new(RwLock::new(ConnectionStats::new())),
                    circuit_breaker: Arc::new(RwLock::new(CircuitBreaker::new(5, Duration::from_secs(30)))),
                    backup_pools: Arc::new(TokioMutex::new(Vec::new())),
                    is_closed: Arc::new(AtomicBool::new(false)),
                })
            }
            ConnectionMode::Cluster { .. } => {
                let pool = Self::create_cluster_pool(config).await?;

                Ok(Self {
                    config: config.clone(),
                    single_pool: None,
                    cluster_pool: Some(Arc::new(pool)),
                    sentinel_pool: None,
                    connection_cache: Arc::new(TokioMutex::new(LruCache::new(
                        NonZeroUsize::new(config.pool_size as usize * 2).unwrap()
                    ))),
                    stats: Arc::new(RwLock::new(ConnectionStats::new())),
                    circuit_breaker: Arc::new(RwLock::new(CircuitBreaker::new(5, Duration::from_secs(30)))),
                    backup_pools: Arc::new(TokioMutex::new(Vec::new())),
                    is_closed: Arc::new(AtomicBool::new(false)),
                })
            }
            ConnectionMode::Sentinel { .. } => {
                let pool = Self::create_sentinel_pool(config).await?;

                Ok(Self {
                    config: config.clone(),
                    single_pool: None,
                    cluster_pool: None,
                    sentinel_pool: Some(Arc::new(pool)),
                    connection_cache: Arc::new(TokioMutex::new(LruCache::new(
                        NonZeroUsize::new(config.pool_size as usize * 2).unwrap()
                    ))),
                    stats: Arc::new(RwLock::new(ConnectionStats::new())),
                    circuit_breaker: Arc::new(RwLock::new(CircuitBreaker::new(5, Duration::from_secs(30)))),
                    backup_pools: Arc::new(TokioMutex::new(Vec::new())),
                    is_closed: Arc::new(AtomicBool::new(false)),
                })
            }
        }
    }

    fn validate_config(config: &RedissonConfig) -> RedissonResult<()> {
        if config.pool_size == 0 {
            return Err(RedissonError::ConfigError("Pool size cannot be zero".to_string()));
        }

        match &config.connection_mode {
            ConnectionMode::SingleServer { url, host, port } => {
                if url.is_empty() && (host.is_none() || port.is_none()) {
                    return Err(RedissonError::ConfigError(
                        "Single server mode requires either URL or host/port".to_string()
                    ));
                }
            }
            ConnectionMode::Cluster { node_addresses } => {
                if node_addresses.is_empty() {
                    return Err(RedissonError::ConfigError(
                        "Cluster mode requires at least one node address".to_string()
                    ));
                }
            }
            ConnectionMode::Sentinel { master_name, sentinel_addresses } => {
                if master_name.is_empty() {
                    return Err(RedissonError::ConfigError(
                        "Sentinel mode requires master name".to_string()
                    ));
                }
                if sentinel_addresses.is_empty() {
                    return Err(RedissonError::ConfigError(
                        "Sentinel mode requires at least one sentinel address".to_string()
                    ));
                }
            }
        }

        Ok(())
    }

    async fn create_single_pool(config: &RedissonConfig) -> RedissonResult<AsyncRedisPool> {
        let manager = Self::create_single_manager(config)?;

        let pool = deadpool::managed::Pool::builder(manager)
            .max_size(config.pool_size as usize)
            .timeouts(deadpool::managed::Timeouts {
                wait: Some(config.connection_timeout),
                create: Some(config.connection_timeout),
                recycle: Some(Duration::from_secs(5)),
            })
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| RedissonError::PoolError(e.to_string()))?;

        Ok(pool)
    }

    async fn create_cluster_pool(config: &RedissonConfig) -> RedissonResult<AsyncClusterPool> {
        let manager = Self::create_cluster_manager(config)?;

        let pool = deadpool::managed::Pool::builder(manager)
            .max_size(config.pool_size as usize)
            .timeouts(deadpool::managed::Timeouts {
                wait: Some(config.connection_timeout),
                create: Some(config.connection_timeout),
                recycle: Some(Duration::from_secs(5)),
            })
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| RedissonError::PoolError(e.to_string()))?;

        Ok(pool)
    }

    async fn create_sentinel_pool(config: &RedissonConfig) -> RedissonResult<AsyncSentinelPool> {
        let manager = Self::create_sentinel_manager(config)?;

        let pool = deadpool::managed::Pool::builder(manager)
            .max_size(config.pool_size as usize)
            .timeouts(deadpool::managed::Timeouts {
                wait: Some(config.connection_timeout),
                create: Some(config.connection_timeout),
                recycle: Some(Duration::from_secs(5)),
            })
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| RedissonError::PoolError(e.to_string()))?;

        Ok(pool)
    }

    fn create_single_manager(config: &RedissonConfig) -> RedissonResult<AsyncSingleRedisConnectionManager> {
        let client = Self::create_single_client(config)?;
        Ok(AsyncSingleRedisConnectionManager { client, config: config.clone() })
    }

    fn create_cluster_manager(config: &RedissonConfig) -> RedissonResult<AsyncClusterConnectionManager> {
        let client = Self::create_cluster_client(config)?;
        Ok(AsyncClusterConnectionManager { client })
    }

    fn create_sentinel_manager(config: &RedissonConfig) -> RedissonResult<AsyncSentinelConnectionManager> {
        let client = Self::create_sentinel_client(config)?;
        Ok(AsyncSentinelConnectionManager { client: TokioMutex::new(client) })
    }

    fn create_single_client(config: &RedissonConfig) -> RedissonResult<Client> {
        match &config.connection_mode {
            ConnectionMode::SingleServer { url, host, port } => {
                let conn_info = if !url.is_empty() {
                    Self::build_connection_info_from_url(url, config)?
                } else if let (Some(host), Some(port)) = (host, port) {
                    Self::build_connection_info_from_host_port(host, *port, config)?
                } else {
                    return Err(RedissonError::ConfigError(
                        "Single server mode requires either URL or host/port".to_string()
                    ));
                };

                Client::open(conn_info)
                    .map_err(|e| RedissonError::PoolError(e.to_string()))
            }
            _ => Err(RedissonError::ConfigError("Expected single server mode".to_string())),
        }
    }

    fn create_cluster_client(config: &RedissonConfig) -> RedissonResult<ClusterClient> {
        match &config.connection_mode {
            ConnectionMode::Cluster { node_addresses } => {
                let mut builder = ClusterClientBuilder::new(node_addresses.clone());

                if let Some(username) = &config.username {
                    builder = builder.username(username.clone());
                }
                if let Some(password) = &config.password {
                    builder = builder.password(password.clone());
                }

                builder = builder.response_timeout(config.response_timeout);
                builder = builder.connection_timeout(config.connection_timeout);

                if config.ssl {
                    builder = builder.tls(TlsMode::Secure);
                }

                builder.build()
                    .map_err(|e| RedissonError::PoolError(e.to_string()))
            }
            _ => Err(RedissonError::ConfigError("Expected cluster mode".to_string())),
        }
    }

    fn create_sentinel_client(config: &RedissonConfig) -> RedissonResult<SentinelClient> {
        match &config.connection_mode {
            ConnectionMode::Sentinel { master_name, sentinel_addresses } => {
                let mut sentinel_node_connection_info = SentinelNodeConnectionInfo::default();
                let mut master_connection_info = RedisConnectionInfo::default();

                if let Some(username) = &config.username {
                    master_connection_info = master_connection_info.set_username(username.clone());
                }
                if let Some(password) = &config.password {
                    master_connection_info = master_connection_info.set_password(password.clone());
                }

                sentinel_node_connection_info = sentinel_node_connection_info.set_redis_connection_info(master_connection_info);

                let client = SentinelClient::build(
                    sentinel_addresses.clone(),
                    master_name.clone(),
                    sentinel_node_connection_info.into(),
                    SentinelServerType::Master,
                )
                    .map_err(|e| RedissonError::PoolError(e.to_string()))?;

                Ok(client)
            }
            _ => Err(RedissonError::ConfigError("Expected sentinel mode".to_string())),
        }
    }

    fn build_connection_info_from_url(url: &str, config: &RedissonConfig) -> RedissonResult<ConnectionInfo> {
        let mut conn_info: ConnectionInfo = url
            .into_connection_info()
            .map_err(|e| RedissonError::PoolError(e.to_string()))?;

        let mut redis_connection = RedisConnectionInfo::default();
        if let Some(username) = &config.username {
            redis_connection = redis_connection.set_username(username);
        }
        if let Some(password) = &config.password {
            redis_connection = redis_connection.set_password(password);
        }
        if let Some(db) = config.database {
            redis_connection = redis_connection.set_db(db);
        }
        conn_info = conn_info.set_redis_settings(redis_connection);
        Ok(conn_info)
    }

    fn build_connection_info_from_host_port(host: &str, port: u16, config: &RedissonConfig) -> RedissonResult<ConnectionInfo> {
        let addr = if config.ssl {
            ConnectionAddr::TcpTls {
                host: host.to_string(),
                port,
                insecure: false,
                tls_params: None,
            }
        } else {
            ConnectionAddr::Tcp(host.to_string(), port)
        };

        let mut redis_info = RedisConnectionInfo::default()
            .set_db(config.database.unwrap_or(0));
        if let Some(username) = &config.username {
            redis_info = redis_info.set_username(username);
        }
        if let Some(password) = &config.password {
            redis_info = redis_info.set_password(password);
        }

        let mut connection = addr.into_connection_info()?;
        connection = connection.set_redis_settings(redis_info);
        Ok(connection)
    }

    pub async fn get_connection(&self) -> RedissonResult<AsyncRedisConnection> {
        if self.is_closed.load(Ordering::Acquire) {
            return Err(RedissonError::PoolError("Connection manager is closed".to_string()));
        }

        let start = Instant::now();

        // Check the circuit breaker
        {
            let mut circuit_breaker = self.circuit_breaker.write().await;
            if !circuit_breaker.allow_request() {
                return Err(RedissonError::CircuitBreakerError("circuit breaker error".to_string()));
            }
        }

        // Try fetching from the cache
        let thread_id = thread_id_to_u64();
        let cache_key = format!("conn:{}", thread_id);

        // Checking the cache
        {
            let mut cache = self.connection_cache.lock().await;
            if let Some(cached_conn) = cache.pop(&cache_key) {
                if Instant::now().duration_since(cached_conn.last_used) < Duration::from_secs(30) {
                    return match cached_conn.connection_type {
                        ConnectionType::Single => {
                            if let Some(pool) = &self.single_pool {
                                self.get_single_connection(pool).await
                            } else {
                                self.try_primary_connection().await
                            }
                        }
                        ConnectionType::Cluster => {
                            if let Some(pool) = &self.cluster_pool {
                                self.get_cluster_connection(pool).await
                            } else {
                                self.try_primary_connection().await
                            }
                        }
                        ConnectionType::Sentinel => {
                            if let Some(pool) = &self.sentinel_pool {
                                self.get_sentinel_connection(pool).await
                            } else {
                                self.try_primary_connection().await
                            }
                        }
                    };
                }
            }
        }

        // From the main connection pool
        let result = self.try_primary_connection().await;

        // Logging statistics
        let elapsed = start.elapsed();
        {
            let mut stats = self.stats.write().await;
            if result.is_ok() {
                stats.record_success(elapsed);

                // Updating the cache
                let mut cache = self.connection_cache.lock().await;
                let connection_type = match &self.config.connection_mode {
                    ConnectionMode::SingleServer { .. } => ConnectionType::Single,
                    ConnectionMode::Cluster { .. } => ConnectionType::Cluster,
                    ConnectionMode::Sentinel { .. } => ConnectionType::Sentinel,
                };
                cache.put(cache_key, CachedConnection {
                    last_used: Instant::now(),
                    connection_type,
                });

                // Update the circuit breaker
                let mut circuit_breaker = self.circuit_breaker.write().await;
                circuit_breaker.record_success();
            } else {
                stats.record_failure(elapsed);

                // Update the circuit breaker
                let mut circuit_breaker = self.circuit_breaker.write().await;
                circuit_breaker.record_failure();

                // Try alternate connections
                return self.try_backup_connections().await;
            }
        }

        result
    }

    async fn try_primary_connection(&self) -> RedissonResult<AsyncRedisConnection> {
        match &self.config.connection_mode {
            ConnectionMode::SingleServer { .. } => {
                if let Some(pool) = &self.single_pool {
                    self.get_single_connection(pool).await
                } else {
                    Err(RedissonError::PoolError("no connection available".to_string()))
                }
            }
            ConnectionMode::Cluster { .. } => {
                if let Some(pool) = &self.cluster_pool {
                    self.get_cluster_connection(pool).await
                } else {
                    Err(RedissonError::PoolError("no connection available".to_string()))
                }
            }
            ConnectionMode::Sentinel { .. } => {
                if let Some(pool) = &self.sentinel_pool {
                    self.get_sentinel_connection(pool).await
                } else {
                    Err(RedissonError::PoolError("no connection available".to_string()))
                }
            }
        }
    }

    async fn get_single_connection(&self, pool: &Arc<AsyncRedisPool>) -> RedissonResult<AsyncRedisConnection> {
        let conn = timeout(
            self.config.connection_timeout,
            pool.get()
        )
            .await
            .map_err(|_| RedissonError::PoolError("timeout".to_string()))?
            .map_err(|e| RedissonError::PoolError(e.to_string()))?;

        Ok(AsyncRedisConnection::Single(conn))
    }

    async fn get_cluster_connection(&self, pool: &Arc<AsyncClusterPool>) -> RedissonResult<AsyncRedisConnection> {
        let conn = timeout(
            self.config.connection_timeout,
            pool.get()
        )
            .await
            .map_err(|_| RedissonError::PoolError("timeout".to_string()))?
            .map_err(|e| RedissonError::PoolError(e.to_string()))?;

        Ok(AsyncRedisConnection::Cluster(conn))
    }

    async fn get_sentinel_connection(&self, pool: &Arc<AsyncSentinelPool>) -> RedissonResult<AsyncRedisConnection> {
        let conn = timeout(
            self.config.connection_timeout,
            pool.get()
        )
            .await
            .map_err(|_| RedissonError::PoolError("timeout".to_string()))?
            .map_err(|e| RedissonError::PoolError(e.to_string()))?;

        Ok(AsyncRedisConnection::Sentinel(conn))
    }

    async fn try_backup_connections(&self) -> RedissonResult<AsyncRedisConnection> {
        let mut backup_pools = self.backup_pools.lock().await;
        let now = Instant::now();

        // Clean up the pool of unhealthy alternate connections
        backup_pools.retain(|pool| {
            pool.is_healthy && now.duration_since(pool.created_at) < Duration::from_secs(300)
        });

        // Try every alternate connection pool
        for backup_pool in backup_pools.iter_mut() {
            match timeout(
                self.config.connection_timeout,
                backup_pool.pool.get()
            )
                .await {
                Ok(Ok(mut conn)) => {
                    // Health check up
                    let is_healthy = match redis::cmd("PING").query_async::<String>(conn.deref_mut()).await {
                        Ok(pong) => pong == "PONG",
                        Err(_) => false,
                    };

                    if is_healthy {
                        backup_pool.is_healthy = true;
                        return Ok(AsyncRedisConnection::Single(conn));
                    } else {
                        backup_pool.is_healthy = false;
                        continue;
                    }
                }
                _ => {
                    backup_pool.is_healthy = false;
                    continue;
                }
            }
        }

        Err(RedissonError::PoolError("no connection available".to_string()))
    }

    pub async fn add_backup_pool(&self, url: &str) -> RedissonResult<()> {
        let mut config = self.config.clone();
        config.connection_mode = ConnectionMode::SingleServer {
            url: url.to_string(),
            host: None,
            port: None,
        };

        let pool = Self::create_single_pool(&config).await?;

        let mut backup_pools = self.backup_pools.lock().await;
        if backup_pools.len() >= self.config.backup_pool_count as usize {
            backup_pools.remove(0);
        }

        backup_pools.push(AsyncBackupPool {
            pool: Arc::new(pool),
            created_at: Instant::now(),
            is_healthy: true,
        });

        Ok(())
    }

    pub async fn cleanup(&self) {
        // Clearing the cache
        {
            let mut cache = self.connection_cache.lock().await;
            let now = Instant::now();

            let expired_keys: Vec<String> = cache.iter()
                .filter(|(_, cached_conn)| now.duration_since(cached_conn.last_used) > Duration::from_secs(60))
                .map(|(key, _)| key.clone())
                .collect();

            for key in expired_keys {
                cache.pop(&key);
            }
        }

        // Clean up the standby connection pool
        {
            let mut backup_pools = self.backup_pools.lock().await;
            let now = Instant::now();

            backup_pools.retain(|pool| {
                now.duration_since(pool.created_at) < Duration::from_secs(300) || pool.is_healthy
            });
        }
    }

    pub async fn get_stats(&self) -> ConnectionStats {
        self.stats.read().await.clone()
    }

    pub async fn get_detailed_stats(&self) -> DetailedConnectionStats {
        let stats = self.stats.read().await;
        DetailedConnectionStats {
            total_connections_created: stats.total_connections_created(),
            total_connections_reused: stats.total_connections_reused(),
            active_connections: stats.active_connections(),
            peak_connections: stats.peak_connections(),
            avg_wait_time_ms: stats.avg_wait_time_ms(),
            total_operations: stats.total_operations(),
            error_count: stats.error_count(),
            error_rate: stats.error_rate(),
            last_reset: stats.last_reset(),
        }
    }

    pub async fn reset_circuit_breaker(&self) {
        let mut circuit_breaker = self.circuit_breaker.write().await;
        circuit_breaker.reset();
    }

    pub async fn health_check(&self) -> bool {
        match self.try_primary_connection().await {
            Ok(mut conn) => {
                let is_healthy = match redis::cmd("PING").query_async::<String>(&mut conn).await {
                    Ok(pong) => pong == "PONG",
                    Err(_) => false,
                };

                if !is_healthy {
                    let mut circuit_breaker = self.circuit_breaker.write().await;
                    circuit_breaker.record_failure();
                }

                is_healthy
            }
            Err(_) => {
                let mut circuit_breaker = self.circuit_breaker.write().await;
                circuit_breaker.record_failure();
                false
            }
        }
    }

    pub fn close(&self) {
        self.is_closed.store(true, Ordering::Release);
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }
}

// Provides command execution methods for asynchronous connections
impl AsyncRedisConnection {
    
    pub async fn execute_command<T: redis::FromRedisValue>(&mut self, cmd: &mut redis::Cmd) -> RedissonResult<T> {
        match self {
            AsyncRedisConnection::Single(conn) => {
                let conn = conn.deref_mut();
                cmd.query_async(conn)
                    .await
                    .map_err(|e| RedissonError::RedisError(e))
            }
            AsyncRedisConnection::Cluster(conn) => {
                let conn = conn.deref_mut();
                cmd.query_async(conn)
                    .await
                    .map_err(|e| RedissonError::RedisError(e))
            }
            AsyncRedisConnection::Sentinel(conn) => {
                let conn = conn.deref_mut();
                cmd.query_async(conn)
                    .await
                    .map_err(|e| RedissonError::RedisError(e))
            }
        }
    }

    pub async fn execute_pipeline<T: redis::FromRedisValue>(&mut self, pipeline: &redis::Pipeline) -> RedissonResult<Vec<T>> {
        match self {
            AsyncRedisConnection::Single(conn) => {
                let conn = conn.deref_mut();
                pipeline.query_async(conn)
                    .await
                    .map_err(|e| RedissonError::RedisError(e))
            }
            AsyncRedisConnection::Cluster(conn) => {
                let conn = conn.deref_mut();
                pipeline.query_async(conn)
                    .await
                    .map_err(|e| RedissonError::RedisError(e))
            }
            AsyncRedisConnection::Sentinel(conn) => {
                let conn = conn.deref_mut();
                pipeline.query_async(conn)
                    .await
                    .map_err(|e| RedissonError::RedisError(e))
            }
        }
    }

    pub async fn execute_script<T: redis::FromRedisValue>(
        &mut self,
        script: &redis::Script,
        keys: &[&str],
        args: &[&str],
    ) -> RedissonResult<T> {
        let mut invocation = &mut script.key(keys);
        for arg in args {
            invocation = invocation.arg(arg);
        }

        match self {
            AsyncRedisConnection::Single(conn) => {
                let conn = conn.deref_mut();
                invocation.invoke_async(conn)
                    .await
                    .map_err(|e| RedissonError::RedisError(e))
            }
            AsyncRedisConnection::Cluster(conn) => {
                let conn = conn.deref_mut();
                invocation.invoke_async(conn)
                    .await
                    .map_err(|e| RedissonError::RedisError(e))
            }
            AsyncRedisConnection::Sentinel(conn) => {
                let conn = conn.deref_mut();
                invocation.invoke_async(conn)
                    .await
                    .map_err(|e| RedissonError::RedisError(e))
            }
        }
    }
}

// Synchronous wrappers for use in an asynchronous environment
pub struct AsyncSyncRedisConnectionManager {
    pub inner: Arc<AsyncRedisConnectionManager>,
}

impl AsyncSyncRedisConnectionManager {
    pub async fn new(config: &RedissonConfig) -> RedissonResult<Self> {
        let manager = AsyncRedisConnectionManager::new(config).await?;
        Ok(Self {
            inner: Arc::new(manager),
        })
    }

    pub async fn get_connection(&self) -> RedissonResult<AsyncRedisConnection> {
        self.inner.get_connection().await
    }

    pub async fn get_stats(&self) -> ConnectionStats {
        self.inner.get_stats().await
    }

    pub async fn get_detailed_stats(&self) -> DetailedConnectionStats {
        self.inner.get_detailed_stats().await
    }

    pub async fn health_check(&self) -> bool {
        self.inner.health_check().await
    }

    pub async fn cleanup(&self) {
        self.inner.cleanup().await
    }

    pub async fn reset_circuit_breaker(&self) {
        self.inner.reset_circuit_breaker().await
    }

    pub async fn add_backup_pool(&self, url: &str) -> RedissonResult<()> {
        self.inner.add_backup_pool(url).await
    }

    pub fn close(&self) {
        self.inner.close()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}