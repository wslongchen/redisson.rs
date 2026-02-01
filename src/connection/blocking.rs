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
use crate::config::RedissonConfig;
use crate::errors::{RedissonError, RedissonResult};
use crate::{thread_id_to_u64, CachedConnection, CircuitBreaker, ConnectionMode, ConnectionStats, ConnectionType, DetailedConnectionStats};
use lru::LruCache;
use parking_lot::RwLock;
use r2d2::{Pool, PooledConnection};
use redis::cluster::{ClusterClient, ClusterClientBuilder};
use redis::sentinel::{LockedSentinelClient, SentinelClient, SentinelNodeConnectionInfo, SentinelServerType};
use redis::{Client, ConnectionAddr, ConnectionInfo, ConnectionLike, IntoConnectionInfo, RedisConnectionInfo, ScriptInvocation, TlsMode};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type RedisPool = Pool<Client>;
type ClusterRedisPool = Pool<ClusterClient>;
type SentinelRedisPool = Pool<LockedSentinelClient>;

pub enum RedisConnection {
    Single(PooledConnection<Client>),
    Cluster(PooledConnection<ClusterClient>),
    Sentinel(PooledConnection<LockedSentinelClient>),
}

// Implement the ConnectionLike trait for uniform usage
impl ConnectionLike for RedisConnection {
    fn req_packed_command(&mut self, cmd: &[u8]) -> redis::RedisResult<redis::Value> {
        match self {
            RedisConnection::Single(conn) => conn.req_packed_command(cmd),
            RedisConnection::Cluster(conn) => conn.req_packed_command(cmd),
            RedisConnection::Sentinel(conn) => conn.req_packed_command(cmd),
        }
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> redis::RedisResult<Vec<redis::Value>> {
        match self {
            RedisConnection::Single(conn) => conn.req_packed_commands(cmd, offset, count),
            RedisConnection::Cluster(conn) => conn.req_packed_commands(cmd, offset, count),
            RedisConnection::Sentinel(conn) => conn.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            RedisConnection::Single(conn) => conn.get_db(),
            RedisConnection::Cluster(conn) => conn.get_db(),
            RedisConnection::Sentinel(conn) => conn.get_db(),
        }
    }

    fn check_connection(&mut self) -> bool {
        match self {
            RedisConnection::Single(conn) => conn.check_connection(),
            RedisConnection::Cluster(conn) => conn.check_connection(),
            RedisConnection::Sentinel(conn) => conn.check_connection(),
        }
    }

    fn is_open(&self) -> bool {
        match self {
            RedisConnection::Single(conn) => conn.is_open(),
            RedisConnection::Cluster(conn) => conn.is_open(),
            RedisConnection::Sentinel(conn) => conn.is_open(),
        }
    }
}


// Synchronize the connection manager
pub struct SyncRedisConnectionManager {
    inner: Arc<RedisConnectionManager>,
}

impl SyncRedisConnectionManager {
    pub fn new(config: &RedissonConfig) -> RedissonResult<Self> {
        let manager = RedisConnectionManager::new(config)?;
        Ok(Self {
            inner: Arc::new(manager),
        })
    }

    pub fn get_connection(&self) -> RedissonResult<RedisConnection> {
        self.inner.get_connection()
    }

    pub fn get_stats(&self) -> ConnectionStats {
        self.inner.get_stats()
    }

    pub fn health_check(&self) -> bool {
        self.inner.health_check()
    }

    pub fn cleanup(&self) {
        self.inner.cleanup()
    }

    pub fn reset_circuit_breaker(&self) {
        self.inner.reset_circuit_breaker()
    }

    pub fn get_detailed_stats(&self) -> DetailedConnectionStats {
        self.inner.get_detailed_stats()
    }
}


// The main connection manager, which supports multiple modes
pub struct RedisConnectionManager {
    config: RedissonConfig,
    // Single machine mode
    single_pool: Option<Arc<RedisPool>>,
    // Cluster mode
    cluster_pool: Option<Arc<ClusterRedisPool>>,
    // Sentinel mode
    sentinel_pool: Option<Arc<SentinelRedisPool>>,
    // Connection cache (cache connections by thread)
    connection_cache: Arc<RwLock<LruCache<String, CachedConnection>>>,
    // Statistical information
    stats: Arc<RwLock<ConnectionStats>>,
    // Circuit breaker
    circuit_breaker: Arc<RwLock<CircuitBreaker>>,
    // Standby connection pool (for failover)
    backup_pools: Arc<Mutex<Vec<BackupPool>>>,
    // Is it closed?
    is_closed: Arc<AtomicBool>,
}

struct BackupPool {
    pool: Arc<RedisPool>,
    created_at: Instant,
    is_healthy: bool,
}


impl RedisConnectionManager {
    pub fn new(config: &RedissonConfig) -> RedissonResult<Self> {
        // Validate configuration
        Self::validate_config(config)?;

        match &config.connection_mode {
            ConnectionMode::SingleServer { .. } => {
                let pool = Self::create_single_pool(config)?;

                Ok(Self {
                    config: config.clone(),
                    single_pool: Some(Arc::new(pool)),
                    cluster_pool: None,
                    sentinel_pool: None,
                    connection_cache: Arc::new(RwLock::new(LruCache::new(
                        NonZeroUsize::new(config.pool_size as usize * 2).unwrap()
                    ))),
                    stats: Arc::new(RwLock::new(ConnectionStats::new())),
                    circuit_breaker: Arc::new(RwLock::new(CircuitBreaker::new(
                        5, // Failure threshold
                        Duration::from_secs(30) // Recovery time
                    ))),
                    backup_pools: Arc::new(Mutex::new(Vec::new())),
                    is_closed: Arc::new(AtomicBool::new(false)),
                })
            }
            ConnectionMode::Cluster { .. } => {
                let pool = Self::create_cluster_pool(config)?;

                Ok(Self {
                    config: config.clone(),
                    single_pool: None,
                    cluster_pool: Some(Arc::new(pool)),
                    sentinel_pool: None,
                    connection_cache: Arc::new(RwLock::new(LruCache::new(
                        NonZeroUsize::new(config.pool_size as usize * 2).unwrap()
                    ))),
                    stats: Arc::new(RwLock::new(ConnectionStats::new())),
                    circuit_breaker: Arc::new(RwLock::new(CircuitBreaker::new(5, Duration::from_secs(30)))),
                    backup_pools: Arc::new(Mutex::new(Vec::new())),
                    is_closed: Arc::new(AtomicBool::new(false)),
                })
            }
            ConnectionMode::Sentinel { .. } => {
                let pool = Self::create_sentinel_pool(config)?;

                Ok(Self {
                    config: config.clone(),
                    single_pool: None,
                    cluster_pool: None,
                    sentinel_pool: Some(Arc::new(pool)),
                    connection_cache: Arc::new(RwLock::new(LruCache::new(
                        NonZeroUsize::new(config.pool_size as usize * 2).unwrap()
                    ))),
                    stats: Arc::new(RwLock::new(ConnectionStats::new())),
                    circuit_breaker: Arc::new(RwLock::new(CircuitBreaker::new(5, Duration::from_secs(30)))),
                    backup_pools: Arc::new(Mutex::new(Vec::new())),
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

    fn create_single_pool(config: &RedissonConfig) -> RedissonResult<RedisPool> {
        let client = Self::create_single_client(config)?;
        Self::build_pool(client, config)
    }

    fn create_cluster_pool(config: &RedissonConfig) -> RedissonResult<ClusterRedisPool> {
        let client = Self::create_cluster_client(config)?;
        Self::build_cluster_pool(client, config)
    }

    fn create_sentinel_pool(config: &RedissonConfig) -> RedissonResult<SentinelRedisPool> {
        let client = Self::create_sentinel_client(config)?;
        Self::build_sentinel_pool(client, config)
    }

    fn create_single_client(config: &RedissonConfig) -> RedissonResult<Client> {
        match &config.connection_mode {
            ConnectionMode::SingleServer { url, host, port } => {
                // Building connection information
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

    fn build_connection_info_from_url(url: &str, config: &RedissonConfig) -> RedissonResult<ConnectionInfo> {
        let mut conn_info: ConnectionInfo = url
            .into_connection_info()
            .map_err(|e| RedissonError::PoolError(e.to_string()))?;

        // Setting authentication information
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

        let mut redis_info =RedisConnectionInfo::default()
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

    fn create_cluster_client(config: &RedissonConfig) -> RedissonResult<ClusterClient> {
        match &config.connection_mode {
            ConnectionMode::Cluster { node_addresses } => {
                let mut builder = ClusterClientBuilder::new(node_addresses.clone());

                // Setting authentication information
                if let Some(username) = &config.username {
                    builder = builder.username(username.clone());
                }
                if let Some(password) = &config.password {
                    builder = builder.password(password.clone());
                }

                // Set the timeout
                builder = builder.response_timeout(config.response_timeout);
                builder = builder.connection_timeout(config.connection_timeout);

                // Setting up SSL
                if config.ssl {
                    builder = builder.tls(TlsMode::Secure);
                }

                builder.build()
                    .map_err(|e| RedissonError::PoolError(e.to_string()))
            }
            _ => Err(RedissonError::ConfigError("Expected cluster mode".to_string())),
        }
    }

    fn create_sentinel_client(config: &RedissonConfig) -> RedissonResult<LockedSentinelClient> {
        match &config.connection_mode {
            ConnectionMode::Sentinel { master_name, sentinel_addresses } => {
                let mut sentinel_node_connection_info = SentinelNodeConnectionInfo::default();
                let mut master_connection_info = RedisConnectionInfo::default();
                // Setting authentication information
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

                Ok(LockedSentinelClient::new(client))
            }
            _ => Err(RedissonError::ConfigError("Expected sentinel mode".to_string())),
        }
    }

    fn build_pool<T>(client: T, config: &RedissonConfig) -> RedissonResult<Pool<T>>
    where
        T: r2d2::ManageConnection,
        T::Error: std::fmt::Display,
    {
        Pool::builder()
            .max_size(config.pool_size)
            .min_idle(Some(config.pool_size / 2))
            .connection_timeout(config.connection_timeout)
            .max_lifetime(Some(config.max_lifetime))
            .idle_timeout(Some(config.idle_timeout))
            .test_on_check_out(true)
            .build(client)
            .map_err(|e| RedissonError::PoolError(e.to_string()))
    }

    fn build_cluster_pool(client: ClusterClient, config: &RedissonConfig) -> RedissonResult<ClusterRedisPool> {
        Self::build_pool(client, config)
    }

    fn build_sentinel_pool(client: LockedSentinelClient, config: &RedissonConfig) -> RedissonResult<SentinelRedisPool> {
        Self::build_pool(client, config)
    }

    pub fn get_connection(&self) -> RedissonResult<RedisConnection> {
        if self.is_closed.load(Ordering::Acquire) {
            return Err(RedissonError::PoolError("Connection manager is closed".to_string()));
        }

        let start = Instant::now();

        // Check the circuit breaker
        {
            let mut circuit_breaker = self.circuit_breaker.write();
            if !circuit_breaker.allow_request() {
                return Err(RedissonError::CircuitBreakerError("circuit breaker error".to_string()));
            }
        }

        // Try fetching from the cache
        let thread_id = thread_id_to_u64();
        let cache_key = format!("conn:{}", thread_id);

        // Checking the cache
        {
            let mut cache = self.connection_cache.write();
            if let Some(cached_conn) = cache.pop(&cache_key) {
                // Check if the cache is valid (used within 30 seconds)
                if Instant::now().duration_since(cached_conn.last_used) < Duration::from_secs(30) {
                    // Returns the cached connection
                    return match cached_conn.connection_type {
                        ConnectionType::Single => {
                            if let Some(pool) = &self.single_pool {
                                self.get_single_connection(pool)
                            } else {
                                self.try_primary_connection()
                            }
                        }
                        ConnectionType::Cluster => {
                            if let Some(pool) = &self.cluster_pool {
                                self.get_cluster_connection(pool)
                            } else {
                                self.try_primary_connection()
                            }
                        }
                        ConnectionType::Sentinel => {
                            if let Some(pool) = &self.sentinel_pool {
                                self.get_sentinel_connection(pool)
                            } else {
                                self.try_primary_connection()
                            }
                        }
                    };
                }
            }
        }

        // From the main connection pool
        let result = self.try_primary_connection();

        // Logging statistics
        let elapsed = start.elapsed();
        {
            let mut stats = self.stats.write();
            if result.is_ok() {
                stats.record_success(elapsed);

                // Updating the cache
                let mut cache = self.connection_cache.write();
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
                let mut circuit_breaker = self.circuit_breaker.write();
                circuit_breaker.record_success();
            } else {
                stats.record_failure(elapsed);

                // Update the circuit breaker
                let mut circuit_breaker = self.circuit_breaker.write();
                circuit_breaker.record_failure();

                // Try alternate connections
                return self.try_backup_connections();
            }
        }

        result
    }

    pub fn get_detailed_stats(&self) -> DetailedConnectionStats {
        let stats = self.stats.read();
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

    fn try_primary_connection(&self) -> RedissonResult<RedisConnection> {
        match &self.config.connection_mode {
            ConnectionMode::SingleServer { .. } => {
                if let Some(pool) = &self.single_pool {
                    self.get_single_connection(pool)
                } else {
                    Err(RedissonError::PoolError("no connection available".to_string()))
                }
            }
            ConnectionMode::Cluster { .. } => {
                if let Some(pool) = &self.cluster_pool {
                    self.get_cluster_connection(pool)
                } else {
                    Err(RedissonError::PoolError("no connection available".to_string()))
                }
            }
            ConnectionMode::Sentinel { .. } => {
                if let Some(pool) = &self.sentinel_pool {
                    self.get_sentinel_connection(pool)
                } else {
                    Err(RedissonError::PoolError("no connection available".to_string()))
                }
            }
        }
    }

    fn get_single_connection(&self, pool: &Arc<RedisPool>) -> RedissonResult<RedisConnection> {
        let conn = pool.get()
            .map_err(|e| RedissonError::PoolError(e.to_string()))?;
        // Get the actual Redis connection from the client
        Ok(RedisConnection::Single(conn))
    }

    fn get_cluster_connection(&self, pool: &Arc<ClusterRedisPool>) -> RedissonResult<RedisConnection> {
        let conn = pool.get()
            .map_err(|e| RedissonError::PoolError(e.to_string()))?;

        Ok(RedisConnection::Cluster(conn))
    }

    fn get_sentinel_connection(&self, pool: &Arc<SentinelRedisPool>) -> RedissonResult<RedisConnection> {
        let conn = pool.get()
            .map_err(|e| RedissonError::PoolError(e.to_string()))?;

        Ok(RedisConnection::Sentinel(conn))
    }

    fn try_backup_connections(&self) -> RedissonResult<RedisConnection> {
        let mut backup_pools = self.backup_pools.lock().unwrap();

        // Clean up the pool of unhealthy alternate connections
        let now = Instant::now();
        backup_pools.retain(|pool| {
            pool.is_healthy && now.duration_since(pool.created_at) < Duration::from_secs(300)
        });

        // Try every alternate connection pool
        for backup_pool in backup_pools.iter_mut() {
            match backup_pool.pool.get() {
                Ok(mut conn) => {
                    // 简单健康检查
                    let is_healthy = conn.is_open() && conn.check_connection();
                    if is_healthy {
                        backup_pool.is_healthy = true;
                        return Ok(RedisConnection::Single(conn));
                    } else {
                        backup_pool.is_healthy = false;
                        continue;
                    }
                }
                Err(_) => {
                    backup_pool.is_healthy = false;
                    continue;
                }
            }
        }

        Err(RedissonError::PoolError("no connection available".to_string()))
    }

    pub fn add_backup_pool(&self, url: &str) -> RedissonResult<()> {
        // Create alternate connections
        let mut config = self.config.clone();
        config.connection_mode = ConnectionMode::SingleServer {
            url: url.to_string(),
            host: None,
            port: None,
        };

        let client = Self::create_single_client(&config)?;
        let pool = Self::build_pool(client, &config)?;

        let mut backup_pools = self.backup_pools.lock().unwrap();
        if backup_pools.len() >= self.config.backup_pool_count as usize {
            // Remove the oldest standby pool
            backup_pools.remove(0);
        }

        backup_pools.push(BackupPool {
            pool: Arc::new(pool),
            created_at: Instant::now(),
            is_healthy: true,
        });

        Ok(())
    }

    pub fn cleanup(&self) {
        // Clearing the cache
        {
            let mut cache = self.connection_cache.write();
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
            let mut backup_pools = self.backup_pools.lock().unwrap();
            let now = Instant::now();

            backup_pools.retain(|pool| {
                // 保留最近创建的或健康的池
                now.duration_since(pool.created_at) < Duration::from_secs(300) || pool.is_healthy
            });
        }
    }

    pub fn get_stats(&self) -> ConnectionStats {
        self.stats.read().clone()
    }

    pub fn reset_circuit_breaker(&self) {
        let mut circuit_breaker = self.circuit_breaker.write();
        circuit_breaker.reset();
    }

    pub fn health_check(&self) -> bool {
        match self.try_primary_connection() {
            Ok(mut conn) => {
                // Simple health check: PING
                let is_healthy = match redis::cmd("PING").query::<String>(&mut conn) {
                    Ok(response) => response == "PONG",
                    Err(_) => false,
                };

                if !is_healthy {
                    let mut circuit_breaker = self.circuit_breaker.write();
                    circuit_breaker.record_failure();
                }

                is_healthy
            }
            Err(_) => {
                let mut circuit_breaker = self.circuit_breaker.write();
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

/// Implements Redis command execution interface
impl RedisConnection {
    
    pub fn as_pubsub(&mut self) -> RedissonResult<redis::PubSub<'_>> {
        match self {
            RedisConnection::Single(conn) => {
                Ok(conn.as_pubsub())
            }
            _ => Err(RedissonError::InvalidOperation("Unsupported types".to_string()))
        }
    }
    
    pub fn execute_command<T: redis::FromRedisValue>(&mut self, cmd: &mut redis::Cmd) -> RedissonResult<T> {
        match self {
            RedisConnection::Single(conn) => {
                cmd.query(conn)
                    .map_err(|e| RedissonError::RedisError(e))
            }
            RedisConnection::Cluster(conn) => {
                cmd.query(conn)
                    .map_err(|e| RedissonError::RedisError(e))
            }
            RedisConnection::Sentinel(conn) => {
                cmd.query(conn)
                    .map_err(|e| RedissonError::RedisError(e))
            }
        }
    }

    pub fn execute_pipeline<T: redis::FromRedisValue>(&mut self, pipeline: &redis::Pipeline) -> RedissonResult<Vec<T>> {
        match self {
            RedisConnection::Single(conn) => {
                pipeline.query(conn)
                    .map_err(|e| RedissonError::RedisError(e))
            }
            RedisConnection::Cluster(conn) => {
                pipeline.query(conn)
                    .map_err(|e| RedissonError::RedisError(e))
            }
            RedisConnection::Sentinel(conn) => {
                pipeline.query(conn)
                    .map_err(|e| RedissonError::RedisError(e))
            }
        }
    }

    pub fn execute_script<T: redis::FromRedisValue>(
        &mut self,
        script: &redis::Script,
        keys: &[&str],
        args: &[&str]
    ) -> RedissonResult<T> {
        let mut invocation = &mut script.key(keys);
        for arg in args {
            invocation = invocation.arg(arg);
        }

        match self {
            RedisConnection::Single(conn) => {
                invocation.invoke(conn)
                    .map_err(|e| RedissonError::RedisError(e))
            }
            RedisConnection::Cluster(conn) => {
                invocation.invoke(conn)
                    .map_err(|e| RedissonError::RedisError(e))
            }
            RedisConnection::Sentinel(conn) => {
                invocation.invoke(conn)
                    .map_err(|e| RedissonError::RedisError(e))
            }
        }
    }
}

