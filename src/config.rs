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
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConnectionMode {
    SingleServer {
        url: String,
        host: Option<String>,
        port: Option<u16>,
    },
    Sentinel {
        master_name: String,
        sentinel_addresses: Vec<String>,
    },
    Cluster {
        node_addresses: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedissonConfig {
    /// Connection mode
    pub connection_mode: ConnectionMode,
    /// Connection pool size
    pub pool_size: u32,
    /// Connection timeout time
    pub connection_timeout: Duration,
    /// Response timeout time
    pub response_timeout: Duration,
    /// Watchdog timeout (for lock renewal)
    pub watchdog_timeout: Duration,
    /// Lock expiration time
    pub lock_expire_time: Duration,
    /// Gets the number of lock retries
    pub retry_count: u32,
    /// Retry latency
    pub retry_delay: Duration,
    /// Clock offset factor
    pub drift_factor: f64,
    /// User name
    pub username: Option<String>,
    /// PASSWORD
    pub password: Option<String>,
    /// Database number
    pub database: Option<i64>,
    /// SSL enabled or not
    pub ssl: bool,
    /// Number of backup connection pools
    pub backup_pool_count: u32,
    /// Maximum connection wait queue size
    pub max_wait_queue_size: u32,
    /// Connection maximum lifetime
    pub max_lifetime: Duration,
    /// Connection idle time out
    pub idle_timeout: Duration,
    pub batch_config: Option<BatchConfig>,
    pub transaction_config: Option<TransactionConfig>,
}

impl Default for RedissonConfig {
    fn default() -> Self {
        Self {
            connection_mode: ConnectionMode::SingleServer { url: "".to_string(), host: Some("localhost".to_string()), port: Some(6379) },
            pool_size: 10,
            connection_timeout: Duration::from_secs(3),
            response_timeout: Duration::from_secs(3),
            watchdog_timeout: Duration::from_secs(30),
            lock_expire_time: Duration::from_secs(30),
            retry_count: 3,
            retry_delay: Duration::from_millis(200),
            drift_factor: 0.01,
            username: None,
            password: None,
            transaction_config: None,
            database: Some(0),
            ssl: false,
            backup_pool_count: 2,
            max_wait_queue_size: 100,
            max_lifetime: Duration::from_secs(300),
            idle_timeout: Duration::from_secs(60),
            batch_config: Some(BatchConfig::default())
        }
    }
}



impl RedissonConfig {
    pub fn single_server(address: &str) -> Self {
        Self {
            connection_mode: ConnectionMode::SingleServer { url: address.to_string(), host: Some("localhost".to_string()), port: Some(6379) },
            ..Default::default()
        }
    }

    pub fn sentinel(master_name: &str, sentinel_addresses: Vec<String>) -> Self {
        Self {
            connection_mode: ConnectionMode::Sentinel {
                master_name: master_name.to_string(),
                sentinel_addresses,
            },
            ..Default::default()
        }
    }

    pub fn cluster(node_addresses: Vec<String>) -> Self {
        Self {
            connection_mode: ConnectionMode::Cluster { node_addresses },
            ..Default::default()
        }
    }

    pub fn with_pool_size(mut self, size: u32) -> Self {
        self.pool_size = size;
        self
    }

    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    pub fn with_response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = timeout;
        self
    }

    pub fn with_watchdog_timeout(mut self, timeout: Duration) -> Self {
        self.watchdog_timeout = timeout;
        self
    }

    pub fn with_lock_expire_time(mut self, timeout: Duration) -> Self {
        self.lock_expire_time = timeout;
        self
    }

    pub fn with_retry_count(mut self, count: u32) -> Self {
        self.retry_count = count;
        self
    }

    pub fn with_drift_factor(mut self, factor: f64) -> Self {
        self.drift_factor = factor;
        self
    }

    pub fn with_password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    pub fn with_database(mut self, db: i64) -> Self {
        self.database = Some(db);
        self
    }

    pub fn with_backup_pool_count(mut self, count: u32) -> Self {
        self.backup_pool_count = count;
        self
    }

    pub fn with_max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }
    pub fn with_transaction_config(mut self, transaction_config: TransactionConfig) -> Self {
        self.transaction_config = Some(transaction_config);
        self
    }
    pub fn with_batch_config(mut self, batch_config: BatchConfig) -> Self {
        self.batch_config = Some(batch_config);
        self
    }
}


// ================ Batch configuration ================
/// Batch configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    pub max_batch_size: usize,
    pub enable_pipeline: bool,
    pub flush_interval: Duration,
    pub max_retries: u32,
    pub enable_cache: bool,
    pub cache_size: usize,
    pub cache_ttl: Duration,
    pub enable_background_flush: bool,
    pub max_queue_size: usize,
    pub enable_priority: bool,
    pub enable_async: bool,
    pub backoff_strategy: Option<BackoffStrategyConfig>,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub max_concurrent_batches: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffStrategyConfig {
    Linear,
    Exponential,
    Fixed,
}

impl Default for BackoffStrategyConfig {
    fn default() -> Self {
        Self::Linear
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            enable_pipeline: true,
            flush_interval: Duration::from_millis(100),
            max_retries: 3,
            enable_cache: false,
            cache_size: 1000,
            backoff_strategy: Some(BackoffStrategyConfig::Linear),
            cache_ttl: Duration::from_secs(300),
            enable_background_flush: false,
            max_queue_size: 10000,
            enable_priority: false,
            enable_async: false,
            initial_backoff_ms: 100,
            max_backoff_ms: 5000,
            max_concurrent_batches: 10,
        }
    }
}

impl BatchConfig {
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    pub fn with_pipeline(mut self, enabled: bool) -> Self {
        self.enable_pipeline = enabled;
        self
    }

    pub fn with_cache(mut self, enabled: bool) -> Self {
        self.enable_cache = enabled;
        self
    }

    pub fn with_cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    pub fn with_retry(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }
}

/// Transaction configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionConfig {
    pub max_retries: u32,
    pub enable_cache: bool,
    pub enable_watch: bool,
    pub cache_size: usize,
    pub cache_ttl: Duration,
    pub timeout: Option<Duration>,
    pub max_queue_size: usize,
    pub max_concurrent_batches: usize,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            enable_watch: false,
            max_retries: 3,
            enable_cache: false,
            cache_size: 1000,
            cache_ttl: Duration::from_secs(300),
            timeout: Some(Duration::from_secs(6)),
            max_queue_size: 10000,
            max_concurrent_batches: 10,
            initial_backoff_ms: 100,
            max_backoff_ms: 5000,
        }
    }
}