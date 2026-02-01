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
use parking_lot::RwLock;
use redis::Commands;
use serde::{de::DeserializeOwned, Serialize};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crate::errors::RedissonResult;
use crate::{Cache, CacheStats, LocalCache, RedissonError, SyncRedisConnectionManager};

/// Integrated caching with Redis
pub struct RedisIntegratedCache<K, V> {
    connection_manager: Arc<SyncRedisConnectionManager>,
    local_cache: Arc<LocalCache<K, V>>,
    cache_key_prefix: String,
    read_through: bool,
    write_through: bool,
}

impl<K: Eq + Hash + Clone + Serialize + DeserializeOwned + std::fmt::Debug,
    V: Clone + Serialize + DeserializeOwned> RedisIntegratedCache<K, V> {

    pub fn new(
        connection_manager: Arc<SyncRedisConnectionManager>,
        cache_name: &str,
        ttl: Duration,
        max_size: usize,
    ) -> Self {
        let local_cache = LocalCache::new(
            ttl,
            max_size,
            Arc::new(RwLock::new(CacheStats::new())),
        );

        Self {
            connection_manager,
            local_cache: Arc::new(local_cache),
            cache_key_prefix: format!("cache:{}", cache_name),
            read_through: true,
            write_through: true,
        }
    }

    pub fn with_read_through(mut self, enabled: bool) -> Self {
        self.read_through = enabled;
        self
    }

    pub fn with_write_through(mut self, enabled: bool) -> Self {
        self.write_through = enabled;
        self
    }

    fn build_redis_key(&self, key: &K) -> String {
        let key_json = serde_json::to_string(key)
            .unwrap_or_else(|_| format!("{:?}", key));

        format!("{}:{}", self.cache_key_prefix, key_json)
    }

    pub fn get_local_cache(&self) -> &Arc<LocalCache<K, V>> {
        &self.local_cache
    }
}

impl <K: Eq + Hash + Clone + Serialize + DeserializeOwned + std::fmt::Debug,
    V: Clone + Serialize + DeserializeOwned> Cache<K, V> for RedisIntegratedCache<K, V> {
    fn get(&self, key: &K) -> RedissonResult<Option<V>> {
        // 1. Try local caching
        if let Some(value) = self.local_cache.get(key)? {
            return Ok(Some(value));
        }

        if !self.read_through {
            return Ok(None);
        }

        // 2. Read from Redis
        let redis_key = self.build_redis_key(key);
        let mut conn = self.connection_manager.get_connection()?;

        let value_json: Option<String> = conn.get(&redis_key)?;

        if let Some(json) = value_json {
            let value: V = serde_json::from_str(&json)
                .map_err(|e| RedissonError::DeserializationError(e.to_string()))?;

            // 3. Updating the local cache
            self.local_cache.set(key.clone(), value.clone())?;

            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn set(&self, key: K, value: V) -> RedissonResult<()> {
        // 1. Updating the local cache
        self.local_cache.set(key.clone(), value.clone())?;

        if !self.write_through {
            return Ok(());
        }

        // 2. Updating Redis
        let redis_key = self.build_redis_key(&key);
        let value_json = serde_json::to_string(&value)
            .map_err(|e| RedissonError::SerializationError(e.to_string()))?;

        let mut conn = self.connection_manager.get_connection()?;

        // Set an expiration time (using a locally cached TTL)
        let ttl_secs = self.local_cache.ttl.as_secs() as i64;
        redis::pipe()
            .atomic()
            .set(&redis_key, &value_json)
            .expire(&redis_key, ttl_secs)
            .query::<()>(&mut conn)?;

        Ok(())
    }

    fn set_with_ttl(&self, key: K, value: V, ttl: Duration) -> RedissonResult<()> {
        // 1. Updating the local cache
        self.local_cache.set(key.clone(), value.clone())?;

        if !self.write_through {
            return Ok(());
        }

        // 2. Updating Redis
        let redis_key = self.build_redis_key(&key);
        let value_json = serde_json::to_string(&value)
            .map_err(|e| RedissonError::SerializationError(e.to_string()))?;

        let mut conn = self.connection_manager.get_connection()?;

        // Set an expiration time
        redis::pipe()
            .atomic()
            .set(&redis_key, &value_json)
            .expire(&redis_key, ttl.as_secs() as i64)
            .query::<()>(&mut conn)?;

        Ok(())
    }

    fn remove(&self, key: &K) -> RedissonResult<bool> {
        // 1. Clearing the local cache
        self.local_cache.remove(key)?;

        if !self.write_through {
            return Ok(true);
        }

        // 2. Clear Redis
        let redis_key = self.build_redis_key(key);
        let mut conn = self.connection_manager.get_connection()?;

        let deleted: i32 = conn.del(&redis_key)?;

        Ok(deleted > 0)
    }

    fn clear(&self) -> RedissonResult<()> {
        // 1. Clearing the local cache
        self.local_cache.clear()?;

        if !self.write_through {
            return Ok(());
        }

        // 2. Clear all related keys from Redis
        let pattern = format!("{}:*", self.cache_key_prefix);
        let mut conn = self.connection_manager.get_connection()?;

        let keys: Vec<String> = conn.keys(&pattern)?;

        if !keys.is_empty() {
            conn.del::<_, ()>(keys)?;
        }

        Ok(())
    }

    fn refresh(&self, key: &K) -> RedissonResult<bool> {
        // Flush from Redis to the local cache
        let redis_key = self.build_redis_key(key);
        let mut conn = self.connection_manager.get_connection()?;

        let value_json: Option<String> = conn.get(&redis_key)?;

        if let Some(json) = value_json {
            let value: V = serde_json::from_str(&json)
                .map_err(|e| RedissonError::DeserializationError(e.to_string()))?;

            self.local_cache.set(key.clone(), value)?;
            Ok(true)
        } else {
            self.local_cache.remove(key)?;
            Ok(false)
        }
    }
}