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

use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::hash::Hash;
use std::num::NonZeroUsize;
use async_trait::async_trait;
use parking_lot::{RwLock, Mutex};
use lru::LruCache;
use redis::Commands;
use serde::{Serialize, de::DeserializeOwned};

use crate::errors::RedissonResult;
use crate::{estimate_size, AsyncCache, AsyncLocalCache, AsyncLocalCacheManager, AsyncRedisConnectionManager, AsyncRedisIntegratedCache, Cache, CacheEntryStats, CacheStats, CachedValue, RedisIntegratedCache, RedissonError, SyncRedisConnectionManager};

/// The local cache manager
pub struct LocalCacheManager<K, V> {
    caches: Arc<RwLock<HashMap<String, Arc<LocalCache<K, V>>>>>,
    default_ttl: Duration,
    default_max_size: usize,
    stats: Arc<RwLock<CacheStats>>,
    cleanup_interval: Duration,
}

/// Local cache instance
pub struct LocalCache<K, V> {
    cache: Arc<RwLock<LruCache<K, CachedValue<V>>>>,
    pub ttl: Duration,
    max_size: usize,
    stats: Arc<RwLock<CacheStats>>,
    name: String,
}


impl<K: Eq + Hash + Clone + std::marker::Send + std::marker::Sync + 'static, V: Clone + Serialize + DeserializeOwned + std::marker::Send + std::marker::Sync + 'static> LocalCacheManager<K, V> {
    pub fn new(default_ttl: Duration, default_max_size: usize) -> Self {
        let manager = Self {
            caches: Arc::new(RwLock::new(HashMap::new())),
            default_ttl,
            default_max_size,
            stats: Arc::new(RwLock::new(CacheStats::new())),
            cleanup_interval: Duration::from_secs(60),
        };

        // 启动后台清理线程
        manager.start_cleanup_thread();

        manager
    }

    pub fn with_cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    pub fn get_or_create_cache(&self, name: &str) -> Arc<LocalCache<K, V>> {
        let caches = self.caches.read();

        if let Some(cache) = caches.get(name) {
            return cache.clone();
        }

        drop(caches);

        let mut caches = self.caches.write();

        // 双重检查
        if let Some(cache) = caches.get(name) {
            return cache.clone();
        }

        let cache = Arc::new(LocalCache::new(
            name.to_string(),
            self.default_ttl,
            self.default_max_size,
            self.stats.clone(),
        ));

        caches.insert(name.to_string(), cache.clone());

        cache
    }

    pub fn remove_cache(&self, name: &str) -> bool {
        self.caches.write().remove(name).is_some()
    }

    pub fn clear_all(&self) {
        self.caches.write().clear();
    }

    pub fn get_stats(&self) -> CacheStats {
        self.stats.read().clone()
    }

    fn start_cleanup_thread(&self) {
        let manager = self.clone();

        std::thread::spawn(move || {
            let interval = manager.cleanup_interval;

            loop {
                std::thread::sleep(interval);
                manager.cleanup_expired();
            }
        });
    }

    fn cleanup_expired(&self) {
        let caches = self.caches.read();

        for cache in caches.values() {
            cache.cleanup();
        }

        {
            let mut stats = self.stats.write();
            stats.last_cleanup = Some(Instant::now());
        }
    }
    
    
}


impl<K: Eq + Hash + Clone, V: Clone + Serialize + DeserializeOwned> LocalCache<K, V> {
    pub fn new(name: String, ttl: Duration, max_size: usize, stats: Arc<RwLock<CacheStats>>) -> Self {
        Self {
            cache: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(max_size).unwrap()))),
            ttl,
            max_size,
            stats,
            name,
        }
    }

    pub fn len(&self) -> usize {
        self.cache.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.read().is_empty()
    }

    pub fn get_ttl(&self, key: &K) -> Option<Duration> {
        let cache = self.cache.read();

        cache.peek(key)
            .map(|cached| {
                let now = Instant::now();
                if cached.expiry > now {
                    Some(cached.expiry.duration_since(now))
                } else {
                    None
                }
            })
            .flatten()
    }

    pub fn get_stats(&self) -> CacheEntryStats {
        let cache = self.cache.read();
        let now = Instant::now();

        let mut total_hits = 0;
        let mut total_size = 0;
        let mut expired_count = 0;
        let mut active_count = 0;

        for cached in cache.iter() {
            total_hits += cached.1.hits;
            total_size += cached.1.size_bytes;

            if cached.1.expiry > now {
                active_count += 1;
            } else {
                expired_count += 1;
            }
        }

        CacheEntryStats {
            total_entries: cache.len(),
            active_entries: active_count,
            expired_entries: expired_count,
            total_hits,
            total_size_bytes: total_size,
            avg_hits_per_entry: if cache.len() > 0 {
                total_hits as f64 / cache.len() as f64
            } else {
                0.0
            },
        }
    }

    pub fn cleanup(&self) {
        let mut cache = self.cache.write();
        let now = Instant::now();

        let expired_keys: Vec<K> = cache.iter()
            .filter(|(_, v)| v.expiry <= now)
            .map(|(k, _)| k.clone())
            .collect();

        let evicted_count = expired_keys.len();

        for key in expired_keys {
            cache.pop(&key);
        }

        if evicted_count > 0 {
            let mut stats = self.stats.write();
            stats.record_eviction(evicted_count);
            stats.total_entries = cache.len();
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        let cache = self.cache.read();

        if let Some(cached) = cache.peek(key) {
            cached.expiry > Instant::now()
        } else {
            false
        }
    }
}


impl<K: Eq + Hash + Clone, V: Clone + Serialize + DeserializeOwned> Cache<K,V> for LocalCache<K, V> {

    fn get(&self, key: &K) -> RedissonResult<Option<V>> {
        let mut cache = self.cache.write();

        if let Some(cached) = cache.get_mut(key) {
            if cached.expiry > Instant::now() {
                // 缓存命中
                cached.hits += 1;
                self.stats.write().record_hit();
                return Ok(Some(cached.value.clone()));
            } else {
                // 缓存过期
                cache.pop(key);
                self.stats.write().record_miss();
                return Ok(None);
            }
        }

        self.stats.write().record_miss();
        Ok(None)
    }

    fn set(&self, key: K, value: V) -> RedissonResult<()> {
        let size_bytes = estimate_size(&value);

        let cached_value = CachedValue {
            value,
            expiry: Instant::now() + self.ttl,
            created: Instant::now(),
            hits: 0,
            size_bytes,
        };

        let mut cache = self.cache.write();
        let evicted = cache.put(key, cached_value);

        // 记录淘汰
        if evicted.is_some() {
            let mut stats = self.stats.write();
            stats.record_eviction(1);
            stats.total_entries = cache.len();
        }
        
        Ok(())
    }

    fn set_with_ttl(&self, key: K, value: V, ttl: Duration) -> RedissonResult<()> {
        let size_bytes = estimate_size(&value);

        let cached_value = CachedValue {
            value,
            expiry: Instant::now() + ttl,
            created: Instant::now(),
            hits: 0,
            size_bytes,
        };

        let mut cache = self.cache.write();
        let evicted = cache.put(key, cached_value);

        if evicted.is_some() {
            let mut stats = self.stats.write();
            stats.record_eviction(1);
        }
        Ok(())
    }

    fn remove(&self, key: &K) -> RedissonResult<bool> {
        let mut cache = self.cache.write();
        let value = cache.pop(key).map(|cached| cached.value);
        Ok(value.is_some())
    }

    fn clear(&self) -> RedissonResult<()> {
        let mut cache = self.cache.write();
        let evicted_count = cache.len();
        cache.clear();

        let mut stats = self.stats.write();
        stats.record_eviction(evicted_count);
        stats.total_entries = 0;
        Ok(())
    }

    

    fn refresh(&self, key: &K) -> RedissonResult<bool> {
        let mut cache = self.cache.write();

        if let Some(cached) = cache.get_mut(key) {
            if cached.expiry > Instant::now() {
                cached.expiry = Instant::now() + self.ttl;
                return Ok(true);
            }
        }
        Ok(false)
    }
}


impl<K: Eq + Hash + Clone, V: Clone + Serialize + DeserializeOwned> Clone for LocalCacheManager<K, V> {
    fn clone(&self) -> Self {
        Self {
            caches: self.caches.clone(),
            default_ttl: self.default_ttl,
            default_max_size: self.default_max_size,
            stats: self.stats.clone(),
            cleanup_interval: self.cleanup_interval,
        }
    }
}

/// 缓存工厂
pub struct CacheFactory {
    connection_manager: Arc<SyncRedisConnectionManager>,
    local_cache_manager: Arc<LocalCacheManager<String, serde_json::Value>>,
}

impl CacheFactory {
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>) -> Self {
        let local_cache_manager = Arc::new(LocalCacheManager::new(
            Duration::from_secs(300), // 默认5分钟TTL
            1000, // 默认最大1000个条目
        ));

        Self {
            connection_manager,
            local_cache_manager,
        }
    }

    pub fn create_cache<K, V>(
        &self,
        cache_name: &str,
        ttl: Duration,
        max_size: usize,
    ) -> Arc<dyn Cache<K, V>>
    where
        K: Eq + Hash + Clone + Serialize + DeserializeOwned + std::fmt::Debug + 'static,
        V: Clone + Serialize + DeserializeOwned + 'static,
    {
        let cache = RedisIntegratedCache::new(
            self.connection_manager.clone(),
            cache_name,
            ttl,
            max_size,
        );

        Arc::new(cache)
    }

    pub fn get_local_cache(&self, name: &str) -> Arc<LocalCache<String, serde_json::Value>> {
        self.local_cache_manager.get_or_create_cache(name)
    }
}