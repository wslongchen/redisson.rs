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
use std::time::Duration;
use redis::Commands;
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::{BaseDistributedObject, RFairLock, RLock, RLockable, RObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};

/// === RMap Optimized implementation (inherited lock functionality)===
pub struct RMap<K, V> {
    base: BaseDistributedObject,
    _key_marker: std::marker::PhantomData<K>,
    _value_marker: std::marker::PhantomData<V>,
    // Map-specific configuration
    write_back_cache: bool,
    max_size: Option<usize>,
}

impl<K, V> RMap<K, V>
where
    K: Serialize + DeserializeOwned + Send + Sync + Eq + std::hash::Hash + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: BaseDistributedObject::new(connection_manager, name),
            _key_marker: std::marker::PhantomData,
            _value_marker: std::marker::PhantomData,
            write_back_cache: false,
            max_size: None,
        }
    }

    pub fn with_write_back_cache(mut self) -> Self {
        self.write_back_cache = true;
        self
    }

    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_size = Some(max_size);
        self
    }

    pub fn put(&self, key: &K, value: &V) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection()?;
        let key_json = BaseDistributedObject::serialize(key)?;
        let value_json = BaseDistributedObject::serialize(value)?;

        // Getting the old value
        let old_json: Option<String> = conn.hget(self.base.get_full_key(), &key_json)?;

        // Setting a new value
        conn.hset::<_, _, _, ()>(self.base.get_full_key(), key_json, value_json)?;

        // Checking size limits
        if let Some(max_size) = self.max_size {
            let size: i32 = conn.hlen(self.base.get_full_key())?;
            if size as usize > max_size {
                // 淘汰策略：移除第一个元素
                let first_key: Option<String> = conn.hkeys(self.base.get_full_key())?;
                if let Some(first) = first_key {
                    conn.hdel::<_, _, ()>(self.base.get_full_key(), first)?;
                }
            }
        }

        match old_json {
            Some(old) => {
                let old_value: V = BaseDistributedObject::deserialize(&old)?;
                Ok(Some(old_value))
            }
            None => Ok(None),
        }
    }

    pub fn put_if_absent(&self, key: &K, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let key_json = BaseDistributedObject::serialize(key)?;
        let value_json = BaseDistributedObject::serialize(value)?;

        let result: i32 = conn.hset_nx(self.base.get_full_key(), key_json, value_json)?;
        Ok(result > 0)
    }

    pub fn get(&self, key: &K) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection()?;
        let key_json = BaseDistributedObject::serialize(key)?;

        let result: Option<String> = conn.hget(self.base.get_full_key(), key_json)?;

        match result {
            Some(value_json) => {
                let value: V = BaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub fn remove(&self, key: &K) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection()?;
        let key_json = BaseDistributedObject::serialize(key)?;

        // Get the old value first
        let old_json: Option<String> = conn.hget(self.base.get_full_key(), &key_json)?;

        // DELETE
        let deleted: i32 = conn.hdel(self.base.get_full_key(), key_json)?;

        if deleted > 0 {
            match old_json {
                Some(old) => {
                    let old_value: V = BaseDistributedObject::deserialize(&old)?;
                    Ok(Some(old_value))
                }
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    pub fn fast_put(&self, key: &K, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let key_json = BaseDistributedObject::serialize(key)?;
        let value_json = BaseDistributedObject::serialize(value)?;

        let result: i32 = conn.hset(self.base.get_full_key(), key_json, value_json)?;
        Ok(result > 0)
    }

    pub fn fast_remove(&self, key: &K) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let key_json = BaseDistributedObject::serialize(key)?;

        let deleted: i32 = conn.hdel(self.base.get_full_key(), key_json)?;
        Ok(deleted > 0)
    }

    pub fn size(&self) -> RedissonResult<usize> {
        let mut conn = self.base.get_connection()?;
        let size: i32 = conn.hlen(self.base.get_full_key())?;
        Ok(size as usize)
    }

    pub fn contains_key(&self, key: &K) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let key_json = BaseDistributedObject::serialize(key)?;

        let exists: i32 = conn.hexists(self.base.get_full_key(), key_json)?;
        Ok(exists > 0)
    }

    pub fn contains_value(&self, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let values: Vec<String> = conn.hvals(self.base.get_full_key())?;

        let target_json = BaseDistributedObject::serialize(value)?;
        for value_json in values {
            if value_json == target_json {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn keys(&self) -> RedissonResult<Vec<K>> {
        let mut conn = self.base.get_connection()?;
        let keys_json: Vec<String> = conn.hkeys(self.base.get_full_key())?;

        let mut keys = Vec::with_capacity(keys_json.len());
        for key_json in keys_json {
            let key: K = BaseDistributedObject::deserialize(&key_json)?;
            keys.push(key);
        }

        Ok(keys)
    }

    pub fn values(&self) -> RedissonResult<Vec<V>> {
        let mut conn = self.base.get_connection()?;
        let values_json: Vec<String> = conn.hvals(self.base.get_full_key())?;

        let mut values = Vec::with_capacity(values_json.len());
        for value_json in values_json {
            let value: V = BaseDistributedObject::deserialize(&value_json)?;
            values.push(value);
        }

        Ok(values)
    }

    pub fn entry_set(&self) -> RedissonResult<Vec<(K, V)>> {
        let mut conn = self.base.get_connection()?;
        let map: std::collections::HashMap<String, String> = conn.hgetall(self.base.get_full_key())?;

        let mut entries = Vec::with_capacity(map.len());
        for (key_json, value_json) in map {
            let key: K = BaseDistributedObject::deserialize(&key_json)?;
            let value: V = BaseDistributedObject::deserialize(&value_json)?;
            entries.push((key, value));
        }

        Ok(entries)
    }

    pub fn clear(&self) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        conn.del::<_, ()>(self.base.get_full_key())?;
        Ok(())
    }

    // Map-specific locking: Locks for specific keys
    pub fn get_lock_for_key(&self, key: &K) -> RedissonResult<RLock> {
        let key_json = BaseDistributedObject::serialize(key)?;
        let lock_name = format!("{}:key_lock:{}", self.base.get_full_key(), key_json);
        Ok(RLock::new(self.base.connection_manager(), lock_name, Duration::from_secs(30)))
    }

    // Map-specific locking: Write locks for the entire Map
    pub fn write_lock(&self) -> RLock {
        let lock_name = format!("{}:write_lock", self.base.get_full_key());
        RLock::new(self.base.connection_manager(), lock_name, Duration::from_secs(30))
    }

    // Map-specific locking: Read locks for the entire Map
    pub fn read_lock(&self) -> RLock {
        let lock_name = format!("{}:read_lock", self.base.get_full_key());
        RLock::new(self.base.connection_manager(), lock_name, Duration::from_secs(30))
    }
}

// Implement lock attributes for RMap (inheriting the lock functionality of BaseDistributedObject)
impl<K, V> RObject for RMap<K, V>
where
    K: Serialize + DeserializeOwned + Send + Sync + Eq + std::hash::Hash + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn get_name(&self) -> &str {
        self.base.get_name()
    }

    fn delete(&self) -> RedissonResult<bool> {
        self.base.delete()
    }

    fn rename(&self, new_name: &str) -> RedissonResult<()> {
        self.base.rename(new_name)
    }

    fn is_exists(&self) -> RedissonResult<bool> {
        self.base.is_exists()
    }

    fn move_to_db(&self, db_index: i32) -> RedissonResult<bool> {
        self.base.move_to_db(db_index)
    }

    fn get_expire_time(&self) -> RedissonResult<Option<Duration>> {
        self.base.get_expire_time()
    }

    fn expire(&self, duration: Duration) -> RedissonResult<bool> {
        self.base.expire(duration)
    }

    fn expire_at(&self, timestamp: i64) -> RedissonResult<bool> {
        self.base.expire_at(timestamp)
    }

    fn clear_expire(&self) -> RedissonResult<bool> {
        self.base.clear_expire()
    }
}

impl<K, V> RLockable for RMap<K, V>
where
    K: Serialize + DeserializeOwned + Send + Sync + Eq + std::hash::Hash + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn get_lock(&self) -> RLock {
        self.base.get_lock()
    }

    fn get_fair_lock(&self) -> RFairLock {
        self.base.get_fair_lock()
    }

    fn lock(&self) -> RedissonResult<()> {
        self.base.lock()
    }

    fn try_lock(&self) -> RedissonResult<bool> {
        self.base.try_lock()
    }

    fn try_lock_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        self.base.try_lock_timeout(wait_time)
    }

    fn lock_lease(&self, lease_time: Duration) -> RedissonResult<()> {
        self.base.lock_lease(lease_time)
    }

    fn unlock(&self) -> RedissonResult<bool> {
        self.base.unlock()
    }

    fn force_unlock(&self) -> RedissonResult<bool> {
        self.base.force_unlock()
    }

    fn is_locked(&self) -> RedissonResult<bool> {
        self.base.is_locked()
    }

    fn is_held_by_current_thread(&self) -> bool {
        self.base.is_held_by_current_thread()
    }
}