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
use serde::de::DeserializeOwned;
use serde::Serialize;
use async_trait::async_trait;
use crate::{RedissonResult, AsyncRedisConnectionManager, AsyncRedisConnection, AsyncBaseDistributedObject, AsyncRObject, AsyncRLockable, AsyncRLock, AsyncRFairLock, AsyncRObjectBase};


// === AsyncRMap Asynchronous mapping ===
pub struct AsyncRMap<K, V> {
    base: AsyncBaseDistributedObject,
    _key_marker: std::marker::PhantomData<K>,
    _value_marker: std::marker::PhantomData<V>,
    write_back_cache: bool,
    max_size: Option<usize>,
}

impl<K, V> AsyncRMap<K, V>
where
    K: Serialize + DeserializeOwned + Send + Sync + Eq + std::hash::Hash + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
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

    pub async fn put(&self, key: &K, value: &V) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection().await?;
        let key_json = AsyncBaseDistributedObject::serialize(key)?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        // Getting the old value
        let old_json: Option<String> = conn
            .execute_command(&mut redis::cmd("HGET").arg(self.base.get_full_key()).arg(&key_json))
            .await?;

        // Setting a new value
        conn.execute_command::<()>(&mut redis::cmd("HSET").arg(self.base.get_full_key()).arg(&key_json).arg(&value_json))
            .await?;

        // Checking size limits
        if let Some(max_size) = self.max_size {
            let size: i32 = conn
                .execute_command(&mut redis::cmd("HLEN").arg(self.base.get_full_key()))
                .await?;

            if size as usize > max_size {
                // Elimination strategy: Remove the first element
                let first_key: Option<String> = conn
                    .execute_command(&mut redis::cmd("HKEYS").arg(self.base.get_full_key()))
                    .await?;

                if let Some(first) = first_key {
                    conn.execute_command::<()>(&mut redis::cmd("HDEL").arg(self.base.get_full_key()).arg(first))
                        .await?;
                }
            }
        }

        match old_json {
            Some(old) => {
                let old_value: V = AsyncBaseDistributedObject::deserialize(&old)?;
                Ok(Some(old_value))
            }
            None => Ok(None),
        }
    }

    pub async fn put_if_absent(&self, key: &K, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let key_json = AsyncBaseDistributedObject::serialize(key)?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        let result: i32 = conn
            .execute_command(&mut redis::cmd("HSETNX").arg(self.base.get_full_key()).arg(key_json).arg(value_json))
            .await?;

        Ok(result > 0)
    }

    pub async fn get(&self, key: &K) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection().await?;
        let key_json = AsyncBaseDistributedObject::serialize(key)?;

        let result: Option<String> = conn
            .execute_command(&mut redis::cmd("HGET").arg(self.base.get_full_key()).arg(key_json))
            .await?;

        match result {
            Some(value_json) => {
                let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub async fn remove(&self, key: &K) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection().await?;
        let key_json = AsyncBaseDistributedObject::serialize(key)?;

        // Get the old value first
        let old_json: Option<String> = conn
            .execute_command(&mut redis::cmd("HGET").arg(self.base.get_full_key()).arg(&key_json))
            .await?;

        // delete
        let deleted: i32 = conn
            .execute_command(&mut redis::cmd("HDEL").arg(self.base.get_full_key()).arg(key_json))
            .await?;

        if deleted > 0 {
            match old_json {
                Some(old) => {
                    let old_value: V = AsyncBaseDistributedObject::deserialize(&old)?;
                    Ok(Some(old_value))
                }
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    pub async fn size(&self) -> RedissonResult<usize> {
        let mut conn = self.base.get_connection().await?;
        let size: i32 = conn
            .execute_command(&mut redis::cmd("HLEN").arg(self.base.get_full_key()))
            .await?;

        Ok(size as usize)
    }

    pub async fn contains_key(&self, key: &K) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let key_json = AsyncBaseDistributedObject::serialize(key)?;

        let exists: i32 = conn
            .execute_command(&mut redis::cmd("HEXISTS").arg(self.base.get_full_key()).arg(key_json))
            .await?;

        Ok(exists > 0)
    }

    pub async fn keys(&self) -> RedissonResult<Vec<K>> {
        let mut conn = self.base.get_connection().await?;
        let keys_json: Vec<String> = conn
            .execute_command(&mut redis::cmd("HKEYS").arg(self.base.get_full_key()))
            .await?;

        let mut keys = Vec::with_capacity(keys_json.len());
        for key_json in keys_json {
            let key: K = AsyncBaseDistributedObject::deserialize(&key_json)?;
            keys.push(key);
        }

        Ok(keys)
    }

    pub async fn values(&self) -> RedissonResult<Vec<V>> {
        let mut conn = self.base.get_connection().await?;
        let values_json: Vec<String> = conn
            .execute_command(&mut redis::cmd("HVALS").arg(self.base.get_full_key()))
            .await?;

        let mut values = Vec::with_capacity(values_json.len());
        for value_json in values_json {
            let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
            values.push(value);
        }

        Ok(values)
    }

    pub async fn entry_set(&self) -> RedissonResult<Vec<(K, V)>> {
        let mut conn = self.base.get_connection().await?;
        let map: std::collections::HashMap<String, String> = conn
            .execute_command(&mut redis::cmd("HGETALL").arg(self.base.get_full_key()))
            .await?;

        let mut entries = Vec::with_capacity(map.len());
        for (key_json, value_json) in map {
            let key: K = AsyncBaseDistributedObject::deserialize(&key_json)?;
            let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
            entries.push((key, value));
        }

        Ok(entries)
    }

    pub async fn clear(&self) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;
        conn.execute_command::<()>(&mut redis::cmd("DEL").arg(self.base.get_full_key()))
            .await?;
        Ok(())
    }

    // Map-specific locking: Locks for specific keys
    pub fn get_lock_for_key(&self, key: &K) -> RedissonResult<AsyncRLock> {
        let key_json = AsyncBaseDistributedObject::serialize(key)?;
        let lock_name = format!("{}:key_lock:{}", self.base.get_full_key(), key_json);
        Ok(AsyncRLock::new(self.base.connection_manager(), lock_name, Duration::from_secs(30)))
    }

    // Map-specific locking: Write locks for the entire Map
    pub fn write_lock(&self) -> AsyncRLock {
        let lock_name = format!("{}:write_lock", self.base.get_full_key());
        AsyncRLock::new(self.base.connection_manager(), lock_name, Duration::from_secs(30))
    }

    // Map-specific locking: Read locks for the entire Map
    pub fn read_lock(&self) -> AsyncRLock {
        let lock_name = format!("{}:read_lock", self.base.get_full_key());
        AsyncRLock::new(self.base.connection_manager(), lock_name, Duration::from_secs(30))
    }
}


#[async_trait]
impl<K, V> AsyncRObject for AsyncRMap<K, V>
where
    K: Serialize + DeserializeOwned + Send + Sync + Eq + std::hash::Hash + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn delete(&self) -> RedissonResult<bool> {
        self.base.delete().await
    }

    async fn rename(&self, new_name: &str) -> RedissonResult<()> {
        self.base.rename(new_name).await
    }

    async fn is_exists(&self) -> RedissonResult<bool> {
        self.base.is_exists().await
    }

    async fn move_to_db(&self, db_index: i32) -> RedissonResult<bool> {
        self.base.move_to_db(db_index).await
    }

    async fn get_expire_time(&self) -> RedissonResult<Option<Duration>> {
        self.base.get_expire_time().await
    }

    async fn expire(&self, duration: Duration) -> RedissonResult<bool> {
        self.base.expire(duration).await
    }

    async fn expire_at(&self, timestamp: i64) -> RedissonResult<bool> {
        self.base.expire_at(timestamp).await
    }

    async fn clear_expire(&self) -> RedissonResult<bool> {
        self.base.clear_expire().await
    }
}

#[async_trait]
impl<K, V> AsyncRLockable for AsyncRMap<K, V>
where
    K: Serialize + DeserializeOwned + Send + Sync + Eq + std::hash::Hash + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn get_lock(&self) -> AsyncRLock {
        AsyncRLock::new(
            self.base.connection_manager(),
            format!("{}:lock", self.base.get_full_key()),
            Duration::from_secs(30)
        )
    }

    fn get_fair_lock(&self) -> AsyncRFairLock {
        AsyncRFairLock::new(
            self.base.connection_manager(),
            format!("{}:fair_lock", self.base.get_full_key()),
            Duration::from_secs(30)
        )
    }

    async fn lock(&self) -> RedissonResult<()> {
        self.get_lock().lock().await
    }

    async fn try_lock(&self) -> RedissonResult<bool> {
        self.get_lock().try_lock().await
    }

    async fn try_lock_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        self.get_lock().try_lock_with_timeout(wait_time).await
    }

    async fn lock_lease(&self, lease_time: Duration) -> RedissonResult<()> {
        self.get_lock().lock_with_lease_time(lease_time).await
    }

    async fn unlock(&self) -> RedissonResult<bool> {
        self.get_lock().unlock().await
    }

    async fn force_unlock(&self) -> RedissonResult<bool> {
        self.get_lock().force_unlock().await
    }

    async fn is_locked(&self) -> RedissonResult<bool> {
        self.get_lock().is_locked().await
    }

    async fn is_held_by_current_thread(&self) -> bool {
        self.get_lock().is_held_by_current_thread().await
    }
}