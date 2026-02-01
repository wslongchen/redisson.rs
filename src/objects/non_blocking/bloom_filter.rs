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
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use serde::de::DeserializeOwned;
use serde::Serialize;
use async_trait::async_trait;
use bloom::{BloomFilter, ASMS};
use tokio::sync::RwLock;
use crate::{RedissonResult, AsyncRedisConnectionManager, AsyncRedisConnection, AsyncBaseDistributedObject, AsyncRObject, AsyncRLockable, AsyncRLock, AsyncRFairLock, AsyncRObjectBase, AsyncRSet, BloomFilterInfo};

// === AsyncRBloomFilter (Asynchronous Bloom filters) ===
pub struct AsyncRBloomFilter<V> {
    base: AsyncBaseDistributedObject,
    bloom_filter: Arc<RwLock<BloomFilter>>,
    _marker: PhantomData<V>,
}

impl<V> AsyncRBloomFilter<V>
where
    V: AsRef<[u8]> + Send + Sync + 'static,
{
    pub fn new(
        connection_manager: Arc<AsyncRedisConnectionManager>,
        name: String,
        expected_insertions: usize,
        false_positive_rate: f64,
    ) -> Self {
        let bloom_filter = BloomFilter::with_rate(false_positive_rate as f32, expected_insertions as u32);
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
            bloom_filter: Arc::new(RwLock::new(bloom_filter)),
            _marker: PhantomData,
        }
    }

    pub async fn add(&self, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let value_bytes = value.as_ref();

        // Use the BF.ADD command (requires the RedisBloom module)
        let added: i32 = conn
            .execute_command(
                &mut redis::cmd("BF.ADD")
                    .arg(self.base.get_name())
                    .arg(value_bytes)
            )
            .await?;

        // Update the local bloom filter
        if added > 0 {
            let mut bloom = self.bloom_filter.write().await;
            bloom.insert(&value_bytes);
        }

        Ok(added > 0)
    }

    pub async fn contains(&self, value: &V) -> RedissonResult<bool> {
        // Check the local bloom filter first
        {
            let bloom = self.bloom_filter.read().await;
            if !bloom.contains(&value.as_ref()) {
                return Ok(false);
            }
        }

        // Local might exist, check Redis
        let mut conn = self.base.get_connection().await?;
        let value_bytes = value.as_ref();
        let exists: i32 = conn
            .execute_command(
                &mut redis::cmd("BF.EXISTS")
                    .arg(self.base.get_name())
                    .arg(value_bytes)
            )
            .await?;

        Ok(exists > 0)
    }

    pub async fn init(&self, capacity: usize, error_rate: f64) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;
        conn.execute_command::<()>(
            &mut redis::cmd("BF.RESERVE")
                .arg(self.base.get_name())
                .arg(error_rate)
                .arg(capacity)
        ).await?;
        Ok(())
    }

    // Adding elements in bulk
    pub async fn add_all(&self, values: &[V]) -> RedissonResult<Vec<bool>> {
        let mut results = Vec::with_capacity(values.len());

        for value in values {
            let result = self.add(value).await?;
            results.push(result);
        }

        Ok(results)
    }

    // Inspecting elements in bulk
    pub async fn contains_all(&self, values: &[V]) -> RedissonResult<Vec<bool>> {
        let mut results = Vec::with_capacity(values.len());

        for value in values {
            let result = self.contains(value).await?;
            results.push(result);
        }

        Ok(results)
    }

    // Batch add using BF.MADD (requires RedisBloom module)
    pub async fn madd(&self, values: &[V]) -> RedissonResult<Vec<bool>> {
        let mut conn = self.base.get_connection().await?;

        let mut cmd = redis::cmd("BF.MADD");
        cmd.arg(self.base.get_name());

        for value in values {
            cmd.arg(value.as_ref());
        }

        let results: Vec<i32> = cmd.query_async(&mut conn).await?;

        // Update the local bloom filter
        {
            let mut bloom = self.bloom_filter.write().await;
            for value in values {
                bloom.insert(&value.as_ref());
            }
        }

        Ok(results.into_iter().map(|r| r > 0).collect())
    }

    // Batch inspection using BF.MEXISTS (requires RedisBloom module)
    pub async fn mexists(&self, values: &[V]) -> RedissonResult<Vec<bool>> {
        // We start with a quick filter using the local bloom filter
        let mut filtered_values = Vec::new();
        let mut local_results = Vec::new();

        {
            let bloom = self.bloom_filter.read().await;
            for value in values {
                let exists = bloom.contains(&value.as_ref());
                local_results.push(exists);
                if exists {
                    filtered_values.push(value);
                }
            }
        }

        // If none of the local judgments exist, the result is returned directly
        if filtered_values.is_empty() {
            return Ok(local_results);
        }

        // Inspecting Redis
        let mut conn = self.base.get_connection().await?;

        let mut cmd = redis::cmd("BF.MEXISTS");
        cmd.arg(self.base.get_name());

        for value in &filtered_values {
            cmd.arg(value.as_ref());
        }

        let redis_results: Vec<i32> = cmd.query_async(&mut conn).await?;

        // Combining results
        let mut final_results = Vec::with_capacity(values.len());
        let mut redis_idx = 0;

        for local_result in local_results {
            if local_result {
                // Local judgments may exist and need to be confirmed by Redis
                let redis_result = redis_results.get(redis_idx).map(|&r| r > 0).unwrap_or(false);
                final_results.push(redis_result);
                redis_idx += 1;
            } else {
                // The local test does not exist and returns false
                final_results.push(false);
            }
        }

        Ok(final_results)
    }

    // Get Bloom filter information (requires the BF.INFO command of the RedisBloom module)
    pub async fn info(&self) -> RedissonResult<Option<BloomFilterInfo>> {
        let mut conn = self.base.get_connection().await?;

        match conn.execute_command(
            &mut redis::cmd("BF.INFO").arg(self.base.get_name())
        ).await {
            Ok(result) => {
                let info: Vec<redis::Value> = result;
                Self::parse_bf_info(&info)
            }
            Err(e) => {
                // If the command does not exist, None is returned
                if e.to_string().contains("unknown command") {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    fn parse_bf_info(info: &[redis::Value]) -> RedissonResult<Option<BloomFilterInfo>> {
        let mut bf_info = BloomFilterInfo::default();

        // Common return format：["Capacity", 10000, "Size", 12345, "Number of filters", 1, ...]
        for chunk in info.chunks(2) {
            if chunk.len() == 2 {
                if let (redis::Value::BulkString(key), redis::Value::Int(value)) = (&chunk[0], &chunk[1]) {
                    match std::str::from_utf8(key).unwrap_or("") {
                        "Capacity" => bf_info.capacity = *value as usize,
                        "Size" => bf_info.size = *value as usize,
                        "Number of filters" => bf_info.number_of_filters = *value as usize,
                        "Number of items inserted" => bf_info.items_inserted = *value as usize,
                        "Expansion rate" => bf_info.expansion_rate = *value as usize,
                        _ => {}
                    }
                }
            }
        }

        Ok(Some(bf_info))
    }

    // Reset the local bloom filter (reload from Redis)
    pub async fn reset_local_filter(&self) -> RedissonResult<()> {
        // TODO: Note: This method will clear the local filter and require re-syncing the data from Redis
        // In practice, this might require scanning all the data or using some other mechanism

        // Simple implementation: Clear the local filter
        {
            let mut bloom = self.bloom_filter.write().await;
            // Getting the original parameters
            let expected_insertions = bloom.num_bits() / 10; // ESTIMATION
            let false_positive_rate = 0.01; // Default values

            *bloom = BloomFilter::with_rate(false_positive_rate as f32, expected_insertions as u32);
        }

        Ok(())
    }
}

#[async_trait]
impl<V> AsyncRObject for AsyncRBloomFilter<V>
where
    V: AsRef<[u8]> + Send + Sync + 'static, {
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
impl<V> AsyncRLockable for AsyncRBloomFilter<V>
where
V: AsRef<[u8]> + Send + Sync + 'static, {
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