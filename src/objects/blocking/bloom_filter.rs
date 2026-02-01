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
use bloom::{BloomFilter, ASMS};
use parking_lot::RwLock;
use crate::{BaseDistributedObject, BloomFilterInfo, RLockable, RObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};

// === RBloomFilter (Bloom filter)===
pub struct RBloomFilter<V> {
    base: BaseDistributedObject,
    bloom_filter: Arc<RwLock<BloomFilter>>,
    _marker: std::marker::PhantomData<V>,
}

impl<V: AsRef<[u8]>> RBloomFilter<V> {
    pub fn new(
        connection_manager: Arc<SyncRedisConnectionManager>,
        name: String,
        expected_insertions: usize,
        false_positive_rate: f64,
    ) -> Self {
        let bloom_filter = BloomFilter::with_rate(false_positive_rate as f32, expected_insertions as u32);
        Self {
            base: BaseDistributedObject::new(connection_manager, name),
            bloom_filter: Arc::new(RwLock::new(bloom_filter)),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn add(&self, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let value_bytes = value.as_ref();
        let added: i32 = redis::cmd("BF.ADD")
            .arg(self.base.get_name())
            .arg(value_bytes)
            .query(&mut conn)?;

        // Update the local bloom filter
        let mut bloom = self.bloom_filter.write();
        bloom.insert(&value_bytes);

        Ok(added > 0)
    }

    pub fn contains(&self, value: &V) -> RedissonResult<bool> {
        // Check the local bloom filter first
        {
            let bloom = self.bloom_filter.read();
            if !bloom.contains(&value.as_ref()) {
                return Ok(false);
            }
        }

        // Local might exist, check Redis
        let mut conn = self.base.get_connection()?;
        let value_bytes = value.as_ref();
        let exists: i32 = redis::cmd("BF.EXISTS")
            .arg(self.base.get_name())
            .arg(value_bytes)
            .query(&mut conn)?;

        Ok(exists > 0)
    }

    pub fn init(&self, capacity: usize, error_rate: f64) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        redis::cmd("BF.RESERVE")
            .arg(self.base.get_name())
            .arg(error_rate)
            .arg(capacity)
            .query::<()>(&mut conn)?;
        Ok(())
    }

    // Adding elements in bulk
    pub fn add_all(&self, values: &[V]) -> RedissonResult<Vec<bool>> {
        let mut results = Vec::with_capacity(values.len());

        for value in values {
            let result = self.add(value)?;
            results.push(result);
        }

        Ok(results)
    }

    // Inspecting elements in bulk
    pub fn contains_all(&self, values: &[V]) -> RedissonResult<Vec<bool>> {
        let mut results = Vec::with_capacity(values.len());

        for value in values {
            let result = self.contains(value)?;
            results.push(result);
        }

        Ok(results)
    }

    // Batch add using BF.MADD (requires RedisBloom module)
    pub fn madd(&self, values: &[V]) -> RedissonResult<Vec<bool>> {
        let mut conn = self.base.get_connection()?;

        let mut cmd = redis::cmd("BF.MADD");
        cmd.arg(self.base.get_name());

        for value in values {
            cmd.arg(value.as_ref());
        }

        let results: Vec<i32> = cmd.query(&mut conn)?;

        // Update the local bloom filter
        {
            let mut bloom = self.bloom_filter.write();
            for value in values {
                bloom.insert(&value.as_ref());
            }
        }

        Ok(results.into_iter().map(|r| r > 0).collect())
    }

    // Batch inspection using BF.MEXISTS (requires RedisBloom module)
    pub fn mexists(&self, values: &[V]) -> RedissonResult<Vec<bool>> {
        // We start with a quick filter using the local bloom filter
        let mut filtered_values = Vec::new();
        let mut local_results = Vec::new();

        {
            let bloom = self.bloom_filter.read();
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
        let mut conn = self.base.get_connection()?;

        let mut cmd = redis::cmd("BF.MEXISTS");
        cmd.arg(self.base.get_name());

        for value in &filtered_values {
            cmd.arg(value.as_ref());
        }

        let redis_results: Vec<i32> = cmd.query(&mut conn)?;

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
    pub fn info(&self) -> RedissonResult<Option<BloomFilterInfo>> {
        let mut conn = self.base.get_connection()?;

        match conn.execute_command(
            &mut redis::cmd("BF.INFO").arg(self.base.get_name())
        ) {
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
    pub fn reset_local_filter(&self) -> RedissonResult<()> {
        // 1. First, get the bloom filter parameters from Redis
        if let Some(info) = self.info()? {
            // 2. Rebuild the local filter based on the actual parameters of Redis
            let mut bloom = self.bloom_filter.write();

            // Reconstruction using actual parameters of Redis
            *bloom = BloomFilter::with_rate(
                self.calculate_error_rate(info.capacity, info.items_inserted),
                info.capacity as u32
            );

            tracing::info!(
            "Local bloom filter reset with Redis parameters: capacity={}, items_inserted={}",
            info.capacity, info.items_inserted
        );
        } else {
            // If no information is available, it is reconstructed with the default parameters
            let mut bloom = self.bloom_filter.write();

            // Keep the original number of bits and hash functions
            let bits = bloom.num_bits();
            let hash_functions = bloom.num_hashes();

            // The same parameters are used for reconstruction
            *bloom = BloomFilter::with_size(bits, hash_functions);

            tracing::debug!(
            "Local bloom filter reset with preserved parameters: bits={}, hash_functions={}",
            bits, hash_functions
        );
        }

        Ok(())
    }

    /// Calculate the error rate (based on capacity and number of inserted items)
    fn calculate_error_rate(&self, capacity: usize, items_inserted: usize) -> f32 {
        if capacity == 0 {
            return 0.01; // Default error rate
        }

        // The error rate is estimated based on the fill rate
        let fill_ratio = items_inserted as f32 / capacity as f32;

        // Prevent overfitting: If the fill rate is too high, use a conservative error rate
        if fill_ratio > 0.8 {
            0.05 // A higher error rate is used when the fill rate is high
        } else if fill_ratio < 0.1 {
            0.01 // A lower error rate is used when the fill rate is low
        } else {
            // Linear interpolation
            let base_rate = 0.01;
            let max_rate = 0.05;
            base_rate + (max_rate - base_rate) * (fill_ratio - 0.1) / 0.7
        }
    }
}



impl <V: AsRef<[u8]>> RObject for RBloomFilter<V> {
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

impl <V: AsRef<[u8]>> RLockable for RBloomFilter<V> {
    fn get_lock(&self) -> crate::lock::RLock {
        self.base.get_lock()
    }

    fn get_fair_lock(&self) -> crate::lock::RFairLock {
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
