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
use crate::{scripts, BaseDistributedObject, RObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};

/// === RSemaphore (SEMAPHORE) ===
pub struct RSemaphore {
    base: BaseDistributedObject,
    max_permits: usize,
}

impl RSemaphore {
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String, max_permits: usize) -> Self {
        let mut semaphore = Self {
            base: BaseDistributedObject::new(connection_manager, name),
            max_permits,
        };

        // Initialize the maximum number of permits
        semaphore.initialize_max_permits().unwrap_or_default();

        semaphore
    }

    fn initialize_max_permits(&mut self) -> RedissonResult<()> {
        let max_permits_key = format!("{}:max", self.base.get_full_key());
        let mut conn = self.base.get_connection()?;

        // Only set if it does not exist
        let exists: i32 = conn.exists(&max_permits_key)?;
        if exists == 0 {
            conn.set::<_, _, ()>(&max_permits_key, self.max_permits.to_string())?;
        }

        Ok(())
    }

    pub fn acquire(&self) -> RedissonResult<bool> {
        self.try_acquire(1, Duration::from_secs(0))
    }

    pub fn try_acquire(&self, permits: usize, timeout: Duration) -> RedissonResult<bool> {
        let permit_key = uuid::Uuid::new_v4().to_string();
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let timeout_ms = timeout.as_millis() as i64;

        let mut conn = self.base.get_connection()?;
        let result: i32 = scripts::SEMAPHORE_ACQUIRE_SCRIPT
            .key(self.base.get_full_key())
            .arg(&permit_key)
            .arg(permits as i32)
            .arg(timeout_ms)
            .arg(current_time)
            .invoke(&mut conn)?;

        Ok(result > 0)
    }

    pub fn release(&self, permits: usize) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;

        // 移除许可
        let _pattern = format!("*:{}:*", self.base.get_name());
        let permits_to_remove: Vec<String> = conn.zrangebyscore_limit(
            self.base.get_full_key(),
            0,
            std::i64::MAX,
            0,
            permits as isize,
        )?;

        for permit in permits_to_remove {
            conn.zrem::<_, _, ()>(self.base.get_full_key(), permit)?;
        }

        Ok(())
    }

    pub fn available_permits(&self) -> RedissonResult<usize> {
        let mut conn = self.base.get_connection()?;
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        // Cleaning up expired permits
        conn.zremrangebyrank::<_, ()>(self.base.get_full_key(), 0, (current_time - 30000) as isize)?;

        // Gets the current number of licenses
        let current_permits: i32 = conn.zcard(self.base.get_full_key())?;

        // Get the maximum number of licenses
        let max_permits_key = format!("{}:max", self.base.get_full_key());
        let max_permits_str: Option<String> = conn.get(&max_permits_key)?;
        let max_permits = max_permits_str
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(self.max_permits as i32);

        Ok((max_permits - current_permits).max(0) as usize)
    }

    pub fn drain_permits(&self) -> RedissonResult<usize> {
        let available = self.available_permits()?;
        if available > 0 {
            self.try_acquire(available, Duration::from_secs(0))?;
        }
        Ok(available)
    }
}
