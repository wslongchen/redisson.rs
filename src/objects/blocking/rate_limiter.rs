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
use crate::{scripts, BaseDistributedObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};

/// === RRateLimiter (current limiter) ===
pub struct RRateLimiter {
    base: BaseDistributedObject,
    rate: f64,      // Number of tokens generated per second
    capacity: f64,  // Token bucket capacity
}

impl RRateLimiter {
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String, rate: f64, capacity: f64) -> Self {
        Self {
            base: BaseDistributedObject::new(connection_manager, name),
            rate,
            capacity,
        }
    }

    pub fn try_acquire(&self, permits: f64) -> RedissonResult<bool> {
        self.try_acquire_with_timeout(permits, Duration::from_secs(0))
    }

    pub fn try_acquire_with_timeout(&self, permits: f64, timeout: Duration) -> RedissonResult<bool> {
        let start_time = std::time::Instant::now();

        while start_time.elapsed() < timeout {
            if self.acquire_once(permits)? {
                return Ok(true);
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // One last attempt
        self.acquire_once(permits)
    }

    pub fn acquire(&self, permits: f64) -> RedissonResult<()> {
        let mut backoff = Duration::from_millis(10);
        let max_backoff = Duration::from_secs(1);

        while !self.acquire_once(permits)? {
            std::thread::sleep(backoff);
            backoff = backoff * 2;
            if backoff > max_backoff {
                backoff = max_backoff;
            }
        }

        Ok(())
    }

    fn acquire_once(&self, permits: f64) -> RedissonResult<bool> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut conn = self.base.get_connection()?;
        let result: i32 = scripts::RATE_LIMITER_SCRIPT
            .key(self.base.get_full_key())
            .arg(self.rate)
            .arg(self.capacity)
            .arg(permits)
            .arg(now)
            .invoke(&mut conn)?;

        Ok(result > 0)
    }

    pub fn get_rate(&self) -> f64 {
        self.rate
    }

    pub fn set_rate(&self, new_rate: f64) -> RedissonResult<()> {
        // Atomic operations are required to update the rate
        let rate_key = format!("{}:rate", self.base.get_full_key());
        let mut conn = self.base.get_connection()?;
        conn.set::<_, _, ()>(&rate_key, new_rate.to_string())?;
        Ok(())
    }
}
