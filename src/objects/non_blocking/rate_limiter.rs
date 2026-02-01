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
use crate::{AsyncBaseDistributedObject, AsyncRFairLock, AsyncRLock, AsyncRLockable, AsyncRObject, AsyncRObjectBase, AsyncRedisConnectionManager, RedissonResult};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, SystemTime};


// === AsyncRRateLimiter Asynchronous current limiter ===
pub struct AsyncRRateLimiter {
    base: AsyncBaseDistributedObject,
    rate: f64,      // Number of tokens generated per second
    capacity: f64,  // Token bucket capacity
}

impl AsyncRRateLimiter {
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String, rate: f64, capacity: f64) -> Self {
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
            rate,
            capacity,
        }
    }

    pub async fn try_acquire(&self, permits: f64) -> RedissonResult<bool> {
        self.try_acquire_with_timeout(permits, Duration::from_secs(0)).await
    }

    pub async fn try_acquire_with_timeout(&self, permits: f64, timeout: Duration) -> RedissonResult<bool> {
        let start_time = tokio::time::Instant::now();

        while start_time.elapsed() < timeout {
            if self.acquire_once(permits).await? {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // One last attempt
        self.acquire_once(permits).await
    }

    pub async fn acquire(&self, permits: f64) -> RedissonResult<()> {
        let mut backoff = Duration::from_millis(10);
        let max_backoff = Duration::from_secs(1);

        while !self.acquire_once(permits).await? {
            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, max_backoff);
        }

        Ok(())
    }

    async fn acquire_once(&self, permits: f64) -> RedissonResult<bool> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut conn = self.base.get_connection().await?;

        let script = redis::Script::new(r#"
            local key = KEYS[1]
            local rate = tonumber(ARGV[1])
            local capacity = tonumber(ARGV[2])
            local requested = tonumber(ARGV[3])
            local now = tonumber(ARGV[4])
            
            -- Gets the current number of tokens
            local current_tokens = tonumber(redis.call('GET', key) or capacity)
            local last_refill = tonumber(redis.call('GET', key .. ':last_refill') or now)
            
            -- Calculate the token that should be added
            local time_passed = now - last_refill
            local tokens_to_add = time_passed * rate
            
            if tokens_to_add > 0 then
                current_tokens = math.min(capacity, current_tokens + tokens_to_add)
                redis.call('SET', key .. ':last_refill', now)
            end
            
            -- Check that there are enough tokens
            if current_tokens >= requested then
                current_tokens = current_tokens - requested
                redis.call('SET', key, current_tokens)
                return 1
            else
                return 0
            end
        "#);

        let result: i32 = script
            .key(self.base.get_full_key())
            .arg(self.rate)
            .arg(self.capacity)
            .arg(permits)
            .arg(now)
            .invoke_async(&mut conn)
            .await?;

        Ok(result > 0)
    }

    pub fn get_rate(&self) -> f64 {
        self.rate
    }

    pub async fn set_rate(&self, new_rate: f64) -> RedissonResult<()> {
        // Atomic operations are required to update the rate
        let rate_key = format!("{}:rate", self.base.get_full_key());
        let mut conn = self.base.get_connection().await?;
        conn.execute_command::<()>(&mut redis::cmd("SET").arg(&rate_key).arg(new_rate.to_string()))
            .await?;
        Ok(())
    }
}


#[async_trait]
impl AsyncRObject for AsyncRRateLimiter  {
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
impl AsyncRLockable for AsyncRRateLimiter
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