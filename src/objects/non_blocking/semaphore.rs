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
use std::time::{Duration, SystemTime};
use serde::de::DeserializeOwned;
use serde::Serialize;
use async_trait::async_trait;
use uuid::Uuid;
use crate::{RedissonResult, AsyncRedisConnectionManager, AsyncRedisConnection, AsyncBaseDistributedObject, AsyncRObject, AsyncRLockable, AsyncRLock, AsyncRFairLock, AsyncRObjectBase, AsyncRRateLimiter, AsyncRMap};



// === AsyncRSemaphore Asynchronous semaphores ===
pub struct AsyncRSemaphore {
    base: AsyncBaseDistributedObject,
    max_permits: usize,
}

impl AsyncRSemaphore {
    pub async fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String, max_permits: usize) -> Self {
        let mut semaphore = Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
            max_permits,
        };

        // Initialize the maximum number of permits
        let _ = semaphore.initialize_max_permits().await;

        semaphore
    }

    async fn initialize_max_permits(&mut self) -> RedissonResult<()> {
        let max_permits_key = format!("{}:max", self.base.get_full_key());
        let mut conn = self.base.get_connection().await?;

        // Only set if it does not exist
        let exists: i32 = conn
            .execute_command(&mut redis::cmd("EXISTS").arg(&max_permits_key))
            .await?;

        if exists == 0 {
            conn.execute_command::<()>(&mut redis::cmd("SET").arg(&max_permits_key).arg(self.max_permits.to_string()))
                .await?;
        }

        Ok(())
    }

    pub async fn acquire(&self) -> RedissonResult<bool> {
        self.try_acquire(1, Duration::from_secs(0)).await
    }

    pub async fn try_acquire(&self, permits: usize, timeout: Duration) -> RedissonResult<bool> {
        let permit_key = Uuid::new_v4().to_string();
        let current_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let timeout_ms = timeout.as_millis() as i64;

        let mut conn = self.base.get_connection().await?;

        let script = redis::Script::new(r#"
            local key = KEYS[1]
            local permit_key = ARGV[1]
            local requested = tonumber(ARGV[2])
            local timeout = tonumber(ARGV[3])
            local now = tonumber(ARGV[4])
            
            -- Gets the current number of licenses
            local current_permits = redis.call('ZCARD', key)
            local max_permits_str = redis.call('GET', key .. ':max')
            local max_permits = max_permits_str and tonumber(max_permits_str) or requested
            
            -- Check that you have enough permissions
            local available = max_permits - current_permits
            if available >= requested then
                -- Assigning permissions
                for i = 1, requested do
                    local permit_id = permit_key .. ':' .. i
                    redis.call('ZADD', key, now + timeout, permit_id)
                end
                return 1
            end
            
            return 0
        "#);

        let result: i32 = script
            .key(self.base.get_full_key())
            .arg(&permit_key)
            .arg(permits as i32)
            .arg(timeout_ms)
            .arg(current_time)
            .invoke_async(&mut conn)
            .await?;

        Ok(result > 0)
    }

    pub async fn release(&self, permits: usize) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;

        let current_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        // Remove expired licenses
        conn.execute_command::<()>(
            &mut redis::cmd("ZREMRANGEBYSCORE")
                .arg(self.base.get_full_key())
                .arg(0)
                .arg(current_time)
        ).await?;

        // Remove a specified number of permissions at random
        let members: Vec<String> = conn
            .execute_command(
                &mut redis::cmd("ZRANGE")
                    .arg(self.base.get_full_key())
                    .arg(0)
                    .arg(permits as i64 - 1)
            )
            .await?;

        for member in members {
            conn.execute_command::<()>(&mut redis::cmd("ZREM").arg(self.base.get_full_key()).arg(member))
                .await?;
        }

        Ok(())
    }

    pub async fn available_permits(&self) -> RedissonResult<usize> {
        let mut conn = self.base.get_connection().await?;
        let current_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        // Cleaning up expired permits
        conn.execute_command::<()>(
            &mut redis::cmd("ZREMRANGEBYSCORE")
                .arg(self.base.get_full_key())
                .arg(0)
                .arg(current_time - 30000)  // 30秒前过期的
        ).await?;

        // Gets the current number of licenses
        let current_permits: i32 = conn
            .execute_command(&mut redis::cmd("ZCARD").arg(self.base.get_full_key()))
            .await?;

        // Get the maximum number of licenses
        let max_permits_key = format!("{}:max", self.base.get_full_key());
        let max_permits_str: Option<String> = conn
            .execute_command(&mut redis::cmd("GET").arg(&max_permits_key))
            .await?;

        let max_permits = max_permits_str
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(self.max_permits as i32);

        Ok((max_permits - current_permits).max(0) as usize)
    }

    pub async fn drain_permits(&self) -> RedissonResult<usize> {
        let available = self.available_permits().await?;
        if available > 0 {
            self.try_acquire(available, Duration::from_secs(0)).await?;
        }
        Ok(available)
    }
}



#[async_trait]
impl AsyncRObject for AsyncRSemaphore  {
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
impl AsyncRLockable for AsyncRSemaphore
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