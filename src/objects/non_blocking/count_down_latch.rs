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
use std::time::Duration;


// === AsyncRCountDownLatch An asynchronous counter ===
pub struct AsyncRCountDownLatch {
    base: AsyncBaseDistributedObject,
}

impl AsyncRCountDownLatch {
    pub async fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String, count: i32) -> Self {
        let latch = Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
        };

        // Initialize the counter
        let _ = latch.try_set_count(count).await;

        latch
    }

    pub async fn await_async(&self, timeout: Option<Duration>) -> RedissonResult<bool> {
        let count = self.get_count().await?;
        if count <= 0 {
            return Ok(true);
        }

        let start = tokio::time::Instant::now();

        if let Some(timeout) = timeout {
            // With a timeout
            while start.elapsed() < timeout {
                let current_count = self.get_count().await?;
                if current_count <= 0 {
                    return Ok(true);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(false)
        } else {
            // Unlimited waiting
            loop {
                let current_count = self.get_count().await?;
                if current_count <= 0 {
                    return Ok(true);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    pub async fn count_down(&self) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;

        let script = redis::Script::new(r#"
            local count = redis.call('GET', KEYS[1])
            if not count then
                count = 0
            else
                count = tonumber(count)
            end
            
            if count > 0 then
                count = count - 1
                redis.call('SET', KEYS[1], count)
                
                if count == 0 then
                    -- Notify all waiters
                    local pattern = KEYS[1] .. ':await:*'
                    local keys = redis.call('KEYS', pattern)
                    for _, key in ipairs(keys) do
                        redis.call('SET', key, '1')
                        redis.call('EXPIRE', key, 1)
                    end
                end
            end
            
            return count
        "#);

        script.key(self.base.get_full_key()).invoke_async::<i32>(&mut conn).await?;
        Ok(())
    }

    pub async fn get_count(&self) -> RedissonResult<i32> {
        let mut conn = self.base.get_connection().await?;
        let count: Option<String> = conn
            .execute_command(&mut redis::cmd("GET").arg(self.base.get_full_key()))
            .await?;

        match count {
            Some(count_str) => Ok(count_str.parse::<i32>().unwrap_or(0)),
            None => Ok(0),
        }
    }

    pub async fn try_set_count(&self, count: i32) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;

        let script = redis::Script::new(r#"
            local current = redis.call('GET', KEYS[1])
            if not current then
                redis.call('SET', KEYS[1], ARGV[1])
                return 1
            else
                return 0
            end
        "#);

        let result: i32 = script
            .key(self.base.get_full_key())
            .arg(count)
            .invoke_async(&mut conn)
            .await?;

        Ok(result > 0)
    }
}


#[async_trait]
impl AsyncRObject for AsyncRCountDownLatch {
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
impl AsyncRLockable for AsyncRCountDownLatch {
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