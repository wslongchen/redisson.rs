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
use crate::{RedissonResult, AsyncRedisConnectionManager, AsyncRedisConnection, AsyncBaseDistributedObject, AsyncRObject, AsyncRLockable, AsyncRLock, AsyncRFairLock, AsyncRObjectBase, AsyncRSortedSet};

// === AsyncRAtomicLong Asynchronous atomic long ===
pub struct AsyncRAtomicLong {
    base: AsyncBaseDistributedObject,
}

impl AsyncRAtomicLong {
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
        }
    }

    pub async fn get(&self) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection().await?;
        let value: Option<String> = conn
            .execute_command(&mut redis::cmd("GET").arg(self.base.get_full_key()))
            .await?;

        match value {
            Some(val_str) => Ok(val_str.parse::<i64>().unwrap_or(0)),
            None => Ok(0),
        }
    }

    pub async fn set(&self, value: i64) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;
        conn.execute_command::<()>(&mut redis::cmd("SET").arg(self.base.get_full_key()).arg(value.to_string()))
            .await?;
        Ok(())
    }

    pub async fn increment_and_get(&self) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection().await?;
        let result: i64 = conn
            .execute_command(&mut redis::cmd("INCR").arg(self.base.get_full_key()))
            .await?;
        Ok(result)
    }

    pub async fn decrement_and_get(&self) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection().await?;
        let result: i64 = conn
            .execute_command(&mut redis::cmd("DECR").arg(self.base.get_full_key()))
            .await?;
        Ok(result)
    }

    pub async fn add_and_get(&self, delta: i64) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection().await?;
        let result: i64 = conn
            .execute_command(&mut redis::cmd("INCRBY").arg(self.base.get_full_key()).arg(delta))
            .await?;
        Ok(result)
    }

    pub async fn compare_and_set(&self, expect: i64, update: i64) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;

        let script = redis::Script::new(r#"
            local current = redis.call('GET', KEYS[1])
            if (not current and ARGV[1] == '0') or (current and current == ARGV[1]) then
                redis.call('SET', KEYS[1], ARGV[2])
                return 1
            else
                return 0
            end
        "#);

        let result: i32 = script
            .key(self.base.get_full_key())
            .arg(expect.to_string())
            .arg(update.to_string())
            .invoke_async(&mut conn)
            .await?;

        Ok(result > 0)
    }

    pub async fn get_and_set(&self, value: i64) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection().await?;
        let old_value: Option<String> = conn
            .execute_command(&mut redis::cmd("GETSET").arg(self.base.get_full_key()).arg(value.to_string()))
            .await?;

        match old_value {
            Some(val_str) => Ok(val_str.parse::<i64>().unwrap_or(0)),
            None => Ok(0),
        }
    }
}

#[async_trait]
impl AsyncRObject for AsyncRAtomicLong {
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
impl AsyncRLockable for AsyncRAtomicLong {
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
