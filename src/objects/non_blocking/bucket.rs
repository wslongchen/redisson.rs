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
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

// === AsyncRBucket (Asynchronous string key-value pairs) ===
pub struct AsyncRBucket<V> {
    base: AsyncBaseDistributedObject,
    _marker: std::marker::PhantomData<V>,
    default_ttl: Option<Duration>,
}

impl<V> AsyncRBucket<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
            _marker: std::marker::PhantomData,
            default_ttl: None,
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }

    pub async fn set(&self, value: &V) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;
        let json = AsyncBaseDistributedObject::serialize(value)?;

        if let Some(ttl) = self.default_ttl {
            let seconds = ttl.as_secs() as i64;
            // Use pipeline atomic operations
            let mut pipeline = redis::Pipeline::new();
            pipeline
                .cmd("SET")
                .arg(self.base.get_full_key())
                .arg(&json)
                .ignore();
            pipeline
                .cmd("EXPIRE")
                .arg(self.base.get_full_key())
                .arg(seconds)
                .ignore();

            pipeline.query_async::<()>(&mut conn).await?;
        } else {
            conn.execute_command::<()>(&mut redis::cmd("SET").arg(self.base.get_full_key()).arg(&json))
                .await?;
        }

        Ok(())
    }

    pub async fn set_with_ttl(&self, value: &V, ttl: Duration) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;
        let json = AsyncBaseDistributedObject::serialize(value)?;
        let seconds = ttl.as_secs() as i64;

        // Use SETEX atomic operations
        conn.execute_command::<()>(
            &mut redis::cmd("SETEX")
                .arg(self.base.get_full_key())
                .arg(seconds)
                .arg(&json)
        ).await?;

        Ok(())
    }

    pub async fn get(&self) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection().await?;
        let result: Option<String> = conn
            .execute_command(&mut redis::cmd("GET").arg(self.base.get_full_key()))
            .await?;

        match result {
            Some(json) => {
                let value: V = AsyncBaseDistributedObject::deserialize(&json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub async fn get_and_set(&self, value: &V) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection().await?;
        let json = AsyncBaseDistributedObject::serialize(value)?;

        let old_json: Option<String> = conn
            .execute_command(&mut redis::cmd("GETSET").arg(self.base.get_full_key()).arg(&json))
            .await?;

        match old_json {
            Some(old) => {
                let old_value: V = AsyncBaseDistributedObject::deserialize(&old)?;
                Ok(Some(old_value))
            }
            None => Ok(None),
        }
    }

    pub async fn compare_and_set(&self, expect: Option<&V>, update: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;

        match expect {
            Some(exp) => {
                let expect_json = AsyncBaseDistributedObject::serialize(exp)?;
                let update_json = AsyncBaseDistributedObject::serialize(update)?;

                // Atomic CAS using Lua scripts
                let script = redis::Script::new(r#"
                    local current = redis.call('GET', KEYS[1])
                    if current == ARGV[1] or (current == false and ARGV[1] == '') then
                        redis.call('SET', KEYS[1], ARGV[2])
                        return 1
                    else
                        return 0
                    end
                "#);

                let result: i32 = script
                    .key(self.base.get_full_key())
                    .arg(expect_json)
                    .arg(update_json)
                    .invoke_async(&mut conn)
                    .await?;

                Ok(result > 0)
            }
            None => {
                // If the expected value is None, SETNX is used
                let update_json = AsyncBaseDistributedObject::serialize(update)?;
                let result: i32 = conn
                    .execute_command(&mut redis::cmd("SETNX").arg(self.base.get_full_key()).arg(&update_json))
                    .await?;
                Ok(result > 0)
            }
        }
    }

    // Additional methods specific to RBucket
    pub async fn increment(&self, delta: i64) -> RedissonResult<i64>
    where
        V: From<i64> + Into<i64> + Clone,
    {
        let mut conn = self.base.get_connection().await?;

        // For integer types, use INCRBY
        let current: Option<i64> = conn
            .execute_command(&mut redis::cmd("INCRBY").arg(self.base.get_full_key()).arg(delta))
            .await?;

        Ok(current.unwrap_or(delta))
    }

    pub async fn decrement(&self, delta: i64) -> RedissonResult<i64>
    where
        V: From<i64> + Into<i64> + Clone,
    {
        let mut conn = self.base.get_connection().await?;

        // Use DECRBY
        let current: Option<i64> = conn
            .execute_command(&mut redis::cmd("DECRBY").arg(self.base.get_full_key()).arg(delta))
            .await?;

        Ok(current.unwrap_or(-delta))
    }

    pub async fn get_and_delete(&self) -> RedissonResult<Option<V>> {
        let value = self.get().await?;
        if value.is_some() {
            let _ = self.base.delete().await?;
        }
        Ok(value)
    }

    pub async fn size(&self) -> RedissonResult<usize> {
        let mut conn = self.base.get_connection().await?;
        let len: i64 = conn
            .execute_command(&mut redis::cmd("STRLEN").arg(self.base.get_full_key()))
            .await?;
        Ok(len as usize)
    }

    pub async fn set_if_absent(&self, value: &V) -> RedissonResult<bool> {
        let update_json = AsyncBaseDistributedObject::serialize(value)?;
        let mut conn = self.base.get_connection().await?;

        let result: i32 = conn
            .execute_command(&mut redis::cmd("SETNX").arg(self.base.get_full_key()).arg(&update_json))
            .await?;

        Ok(result > 0)
    }

    pub async fn set_if_exists(&self, value: &V) -> RedissonResult<bool> {
        let update_json = AsyncBaseDistributedObject::serialize(value)?;
        let mut conn = self.base.get_connection().await?;

        // Use the SET with XX option
        let result: String = conn
            .execute_command(
                &mut redis::cmd("SET")
                    .arg(self.base.get_full_key())
                    .arg(&update_json)
                    .arg("XX")
            )
            .await?;

        Ok(result == "OK")
    }
}

// Implement async features for AsyncRBucket
#[async_trait]
impl<V> AsyncRObject for AsyncRBucket<V>
where
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
impl<V> AsyncRLockable for AsyncRBucket<V>
where
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