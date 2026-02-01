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
use crate::{RedissonResult, AsyncRedisConnectionManager, AsyncRedisConnection, AsyncBaseDistributedObject, AsyncRObject, AsyncRLockable, AsyncRLock, AsyncRFairLock, AsyncRObjectBase, AsyncRBitSet, AsyncRGeo};


// === AsyncRBlockingQueue Asynchronously blocking queues ===
pub struct AsyncRBlockingQueue<V> {
    base: AsyncBaseDistributedObject,
    _marker: std::marker::PhantomData<V>,
}

impl<V> AsyncRBlockingQueue<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn offer(&self, value: &V, timeout: Option<Duration>) -> RedissonResult<bool> {
        let value_json = AsyncBaseDistributedObject::serialize(value)?;
        let mut conn = self.base.get_connection().await?;

        if let Some(timeout) = timeout {
            // Blocking push with timeout
            let result: Option<String> = conn
                .execute_command(
                    &mut redis::cmd("BRPOPLPUSH")
                        .arg(self.base.get_full_key())
                        .arg(&value_json)
                        .arg(timeout.as_secs_f64())
                )
                .await?;
            Ok(result.is_some())
        } else {
            // Non-blocking push
            let len: i32 = conn
                .execute_command(&mut redis::cmd("RPUSH").arg(self.base.get_full_key()).arg(value_json))
                .await?;
            Ok(len > 0)
        }
    }

    pub async fn poll(&self, timeout: Option<Duration>) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection().await?;

        if let Some(timeout) = timeout {
            // Blocking pop with timeout
            let result: Option<(String, String)> = conn
                .execute_command(
                    &mut redis::cmd("BRPOP")
                        .arg(self.base.get_full_key())
                        .arg(timeout.as_secs_f64())
                )
                .await?;

            if let Some((_, value_json)) = result {
                let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            } else {
                Ok(None)
            }
        } else {
            // Non-blocking pop
            let result: Option<String> = conn
                .execute_command(&mut redis::cmd("LPOP").arg(self.base.get_full_key()))
                .await?;

            match result {
                Some(value_json) => {
                    let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                    Ok(Some(value))
                }
                None => Ok(None),
            }
        }
    }

    pub async fn peek(&self) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection().await?;
        let result: Option<String> = conn
            .execute_command(&mut redis::cmd("LINDEX").arg(self.base.get_full_key()).arg(0))
            .await?;

        match result {
            Some(value_json) => {
                let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub async fn size(&self) -> RedissonResult<usize> {
        let mut conn = self.base.get_connection().await?;
        let len: i32 = conn
            .execute_command(&mut redis::cmd("LLEN").arg(self.base.get_full_key()))
            .await?;
        Ok(len as usize)
    }

    pub async fn remaining_capacity(&self) -> RedissonResult<usize> {
        // Redis lists have no capacity limit and return a large number
        Ok(usize::MAX)
    }
}



#[async_trait]
impl <V> AsyncRObject for AsyncRBlockingQueue<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static, {
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
impl <V> AsyncRLockable for AsyncRBlockingQueue<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + Default + 'static, {
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