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
use std::time::{Duration, SystemTime};


// === AsyncRDelayedQueue Asynchronous delay queues ===
pub struct AsyncRDelayedQueue<V> {
    base: AsyncBaseDistributedObject,
    delay_queue_name: String,
    _marker: std::marker::PhantomData<V>,
}

impl<V> AsyncRDelayedQueue<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String) -> Self {
        let delay_queue_name = format!("{}:delayed", name);
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
            delay_queue_name,
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn offer(&self, value: &V, delay: Duration) -> RedissonResult<bool> {
        let value_json = AsyncBaseDistributedObject::serialize(value)?;
        let deliver_time = SystemTime::now()
            .checked_add(delay)
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let mut conn = self.base.get_connection().await?;

        // Add to the delay queue
        let added: i32 = conn
            .execute_command(
                &mut redis::cmd("ZADD")
                    .arg(&self.delay_queue_name)
                    .arg(deliver_time)
                    .arg(&value_json)
            )
            .await?;

        if added > 0 {
            // Start deferred task processing
            self.schedule_delivery(deliver_time, value_json).await;
        }

        Ok(added > 0)
    }

    async fn schedule_delivery(&self, deliver_time: i64, value_json: String) {
        let queue_name = self.base.get_full_key().to_string();
        let delay_queue_name = self.delay_queue_name.clone();
        let connection_manager = self.base.connection_manager();

        tokio::spawn(async move {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let delay_ms = (deliver_time - now).max(0) as u64;

            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }

            // Move the element from the delayed queue to the actual queue
            if let Ok(mut conn) = connection_manager.get_connection().await {
                let _: i32 = conn
                    .execute_command(&mut redis::cmd("ZREM").arg(&delay_queue_name).arg(&value_json))
                    .await
                    .unwrap_or(0);

                let _: i32 = conn
                    .execute_command(&mut redis::cmd("RPUSH").arg(&queue_name).arg(&value_json))
                    .await
                    .unwrap_or(0);
            }
        });
    }
}


#[async_trait]
impl <V> AsyncRObject for AsyncRDelayedQueue<V>
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
impl <V> AsyncRLockable for AsyncRDelayedQueue<V>
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