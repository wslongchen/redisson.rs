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


// === AsyncRScript Asynchronous script execution ===
pub struct AsyncRScript {
    base: AsyncBaseDistributedObject,
}

impl AsyncRScript {
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name.to_string()),
        }
    }

    pub async fn eval<T>(
        &self,
        script: &str,
        keys: &[String],
        args: &[String],
    ) -> RedissonResult<T>
    where
        T: redis::FromRedisValue + Send + 'static,
    {
        let mut conn = self.base.get_connection().await?;
        let script_obj = redis::Script::new(script);
        let result = script_obj
            .key(keys)
            .arg(args)
            .invoke_async(&mut conn)
            .await?;
        Ok(result)
    }

    pub async fn eval_sha<T>(
        &self,
        sha1: &str,
        keys: &[String],
        args: &[String],
    ) -> RedissonResult<T>
    where
        T: redis::FromRedisValue + Send + 'static,
    {
        let mut conn = self.base.get_connection().await?;

        let result: T = redis::cmd("EVALSHA")
            .arg(sha1)
            .arg(keys.len())
            .arg(keys)
            .arg(args)
            .query_async(&mut conn)
            .await?;

        Ok(result)
    }

    pub async fn script_load(&self, script: &str) -> RedissonResult<String> {
        let mut conn = self.base.get_connection().await?;
        let sha1: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(script)
            .query_async(&mut conn)
            .await?;

        Ok(sha1)
    }

    pub async fn script_exists(&self, sha1: &str) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let exists: Vec<i32> = redis::cmd("SCRIPT")
            .arg("EXISTS")
            .arg(sha1)
            .query_async(&mut conn)
            .await?;

        Ok(exists.get(0).map_or(false, |&v| v == 1))
    }

    pub async fn script_flush(&self) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;
        redis::cmd("SCRIPT")
            .arg("FLUSH")
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn script_kill(&self) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;
        redis::cmd("SCRIPT")
            .arg("KILL")
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }
}



#[async_trait]
impl AsyncRObject for AsyncRScript  {
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
impl AsyncRLockable for AsyncRScript
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