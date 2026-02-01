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
use crate::{RedissonResult, AsyncRedisConnectionManager, AsyncRedisConnection, AsyncBaseDistributedObject, AsyncRObject, AsyncRLockable, AsyncRLock, AsyncRFairLock, AsyncRObjectBase, AsyncRSet};


// === AsyncRSortedSet Asynchronously ordered collections ===
pub struct AsyncRSortedSet<V> {
    base: AsyncBaseDistributedObject,
    _marker: std::marker::PhantomData<V>,
}

impl<V> AsyncRSortedSet<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn add(&self, value: &V, score: f64) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        let added: i32 = conn
            .execute_command(&mut redis::cmd("ZADD").arg(self.base.get_full_key()).arg(score).arg(value_json))
            .await?;

        Ok(added > 0)
    }

    pub async fn remove(&self, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        let removed: i32 = conn
            .execute_command(&mut redis::cmd("ZREM").arg(self.base.get_full_key()).arg(value_json))
            .await?;

        Ok(removed > 0)
    }

    pub async fn score(&self, value: &V) -> RedissonResult<Option<f64>> {
        let mut conn = self.base.get_connection().await?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        let score: Option<f64> = conn
            .execute_command(&mut redis::cmd("ZSCORE").arg(self.base.get_full_key()).arg(value_json))
            .await?;

        Ok(score)
    }

    pub async fn range(&self, start: i64, stop: i64) -> RedissonResult<Vec<V>> {
        let mut conn = self.base.get_connection().await?;
        let members: Vec<String> = conn
            .execute_command(&mut redis::cmd("ZRANGE").arg(self.base.get_full_key()).arg(start).arg(stop))
            .await?;

        let mut result = Vec::with_capacity(members.len());
        for member_json in members {
            let value: V = AsyncBaseDistributedObject::deserialize(&member_json)?;
            result.push(value);
        }
        Ok(result)
    }

    pub async fn rev_range(&self, start: i64, stop: i64) -> RedissonResult<Vec<V>> {
        let mut conn = self.base.get_connection().await?;
        let members: Vec<String> = conn
            .execute_command(&mut redis::cmd("ZREVRANGE").arg(self.base.get_full_key()).arg(start).arg(stop))
            .await?;

        let mut result = Vec::with_capacity(members.len());
        for member_json in members {
            let value: V = AsyncBaseDistributedObject::deserialize(&member_json)?;
            result.push(value);
        }
        Ok(result)
    }

    pub async fn range_by_score(&self, min: f64, max: f64) -> RedissonResult<Vec<V>> {
        let mut conn = self.base.get_connection().await?;
        let members: Vec<String> = conn
            .execute_command(&mut redis::cmd("ZRANGEBYSCORE").arg(self.base.get_full_key()).arg(min).arg(max))
            .await?;

        let mut result = Vec::with_capacity(members.len());
        for member_json in members {
            let value: V = AsyncBaseDistributedObject::deserialize(&member_json)?;
            result.push(value);
        }
        Ok(result)
    }

    pub async fn rank(&self, value: &V) -> RedissonResult<Option<usize>> {
        let mut conn = self.base.get_connection().await?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        let rank: Option<i64> = conn
            .execute_command(&mut redis::cmd("ZRANK").arg(self.base.get_full_key()).arg(value_json))
            .await?;

        rank.map(|r| Ok(r as usize)).transpose()
    }

    pub async fn rev_rank(&self, value: &V) -> RedissonResult<Option<usize>> {
        let mut conn = self.base.get_connection().await?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        let rank: Option<i64> = conn
            .execute_command(&mut redis::cmd("ZREVRANK").arg(self.base.get_full_key()).arg(value_json))
            .await?;

        rank.map(|r| Ok(r as usize)).transpose()
    }
}

#[async_trait]
impl<V> AsyncRObject for AsyncRSortedSet<V>
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
impl<V> AsyncRLockable for AsyncRSortedSet<V>
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