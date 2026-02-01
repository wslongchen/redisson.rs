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
use crate::{AsyncRedisConnection, RedissonResult, AsyncRedisConnectionManager, AsyncRLock, AsyncRFairLock, LockableObject, AsyncRObject, AsyncRObjectBase, AsyncRLockable};

/// An asynchronous base class for all distributed data structures
#[derive(Clone)]
pub struct AsyncBaseDistributedObject {
    connection_manager: Arc<AsyncRedisConnectionManager>,
    lockable: LockableObject,
}

impl AsyncBaseDistributedObject {
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String) -> Self {
        Self {
            connection_manager,
            lockable: LockableObject::new(name),
        }
    }

    // Common serialization/deserialization methods (same as the synchronous version)
    pub fn serialize<T: Serialize>(value: &T) -> RedissonResult<String> {
        serde_json::to_string(value).map_err(|e| crate::errors::RedissonError::SerializationError(e.to_string()))
    }

    pub fn deserialize<T: DeserializeOwned>(data: &str) -> RedissonResult<T> {
        serde_json::from_str(data).map_err(|e| crate::errors::RedissonError::DeserializationError(e.to_string()))
    }

    // Public key manipulation methods
    pub fn build_key(&self, suffix: &str) -> String {
        format!("{}:{}", self.lockable.get_full_key(), suffix)
    }

    pub fn connection_manager(&self) -> Arc<AsyncRedisConnectionManager> {
        self.connection_manager.clone()
    }

    // Get an asynchronous connection
    async fn get_connection(&self) -> RedissonResult<AsyncRedisConnection> {
        self.connection_manager.get_connection().await
    }

    pub fn get_name(&self) -> &str {
        &self.lockable.name
    }

}




#[async_trait]
impl AsyncRObject for AsyncBaseDistributedObject {
    async fn delete(&self) -> RedissonResult<bool> {
        let mut conn = self.get_connection().await?;
        let deleted: i32 = conn
            .execute_command(&mut redis::cmd("DEL").arg(self.lockable.get_full_key()))
            .await?;
        Ok(deleted > 0)
    }

    async fn rename(&self, new_name: &str) -> RedissonResult<()> {
        let mut conn = self.get_connection().await?;
        conn.execute_command::<()>(
            &mut redis::cmd("RENAME")
                .arg(self.lockable.get_full_key())
                .arg(new_name)
        ).await?;
        Ok(())
    }

    async fn is_exists(&self) -> RedissonResult<bool> {
        let mut conn = self.get_connection().await?;
        let exists: i32 = conn
            .execute_command(&mut redis::cmd("EXISTS").arg(self.lockable.get_full_key()))
            .await?;
        Ok(exists > 0)
    }

    async fn move_to_db(&self, db_index: i32) -> RedissonResult<bool> {
        let mut conn = self.get_connection().await?;
        let moved: i32 = conn
            .execute_command(
                &mut redis::cmd("MOVE")
                    .arg(self.lockable.get_full_key())
                    .arg(db_index)
            )
            .await?;
        Ok(moved > 0)
    }

    async fn get_expire_time(&self) -> RedissonResult<Option<Duration>> {
        let mut conn = self.get_connection().await?;
        let ttl: i64 = conn
            .execute_command(&mut redis::cmd("TTL").arg(self.lockable.get_full_key()))
            .await?;

        if ttl > 0 {
            Ok(Some(Duration::from_secs(ttl as u64)))
        } else {
            Ok(None)
        }
    }

    async fn expire(&self, duration: Duration) -> RedissonResult<bool> {
        let mut conn = self.get_connection().await?;
        let seconds = duration.as_secs() as i64;
        let result: i32 = conn
            .execute_command(
                &mut redis::cmd("EXPIRE")
                    .arg(self.lockable.get_full_key())
                    .arg(seconds)
            )
            .await?;
        Ok(result > 0)
    }

    async fn expire_at(&self, timestamp: i64) -> RedissonResult<bool> {
        let mut conn = self.get_connection().await?;
        let result: i32 = conn
            .execute_command(
                &mut redis::cmd("EXPIREAT")
                    .arg(self.lockable.get_full_key())
                    .arg(timestamp)
            )
            .await?;
        Ok(result > 0)
    }

    async fn clear_expire(&self) -> RedissonResult<bool> {
        let mut conn = self.get_connection().await?;
        let result: i32 = conn
            .execute_command(&mut redis::cmd("PERSIST").arg(self.lockable.get_full_key()))
            .await?;
        Ok(result > 0)
    }
}

// For AsyncRLockable AsyncBaseDistributedObject
#[async_trait]
impl AsyncRLockable for AsyncBaseDistributedObject {
    fn get_lock(&self) -> AsyncRLock {
        AsyncRLock::new(
            self.connection_manager.clone(),
            self.lockable.get_lock_name(),
            Duration::from_secs(30)
        )
    }

    fn get_fair_lock(&self) -> AsyncRFairLock {
        AsyncRFairLock::new(
            self.connection_manager.clone(),
            self.lockable.get_fair_lock_name(),
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

// Implement AsyncRObjectBase on String
#[async_trait]
impl AsyncRObjectBase<String> for AsyncBaseDistributedObject {
    async fn get_connection(&self) -> RedissonResult<AsyncRedisConnection> {
        self.connection_manager.get_connection().await
    }

    fn serialize_value(&self, value: &String) -> RedissonResult<String> {
        Ok(value.clone())
    }

    fn deserialize_value(&self, data: &str) -> RedissonResult<String> {
        Ok(data.to_string())
    }

    fn get_full_key(&self) -> String {
        self.lockable.get_full_key().to_string()
    }
}