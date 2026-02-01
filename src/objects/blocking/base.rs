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
use crate::{LockableObject, RFairLock, RLock, RLockable, RObject, RObjectBase, RedisConnection, RedissonResult, SyncRedisConnectionManager};

/// The base class of all distributed data structures
#[derive(Clone)]
pub struct BaseDistributedObject {
    connection_manager: Arc<SyncRedisConnectionManager>,
    lockable: LockableObject,
}


impl BaseDistributedObject {
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String) -> Self {
        Self {
            connection_manager,
            lockable: LockableObject::new(name),
        }
    }

    // Public serialization/deserialization methods
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
    
    pub fn connection_manager(&self) -> Arc<SyncRedisConnectionManager> {
        self.connection_manager.clone()
    }
}


impl RObjectBase<String> for BaseDistributedObject {
    fn get_connection(&self) -> RedissonResult<RedisConnection> {
        self.connection_manager.get_connection()
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

impl RObject for BaseDistributedObject {
    fn get_name(&self) -> &str {
        &self.lockable.name
    }

    fn delete(&self) -> RedissonResult<bool> {
        let mut conn = self.get_connection()?;
        let deleted: i32 = redis::cmd("DEL").arg(self.get_full_key()).query(&mut conn)?;
        Ok(deleted > 0)
    }

    fn rename(&self, new_name: &str) -> RedissonResult<()> {
        let mut conn = self.get_connection()?;
        redis::cmd("RENAME")
            .arg(self.get_full_key())
            .arg(new_name)
            .query::<()>(&mut conn)?;
        Ok(())
    }

    fn is_exists(&self) -> RedissonResult<bool> {
        let mut conn = self.get_connection()?;
        let exists: i32 = redis::cmd("EXISTS").arg(self.get_full_key()).query(&mut conn)?;
        Ok(exists > 0)
    }

    fn move_to_db(&self, db_index: i32) -> RedissonResult<bool> {
        let mut conn = self.get_connection()?;
        let moved: i32 = redis::cmd("MOVE")
            .arg(self.get_full_key())
            .arg(db_index)
            .query(&mut conn)?;
        Ok(moved > 0)
    }

    fn get_expire_time(&self) -> RedissonResult<Option<Duration>> {
        let mut conn = self.get_connection()?;
        let ttl: i64 = redis::cmd("TTL").arg(self.get_full_key()).query(&mut conn)?;
        if ttl > 0 {
            Ok(Some(Duration::from_secs(ttl as u64)))
        } else {
            Ok(None)
        }
    }

    fn expire(&self, duration: Duration) -> RedissonResult<bool> {
        let mut conn = self.get_connection()?;
        let seconds = duration.as_secs() as i64;
        let result: i32 = redis::cmd("EXPIRE")
            .arg(self.get_full_key())
            .arg(seconds)
            .query(&mut conn)?;
        Ok(result > 0)
    }

    fn expire_at(&self, timestamp: i64) -> RedissonResult<bool> {
        let mut conn = self.get_connection()?;
        let result: i32 = redis::cmd("EXPIREAT")
            .arg(self.get_full_key())
            .arg(timestamp)
            .query(&mut conn)?;
        Ok(result > 0)
    }

    fn clear_expire(&self) -> RedissonResult<bool> {
        let mut conn = self.get_connection()?;
        let result: i32 = redis::cmd("PERSIST").arg(self.get_full_key()).query(&mut conn)?;
        Ok(result > 0)
    }
}

impl RLockable for BaseDistributedObject {
    fn get_lock(&self) -> RLock {
        RLock::new(self.connection_manager.clone(), self.lockable.get_lock_name(), Duration::from_secs(30))
    }

    fn get_fair_lock(&self) -> RFairLock {
        RFairLock::new(self.connection_manager.clone(), self.lockable.get_fair_lock_name(), Duration::from_secs(30))
    }

    fn lock(&self) -> RedissonResult<()> {
        self.get_lock().lock()
    }

    fn try_lock(&self) -> RedissonResult<bool> {
        self.get_lock().try_lock()
    }

    fn try_lock_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        self.get_lock().try_lock_with_timeout(wait_time)
    }

    fn lock_lease(&self, lease_time: Duration) -> RedissonResult<()> {
        self.get_lock().lock_with_lease_time(lease_time)
    }

    fn unlock(&self) -> RedissonResult<bool> {
        self.get_lock().unlock()
    }

    fn force_unlock(&self) -> RedissonResult<bool> {
        self.get_lock().force_unlock()
    }

    fn is_locked(&self) -> RedissonResult<bool> {
        self.get_lock().is_locked()
    }

    fn is_held_by_current_thread(&self) -> bool {
        self.get_lock().is_held_by_current_thread()
    }
}
