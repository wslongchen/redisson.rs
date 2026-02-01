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
use crate::{BaseDistributedObject, RFairLock, RLock, RLockable, RObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};
use redis::Commands;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

// === RSet (Unordered set) ===
pub struct RSet<V> {
    base: BaseDistributedObject,
    _marker: std::marker::PhantomData<V>,
}

impl<V: Serialize + serde::de::DeserializeOwned + Eq + std::hash::Hash> RSet<V> {
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: BaseDistributedObject::new(connection_manager, name),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn add(&self, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let value_json = serde_json::to_string(value)?;
        let added: i32 = conn.sadd(self.base.get_name(), value_json)?;
        Ok(added > 0)
    }

    pub fn remove(&self, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let value_json = serde_json::to_string(value)?;
        let removed: i32 = conn.srem(self.base.get_name(), value_json)?;
        Ok(removed > 0)
    }

    pub fn contains(&self, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let value_json = serde_json::to_string(value)?;
        let contains: i32 = conn.sismember(self.base.get_name(), value_json)?;
        Ok(contains > 0)
    }

    pub fn size(&self) -> RedissonResult<usize> {
        let mut conn = self.base.get_connection()?;
        let size: i32 = conn.scard(self.base.get_name())?;
        Ok(size as usize)
    }

    pub fn members(&self) -> RedissonResult<Vec<V>> {
        let mut conn = self.base.get_connection()?;
        let members: Vec<String> = conn.smembers(self.base.get_name())?;
        let mut result = Vec::with_capacity(members.len());
        for member_json in members {
            let value: V = serde_json::from_str(&member_json)?;
            result.push(value);
        }
        Ok(result)
    }
}

impl<V: Serialize + serde::de::DeserializeOwned + Eq + std::hash::Hash> RObject for RSet<V>
{
    fn get_name(&self) -> &str {
        self.base.get_name()
    }

    fn delete(&self) -> RedissonResult<bool> {
        self.base.delete()
    }

    fn rename(&self, new_name: &str) -> RedissonResult<()> {
        self.base.rename(new_name)
    }

    fn is_exists(&self) -> RedissonResult<bool> {
        self.base.is_exists()
    }

    fn move_to_db(&self, db_index: i32) -> RedissonResult<bool> {
        self.base.move_to_db(db_index)
    }

    fn get_expire_time(&self) -> RedissonResult<Option<Duration>> {
        self.base.get_expire_time()
    }

    fn expire(&self, duration: Duration) -> RedissonResult<bool> {
        self.base.expire(duration)
    }

    fn expire_at(&self, timestamp: i64) -> RedissonResult<bool> {
        self.base.expire_at(timestamp)
    }

    fn clear_expire(&self) -> RedissonResult<bool> {
        self.base.clear_expire()
    }
}

impl<V: Serialize + serde::de::DeserializeOwned + Eq + std::hash::Hash> RLockable for RSet<V>
{
    fn get_lock(&self) -> RLock {
        self.base.get_lock()
    }

    fn get_fair_lock(&self) -> RFairLock {
        self.base.get_fair_lock()
    }

    fn lock(&self) -> RedissonResult<()> {
        self.base.lock()
    }

    fn try_lock(&self) -> RedissonResult<bool> {
        self.base.try_lock()
    }

    fn try_lock_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        self.base.try_lock_timeout(wait_time)
    }

    fn lock_lease(&self, lease_time: Duration) -> RedissonResult<()> {
        self.base.lock_lease(lease_time)
    }

    fn unlock(&self) -> RedissonResult<bool> {
        self.base.unlock()
    }

    fn force_unlock(&self) -> RedissonResult<bool> {
        self.base.force_unlock()
    }

    fn is_locked(&self) -> RedissonResult<bool> {
        self.base.is_locked()
    }

    fn is_held_by_current_thread(&self) -> bool {
        self.base.is_held_by_current_thread()
    }
}