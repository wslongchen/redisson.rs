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
use redis::Commands;
use crate::{scripts, BaseDistributedObject, RLockable, RObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};

/// === RAtomicLong (atomic long) ===
pub struct RAtomicLong {
    base: BaseDistributedObject,
}

impl RAtomicLong {
    pub fn new(client: Arc<SyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: BaseDistributedObject::new(client, name),
        }
    }

    pub fn get(&self) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection()?;
        let value: Option<String> = conn.get(self.base.get_full_key())?;

        match value {
            Some(val_str) => Ok(val_str.parse::<i64>().unwrap_or(0)),
            None => Ok(0),
        }
    }

    pub fn set(&self, value: i64) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        conn.set::<_, _, ()>(self.base.get_full_key(), value.to_string())?;
        Ok(())
    }

    pub fn increment_and_get(&self) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection()?;
        let result: i64 = conn.incr(self.base.get_full_key(), 1)?;
        Ok(result)
    }

    pub fn decrement_and_get(&self) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection()?;
        let result: i64 = conn.decr(self.base.get_full_key(), 1)?;
        Ok(result)
    }

    pub fn add_and_get(&self, delta: i64) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection()?;
        let result: i64 = conn.incr(self.base.get_full_key(), delta)?;
        Ok(result)
    }

    pub fn compare_and_set(&self, expect: i64, update: i64) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;

        let result: i32 = scripts::COMPARE_AND_SET_SCRIPT
            .key(self.base.get_full_key())
            .arg(expect.to_string())
            .arg(update.to_string())
            .invoke(&mut conn)?;

        Ok(result > 0)
    }
}

impl RObject for RAtomicLong {
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

impl RLockable for RAtomicLong {
    fn get_lock(&self) -> crate::lock::RLock {
        self.base.get_lock()
    }

    fn get_fair_lock(&self) -> crate::lock::RFairLock {
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
