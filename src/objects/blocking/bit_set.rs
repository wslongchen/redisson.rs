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
use crate::{BaseDistributedObject, RAtomicLong, RLockable, RObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};

/// === RBitSet (bit set) ===
pub struct RBitSet {
    base: BaseDistributedObject,
}

impl RBitSet {
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: BaseDistributedObject::new(connection_manager, name),
        }
    }

    pub fn set(&self, bit_index: i64) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let previous: i32 = conn.setbit(self.base.get_full_key(), bit_index as usize, true)?;
        Ok(previous > 0)
    }

    pub fn clear(&self, bit_index: i64) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let previous: i32 = conn.setbit(self.base.get_full_key(), bit_index as usize, false)?;
        Ok(previous > 0)
    }

    pub fn get(&self, bit_index: i64) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let value: i32 = conn.getbit(self.base.get_full_key(), bit_index as usize)?;
        Ok(value > 0)
    }

    pub fn cardinality(&self) -> RedissonResult<u64> {
        let mut conn = self.base.get_connection()?;
        let count: u64 = conn.bitcount(self.base.get_full_key())?;
        Ok(count)
    }

    pub fn and(&self, other_set: &RBitSet) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        redis::cmd("BITOP")
            .arg("AND")
            .arg(self.base.get_full_key())
            .arg(self.base.get_full_key())
            .arg(other_set.base.get_full_key())
            .query::<()>(&mut conn)?;
        Ok(())
    }

    pub fn or(&self, other_set: &RBitSet) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        redis::cmd("BITOP")
            .arg("OR")
            .arg(self.base.get_full_key())
            .arg(self.base.get_full_key())
            .arg(other_set.base.get_full_key())
            .query::<()>(&mut conn)?;
        Ok(())
    }

    pub fn xor(&self, other_set: &RBitSet) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        redis::cmd("BITOP")
            .arg("XOR")
            .arg(self.base.get_full_key())
            .arg(self.base.get_full_key())
            .arg(other_set.base.get_full_key())
            .query::<()>(&mut conn)?;
        Ok(())
    }
}



impl RObject for RBitSet {
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

impl RLockable for RBitSet {
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
