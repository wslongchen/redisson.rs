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
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::{BaseDistributedObject, RFairLock, RLock, RLockable, RObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};

// === RBucket(String key-value pairs)===
pub struct RBucket<V> {
    base: BaseDistributedObject,
    _marker: std::marker::PhantomData<V>,
    // Bucket-specific fields
    default_ttl: Option<Duration>,
}
impl<V> RBucket<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: BaseDistributedObject::new(connection_manager, name),
            _marker: std::marker::PhantomData,
            default_ttl: None,
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }

    pub fn set(&self, value: &V) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        let json = BaseDistributedObject::serialize(value)?;

        if let Some(ttl) = self.default_ttl {
            let seconds = ttl.as_secs() as i64;
            redis::pipe()
                .atomic()
                .set(self.base.get_full_key(), &json)
                .expire(self.base.get_full_key(), seconds)
                .query::<()>(&mut conn)?;
        } else {
            conn.set::<_, _, ()>(self.base.get_full_key(), &json)?;
        }

        Ok(())
    }

    pub fn set_with_ttl(&self, value: &V, ttl: Duration) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        let json = BaseDistributedObject::serialize(value)?;
        let seconds = ttl.as_secs() as i64;

        redis::pipe()
            .atomic()
            .set(self.base.get_full_key(), &json)
            .expire(self.base.get_full_key(), seconds)
            .query::<()>(&mut conn)?;

        Ok(())
    }

    pub fn get(&self) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection()?;
        let result: Option<String> = conn.get(self.base.get_full_key())?;

        match result {
            Some(json) => {
                let value: V = BaseDistributedObject::deserialize(&json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub fn get_and_set(&self, value: &V) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection()?;
        let json = BaseDistributedObject::serialize(value)?;

        let old_json: Option<String> = conn.getset(self.base.get_full_key(), &json)?;

        match old_json {
            Some(old) => {
                let old_value: V = BaseDistributedObject::deserialize(&old)?;
                Ok(Some(old_value))
            }
            None => Ok(None),
        }
    }

    pub fn compare_and_set(&self, expect: Option<&V>, update: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;

        match expect {
            Some(exp) => {
                let expect_json = BaseDistributedObject::serialize(exp)?;
                let update_json = BaseDistributedObject::serialize(update)?;

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
                    .invoke(&mut conn)?;

                Ok(result > 0)
            }
            None => {
                // If the expected value is None, SETNX is used
                let update_json = BaseDistributedObject::serialize(update)?;
                let result: i32 = conn.set_nx(self.base.get_full_key(), &update_json)?;
                Ok(result > 0)
            }
        }
    }

    // Additional methods specific to RBucket
    pub fn increment(&self, delta: i64) -> RedissonResult<i64>
    where
        V: From<i64> + Into<i64>,
    {
        let mut conn = self.base.get_connection()?;
        let result: i64 = conn.incr(self.base.get_full_key(), delta)?;
        Ok(result)
    }

    pub fn decrement(&self, delta: i64) -> RedissonResult<i64>
    where
        V: From<i64> + Into<i64>,
    {
        let mut conn = self.base.get_connection()?;
        let result: i64 = conn.decr(self.base.get_full_key(), delta)?;
        Ok(result)
    }
}

// Implement the lock trait for RBucket
impl<V> RObject for RBucket<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
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

impl<V> RLockable for RBucket<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
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