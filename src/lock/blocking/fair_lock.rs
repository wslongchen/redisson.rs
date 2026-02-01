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
use std::thread;
use std::time::{Duration, Instant};
use parking_lot::Mutex;
use redis::Commands;
use uuid::Uuid;
use crate::{scripts, RedissonError, RedissonResult, SyncRedisConnectionManager};

/// === RFairLock ===
pub struct RFairLock {
    connection_manager: Arc<SyncRedisConnectionManager>,
    name: String,
    lease_time: Duration,
    local_lock_value: Arc<Mutex<Option<String>>>,
}

impl RFairLock {
    pub fn new(
        connection_manager: Arc<SyncRedisConnectionManager>,
        name: String,
        lease_time: Duration,
    ) -> Self {
        Self {
            connection_manager,
            name,
            lease_time,
            local_lock_value: Arc::new(Mutex::new(None)),
        }
    }

    pub fn lock(&self) -> RedissonResult<()> {
        self.lock_with_lease_time(self.lease_time)
    }

    pub fn lock_with_lease_time(&self, lease_time: Duration) -> RedissonResult<()> {
        let queue_name = format!("{}:queue", self.name);
        let lock_value = Uuid::new_v4().to_string();

        // Save the lock value locally
        *self.local_lock_value.lock() = Some(lock_value.clone());

        // Join the queue
        let mut conn = self.connection_manager.get_connection()?;
        let position: i64 = conn.rpush(&queue_name, &lock_value)?;

        // Wait to be the team leader
        let mut attempts = 0;
        while position > 1 {
            thread::sleep(Duration::from_millis(100));

            let front: Option<String> = conn.lindex(&queue_name, 0)?;
            if front == Some(lock_value.clone()) {
                break;
            }

            attempts += 1;
            if attempts > 100 { // Max 100 attempts
                // Clearing the queue
                let _: i64 = conn.lrem(&queue_name, 1, &lock_value)?;
                *self.local_lock_value.lock() = None;
                return Err(RedissonError::TimeoutError);
            }
        }

        // Acquiring locks
        let acquired: i32 = scripts::LOCK_SCRIPT
            .key(&self.name)
            .arg(&lock_value)
            .arg(lease_time.as_millis() as i64)
            .invoke(&mut conn)?;

        if acquired > 0 {
            // Remove from the queue
            let _: i64 = conn.lrem(&queue_name, 1, &lock_value)?;
            Ok(())
        } else {
            // Clearing the queue
            let _: i64 = conn.lrem(&queue_name, 1, &lock_value)?;
            *self.local_lock_value.lock() = None;
            Err(RedissonError::LockAcquisitionError)
        }
    }

    pub fn unlock(&self) -> RedissonResult<bool> {
        // Retrieve the lock-value stored locally
        let lock_value = self.local_lock_value.lock().take();

        if let Some(lock_value) = lock_value {
            let mut conn = self.connection_manager.get_connection()?;

            let released: i32 = scripts::UNLOCK_SCRIPT
                .key(&self.name)
                .arg(&lock_value)
                .invoke(&mut conn)?;

            Ok(released > 0)
        } else {
            Ok(false)
        }
    }

    pub fn try_lock(&self) -> RedissonResult<bool> {
        self.try_lock_with_timeout(Duration::from_secs(0))
    }

    pub fn try_lock_with_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        let start = Instant::now();

        while start.elapsed() < wait_time {
            match self.lock() {
                Ok(_) => return Ok(true),
                Err(_) => thread::sleep(Duration::from_millis(50)),
            }
        }

        Ok(false)
    }
}



impl Clone for RFairLock {
    fn clone(&self) -> Self {
        Self {
            connection_manager: self.connection_manager.clone(),
            name: self.name.clone(),
            lease_time: self.lease_time,
            local_lock_value: Arc::new(Mutex::new(None)),
        }
    }
}
