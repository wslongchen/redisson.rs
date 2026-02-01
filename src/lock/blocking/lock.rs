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
use crate::lock::LocalLockState;
use crate::{scripts,  LockInfo, LockWatchdog, RedissonError, RedissonResult, SyncRedisConnectionManager};
use parking_lot::Mutex;
use redis::Commands;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};


/// === RLock (Reentrant lock) ===
pub struct RLock {
    connection_manager: Arc<SyncRedisConnectionManager>,
    name: String,
    lease_time: Duration,
    local_locks: Arc<Mutex<HashMap<String, LocalLockState>>>,
    watchdog: Arc<Mutex<Option<LockWatchdog>>>,
}

impl RLock {
    pub fn new(
        connection_manager: Arc<SyncRedisConnectionManager>,
        name: String,
        lease_time: Duration,
    ) -> Self {
        Self {
            connection_manager,
            name,
            lease_time,
            local_locks: Arc::new(Mutex::new(HashMap::new())),
            watchdog: Arc::new(Mutex::new(None)),
        }
    }

    /// Lock acquisition (blocking)
    pub fn lock(&self) -> RedissonResult<()> {
        self.lock_with_lease_time(self.lease_time)
    }

    /// Lock with lease duration
    pub fn lock_with_lease_time(&self, lease_time: Duration) -> RedissonResult<()> {
        let lock_info = self.try_acquire_with_retry(lease_time, None)?;

        // Start the watchdog (if the lease is 0, it needs to be automatically renewed)
        if lease_time.as_secs() == 0 {
            self.start_watchdog(lock_info.value.clone());
        }

        Ok(())
    }

    /// Attempt to acquire lock (non-blocking)
    pub fn try_lock(&self) -> RedissonResult<bool> {
        self.try_lock_with_timeout(Duration::from_secs(0))
    }

    /// An attempt to acquire a lock with a timeout
    pub fn try_lock_with_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        match self.try_acquire_with_retry(self.lease_time, Some(wait_time)) {
            Ok(lock_info) => {
                // 启动看门狗
                if self.lease_time.as_secs() == 0 {
                    self.start_watchdog(lock_info.value.clone());
                }
                Ok(true)
            }
            Err(RedissonError::TimeoutError) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Release a lock
    pub fn unlock(&self) -> RedissonResult<bool> {
        let mut local_locks = self.local_locks.lock();

        // Checks if the lock is held locally
        if let Some(state) = local_locks.get_mut(&self.name) {
            // Check the reentry count
            if state.lock_count > 1 {
                state.lock_count -= 1;
                return Ok(true);
            }

            // Acquire connection and release Redis lock
            let mut conn = self.connection_manager.get_connection()?;

            // Passing the correct parameter types
            let released: i32 = scripts::UNLOCK_SCRIPT
                .key(&self.name)
                .arg(&state.lock_value)
                .invoke(&mut conn)?;

            // Stop the watchdog
            if let Some(mut watchdog) = self.watchdog.lock().take() {
                watchdog.stop();
            }

            if released > 0 {
                local_locks.remove(&self.name);
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    /// Attempt to acquire lock (with retry)
    fn try_acquire_with_retry(
        &self,
        lease_time: Duration,
        wait_time: Option<Duration>,
    ) -> RedissonResult<LockInfo> {
        let start_time = Instant::now();
        let lock_info = LockInfo::new(self.name.clone(), lease_time);

        loop {
            // Check for local reentry
            {
                let mut local_locks = self.local_locks.lock();
                if let Some(state) = local_locks.get_mut(&self.name) {
                    // Check for reentry of the same thread
                    if state.lock_value == lock_info.value {
                        state.lock_count += 1;
                        state.last_renew_time = Instant::now();
                        return Ok(lock_info);
                    }
                }
            }

            // Attempt to acquire a Redis lock
            let mut conn = self.connection_manager.get_connection()?;
            
            let acquired: i32 = scripts::LOCK_SCRIPT
                .key(&self.name)
                .arg(&lock_info.value)
                .arg(lease_time.as_millis() as i64) 
                .invoke(&mut conn)?;

            if acquired > 0 {
                // Updating local state
                let mut local_locks = self.local_locks.lock();
                local_locks.insert(
                    self.name.clone(),
                    LocalLockState {
                        lock_count: 1,
                        lock_value: lock_info.value.clone(),
                        last_renew_time: Instant::now(),
                    },
                );
                return Ok(lock_info);
            }

            // Check for a timeout
            if let Some(wait_time) = wait_time {
                if start_time.elapsed() >= wait_time {
                    return Err(RedissonError::TimeoutError);
                }
                thread::sleep(Duration::from_millis(100));
            } else {
                thread::sleep(Duration::from_millis(100));
            }
        }
    }

    /// Start the watchdog
    fn start_watchdog(&self, lock_value: String) {
        let connection_manager = self.connection_manager.clone();
        let name = self.name.clone();
        let lease_time = self.lease_time;

        let renew_func = move || {
            match connection_manager.get_connection() {
                Ok(mut conn) => {
                    match scripts::RENEW_SCRIPT
                        .key(&name)
                        .arg(&lock_value)
                        .arg(lease_time.as_millis() as i64)
                        .invoke::<u64>(&mut conn)
                    {
                        Ok(result) => result == 1,
                        Err(_) => false,
                    }
                }
                Err(_) => false,
            }
        };

        let mut watchdog_guard = self.watchdog.lock();
        let mut watchdog = LockWatchdog::new();
        watchdog.start(Duration::from_secs(10), renew_func);
        *watchdog_guard = Some(watchdog);
    }

    /// Force unlock (ignore reentry count)
    pub fn force_unlock(&self) -> RedissonResult<bool> {
        let mut conn = self.connection_manager.get_connection()?;
        let deleted: i32 = conn.del(&self.name)?;

        // Cleaning up local state
        self.local_locks.lock().remove(&self.name);

        // Stop the watchdog
        if let Some(mut watchdog) = self.watchdog.lock().take() {
            watchdog.stop();
        }

        Ok(deleted > 0)
    }

    /// Check if the lock is held
    pub fn is_locked(&self) -> RedissonResult<bool> {
        let mut conn = self.connection_manager.get_connection()?;
        let exists: i32 = conn.exists(&self.name)?;
        Ok(exists > 0)
    }

    /// Checks whether the current thread holds the lock
    pub fn is_held_by_current_thread(&self) -> bool {
        let local_locks = self.local_locks.lock();
        local_locks.get(&self.name).is_some()
    }

    /// Time remaining to acquire the lock
    pub fn remaining_time(&self) -> RedissonResult<Duration> {
        let mut conn = self.connection_manager.get_connection()?;
        let ttl_ms: i64 = conn.pttl(&self.name)?;

        if ttl_ms > 0 {
            Ok(Duration::from_millis(ttl_ms as u64))
        } else {
            Ok(Duration::from_secs(0))
        }
    }
}



impl Clone for RLock {
    fn clone(&self) -> Self {
        Self {
            connection_manager: self.connection_manager.clone(),
            name: self.name.clone(),
            lease_time: self.lease_time,
            local_locks: self.local_locks.clone(),
            watchdog: self.watchdog.clone(),
        }
    }
}