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
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use crate::{scripts, thread_id_to_u64, AsyncRedisConnectionManager, LockInfo, RedissonError, RedissonResult};
use crate::lock::LocalLockState;
use tokio::sync::{Mutex as TokioMutex};
use crate::lock::non_blocking::watchdog::AsyncLockWatchdog;

// === AsyncRReadWriteLock(Asynchronous read-write locks) ===
pub struct AsyncRReadWriteLock {
    connection_manager: Arc<AsyncRedisConnectionManager>,
    name: String,
    lease_time: Duration,
}

impl AsyncRReadWriteLock {
    pub fn new(
        connection_manager: Arc<AsyncRedisConnectionManager>,
        name: String,
        lease_time: Duration,
    ) -> Self {
        Self {
            connection_manager,
            name,
            lease_time,
        }
    }

    /// Acquire the read lock object
    pub fn read_lock(&self) -> AsyncRReadLock {
        AsyncRReadLock::new(
            self.connection_manager.clone(),
            self.name.clone(),
            self.lease_time,
        )
    }

    /// Obtain the write lock object
    pub fn write_lock(&self) -> AsyncRWriteLock {
        AsyncRWriteLock::new(
            self.connection_manager.clone(),
            self.name.clone(),
            self.lease_time,
        )
    }

    /// Force unlock read/write lock (ignore reentry count)
    pub async fn force_unlock(&self) -> RedissonResult<bool> {
        let mut conn = self.connection_manager.get_connection().await?;
        let read_lock_name = format!("{}:read", self.name);
        let write_lock_name = format!("{}:write", self.name);

        // Remove read and write locks
        let deleted_read: i32 = conn.execute_command(&mut redis::cmd("DEL").arg(&read_lock_name)).await?;
        let deleted_write: i32 = conn.execute_command(&mut redis::cmd("DEL").arg(&write_lock_name)).await?;

        Ok(deleted_read > 0 || deleted_write > 0)
    }

    /// Check that the read lock is held
    pub async fn is_read_locked(&self) -> RedissonResult<bool> {
        let mut conn = self.connection_manager.get_connection().await?;
        let read_lock_name = format!("{}:read", self.name);
        let exists: i32 = conn.execute_command(&mut redis::cmd("EXISTS").arg(&read_lock_name)).await?;
        Ok(exists > 0)
    }

    /// Checks whether the write lock is held
    pub async fn is_write_locked(&self) -> RedissonResult<bool> {
        let mut conn = self.connection_manager.get_connection().await?;
        let write_lock_name = format!("{}:write", self.name);
        let exists: i32 = conn.execute_command(&mut redis::cmd("EXISTS").arg(&write_lock_name)).await?;
        Ok(exists > 0)
    }

    /// The number of readers that acquire the lock
    pub async fn get_read_lock_count(&self) -> RedissonResult<u32> {
        let mut conn = self.connection_manager.get_connection().await?;
        let read_lock_name = format!("{}:read", self.name);

        let count: i32 = scripts::GET_READER_COUNT_SCRIPT
            .key(&read_lock_name)
            .invoke_async(&mut conn)
            .await?;

        Ok(count.max(0) as u32)
    }
}

// === AsyncRReadLock (Asynchronous read lock) ===
pub struct AsyncRReadLock {
    connection_manager: Arc<AsyncRedisConnectionManager>,
    name: String,
    lease_time: Duration,
    local_locks: Arc<TokioMutex<HashMap<String, LocalLockState>>>,
    watchdog: Arc<TokioMutex<Option<AsyncLockWatchdog>>>,
}

impl AsyncRReadLock {
    fn new(
        connection_manager: Arc<AsyncRedisConnectionManager>,
        name: String,
        lease_time: Duration,
    ) -> Self {
        Self {
            connection_manager,
            name,
            lease_time,
            local_locks: Arc::new(TokioMutex::new(HashMap::new())),
            watchdog: Arc::new(TokioMutex::new(None)),
        }
    }

    /// Acquiring locks (asynchronously blocking)
    pub async fn lock(&self) -> RedissonResult<()> {
        self.lock_with_lease_time(self.lease_time).await
    }

    /// Lock with lease duration
    pub async fn lock_with_lease_time(&self, lease_time: Duration) -> RedissonResult<()> {
        let lock_info = self.try_acquire_with_retry(lease_time, None).await?;

        // Start the watchdog (if the lease is 0, it needs to be automatically renewed)
        if lease_time.as_secs() == 0 {
            self.start_watchdog(lock_info.value.clone()).await;
        }

        Ok(())
    }

    /// Attempt to acquire lock (asynchronous non-blocking)
    pub async fn try_lock(&self) -> RedissonResult<bool> {
        self.try_lock_with_timeout(Duration::from_secs(0)).await
    }

    /// An attempt to acquire a lock with a timeout
    pub async fn try_lock_with_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        match self.try_acquire_with_retry(self.lease_time, Some(wait_time)).await {
            Ok(lock_info) => {
                // Start the watchdog
                if self.lease_time.as_secs() == 0 {
                    self.start_watchdog(lock_info.value.clone()).await;
                }
                Ok(true)
            }
            Err(RedissonError::TimeoutError) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Release a lock
    pub async fn unlock(&self) -> RedissonResult<bool> {
        let mut local_locks = self.local_locks.lock().await;

        // Checks if the lock is held locally
        if let Some(state) = local_locks.get_mut(&self.name) {
            // 检查重入计数
            if state.lock_count > 1 {
                state.lock_count -= 1;
                return Ok(true);
            }

            // Acquire connection and release Redis lock
            let mut conn = self.connection_manager.get_connection().await?;

            let released: i32 = scripts::READ_UNLOCK_SCRIPT
                .key(&self.name)
                .arg(&state.lock_value)
                .invoke_async(&mut conn)
                .await?;

            // Stop the watchdog
            let mut watchdog_guard = self.watchdog.lock().await;
            if let Some(mut watchdog) = watchdog_guard.take() {
                watchdog.stop().await;
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
    async fn try_acquire_with_retry(
        &self,
        lease_time: Duration,
        wait_time: Option<Duration>,
    ) -> RedissonResult<LockInfo> {
        let start_time = Instant::now();
        let lock_info = LockInfo::new(format!("{}:read", self.name), lease_time);
        let thread_id = thread_id_to_u64();

        loop {
            // Check for local reentry
            {
                let mut local_locks = self.local_locks.lock().await;
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
            let mut conn = self.connection_manager.get_connection().await?;

            let acquired: i32 = scripts::READ_LOCK_SCRIPT
                .key(&self.name)
                .arg(&lock_info.value)
                .arg(thread_id)
                .arg(lease_time.as_millis() as i64)
                .invoke_async(&mut conn)
                .await?;

            if acquired > 0 {
                // Updating local state
                let mut local_locks = self.local_locks.lock().await;
                local_locks.insert(
                    self.name.clone(),
                    LocalLockState {
                        lock_count: 1,
                        lock_value: lock_info.value.clone(),
                        last_renew_time: Instant::now(),
                        thread_id,
                        watchdog_running: false,
                    },
                );
                return Ok(lock_info);
            }

            // Check for a timeout
            if let Some(wait_time) = wait_time {
                if start_time.elapsed() >= wait_time {
                    return Err(RedissonError::TimeoutError);
                }
                sleep(Duration::from_millis(100)).await;
            } else {
                sleep(Duration::from_millis(100)).await;
            }
        }
    }

    /// Start the watchdog
    async fn start_watchdog(&self, lock_value: String) {
        let connection_manager = self.connection_manager.clone();
        let name = self.name.clone();
        let lease_time = self.lease_time;

        let renew_func = move || {
            let connection_manager = connection_manager.clone();
            let name = name.clone();
            let lock_value = lock_value.clone();
            let lease_time = lease_time;

            Box::pin(async move {
                match connection_manager.get_connection().await {
                    Ok(mut conn) => {
                        match scripts::READ_RENEW_SCRIPT
                            .key(&name)
                            .arg(&lock_value)
                            .arg(lease_time.as_millis() as i64)
                            .invoke_async::<u64>(&mut conn)
                            .await
                        {
                            Ok(result) => result == 1,
                            Err(_) => false,
                        }
                    }
                    Err(_) => false,
                }
            }) as Pin<Box<dyn Future<Output = bool> + Send>>
        };

        let mut watchdog_guard = self.watchdog.lock().await;
        let mut watchdog = AsyncLockWatchdog::new();
        watchdog.start(Duration::from_secs(10), renew_func).await;
        *watchdog_guard = Some(watchdog);
    }

    /// Checks whether the current thread holds the lock
    pub async fn is_held_by_current_thread(&self) -> bool {
        let local_locks = self.local_locks.lock().await;
        local_locks.get(&self.name).is_some()
    }

    /// Time remaining to acquire the lock
    pub async fn remaining_time(&self) -> RedissonResult<Duration> {
        let mut conn = self.connection_manager.get_connection().await?;
        let lock_name = format!("{}:read", self.name);
        let ttl_ms: i64 = conn.execute_command(&mut redis::cmd("PTTL").arg(&lock_name)).await?;

        if ttl_ms > 0 {
            Ok(Duration::from_millis(ttl_ms as u64))
        } else {
            Ok(Duration::from_secs(0))
        }
    }
}

// === AsyncRWriteLock (Asynchronous write lock) ===
pub struct AsyncRWriteLock {
    connection_manager: Arc<AsyncRedisConnectionManager>,
    name: String,
    lease_time: Duration,
    local_locks: Arc<TokioMutex<HashMap<String, LocalLockState>>>,
    watchdog: Arc<TokioMutex<Option<AsyncLockWatchdog>>>,
}

impl AsyncRWriteLock {
    fn new(
        connection_manager: Arc<AsyncRedisConnectionManager>,
        name: String,
        lease_time: Duration,
    ) -> Self {
        Self {
            connection_manager,
            name,
            lease_time,
            local_locks: Arc::new(TokioMutex::new(HashMap::new())),
            watchdog: Arc::new(TokioMutex::new(None)),
        }
    }

    /// Acquiring locks (asynchronously blocking)
    pub async fn lock(&self) -> RedissonResult<()> {
        self.lock_with_lease_time(self.lease_time).await
    }

    /// Lock with lease duration
    pub async fn lock_with_lease_time(&self, lease_time: Duration) -> RedissonResult<()> {
        let lock_info = self.try_acquire_with_retry(lease_time, None).await?;

        // Start the watchdog (if the lease is 0, it needs to be automatically renewed)
        if lease_time.as_secs() == 0 {
            self.start_watchdog(lock_info.value.clone()).await;
        }

        Ok(())
    }

    /// Attempt to acquire lock (asynchronous non-blocking)
    pub async fn try_lock(&self) -> RedissonResult<bool> {
        self.try_lock_with_timeout(Duration::from_secs(0)).await
    }

    /// An attempt to acquire a lock with a timeout
    pub async fn try_lock_with_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        match self.try_acquire_with_retry(self.lease_time, Some(wait_time)).await {
            Ok(lock_info) => {
                // Start the watchdog
                if self.lease_time.as_secs() == 0 {
                    self.start_watchdog(lock_info.value.clone()).await;
                }
                Ok(true)
            }
            Err(RedissonError::TimeoutError) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Release a lock
    pub async fn unlock(&self) -> RedissonResult<bool> {
        let mut local_locks = self.local_locks.lock().await;

        // Checks if the lock is held locally
        if let Some(state) = local_locks.get_mut(&self.name) {
            // Check the reentry count
            if state.lock_count > 1 {
                state.lock_count -= 1;
                return Ok(true);
            }

            // Acquire connection and release Redis lock
            let mut conn = self.connection_manager.get_connection().await?;

            let released: i32 = scripts::WRITE_UNLOCK_SCRIPT
                .key(&self.name)
                .arg(&state.lock_value)
                .invoke_async(&mut conn)
                .await?;

            // Stop the watchdog
            let mut watchdog_guard = self.watchdog.lock().await;
            if let Some(mut watchdog) = watchdog_guard.take() {
                watchdog.stop().await;
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
    async fn try_acquire_with_retry(
        &self,
        lease_time: Duration,
        wait_time: Option<Duration>,
    ) -> RedissonResult<LockInfo> {
        let start_time = Instant::now();
        let lock_info = LockInfo::new(format!("{}:write", self.name), lease_time);
        let thread_id = thread_id_to_u64();

        loop {
            // Check for local reentry
            {
                let mut local_locks = self.local_locks.lock().await;
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
            let mut conn = self.connection_manager.get_connection().await?;

            let acquired: i32 = scripts::WRITE_LOCK_SCRIPT
                .key(&self.name)
                .arg(&lock_info.value)
                .arg(thread_id)
                .arg(lease_time.as_millis() as i64)
                .invoke_async(&mut conn)
                .await?;

            if acquired > 0 {
                // Updating local state
                let mut local_locks = self.local_locks.lock().await;
                local_locks.insert(
                    self.name.clone(),
                    LocalLockState {
                        lock_count: 1,
                        lock_value: lock_info.value.clone(),
                        last_renew_time: Instant::now(),
                        thread_id,
                        watchdog_running: false,
                    },
                );
                return Ok(lock_info);
            }

            // Check for a timeout
            if let Some(wait_time) = wait_time {
                if start_time.elapsed() >= wait_time {
                    return Err(RedissonError::TimeoutError);
                }
                sleep(Duration::from_millis(100)).await;
            } else {
                sleep(Duration::from_millis(100)).await;
            }
        }
    }

    /// Start the watchdog
    async fn start_watchdog(&self, lock_value: String) {
        let connection_manager = self.connection_manager.clone();
        let name = self.name.clone();
        let lease_time = self.lease_time;

        let renew_func = move || {
            let connection_manager = connection_manager.clone();
            let name = name.clone();
            let lock_value = lock_value.clone();
            let lease_time = lease_time;

            Box::pin(async move {
                match connection_manager.get_connection().await {
                    Ok(mut conn) => {
                        match scripts::WRITE_RENEW_SCRIPT
                            .key(&name)
                            .arg(&lock_value)
                            .arg(lease_time.as_millis() as i64)
                            .invoke_async::<u64>(&mut conn)
                            .await
                        {
                            Ok(result) => result == 1,
                            Err(_) => false,
                        }
                    }
                    Err(_) => false,
                }
            }) as Pin<Box<dyn Future<Output = bool> + Send>>
        };

        let mut watchdog_guard = self.watchdog.lock().await;
        let mut watchdog = AsyncLockWatchdog::new();
        watchdog.start(Duration::from_secs(10), renew_func).await;
        *watchdog_guard = Some(watchdog);
    }

    /// Checks whether the current thread holds the lock
    pub async fn is_held_by_current_thread(&self) -> bool {
        let local_locks = self.local_locks.lock().await;
        local_locks.get(&self.name).is_some()
    }

    /// Time remaining to acquire the lock
    pub async fn remaining_time(&self) -> RedissonResult<Duration> {
        let mut conn = self.connection_manager.get_connection().await?;
        let lock_name = format!("{}:write", self.name);
        let ttl_ms: i64 = conn.execute_command(&mut redis::cmd("PTTL").arg(&lock_name)).await?;

        if ttl_ms > 0 {
            Ok(Duration::from_millis(ttl_ms as u64))
        } else {
            Ok(Duration::from_secs(0))
        }
    }
}

impl Clone for AsyncRReadWriteLock {
    fn clone(&self) -> Self {
        Self {
            connection_manager: self.connection_manager.clone(),
            name: self.name.clone(),
            lease_time: self.lease_time,
        }
    }
}