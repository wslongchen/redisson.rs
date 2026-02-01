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
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;
use crate::{scripts, AsyncRedisConnectionManager, RedissonError, RedissonResult};
use tokio::sync::{Mutex as TokioMutex};

/// === AsyncRFairLock (Asynchronous fair lock) ===
pub struct AsyncRFairLock {
    connection_manager: Arc<AsyncRedisConnectionManager>,
    name: String,
    lease_time: Duration,
    local_lock_value: Arc<TokioMutex<Option<String>>>,
}

impl AsyncRFairLock {
    pub fn new(
        connection_manager: Arc<AsyncRedisConnectionManager>,
        name: String,
        lease_time: Duration,
    ) -> Self {
        Self {
            connection_manager,
            name,
            lease_time,
            local_lock_value: Arc::new(TokioMutex::new(None)),
        }
    }

    pub async fn lock(&self) -> RedissonResult<()> {
        self.lock_with_lease_time(self.lease_time).await
    }

    pub async fn lock_with_lease_time(&self, lease_time: Duration) -> RedissonResult<()> {
        let queue_name = format!("{}:queue", self.name);
        let lock_value = Uuid::new_v4().to_string();

        // Save the lock value locally
        *self.local_lock_value.lock().await = Some(lock_value.clone());

        // Join the queue
        let mut conn = self.connection_manager.get_connection().await?;
        let position: i64 = conn
            .execute_command(&mut redis::cmd("RPUSH").arg(&queue_name).arg(&lock_value))
            .await?;

        // Wait to be the team leader
        let mut attempts = 0;
        while position > 1 {
            sleep(Duration::from_millis(100)).await;

            let front: Option<String> = conn
                .execute_command(&mut redis::cmd("LINDEX").arg(&queue_name).arg(0))
                .await?;
            if front == Some(lock_value.clone()) {
                break;
            }

            attempts += 1;
            if attempts > 100 {
                // The maximum number of attempts is 100
                // Clearing the queue
                let _: i64 = conn
                    .execute_command(&mut redis::cmd("LREM").arg(&queue_name).arg(1).arg(&lock_value))
                    .await?;
                *self.local_lock_value.lock().await = None;
                return Err(RedissonError::TimeoutError);
            }
        }

        // Acquiring locks
        let acquired: i32 = scripts::LOCK_SCRIPT
            .key(&self.name)
            .arg(&lock_value)
            .arg(lease_time.as_millis() as i64)
            .invoke_async(&mut conn)
            .await?;

        if acquired > 0 {
            // Remove from the queue
            let _: i64 = conn
                .execute_command(&mut redis::cmd("LREM").arg(&queue_name).arg(1).arg(&lock_value))
                .await?;
            Ok(())
        } else {
            // Clearing the queue
            let _: i64 = conn
                .execute_command(&mut redis::cmd("LREM").arg(&queue_name).arg(1).arg(&lock_value))
                .await?;
            *self.local_lock_value.lock().await = None;
            Err(RedissonError::LockAcquisitionError)
        }
    }

    pub async fn unlock(&self) -> RedissonResult<bool> {
        // Retrieve the lock-value stored locally
        let lock_value = self.local_lock_value.lock().await.take();

        if let Some(lock_value) = lock_value {
            let mut conn = self.connection_manager.get_connection().await?;

            let released: i32 = scripts::UNLOCK_SCRIPT
                .key(&self.name)
                .arg(&lock_value)
                .invoke_async(&mut conn)
                .await?;

            Ok(released > 0)
        } else {
            Ok(false)
        }
    }

    pub async fn try_lock(&self) -> RedissonResult<bool> {
        self.try_lock_with_timeout(Duration::from_secs(0)).await
    }

    pub async fn try_lock_with_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        let start = Instant::now();

        while start.elapsed() < wait_time {
            match self.lock().await {
                Ok(_) => return Ok(true),
                Err(_) => sleep(Duration::from_millis(50)).await,
            }
        }

        Ok(false)
    }
}



impl Clone for AsyncRFairLock {
    fn clone(&self) -> Self {
        Self {
            connection_manager: self.connection_manager.clone(),
            name: self.name.clone(),
            lease_time: self.lease_time,
            local_lock_value: Arc::new(TokioMutex::new(None)),
        }
    }
}
