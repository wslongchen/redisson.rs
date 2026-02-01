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
use crate::{AsyncRLock, RedissonError, RedissonResult};
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;

/// === AsyncRMultiLock (Asynchronous interlocking) ===
pub struct AsyncRMultiLock {
    locks: Vec<AsyncRLock>,
    acquired_indices: Arc<TokioMutex<Vec<usize>>>,
}

impl AsyncRMultiLock {
    pub fn new(locks: Vec<AsyncRLock>) -> Self {
        Self {
            locks,
            acquired_indices: Arc::new(TokioMutex::new(Vec::new())),
        }
    }

    pub async fn lock(&self) -> RedissonResult<()> {
        let mut acquired_indices = Vec::new();

        for (i, lock) in self.locks.iter().enumerate() {
            match lock.try_lock().await {
                Ok(true) => acquired_indices.push(i),
                Ok(false) => {
                    // 释放已获取的锁
                    for &idx in &acquired_indices {
                        let _ = self.locks[idx].unlock().await;
                    }
                    return Err(RedissonError::LockAcquisitionError);
                }
                Err(e) => {
                    for &idx in &acquired_indices {
                        let _ = self.locks[idx].unlock().await;
                    }
                    return Err(e);
                }
            }
        }

        // Save the index of the acquired lock
        *self.acquired_indices.lock().await = acquired_indices;
        Ok(())
    }

    pub async fn unlock(&self) -> RedissonResult<()> {
        let mut acquired_indices = self.acquired_indices.lock().await;
        let mut errors = Vec::new();

        for &idx in acquired_indices.iter() {
            if idx < self.locks.len() {
                if let Err(e) = self.locks[idx].unlock().await {
                    errors.push(e);
                }
            }
        }

        acquired_indices.clear();

        if errors.is_empty() {
            Ok(())
        } else {
            Err(RedissonError::LockReleaseError)
        }
    }

    pub async fn try_lock(&self) -> RedissonResult<bool> {
        let mut acquired_indices = Vec::new();

        for (i, lock) in self.locks.iter().enumerate() {
            if lock.try_lock().await? {
                acquired_indices.push(i);
            } else {
                // Release the acquired lock
                for &idx in &acquired_indices {
                    let _ = self.locks[idx].unlock().await;
                }
                return Ok(false);
            }
        }

        *self.acquired_indices.lock().await = acquired_indices;
        Ok(true)
    }
}

impl Clone for AsyncRMultiLock {
    fn clone(&self) -> Self {
        Self {
            locks: self.locks.clone(),
            acquired_indices: self.acquired_indices.clone(),
        }
    }
}
