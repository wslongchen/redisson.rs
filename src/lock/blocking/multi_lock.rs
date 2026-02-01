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
use parking_lot::Mutex;
use crate::{RedissonError, RedissonResult};
use crate::lock::blocking::RLock;

/// === RMultiLock (Interlocking) ===
pub struct RMultiLock {
    locks: Vec<RLock>,
    acquired_indices: Arc<Mutex<Vec<usize>>>,
}

impl RMultiLock {
    pub fn new(locks: Vec<RLock>) -> Self {
        Self {
            locks,
            acquired_indices: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn lock(&self) -> RedissonResult<()> {
        let mut acquired_indices = Vec::new();

        for (i, lock) in self.locks.iter().enumerate() {
            match lock.try_lock() {
                Ok(true) => acquired_indices.push(i),
                Ok(false) => {
                    // Release the acquired lock
                    for &idx in &acquired_indices {
                        let _ = self.locks[idx].unlock();
                    }
                    return Err(RedissonError::LockAcquisitionError);
                }
                Err(e) => {
                    for &idx in &acquired_indices {
                        let _ = self.locks[idx].unlock();
                    }
                    return Err(e);
                }
            }
        }

        // Save the index of the acquired lock
        *self.acquired_indices.lock() = acquired_indices;
        Ok(())
    }

    pub fn unlock(&self) -> RedissonResult<()> {
        let mut acquired_indices = self.acquired_indices.lock();
        let mut errors = Vec::new();

        for &idx in acquired_indices.iter() {
            if idx < self.locks.len() {
                if let Err(e) = self.locks[idx].unlock() {
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

    pub fn try_lock(&self) -> RedissonResult<bool> {
        let mut acquired_indices = Vec::new();

        for (i, lock) in self.locks.iter().enumerate() {
            if lock.try_lock()? {
                acquired_indices.push(i);
            } else {
                // Release the acquired lock
                for &idx in &acquired_indices {
                    let _ = self.locks[idx].unlock();
                }
                return Ok(false);
            }
        }

        *self.acquired_indices.lock() = acquired_indices;
        Ok(true)
    }
}

impl Clone for RMultiLock {
    fn clone(&self) -> Self {
        Self {
            locks: self.locks.clone(),
            acquired_indices: self.acquired_indices.clone(),
        }
    }
}