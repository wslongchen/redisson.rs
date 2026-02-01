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
use crate::{scripts, BaseDistributedObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};

/// === RCountDownLatch (Counter) ===
#[derive(Clone)]
pub struct RCountDownLatch {
    base: BaseDistributedObject,
}

impl RCountDownLatch {
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String, count: i32) -> Self {
        let latch = Self {
            base: BaseDistributedObject::new(connection_manager, name),
        };

        // Initialize the counter
        latch.try_set_count(count).unwrap_or_default();

        latch
    }

    pub fn r#await(&self, timeout: Option<Duration>) -> RedissonResult<bool> {
        let count = self.get_count()?;
        if count <= 0 {
            return Ok(true);
        }

        // Create a subscription channel
        let channel_name = format!("{}:await:{}", self.base.get_full_key(), uuid::Uuid::new_v4());
        let mut conn = self.base.get_connection()?;

        // Register as a waiter
        let result: i32 = scripts::COUNTDOWN_LATCH_SCRIPT
            .key(self.base.get_full_key())
            .arg("await")
            .arg(0)
            .arg(&channel_name)
            .invoke(&mut conn)?;

        if result > 0 {
            return Ok(true);
        }

        // Waiting for notifications
        if let Some(timeout) = timeout {
            let start = std::time::Instant::now();
            while start.elapsed() < timeout {
                // Check the counter
                let current_count = self.get_count()?;
                if current_count <= 0 {
                    return Ok(true);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Ok(false)
        } else {
            // Unlimited waiting
            loop {
                let current_count = self.get_count()?;
                if current_count <= 0 {
                    return Ok(true);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    pub fn count_down(&self) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;

        scripts::COUNTDOWN_LATCH_SCRIPT
            .key(self.base.get_full_key())
            .arg("countDown")
            .arg(1)
            .invoke::<()>(&mut conn)?;

        Ok(())
    }

    pub fn get_count(&self) -> RedissonResult<i32> {
        let mut conn = self.base.get_connection()?;

        let result: i32 = scripts::COUNTDOWN_LATCH_SCRIPT
            .key(self.base.get_full_key())
            .arg("getCount")
            .arg(0)
            .invoke(&mut conn)?;

        Ok(result)
    }

    pub fn try_set_count(&self, count: i32) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;

        let result: i32 = scripts::COUNTDOWN_LATCH_SCRIPT
            .key(self.base.get_full_key())
            .arg("trySetCount")
            .arg(count)
            .invoke(&mut conn)?;

        Ok(result > 0)
    }
}
