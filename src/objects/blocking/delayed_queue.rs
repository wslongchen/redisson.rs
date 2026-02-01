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
use std::time::{Duration, SystemTime};
use redis::Commands;
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::{BaseDistributedObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};

/// === RDelayedQueue (Delay queue) ===
pub struct RDelayedQueue<V> {
    base: BaseDistributedObject,
    delay_queue_name: String,
    _marker: std::marker::PhantomData<V>,
}

impl<V: Serialize + DeserializeOwned> RDelayedQueue<V> {
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String) -> Self {
        let delay_queue_name = format!("{}:delayed", name);
        Self {
            base: BaseDistributedObject::new(connection_manager, name),
            delay_queue_name,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn offer(&self, value: &V, delay: Duration) -> RedissonResult<bool> {
        let value_json = BaseDistributedObject::serialize(value)?;
        let _delay_ms = delay.as_millis() as i64;
        let deliver_time = SystemTime::now()
            .checked_add(delay)
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let mut conn = self.base.get_connection()?;

        // Add to the delay queue
        let added: i32 = conn.zadd(&self.delay_queue_name, deliver_time, &value_json)?;

        if added > 0 {
            // Start lazy task processing (practical implementation requires background thread)
            self.schedule_delivery(deliver_time, value_json);
        }

        Ok(added > 0)
    }

    fn schedule_delivery(&self, deliver_time: i64, value_json: String) {
        let queue_name = self.base.get_full_key().to_string();
        let delay_queue_name = self.delay_queue_name.clone();
        let connection_manager = self.base.connection_manager();
        std::thread::spawn(move || {
            let cm = connection_manager.clone();
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let delay_ms = (deliver_time - now).max(0) as u64;

            if delay_ms > 0 {
                std::thread::sleep(Duration::from_millis(delay_ms));
            }

            // Move the element from the delayed queue to the actual queue
            if let Ok(mut conn) = cm.get_connection() {
                let _: i32 = redis::cmd("ZREM")
                    .arg(&delay_queue_name)
                    .arg(&value_json)
                    .query(&mut conn)
                    .unwrap_or(0);

                let _: i32 = redis::cmd("RPUSH")
                    .arg(&queue_name)
                    .arg(&value_json)
                    .query(&mut conn)
                    .unwrap_or(0);
            }
        });
    }
}