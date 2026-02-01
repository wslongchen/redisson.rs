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
use std::sync::Arc;
use std::time::Duration;
use serde::{Serialize, de::DeserializeOwned};
use redis::{FromRedisValue, ParsingError, RedisResult};

use crate::errors::{RedissonResult, RedissonError};
use crate::{PendingMessage, RedisMapExt, StreamConsumer, StreamGroup, StreamInfo, StreamMessage, SyncRedisConnectionManager};


/// Redisson Stream implementation
pub struct RStream<V> {
    connection_manager: Arc<SyncRedisConnectionManager>,
    name: String,
    max_len: Option<u64>,
    approximate: bool,
    _marker: std::marker::PhantomData<V>,
}

impl<V: Serialize + DeserializeOwned + Clone> RStream<V> {
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String) -> Self {
        Self {
            connection_manager,
            name,
            max_len: None,
            approximate: false,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn with_max_len(mut self, max_len: u64, approximate: bool) -> Self {
        self.max_len = Some(max_len);
        self.approximate = approximate;
        self
    }

    /// Adds a message to a Stream
    pub fn add(&self, id: &str, fields: &HashMap<String, V>) -> RedissonResult<String> {
        let mut conn = self.connection_manager.get_connection()?;

        let mut args = vec![self.name.to_string(), id.to_string()];

        // Add a maximum length limit
        if let Some(max_len) = self.max_len {
            args.push("MAXLEN".to_string());
            if self.approximate {
                args.push("~".to_string());
            }
            args.push(max_len.to_string());
        }

        // Build the XADD command
        let mut cmd = redis::cmd("XADD");

        for arg in args {
            cmd.arg(arg);
        }

        // Adding fields
        for (field, value) in fields {
            let value_json = serde_json::to_string(value)
                .map_err(|e| RedissonError::SerializationError(e.to_string()))?;
            cmd.arg(field).arg(value_json);
        }

        let message_id: String = cmd.query(&mut conn)?;

        Ok(message_id)
    }

    /// Add message (auto-generated ID)
    pub fn add_auto_id(&self, fields: &HashMap<String, V>) -> RedissonResult<String> {
        self.add("*", fields)
    }

    /// Reading messages
    pub fn read(
        &self,
        start_id: &str,
        count: Option<u64>,
        block_ms: Option<u64>,
    ) -> RedissonResult<Vec<StreamMessage<V>>> {
        let mut conn = self.connection_manager.get_connection()?;

        let mut cmd = redis::cmd("XREAD");

        // Adding the COUNT parameter
        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }

        // Adding the BLOCK parameter
        if let Some(block_ms) = block_ms {
            cmd.arg("BLOCK").arg(block_ms);
        }

        // Adding the STREAMS parameter
        cmd.arg("STREAMS").arg(&self.name).arg(start_id);

        let result: RedisResult<redis::Value> = cmd.query(&mut conn);

        self.parse_xread_result(result)
    }

    /// Range read message
    pub fn range(
        &self,
        start_id: &str,
        end_id: &str,
        count: Option<u64>,
    ) -> RedissonResult<Vec<StreamMessage<V>>> {
        let mut conn = self.connection_manager.get_connection()?;

        let mut cmd = redis::cmd("XRANGE");
        cmd.arg(&self.name).arg(start_id).arg(end_id);

        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }

        let result: RedisResult<redis::Value> = cmd.query(&mut conn);

        self.parse_xrange_result(result)
    }

    /// The reverse range reads the message
    pub fn rev_range(
        &self,
        start_id: &str,
        end_id: &str,
        count: Option<u64>,
    ) -> RedissonResult<Vec<StreamMessage<V>>> {
        let mut conn = self.connection_manager.get_connection()?;

        let mut cmd = redis::cmd("XREVRANGE");
        cmd.arg(&self.name).arg(start_id).arg(end_id);

        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }

        let result: RedisResult<redis::Value> = cmd.query(&mut conn);

        self.parse_xrange_result(result)
    }

    /// Getting the length of a Stream
    pub fn len(&self) -> RedissonResult<u64> {
        let mut conn = self.connection_manager.get_connection()?;

        let len: i64 = redis::cmd("XLEN")
            .arg(&self.name)
            .query(&mut conn)?;

        Ok(len as u64)
    }

    /// Deleting messages
    pub fn delete(&self, ids: &[String]) -> RedissonResult<u64> {
        let mut conn = self.connection_manager.get_connection()?;

        let mut cmd = redis::cmd("XDEL");
        cmd.arg(&self.name);

        for id in ids {
            cmd.arg(id);
        }

        let deleted: i64 = cmd.query(&mut conn)?;
        Ok(deleted as u64)
    }

    /// Pruning a Stream (keeping recent messages)
    pub fn trim(&self, max_len: u64, approximate: bool) -> RedissonResult<u64> {
        let mut conn = self.connection_manager.get_connection()?;

        let mut cmd = redis::cmd("XTRIM");
        cmd.arg(&self.name).arg("MAXLEN");

        if approximate {
            cmd.arg("~");
        }

        cmd.arg(max_len.to_string());

        let trimmed: i64 = cmd.query(&mut conn)?;
        Ok(trimmed as u64)
    }

    /// Creating a Consumption group
    pub fn create_group(&self, group_name: &str, start_id: &str) -> RedissonResult<bool> {
        let mut conn = self.connection_manager.get_connection()?;

        let result: String = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.name)
            .arg(group_name)
            .arg(start_id)
            .arg("MKSTREAM")
            .query(&mut conn)?;

        Ok(result == "OK")
    }

    /// Deleting a consumption group
    pub fn delete_group(&self, group_name: &str) -> RedissonResult<bool> {
        let mut conn = self.connection_manager.get_connection()?;

        let deleted: i64 = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(&self.name)
            .arg(group_name)
            .query(&mut conn)?;

        Ok(deleted > 0)
    }

    /// Read the message from the consuming group
    pub fn read_group(
        &self,
        group_name: &str,
        consumer_name: &str,
        count: Option<u64>,
        block_ms: Option<u64>,
        no_ack: bool,
    ) -> RedissonResult<Vec<StreamMessage<V>>> {
        let mut conn = self.connection_manager.get_connection()?;

        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP").arg(group_name).arg(consumer_name);

        // Adding the COUNT parameter
        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }

        // Adding the BLOCK parameter
        if let Some(block_ms) = block_ms {
            cmd.arg("BLOCK").arg(block_ms);
        }

        // Add the NOACK parameter
        if no_ack {
            cmd.arg("NOACK");
        }

        // Adding the STREAMS parameter
        cmd.arg("STREAMS").arg(&self.name).arg(">");

        let result: RedisResult<redis::Value> = cmd.query(&mut conn);

        self.parse_xread_result(result)
    }

    /// Confirm that the message has been processed
    pub fn ack(&self, group_name: &str, ids: &[String]) -> RedissonResult<u64> {
        let mut conn = self.connection_manager.get_connection()?;

        let mut cmd = redis::cmd("XACK");
        cmd.arg(&self.name).arg(group_name);

        for id in ids {
            cmd.arg(id);
        }

        let acked: i64 = cmd.query(&mut conn)?;
        Ok(acked as u64)
    }

    /// Get the pending message
    pub fn pending(
        &self,
        group_name: &str,
        start_id: Option<&str>,
        end_id: Option<&str>,
        count: Option<u64>,
        consumer_name: Option<&str>,
    ) -> RedissonResult<Vec<PendingMessage>> {
        let mut conn = self.connection_manager.get_connection()?;

        let mut cmd = redis::cmd("XPENDING");
        cmd.arg(&self.name).arg(group_name);

        if let Some(start_id) = start_id {
            cmd.arg(start_id);
        }

        if let Some(end_id) = end_id {
            cmd.arg(end_id);
        }

        if let Some(count) = count {
            cmd.arg(count);
        }

        if let Some(consumer_name) = consumer_name {
            cmd.arg(consumer_name);
        }

        let result: RedisResult<redis::Value> = cmd.query(&mut conn);

        self.parse_pending_result(result)
    }

    /// Declare pending messages
    pub fn claim(
        &self,
        group_name: &str,
        consumer_name: &str,
        min_idle_time_ms: u64,
        ids: &[String],
    ) -> RedissonResult<Vec<StreamMessage<V>>> {
        let mut conn = self.connection_manager.get_connection()?;

        let mut cmd = redis::cmd("XCLAIM");
        cmd.arg(&self.name)
            .arg(group_name)
            .arg(consumer_name)
            .arg(min_idle_time_ms.to_string());

        for id in ids {
            cmd.arg(id);
        }

        let result: RedisResult<redis::Value> = cmd.query(&mut conn);

        self.parse_xrange_result(result)
    }

    /// Getting Stream information
    pub fn info(&self) -> RedissonResult<StreamInfo> {
        let mut conn = self.connection_manager.get_connection()?;

        let info: StreamInfo = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(&self.name)
            .query(&mut conn)?;

        Ok(info)
    }

    /// Get consumer group information
    pub fn groups_info(&self) -> RedissonResult<Vec<StreamGroup>> {
        let mut conn = self.connection_manager.get_connection()?;

        let groups_value: redis::Value = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(&self.name)
            .query(&mut conn)?;

        self.parse_groups_info(groups_value)
    }

    /// Get consumer information
    pub fn consumers_info(&self, group_name: &str) -> RedissonResult<Vec<StreamConsumer>> {
        let mut conn = self.connection_manager.get_connection()?;

        let consumers_value: redis::Value = redis::cmd("XINFO")
            .arg("CONSUMERS")
            .arg(&self.name)
            .arg(group_name)
            .query(&mut conn)?;

        self.parse_consumers_info(consumers_value)
    }

    /// Parse the XREAD/XREADGROUP result
    fn parse_xread_result(&self, result: RedisResult<redis::Value>) -> RedissonResult<Vec<StreamMessage<V>>> {
        match result {
            Ok(value) => {
                if let redis::Value::Nil = value {
                    return Ok(Vec::new());
                }

                if let redis::Value::Array(streams) = value {
                    for stream in streams {
                        if let redis::Value::Array(items) = stream {
                            if items.len() >= 2 {
                                // items[0] 是 stream name，items[1] 是消息数组
                                if let redis::Value::Array(messages) = &items[1] {
                                    return self.parse_messages_array(messages);
                                }
                            }
                        }
                    }
                }
                Ok(Vec::new())
            }
            Err(err) => match err.kind() {
                redis::ErrorKind::Extension if err.detail().map(|s| s.contains("timeout")).unwrap_or(false) => {
                    // The read timeout returns an empty result
                    Ok(Vec::new())
                }
                _ => Err(RedissonError::from(err)),
            },
        }
    }

    /// Parse the XRANGE/XREVRANGE/XCLAIM result
    fn parse_xrange_result(&self, result: RedisResult<redis::Value>) -> RedissonResult<Vec<StreamMessage<V>>> {
        match result {
            Ok(value) => {
                if let redis::Value::Nil = value {
                    return Ok(Vec::new());
                }

                if let redis::Value::Array(messages) = value {
                    return self.parse_messages_array(&messages);
                }
                Ok(Vec::new())
            }
            Err(err) => Err(RedissonError::from(err)),
        }
    }

    /// Parsing an array of messages
    fn parse_messages_array(&self, messages: &[redis::Value]) -> RedissonResult<Vec<StreamMessage<V>>> {
        let mut parsed_messages = Vec::new();

        for message_value in messages {
            if let redis::Value::Array(message_items) = message_value {
                if message_items.len() >= 2 {
                    // Resolving ids
                    let id = match &message_items[0] {
                        redis::Value::BulkString(data) =>
                            String::from_utf8_lossy(data).to_string(),
                        redis::Value::SimpleString(s) => s.clone(),
                        _ => continue,
                    };

                    // Parsing fields
                    let mut fields = HashMap::new();
                    if let redis::Value::Array(field_items) = &message_items[1] {
                        for i in (0..field_items.len()).step_by(2) {
                            if i + 1 < field_items.len() {
                                let field = match &field_items[i] {
                                    redis::Value::BulkString(data) =>
                                        String::from_utf8_lossy(data).to_string(),
                                    redis::Value::SimpleString(s) => s.clone(),
                                    _ => continue,
                                };

                                let value_str = match &field_items[i + 1] {
                                    redis::Value::BulkString(data) =>
                                        String::from_utf8_lossy(data).to_string(),
                                    redis::Value::SimpleString(s) => s.clone(),
                                    redis::Value::Int(num) => num.to_string(),
                                    redis::Value::Double(num) => num.to_string(),
                                    _ => continue,
                                };

                                // Deserialize values
                                match serde_json::from_str(&value_str) {
                                    Ok(value) => {
                                        fields.insert(field, value);
                                    }
                                    Err(e) => {
                                        // If deserialization fails, try the original string as the value
                                        if let Ok(value) = serde_json::from_str(&serde_json::to_string(&value_str).unwrap()) {
                                            fields.insert(field, value);
                                        } else {
                                            return Err(RedissonError::DeserializationError(
                                                format!("Failed to deserialize value '{}': {}", value_str, e)
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    parsed_messages.push(StreamMessage { id, fields });
                }
            }
        }

        Ok(parsed_messages)
    }

    /// Parsing Pending results
    fn parse_pending_result(&self, result: RedisResult<redis::Value>) -> RedissonResult<Vec<PendingMessage>> {
        match result {
            Ok(value) => {
                if let redis::Value::Array(pending_info) = value {
                    if pending_info.len() >= 1 {
                        // The first element is the list of messages
                        if let redis::Value::Array(messages) = &pending_info[0] {
                            let mut pending_messages = Vec::new();

                            for message in messages {
                                if let redis::Value::Array(items) = message {
                                    if items.len() >= 4 {
                                        let id = match &items[0] {
                                            redis::Value::BulkString(data) =>
                                                String::from_utf8_lossy(data).to_string(),
                                            redis::Value::SimpleString(s) => s.clone(),
                                            _ => continue,
                                        };

                                        let consumer = match &items[1] {
                                            redis::Value::BulkString(data) =>
                                                String::from_utf8_lossy(data).to_string(),
                                            redis::Value::SimpleString(s) => s.clone(),
                                            _ => continue,
                                        };

                                        let idle_ms = match &items[2] {
                                            redis::Value::Int(val) => *val as u64,
                                            redis::Value::BulkString(data) =>
                                                String::from_utf8_lossy(data).parse().unwrap_or(0),
                                            _ => continue,
                                        };

                                        let delivery_count = match &items[3] {
                                            redis::Value::Int(val) => *val as u32,
                                            redis::Value::BulkString(data) =>
                                                String::from_utf8_lossy(data).parse().unwrap_or(0),
                                            _ => continue,
                                        };

                                        pending_messages.push(PendingMessage {
                                            id,
                                            consumer,
                                            idle_ms,
                                            delivery_count,
                                        });
                                    }
                                }
                            }

                            return Ok(pending_messages);
                        }
                    }
                }
                Ok(Vec::new())
            }
            Err(err) => Err(RedissonError::from(err)),
        }
    }

    /// Parse the consumption group information
    fn parse_groups_info(&self, value: redis::Value) -> RedissonResult<Vec<StreamGroup>> {
        let mut groups = Vec::new();

        if let redis::Value::Array(group_list) = value {
            for group_value in group_list {
                if let redis::Value::Array(group_items) = group_value {
                    let mut group = StreamGroup {
                        name: String::new(),
                        consumers: Vec::new(),
                        pending_count: 0,
                        last_delivered_id: String::new(),
                    };

                    for i in (0..group_items.len()).step_by(2) {
                        if i + 1 < group_items.len() {
                            let key = match &group_items[i] {
                                redis::Value::BulkString(data) =>
                                    String::from_utf8_lossy(data).to_string(),
                                redis::Value::SimpleString(s) => s.clone(),
                                _ => continue,
                            };

                            match key.as_str() {
                                "name" => {
                                    group.name = match &group_items[i + 1] {
                                        redis::Value::BulkString(data) =>
                                            String::from_utf8_lossy(data).to_string(),
                                        redis::Value::SimpleString(s) => s.clone(),
                                        _ => continue,
                                    };
                                }
                                "consumers" => {
                                    if let redis::Value::Int(_count) = &group_items[i + 1] {
                                        // Number of consumers (not a list of consumers)
                                    }
                                }
                                "pending" => {
                                    if let redis::Value::Int(count) = &group_items[i + 1] {
                                        group.pending_count = *count as u64;
                                    }
                                }
                                "last-delivered-id" => {
                                    group.last_delivered_id = match &group_items[i + 1] {
                                        redis::Value::BulkString(data) =>
                                            String::from_utf8_lossy(data).to_string(),
                                        redis::Value::SimpleString(s) => s.clone(),
                                        _ => continue,
                                    };
                                }
                                _ => {}
                            }
                        }
                    }

                    if !group.name.is_empty() {
                        groups.push(group);
                    }
                }
            }
        }

        Ok(groups)
    }

    /// Parsing consumer information
    fn parse_consumers_info(&self, value: redis::Value) -> RedissonResult<Vec<StreamConsumer>> {
        let mut consumers = Vec::new();

        if let redis::Value::Array(consumer_list) = value {
            for consumer_value in consumer_list {
                if let redis::Value::Array(consumer_items) = consumer_value {
                    let mut consumer = StreamConsumer {
                        name: String::new(),
                        pending_count: 0,
                        idle_time_ms: 0,
                    };

                    for i in (0..consumer_items.len()).step_by(2) {
                        if i + 1 < consumer_items.len() {
                            let key = match &consumer_items[i] {
                                redis::Value::BulkString(data) =>
                                    String::from_utf8_lossy(data).to_string(),
                                redis::Value::SimpleString(s) => s.clone(),
                                _ => continue,
                            };

                            match key.as_str() {
                                "name" => {
                                    consumer.name = match &consumer_items[i + 1] {
                                        redis::Value::BulkString(data) =>
                                            String::from_utf8_lossy(data).to_string(),
                                        redis::Value::SimpleString(s) => s.clone(),
                                        _ => continue,
                                    };
                                }
                                "pending" => {
                                    if let redis::Value::Int(count) = &consumer_items[i + 1] {
                                        consumer.pending_count = *count as u64;
                                    }
                                }
                                "idle" => {
                                    if let redis::Value::Int(time) = &consumer_items[i + 1] {
                                        consumer.idle_time_ms = *time as u64;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }

                    if !consumer.name.is_empty() {
                        consumers.push(consumer);
                    }
                }
            }
        }

        Ok(consumers)
    }
}

/// Stream 消费者管理器
pub struct StreamConsumerManager<V> {
    stream: RStream<V>,
    group_name: String,
    consumer_name: String,
    auto_ack: bool,
    batch_size: u64,
    poll_interval: Duration,
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl<V: Serialize + DeserializeOwned + Clone + Send + 'static> StreamConsumerManager<V> {
    pub fn new(
        stream: RStream<V>,
        group_name: String,
        consumer_name: String,
    ) -> Self {
        Self {
            stream,
            group_name,
            consumer_name,
            auto_ack: true,
            batch_size: 10,
            poll_interval: Duration::from_millis(100),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn with_auto_ack(mut self, auto_ack: bool) -> Self {
        self.auto_ack = auto_ack;
        self
    }

    pub fn with_batch_size(mut self, batch_size: u64) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Start consumers
    pub fn start<F>(&mut self, handler: F) -> RedissonResult<()>
    where
        F: Fn(StreamMessage<V>) -> RedissonResult<bool> + Send + Sync + 'static,
    {
        if self.running.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(RedissonError::InvalidOperation("Consumer already running".to_string()));
        }

        self.running.store(true, std::sync::atomic::Ordering::SeqCst);

        let stream = self.stream.clone();
        let group_name = self.group_name.clone();
        let consumer_name = self.consumer_name.clone();
        let auto_ack = self.auto_ack;
        let batch_size = self.batch_size;
        let poll_interval = self.poll_interval;
        let running = self.running.clone();
        let handler = Arc::new(std::sync::Mutex::new(handler));

        std::thread::spawn(move || {
            while running.load(std::sync::atomic::Ordering::SeqCst) {
                match stream.read_group(&group_name, &consumer_name, Some(batch_size), None, false) {
                    Ok(messages) => {
                        for message in messages {
                            let should_ack = if let Ok(handler) = handler.lock() {
                                handler(message.clone()).unwrap_or(false)
                            } else {
                                false
                            };

                            if auto_ack || should_ack {
                                let _ = stream.ack(&group_name, &[message.id.clone()]);
                            }
                        }
                    }
                    Err(err) => {
                        if let RedissonError::RedisError(ref redis_err) = err {
                            if !redis_err.is_timeout() {
                                eprintln!("Error reading from stream: {}", err);
                            }
                        } else {
                            eprintln!("Error reading from stream: {}", err);
                        }
                        std::thread::sleep(poll_interval);
                    }
                }

                std::thread::sleep(poll_interval);
            }
        });

        Ok(())
    }

    /// Stop the consumer
    pub fn stop(&mut self) {
        self.running.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Check if it's running
    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl<V: Serialize + DeserializeOwned + Clone> Clone for RStream<V> {
    fn clone(&self) -> Self {
        Self {
            connection_manager: self.connection_manager.clone(),
            name: self.name.clone(),
            max_len: self.max_len,
            approximate: self.approximate,
            _marker: self._marker,
        }
    }
}