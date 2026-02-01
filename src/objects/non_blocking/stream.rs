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
use crate::errors::RedissonError;
use crate::{AsyncBaseDistributedObject, AsyncRObjectBase, AsyncRedisConnectionManager, PendingMessage, RedissonResult, StreamConsumer, StreamGroup, StreamInfo, StreamMessage};
use futures::future::BoxFuture;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// === AsyncRStream Asynchronous Stream implementation ===

/// The asynchronous Stream type
pub struct AsyncRStream<V> {
    base: AsyncBaseDistributedObject,
    max_len: Option<u64>,
    approximate: bool,
    _marker: std::marker::PhantomData<V>,
}

impl<V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> AsyncRStream<V> {
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
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

    /// Adding messages to a Stream asynchronously
    pub async fn add(&self, id: &str, fields: &HashMap<String, V>) -> RedissonResult<String> {
        let mut conn = self.base.get_connection().await?;

        let mut args = vec![self.base.get_full_key(), id.to_string()];

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

        let message_id: String = cmd.query_async(&mut conn).await?;

        Ok(message_id)
    }

    /// Adding messages asynchronously (automatically generating ids)
    pub async fn add_auto_id(&self, fields: &HashMap<String, V>) -> RedissonResult<String> {
        self.add("*", fields).await
    }

    /// Read messages asynchronously
    pub async fn read(
        &self,
        start_id: &str,
        count: Option<u64>,
        block_ms: Option<u64>,
    ) -> RedissonResult<Vec<StreamMessage<V>>> {
        let mut conn = self.base.get_connection().await?;

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
        cmd.arg("STREAMS").arg(&self.base.get_full_key()).arg(start_id);

        let result = cmd.query_async::<redis::Value>(&mut conn).await;

        self.parse_xread_result(result).await
    }

    /// Asynchronous ranges read messages
    pub async fn range(
        &self,
        start_id: &str,
        end_id: &str,
        count: Option<u64>,
    ) -> RedissonResult<Vec<StreamMessage<V>>> {
        let mut conn = self.base.get_connection().await?;

        let mut cmd = redis::cmd("XRANGE");
        cmd.arg(&self.base.get_full_key()).arg(start_id).arg(end_id);

        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }

        let result = cmd.query_async::<redis::Value>(&mut conn).await;

        self.parse_xrange_result(result).await
    }

    /// Asynchronous reverse range read messages
    pub async fn rev_range(
        &self,
        start_id: &str,
        end_id: &str,
        count: Option<u64>,
    ) -> RedissonResult<Vec<StreamMessage<V>>> {
        let mut conn = self.base.get_connection().await?;

        let mut cmd = redis::cmd("XREVRANGE");
        cmd.arg(&self.base.get_full_key()).arg(start_id).arg(end_id);

        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }

        let result = cmd.query_async::<redis::Value>(&mut conn).await;

        self.parse_xrange_result(result).await
    }

    /// Get the length of the Stream asynchronously
    pub async fn len(&self) -> RedissonResult<u64> {
        let mut conn = self.base.get_connection().await?;

        let len: i64 = redis::cmd("XLEN")
            .arg(&self.base.get_full_key())
            .query_async(&mut conn)
            .await?;

        Ok(len as u64)
    }

    /// Delete messages asynchronously
    pub async fn delete(&self, ids: &[String]) -> RedissonResult<u64> {
        let mut conn = self.base.get_connection().await?;

        let mut cmd = redis::cmd("XDEL");
        cmd.arg(&self.base.get_full_key());

        for id in ids {
            cmd.arg(id);
        }

        let deleted: i64 = cmd.query_async(&mut conn).await?;
        Ok(deleted as u64)
    }

    /// Pruning a Stream asynchronously (keeping recent messages)
    pub async fn trim(&self, max_len: u64, approximate: bool) -> RedissonResult<u64> {
        let mut conn = self.base.get_connection().await?;

        let mut cmd = redis::cmd("XTRIM");
        cmd.arg(&self.base.get_full_key()).arg("MAXLEN");

        if approximate {
            cmd.arg("~");
        }

        cmd.arg(max_len.to_string());

        let trimmed: i64 = cmd.query_async(&mut conn).await?;
        Ok(trimmed as u64)
    }

    /// Create consumption groups asynchronously
    pub async fn create_group(&self, group_name: &str, start_id: &str) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;

        let result: String = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.base.get_full_key())
            .arg(group_name)
            .arg(start_id)
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await?;

        Ok(result == "OK")
    }

    /// Delete a consumption group asynchronously
    pub async fn delete_group(&self, group_name: &str) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;

        let deleted: i64 = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(&self.base.get_full_key())
            .arg(group_name)
            .query_async(&mut conn)
            .await?;

        Ok(deleted > 0)
    }

    /// Read messages from the consuming group asynchronously
    pub async fn read_group(
        &self,
        group_name: &str,
        consumer_name: &str,
        count: Option<u64>,
        block_ms: Option<u64>,
        no_ack: bool,
    ) -> RedissonResult<Vec<StreamMessage<V>>> {
        let mut conn = self.base.get_connection().await?;

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
        cmd.arg("STREAMS").arg(&self.base.get_full_key()).arg(">");

        let result = cmd.query_async::<redis::Value>(&mut conn).await;

        self.parse_xread_result(result).await
    }

    /// Asynchronous confirmation that the message has been processed
    pub async fn ack(&self, group_name: &str, ids: &[String]) -> RedissonResult<u64> {
        let mut conn = self.base.get_connection().await?;

        let mut cmd = redis::cmd("XACK");
        cmd.arg(&self.base.get_full_key()).arg(group_name);

        for id in ids {
            cmd.arg(id);
        }

        let acked: i64 = cmd.query_async(&mut conn).await?;
        Ok(acked as u64)
    }

    /// Retrieve pending messages asynchronously
    pub async fn pending(
        &self,
        group_name: &str,
        start_id: Option<&str>,
        end_id: Option<&str>,
        count: Option<u64>,
        consumer_name: Option<&str>,
    ) -> RedissonResult<Vec<PendingMessage>> {
        let mut conn = self.base.get_connection().await?;

        let mut cmd = redis::cmd("XPENDING");
        cmd.arg(&self.base.get_full_key()).arg(group_name);

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

        let result = cmd.query_async::<redis::Value>(&mut conn).await;

        self.parse_pending_result(result).await
    }

    /// Declare pending messages asynchronously
    pub async fn claim(
        &self,
        group_name: &str,
        consumer_name: &str,
        min_idle_time_ms: u64,
        ids: &[String],
    ) -> RedissonResult<Vec<StreamMessage<V>>> {
        let mut conn = self.base.get_connection().await?;

        let mut cmd = redis::cmd("XCLAIM");
        cmd.arg(&self.base.get_full_key())
            .arg(group_name)
            .arg(consumer_name)
            .arg(min_idle_time_ms.to_string());

        for id in ids {
            cmd.arg(id);
        }

        let result = cmd.query_async::<redis::Value>(&mut conn).await;

        self.parse_xrange_result(result).await
    }

    /// Retrieve Stream information asynchronously
    pub async fn info(&self) -> RedissonResult<StreamInfo> {
        let mut conn = self.base.get_connection().await?;

        let info: StreamInfo = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(&self.base.get_full_key())
            .query_async(&mut conn)
            .await?;

        Ok(info)
    }

    /// Get the consumption group information asynchronously
    pub async fn groups_info(&self) -> RedissonResult<Vec<StreamGroup>> {
        let mut conn = self.base.get_connection().await?;

        let groups_value: redis::Value = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(&self.base.get_full_key())
            .query_async(&mut conn)
            .await?;

        self.parse_groups_info(groups_value).await
    }

    /// Fetch consumer information asynchronously
    pub async fn consumers_info(&self, group_name: &str) -> RedissonResult<Vec<StreamConsumer>> {
        let mut conn = self.base.get_connection().await?;

        let consumers_value: redis::Value = redis::cmd("XINFO")
            .arg("CONSUMERS")
            .arg(&self.base.get_full_key())
            .arg(group_name)
            .query_async(&mut conn)
            .await?;

        self.parse_consumers_info(consumers_value).await
    }

    /// Parse the XREAD/XREADGROUP result
    async fn parse_xread_result(&self, result: Result<redis::Value, redis::RedisError>) -> RedissonResult<Vec<StreamMessage<V>>> {
        match result {
            Ok(value) => {
                if let redis::Value::Nil = value {
                    return Ok(Vec::new());
                }

                if let redis::Value::Array(streams) = value {
                    for stream in streams {
                        if let redis::Value::Array(items) = stream {
                            if items.len() >= 2 {
                                // items[0] is the stream name, and items[1] is the array of messages
                                if let redis::Value::Array(messages) = &items[1] {
                                    return self.parse_messages_array(messages).await;
                                }
                            }
                        }
                    }
                }
                Ok(Vec::new())
            }
            Err(err) => {
                // Check for a timeout error
                let err_str = err.to_string();
                if err_str.contains("timeout") {
                    // The read timeout returns an empty result
                    Ok(Vec::new())
                } else {
                    Err(RedissonError::PoolError(err_str))
                }
            }
        }
    }

    /// Parse the XRANGE/XREVRANGE/XCLAIM result
    async fn parse_xrange_result(&self, result: Result<redis::Value, redis::RedisError>) -> RedissonResult<Vec<StreamMessage<V>>> {
        match result {
            Ok(value) => {
                if let redis::Value::Nil = value {
                    return Ok(Vec::new());
                }

                if let redis::Value::Array(messages) = value {
                    return self.parse_messages_array(&messages).await;
                }
                Ok(Vec::new())
            }
            Err(err) => Err(RedissonError::PoolError(err.to_string())),
        }
    }

    /// Parsing an array of messages
    async fn parse_messages_array(&self, messages: &[redis::Value]) -> RedissonResult<Vec<StreamMessage<V>>> {
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
    async fn parse_pending_result(&self, result: Result<redis::Value, redis::RedisError>) -> RedissonResult<Vec<PendingMessage>> {
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
            Err(err) => Err(RedissonError::PoolError(err.to_string())),
        }
    }

    /// Parse the consumption group information
    async fn parse_groups_info(&self, value: redis::Value) -> RedissonResult<Vec<StreamGroup>> {
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
    async fn parse_consumers_info(&self, value: redis::Value) -> RedissonResult<Vec<StreamConsumer>> {
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


/// Stream Message handler function type
type AsyncStreamHandler<V> = Box<dyn Fn(StreamMessage<V>) -> BoxFuture<'static, RedissonResult<bool>> + Send + Sync>;

/// Asynchronous Stream consumer manager
pub struct AsyncStreamConsumerManager<V> {
    stream: AsyncRStream<V>,
    group_name: String,
    consumer_name: String,
    auto_ack: bool,
    batch_size: u64,
    poll_interval: Duration,
    running: Arc<tokio::sync::watch::Sender<bool>>,
}

impl<V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> AsyncStreamConsumerManager<V> {
    pub fn new(
        stream: AsyncRStream<V>,
        group_name: String,
        consumer_name: String,
    ) -> Self {
        let (sender, _) = tokio::sync::watch::channel(false);

        Self {
            stream,
            group_name,
            consumer_name,
            auto_ack: true,
            batch_size: 10,
            poll_interval: Duration::from_millis(100),
            running: Arc::new(sender),
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

    /// Start the consumer asynchronously
    pub async fn start(&mut self, handler: AsyncStreamHandler<V>) -> RedissonResult<()> {
        // Check if it's running
        if *self.running.borrow() {
            return Err(RedissonError::InvalidOperation("Consumer already running".to_string()));
        }

        // Start consumers
        self.running.send(true).map_err(|_|
            RedissonError::InvalidOperation("Failed to start consumer".to_string())
        )?;

        let stream = self.stream.clone();
        let group_name = self.group_name.clone();
        let consumer_name = self.consumer_name.clone();
        let auto_ack = self.auto_ack;
        let batch_size = self.batch_size;
        let poll_interval = self.poll_interval;
        let running_receiver = self.running.subscribe();
        let handler = Arc::new(handler);

        // Start an asynchronous consume task
        tokio::spawn(async move {
            let mut receiver = running_receiver;
            let handler = handler.clone();

            while *receiver.borrow_and_update() {
                match stream.read_group(
                    &group_name,
                    &consumer_name,
                    Some(batch_size),
                    None,
                    false
                ).await {
                    Ok(messages) => {
                        for message in messages {
                            // Processing messages
                            let should_ack = handler(message.clone()).await.unwrap_or_else(|err| {
                                eprintln!("Error processing message: {}", err);
                                false
                            });

                            // Confirmation message
                            if auto_ack || should_ack {
                                if let Err(err) = stream.ack(&group_name, &[message.id.clone()]).await {
                                    eprintln!("Error acking message: {}", err);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        // Handling read errors
                        eprintln!("Error reading from stream: {}", err);
                        tokio::time::sleep(poll_interval).await;
                    }
                }

                // Sleep briefly to avoid busy cycles
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        Ok(())
    }

    /// Stop the consumer asynchronously
    pub async fn stop(&mut self) -> RedissonResult<()> {
        self.running.send(false).map_err(|_|
            RedissonError::InvalidOperation("Failed to stop consumer".to_string())
        )?;

        // Wait a while to make sure the consumer stops
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    /// Check if it's running
    pub fn is_running(&self) -> bool {
        *self.running.borrow()
    }
}


impl<V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> Clone for AsyncRStream<V> {
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
            max_len: self.max_len,
            approximate: self.approximate,
            _marker: self._marker,
        }
    }
}

impl<V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> AsyncRStream<V> {
    pub async fn rename(&self, new_name: &str) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;
        redis::cmd("RENAME")
            .arg(&self.base.get_full_key())
            .arg(new_name)
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn is_exists(&self) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let exists: i32 = redis::cmd("EXISTS")
            .arg(&self.base.get_full_key())
            .query_async(&mut conn)
            .await?;
        Ok(exists > 0)
    }

    pub async fn move_to_db(&self, db_index: i32) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let moved: i32 = redis::cmd("MOVE")
            .arg(&self.base.get_full_key())
            .arg(db_index)
            .query_async(&mut conn)
            .await?;
        Ok(moved > 0)
    }

    pub async fn get_expire_time(&self) -> RedissonResult<Option<Duration>> {
        let mut conn = self.base.get_connection().await?;
        let ttl: i64 = redis::cmd("PTTL")
            .arg(&self.base.get_full_key())
            .query_async(&mut conn)
            .await?;

        if ttl > 0 {
            Ok(Some(Duration::from_millis(ttl as u64)))
        } else {
            Ok(None)
        }
    }

    pub async fn expire(&self, duration: Duration) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let result: i32 = redis::cmd("PEXPIRE")
            .arg(&self.base.get_full_key())
            .arg(duration.as_millis() as i64)
            .query_async(&mut conn)
            .await?;
        Ok(result > 0)
    }

    pub async fn expire_at(&self, timestamp: i64) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let result: i32 = redis::cmd("EXPIREAT")
            .arg(&self.base.get_full_key())
            .arg(timestamp)
            .query_async(&mut conn)
            .await?;
        Ok(result > 0)
    }

    pub async fn clear_expire(&self) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let result: i32 = redis::cmd("PERSIST")
            .arg(&self.base.get_full_key())
            .query_async(&mut conn)
            .await?;
        Ok(result > 0)
    }
}