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
use crate::{BatchPriority, BatchProcessor, BatchResult, CommandBuilder, DelCommand, ExpireCommand, GenericCommand, GetCommand, HGetCommand, HSetCommand, IncrByCommand, LPushCommand, RedissonError, RedissonResult, SAddCommand, SetCommand};
use redis::ToRedisArgs;
use std::sync::Arc;
use std::time::Duration;

// ================ RBatch ================

/// Batch Operator - user-friendly interface
pub struct RBatch {
    batch_processor: Arc<BatchProcessor>,
    commands: Vec<Box<dyn CommandBuilder>>,
    with_results: bool,
    use_pipeline: bool,
    with_retry: bool,
    max_retries: u32,
    async_execution: bool,
    priority: BatchPriority,
}

impl RBatch {
    /// Create a new batch operator
    pub fn new(batch_processor: Arc<BatchProcessor>) -> Self {
        Self {
            batch_processor,
            commands: Vec::new(),
            with_results: false,
            use_pipeline: true,
            with_retry: false,
            max_retries: 3,
            async_execution: false,
            priority: BatchPriority::Normal,
        }
    }

    /// Sets whether the result is required
    pub fn with_results(mut self, with_results: bool) -> Self {
        self.with_results = with_results;
        self
    }

    /// Sets whether pipes should be used
    pub fn with_pipeline(mut self, use_pipeline: bool) -> Self {
        self.use_pipeline = use_pipeline;
        self
    }

    /// Enable retry
    pub fn with_retry(mut self, max_retries: u32) -> Self {
        self.with_retry = true;
        self.max_retries = max_retries;
        self
    }

    /// Enable asynchronous execution
    pub fn async_execution(mut self) -> Self {
        self.async_execution = true;
        self
    }

    /// Set execution priority
    pub fn with_priority(mut self, priority: BatchPriority) -> Self {
        self.priority = priority;
        self
    }

    // ============ Convenience method ============

    /// Sets key/value pairs
    pub fn set<K: ToString, V: ToString>(&mut self, key: K, value: V) -> &mut Self {
        self.commands.push(Box::new(SetCommand::new(key, value)));
        self
    }

    /// Sets the key-value pair and specifies an expiration time
    pub fn set_ex<K: ToString, V: ToString>(&mut self, key: K, value: V, ttl: Duration) -> &mut Self {
        self.commands.push(Box::new(SetCommand::new(key, value).with_ttl(ttl)));
        self
    }

    /// Getting keys
    pub fn get<K: ToString>(&mut self, key: K) -> &mut Self {
        self.commands.push(Box::new(GetCommand::new(key)));
        self
    }

    /// Delete key
    pub fn delete<K: ToString>(&mut self, key: K) -> &mut Self {
        self.commands.push(Box::new(DelCommand::new(key)));
        self
    }

    /// Deleting keys in bulk
    pub fn delete_multi<K: ToString>(&mut self, keys: &[K]) -> &mut Self {
        self.commands.push(Box::new(DelCommand::multiple(keys)));
        self
    }

    /// Incrementing integer values
    pub fn incr<K: ToString>(&mut self, key: K, delta: i64) -> &mut Self {
        self.commands.push(Box::new(IncrByCommand::new(key, delta)));
        self
    }

    /// Hash table setting field
    pub fn hset<K: ToString, F: ToString, V: ToString>(&mut self, key: K, field: F, value: V) -> &mut Self {
        self.commands.push(Box::new(HSetCommand::new(key, field, value)));
        self
    }

    /// The hash table gets the field
    pub fn hget<K: ToString, F: ToString>(&mut self, key: K, field: F) -> &mut Self {
        self.commands.push(Box::new(HGetCommand::new(key, field)));
        self
    }

    /// Set the key expiration time
    pub fn expire<K: ToString>(&mut self, key: K, seconds: i64) -> &mut Self {
        self.commands.push(Box::new(ExpireCommand::new(key, seconds)));
        self
    }

    /// Adds to the head of the list
    pub fn lpush<K: ToString, V: ToString>(&mut self, key: K, value: V) -> &mut Self {
        self.commands.push(Box::new(LPushCommand::new(key, value)));
        self
    }

    /// Add to head of list (multiple values)
    pub fn lpush_multi<K: ToString, V: ToString>(&mut self, key: K, values: &[V]) -> &mut Self {
        self.commands.push(Box::new(LPushCommand::multiple(key, values)));
        self
    }

    /// Add to collection
    pub fn sadd<K: ToString, M: ToString>(&mut self, key: K, member: M) -> &mut Self {
        self.commands.push(Box::new(SAddCommand::new(key, member)));
        self
    }

    /// Add to a collection (multiple members)
    pub fn sadd_multi<K: ToString, M: ToString>(&mut self, key: K, members: &[M]) -> &mut Self {
        self.commands.push(Box::new(SAddCommand::multiple(key, members)));
        self
    }

    /// Adding generic commands
    pub fn cmd<'a, T: ToRedisArgs + ToString>(&mut self, args: &'a [T], needs_result: bool) -> RedissonResult<&mut Self> {
        self.commands.push(Box::new(GenericCommand::new(args, needs_result)));
        Ok(self)
    }

    /// Add a custom command builder
    pub fn add_command<C: CommandBuilder + 'static>(&mut self, command: C) -> &mut Self {
        self.commands.push(Box::new(command));
        self
    }

    // ============ Executing methods ============

    /// Performing batch processing
    pub fn execute(&mut self) -> RedissonResult<Option<Vec<BatchResult>>> {
        if self.commands.is_empty() {
            return Ok(None);
        }

        // If you do it asynchronously
        if self.async_execution {
            return self.execute_async();
        }

        let result = if self.with_retry {
            // Execution with retry
            self.batch_processor.execute_batch_with_retry(
                self.commands.clone(),
                self.with_results,
                self.max_retries,
            )?
        } else if self.with_results {
            // Queries are processed in batches and results are returned
            let results = self.batch_processor.query_batch(self.commands.clone())?;
            Some(results)
        } else {
            // Batch processing is performed without returning the results
            self.batch_processor.exec_batch(self.commands.clone())?;
            None
        };

        // Empty command
        self.commands.clear();

        Ok(result)
    }

    /// Asynchronous execution
    fn execute_async(&self) -> RedissonResult<Option<Vec<BatchResult>>> {
        if self.with_results {
            // Asynchronous query
            let (tx, rx) = std::sync::mpsc::channel();

            self.batch_processor.query_batch_async(
                self.commands.clone(),
                move |result| {
                    let _ = tx.send(result);
                },
            )?;

            // Triggering a refresh
            self.batch_processor.trigger_flush()?;

            // Waiting for results
            let results = rx.recv()
                .map_err(|_| RedissonError::PoolError("Failed to receive async result".to_string()))??;

            Ok(Some(results))
        } else {
            // Asynchronous execution
            let (tx, rx) = std::sync::mpsc::channel();

            self.batch_processor.exec_batch_async(
                self.commands.clone(),
                move |result| {
                    let _ = tx.send(result);
                },
            )?;

            // Triggering a refresh
            self.batch_processor.trigger_flush()?;

            // Waiting for completion
            rx.recv()
                .map_err(|_| RedissonError::PoolError("Failed to receive async result".to_string()))??;

            Ok(None)
        }
    }

    /// Getting the number of commands
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Is empty?
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// Clear all commands
    pub fn clear(&mut self) {
        self.commands.clear();
    }

    /// Execute without waiting for the result (asynchronous mode only)
    pub fn execute_fire_and_forget(&mut self) -> RedissonResult<()> {
        if !self.async_execution {
            return Err(RedissonError::InvalidOperation(
                "Fire and forget Only asynchronous mode is supported".to_string(),
            ));
        }

        // Executed asynchronously, without handling callbacks
        if self.with_results {
            self.batch_processor.query_batch_async(
                self.commands.clone(),
                |_| {}, // Empty callback
            )?;
        } else {
            self.batch_processor.exec_batch_async(
                self.commands.clone(),
                |_| {}, // Empty callback
            )?;
        }

        // Triggering a refresh
        self.batch_processor.trigger_flush()?;

        // Empty command
        self.commands.clear();

        Ok(())
    }
}

/// Chained batch builder
pub struct RBatchBuilder {
    batch: RBatch,
}

impl RBatchBuilder {
    pub fn new(batch_processor: Arc<BatchProcessor>) -> Self {
        Self {
            batch: RBatch::new(batch_processor),
        }
    }

    pub fn set<K: ToString, V: ToString>(mut self, key: K, value: V) -> Self {
        self.batch.set(key, value);
        self
    }

    pub fn get<K: ToString>(mut self, key: K) -> Self {
        self.batch.get(key);
        self
    }

    pub fn delete<K: ToString>(mut self, key: K) -> Self {
        self.batch.delete(key);
        self
    }

    pub fn with_results(mut self, with_results: bool) -> Self {
        self.batch = self.batch.with_results(with_results);
        self
    }

    pub fn with_pipeline(mut self, use_pipeline: bool) -> Self {
        self.batch = self.batch.with_pipeline(use_pipeline);
        self
    }

    pub fn with_retry(mut self, max_retries: u32) -> Self {
        self.batch = self.batch.with_retry(max_retries);
        self
    }

    pub fn async_execution(mut self) -> Self {
        self.batch = self.batch.async_execution();
        self
    }

    pub fn with_priority(mut self, priority: BatchPriority) -> Self {
        self.batch = self.batch.with_priority(priority);
        self
    }

    pub fn build(self) -> RBatch {
        self.batch
    }
}

// ================ Examples of Use ================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::RedissonClient;
    use crate::config::RedissonConfig;
    use crate::{BatchConfig, GetCommand, SetCommand};

    fn create_test_client() -> RedissonClient {
        let config = RedissonConfig::single_server("redis://127.0.0.1:6379")
            .with_pool_size(5);
        RedissonClient::new(config).unwrap()
    }

    #[test]
    fn test_batch_execute() {
        let client = create_test_client();
        let processor = BatchProcessor::new(
            client.get_connection_manager().clone(),
            BatchConfig::default(),
        ).unwrap();

        let mut batch = RBatch::new(Arc::new(processor));
        batch.set("test:key1", "value1");
        batch.set("test:key2", "value2");
        batch.get("test:key1");

        // Executes but does not return a result
        let result = batch.execute().unwrap();
        assert!(result.is_none());

        // Validation results
        let mut batch2 = RBatch::new(batch.batch_processor.clone());
        batch2.get("test:key1").get("test:key2").with_results = true;

        let results = batch2.execute().unwrap().unwrap();
        assert_eq!(results.len(), 2);

        if let BatchResult::String(value) = &results[0] {
            assert_eq!(value, "value1");
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_batch_with_retry() {
        let client = create_test_client();
        let processor = BatchProcessor::new(
            client.get_connection_manager().clone(),
            BatchConfig::default().with_retry(3),
        ).unwrap();

        let mut batch = RBatchBuilder::new(Arc::new(processor))
            .set("retry:key1", "value1")
            .with_retry(3)
            .with_results(false)
            .build();

        let result = batch.execute().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_command_builder() {
        let set_cmd = SetCommand::new("test:key", "test:value");
        assert_eq!(set_cmd.command_name(), "SET");
        assert!(!set_cmd.needs_result());

        let get_cmd = GetCommand::new("test:key");
        assert_eq!(get_cmd.command_name(), "GET");
        assert!(get_cmd.needs_result());

    }
}