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
use redis::ToRedisArgs;
use tokio::time;
use tracing::warn;
use crate::{AsyncBatchProcessor, BatchPriority, BatchResult, CommandBuilder, DelCommand, ExpireCommand, GenericCommand, GetCommand, HGetCommand, HSetCommand, IncrByCommand, LPushCommand, RedissonResult, SAddCommand, SetCommand};

/// Asynchronous batch operators
pub struct AsyncRBatch {
    batch_processor: Arc<AsyncBatchProcessor>,
    commands: Vec<Box<dyn CommandBuilder>>,
    with_results: bool,
    use_pipeline: bool,
    with_retry: bool,
    max_retries: u32,
    priority: BatchPriority,
    callback: Option<Box<dyn FnOnce(RedissonResult<Option<Vec<BatchResult>>>) + Send + Sync>>,
}

impl AsyncRBatch {
    /// Create a new asynchronous batch operator
    pub fn new(batch_processor: Arc<AsyncBatchProcessor>) -> Self {
        Self {
            batch_processor,
            commands: Vec::new(),
            with_results: false,
            use_pipeline: true,
            with_retry: false,
            max_retries: 3,
            priority: BatchPriority::Normal,
            callback: None,
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

    /// Set execution priority
    pub fn with_priority(mut self, priority: BatchPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Set up the callback
    pub fn with_callback<F>(mut self, callback: F) -> Self
    where
        F: FnOnce(RedissonResult<Option<Vec<BatchResult>>>) + Send + Sync + 'static,
    {
        self.callback = Some(Box::new(callback));
        self
    }

    // ============ Convenience method ============

    /// Sets key/value pairs
    pub fn set<K: ToString, V: ToString>(&mut self, key: K, value: V) -> &mut Self {
        self.commands.push(Box::new(SetCommand::new(key, value)));
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

    /// Add to collection
    pub fn sadd<K: ToString, M: ToString>(&mut self, key: K, member: M) -> &mut Self {
        self.commands.push(Box::new(SAddCommand::new(key, member)));
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

    /// Perform batch processing asynchronously
    pub async fn execute(&mut self) -> RedissonResult<Option<Vec<BatchResult>>> {
        if self.commands.is_empty() {
            return Ok(None);
        }

        // If there are callbacks, they are executed asynchronously using a queue
        if let Some(callback) = self.callback.take() {
            let commands = std::mem::take(&mut self.commands);
            let processor = self.batch_processor.clone();
            let priority = self.priority;
            let needs_result = self.with_results;

            // Add to the queue asynchronously
            processor.add_to_queue(
                commands,
                priority,
                move |result| {
                    let final_result = match result {
                        Ok(results) => {
                            if needs_result {
                                Ok(Some(results))
                            } else {
                                Ok(None)
                            }
                        }
                        Err(e) => Err(e),
                    };
                    callback(final_result);
                },
            ).await?;

            return Ok(None);
        }

        // Direct execution
        let result = if self.with_retry {
            self.execute_with_retry().await?
        } else if self.with_results {
            let results = self.batch_processor.query_batch(self.commands.clone()).await?;
            Some(results)
        } else {
            self.batch_processor.exec_batch(self.commands.clone()).await?;
            None
        };

        self.commands.clear();
        Ok(result)
    }

    /// Execution with retry
    async fn execute_with_retry(&self) -> RedissonResult<Option<Vec<BatchResult>>> {
        let mut retry_count = 0;
        let mut backoff_duration = Duration::from_millis(self.batch_processor.get_batch_config().initial_backoff_ms);

        loop {
            let result = if self.with_results {
                self.batch_processor.query_batch(self.commands.clone()).await
                    .map(|results| Some(results))
            } else {
                self.batch_processor.exec_batch(self.commands.clone()).await
                    .map(|_| None)
            };

            match result {
                Ok(results) => return Ok(results),
                Err(err) => {
                    if retry_count < self.max_retries {
                        retry_count += 1;
                        warn!("Batch execution failed and is retrying({}/{}): {}", 
                              retry_count, self.max_retries, err);

                        // Exponential backoff
                        time::sleep(backoff_duration).await;
                        backoff_duration = (backoff_duration * 2)
                            .min(Duration::from_millis(self.batch_processor.get_batch_config().max_backoff_ms));

                        continue;
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }

    /// Execute and wait for results (synchronous, but internally asynchronous)
    pub async fn execute_and_wait(&mut self) -> RedissonResult<Option<Vec<BatchResult>>> {
        self.execute().await
    }

    /// Execute without waiting for the result (fire and forget)
    pub async fn execute_fire_and_forget(&mut self) -> RedissonResult<()> {
        if self.commands.is_empty() {
            return Ok(());
        }

        // Use an empty callback
        let commands = std::mem::take(&mut self.commands);
        let processor = self.batch_processor.clone();
        let priority = self.priority;

        processor.add_to_queue(
            commands,
            priority,
            |_| {}, // Empty callback
        ).await?;

        Ok(())
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
}

/// Asynchronous batch operator builder
pub struct AsyncRBatchBuilder {
    batch: AsyncRBatch,
}

impl AsyncRBatchBuilder {
    pub fn new(batch_processor: Arc<AsyncBatchProcessor>) -> Self {
        Self {
            batch: AsyncRBatch::new(batch_processor),
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

    pub fn with_priority(mut self, priority: BatchPriority) -> Self {
        self.batch = self.batch.with_priority(priority);
        self
    }

    pub fn with_callback<F>(mut self, callback: F) -> Self
    where
        F: FnOnce(RedissonResult<Option<Vec<BatchResult>>>) + Send + Sync + 'static,
    {
        self.batch = self.batch.with_callback(callback);
        self
    }

    pub fn build(self) -> AsyncRBatch {
        self.batch
    }
}

// ================ Examples of Use ================

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use super::*;
    use tokio::runtime::Runtime;
    use crate::AsyncBatchProcessor;

    fn create_test_async_processor() -> AsyncBatchProcessor {
        todo!("Create an asynchronous connection manager for testing")
    }

    #[tokio::test]
    async fn test_async_batch_execute() {
        let processor = create_test_async_processor();
        let processor = Arc::new(processor);

        let mut batch = AsyncRBatch::new(processor.clone());
        batch.set("async:test:key1", "value1");
        batch.set("async:test:key2", "value2");
        batch.get("async:test:key1");
        batch.with_results = true;

        let results = batch.execute().await.unwrap().unwrap();
        assert_eq!(results.len(), 3);

        if let BatchResult::String(value) = &results[2] {
            assert_eq!(value, "value1");
        } else {
            panic!("Expected string result");
        }
    }

    #[tokio::test]
    async fn test_async_batch_callback() {
        let processor = create_test_async_processor();
        let processor = Arc::new(processor);

        let (tx, rx) = tokio::sync::oneshot::channel();

        let mut batch = AsyncRBatchBuilder::new(processor.clone())
            .set("async:callback:key1", "callback_value1")
            .with_results(true)
            .with_callback(move |result| {
                let _ = tx.send(result);
            })
            .build();

        // Asynchronous execution
        batch.execute().await.unwrap();

        // Wait for the result of the callback
        let result = rx.await.unwrap().unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_async_batch_retry() {
        let processor = create_test_async_processor();
        let processor = Arc::new(processor);

        let mut batch = AsyncRBatchBuilder::new(processor.clone())
            .set("async:retry:key1", "retry_value1")
            .with_retry(3)
            .with_results(false)
            .build();

        let result = batch.execute().await.unwrap();
        assert!(result.is_none());
    }
}