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

// ================ Synchronous transaction batch processor ================

use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::Mutex;
use redis::Value;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::{convert_value_to_batch_result, BatchConfig, BatchProcessor, BatchResult, BatchStats, CommandBuilder, DelCommand, ExpireCommand, GenericCommand, GetCommand, HGetCommand, HSetCommand, IncrByCommand, LPushCommand, RedissonError, RedissonResult, SAddCommand, SetCommand, SyncRedisConnectionManager, TransactionConfig, TransactionResult, TransactionStats};

// ================ Synchronous transaction context ================

/// Synchronous transaction context
pub struct SyncTransactionContext {
    manager: Arc<SyncRedisConnectionManager>,
    watch_keys: Vec<String>,
    commands: Vec<Box<dyn CommandBuilder>>,
    read_only: bool,
    optimistic_lock_attempts: usize,
    batch_processor: Arc<BatchProcessor>,
    transaction_config: TransactionConfig,
}

impl SyncTransactionContext {
    /// Create a new transaction context
    pub fn new(manager: Arc<SyncRedisConnectionManager>) -> Self {
        let transaction_config = TransactionConfig::default();
        let batch_config = BatchConfig::default();
        let batch_processor = BatchProcessor::new(
            manager.clone(),
            batch_config,
        ).expect("Failed to create batch processor");

        Self {
            manager,
            watch_keys: Vec::new(),
            commands: Vec::new(),
            read_only: false,
            optimistic_lock_attempts: 0,
            batch_processor: Arc::new(batch_processor),
            transaction_config,
        }
    }

    /// Create a transaction context using a configuration
    pub fn with_config(
        manager: Arc<SyncRedisConnectionManager>,
        config: TransactionConfig,
        batch_config: Option<BatchConfig>,
    ) -> Self {
        let batch_config = batch_config.unwrap_or_default();
        let batch_processor = BatchProcessor::new(
            manager.clone(),
            batch_config,
        ).expect("Failed to create batch processor");

        Self {
            manager,
            watch_keys: Vec::new(),
            commands: Vec::new(),
            read_only: false,
            optimistic_lock_attempts: 0,
            batch_processor: Arc::new(batch_processor),
            transaction_config: config,
        }
    }

    /// Setting up transaction configuration
    pub fn set_config(&mut self, config: TransactionConfig) -> &mut Self {
        self.transaction_config = config;
        self
    }

    /// Set the batch configuration
    pub fn set_batch_config(&mut self, config: BatchConfig) -> RedissonResult<&mut Self> {
        self.batch_processor = Arc::new(BatchProcessor::new(
            self.manager.clone(),
            config,
        )?);
        Ok(self)
    }

    /// Watch key
    pub fn watch(&mut self, key: &str) -> &mut Self {
        if self.transaction_config.enable_watch {
            self.watch_keys.push(key.to_string());
        }
        self
    }

    /// Monitoring multiple keys
    pub fn watch_multi(&mut self, keys: &[&str]) -> &mut Self {
        if self.transaction_config.enable_watch {
            for key in keys {
                self.watch_keys.push(key.to_string());
            }
        }
        self
    }

    /// Set as a read-only transaction
    pub fn read_only(&mut self) -> &mut Self {
        self.read_only = true;
        self
    }

    // ========== Data manipulation commands ==========

    pub fn query<K, T>(&mut self, key: K) -> RedissonResult<T>
    where
        K: ToString,
        T: for<'de> Deserialize<'de>,
    {
        let key_str = key.to_string();
        let mut conn = self.manager.get_connection()?;

        // If you need to monitor, monitor this key first
        if !self.watch_keys.contains(&key_str) && self.transaction_config.enable_watch {
            self.watch_keys.push(key_str.clone());
        }

        // Executing queries
        let result: Option<String> = redis::cmd("GET").arg(&key_str).query(&mut conn)?;

        match result {
            Some(json_str) => {
                serde_json::from_str(&json_str).map_err(|e| RedissonError::SerializationError(e.to_string()))
            }
            None => Err(RedissonError::InvalidOperation(key_str)),
        }
    }

    // The hash field is queried immediately and the result is returned
    pub fn hquery<K, F, T>(&mut self, key: K, field: F) -> RedissonResult<T>
    where
        K: ToString,
        F: ToString,
        T: for<'de> Deserialize<'de>,
    {
        let key_str = key.to_string();
        let field_str = field.to_string();
        let mut conn = self.manager.get_connection()?;

        // If you need to monitor, monitor this key first
        if !self.watch_keys.contains(&key_str) && self.transaction_config.enable_watch {
            self.watch_keys.push(key_str.clone());
        }

        // Executing queries
        let result: Option<String> = redis::cmd("HGET").arg(&key_str).arg(&field_str).query(&mut conn)?;

        match result {
            Some(json_str) => {
                serde_json::from_str(&json_str).map_err(|e| RedissonError::SerializationError(e.to_string()))
            }
            None => Err(RedissonError::InvalidOperation(format!("{}.{}", key_str, field_str))),
        }
    }

    /// Execute immediately and get the result
    pub fn exec_and_get<K, T>(&mut self, key: K) -> RedissonResult<T>
    where
        K: ToString,
        T: for<'de> Deserialize<'de>,
    {
        // First add the query command to the transaction
        self.get(key);

        // Executing transactions
        let result = self.execute()?;

        // Extract the result of the last command from the result
        if let Some(last_result) = result.results.last() {
            match last_result {
                BatchResult::String(s) => {
                    return serde_json::from_str(s)
                        .map_err(|e| RedissonError::SerializationError(e.to_string()));
                }
                BatchResult::Nil => {
                    return Err(RedissonError::InvalidOperation("Key Data not exist".to_string()));
                }
                _ => Err(RedissonError::InvalidOperation("Unexpected result type".to_string())),
            }
        } else {
            Err(RedissonError::InvalidOperation("No results returned".to_string()))
        }
    }

    /// Execute immediately and get all results
    pub fn exec_and_get_all(&mut self) -> RedissonResult<TransactionResult> {
        self.execute()
    }

    /// Setting keys
    pub fn set<K, V>(&mut self, key: K, value: V) -> RedissonResult<&mut Self>
    where
        K: ToString,
        V: Serialize,
    {
        let value_json = serde_json::to_string(&value)?;
        let command = SetCommand::new(key, value_json);
        self.commands.push(Box::new(command));
        Ok(self)
    }

    /// Sets the key value with an expiration time
    pub fn set_ex<K, V>(&mut self, key: K, value: V, ttl: Duration) -> RedissonResult<&mut Self>
    where
        K: ToString,
        V: Serialize,
    {
        let value_json = serde_json::to_string(&value)?;
        let command = SetCommand::new(key, value_json).with_ttl(ttl);
        self.commands.push(Box::new(command));
        Ok(self)
    }

    /// Set only if the key does not exist
    pub fn set_nx<K, V>(&mut self, key: K, value: V) -> RedissonResult<&mut Self>
    where
        K: ToString,
        V: Serialize,
    {
        let value_json = serde_json::to_string(&value)?;
        // Build the SETNX command using GenericCommand
        let command = GenericCommand::new(&["SETNX", &key.to_string(), &value_json], true);
        self.commands.push(Box::new(command));
        Ok(self)
    }

    /// Getting keys
    pub fn get<K>(&mut self, key: K) -> &mut Self
    where
        K: ToString,
    {
        let command = GetCommand::new(key);
        self.commands.push(Box::new(command));
        self
    }

    /// Delete key
    pub fn del<K>(&mut self, key: K) -> &mut Self
    where
        K: ToString,
    {
        let command = DelCommand::new(key);
        self.commands.push(Box::new(command));
        self
    }

    /// Removing multiple keys
    pub fn del_multi<K, I>(&mut self, keys: I) -> &mut Self
    where
        K: ToString,
        I: IntoIterator<Item = K>,
    {
        let keys_vec: Vec<String> = keys.into_iter().map(|k| k.to_string()).collect();
        let command = DelCommand::multiple(&keys_vec);
        self.commands.push(Box::new(command));
        self
    }

    /// Increment operation
    pub fn incr<K>(&mut self, key: K, delta: i64) -> &mut Self
    where
        K: ToString,
    {
        let command = IncrByCommand::new(key, delta);
        self.commands.push(Box::new(command));
        self
    }

    /// Hash table setting field
    pub fn hset<K, F, V>(&mut self, key: K, field: F, value: V) -> RedissonResult<&mut Self>
    where
        K: ToString,
        F: ToString,
        V: Serialize,
    {
        let value_json = serde_json::to_string(&value)?;
        let command = HSetCommand::new(key, field, value_json);
        self.commands.push(Box::new(command));
        Ok(self)
    }

    /// The hash table gets the field
    pub fn hget<K, F>(&mut self, key: K, field: F) -> &mut Self
    where
        K: ToString,
        F: ToString,
    {
        let command = HGetCommand::new(key, field);
        self.commands.push(Box::new(command));
        self
    }

    /// Hash table removes fields
    pub fn hdel<K, F, I>(&mut self, key: K, fields: I) -> &mut Self
    where
        K: ToString,
        F: ToString,
        I: IntoIterator<Item = F>,
    {
        let fields_vec: Vec<String> = fields.into_iter().map(|f| f.to_string()).collect();
        // Build the HDEL command using GenericCommand
        let mut args = vec!["HDEL".to_string(), key.to_string()];
        args.extend(fields_vec.clone());
        let command = GenericCommand::new(&args, true);
        self.commands.push(Box::new(command));
        self
    }

    /// Add to collection
    pub fn sadd<K, V, I>(&mut self, key: K, members: I) -> RedissonResult<&mut Self>
    where
        K: ToString,
        V: Serialize,
        I: IntoIterator<Item = V>,
    {
        let members_json: Vec<String> = members
            .into_iter()
            .map(|m| serde_json::to_string(&m))
            .collect::<Result<_, _>>()?;

        let command = SAddCommand::multiple(key, &members_json);
        self.commands.push(Box::new(command));
        Ok(self)
    }

    /// Remove from a collection
    pub fn srem<K, V, I>(&mut self, key: K, members: I) -> RedissonResult<&mut Self>
    where
        K: ToString,
        V: Serialize,
        I: IntoIterator<Item = V>,
    {
        let members_json: Vec<String> = members
            .into_iter()
            .map(|m| serde_json::to_string(&m))
            .collect::<Result<_, _>>()?;

        // Build the SREM command using GenericCommand
        let mut args = vec!["SREM".to_string(), key.to_string()];
        args.extend(members_json.clone());
        let command = GenericCommand::new(&args, true);
        self.commands.push(Box::new(command));
        Ok(self)
    }

    /// Add to list
    pub fn lpush<K, V, I>(&mut self, key: K, values: I) -> RedissonResult<&mut Self>
    where
        K: ToString,
        V: Serialize,
        I: IntoIterator<Item = V>,
    {
        let values_json: Vec<String> = values
            .into_iter()
            .map(|v| serde_json::to_string(&v))
            .collect::<Result<_, _>>()?;

        let command = LPushCommand::multiple(key, &values_json);
        self.commands.push(Box::new(command));
        Ok(self)
    }

    /// Popping from the list
    pub fn lpop<K>(&mut self, key: K) -> &mut Self
    where
        K: ToString,
    {
        // Build the LPOP command using GenericCommand
        let command = GenericCommand::new(&["LPOP", &key.to_string()], true);
        self.commands.push(Box::new(command));
        self
    }

    /// Set an expiration time
    pub fn expire<K>(&mut self, key: K, seconds: i64) -> &mut Self
    where
        K: ToString,
    {
        let command = ExpireCommand::new(key, seconds);
        self.commands.push(Box::new(command));
        self
    }

    /// Batch add command
    pub fn add_commands<I>(&mut self, commands: I) -> &mut Self
    where
        I: IntoIterator<Item = Box<dyn CommandBuilder>>,
    {
        self.commands.extend(commands);
        self
    }

    /// Adding commands directly
    pub fn add_command(&mut self, command: Box<dyn CommandBuilder>) -> &mut Self {
        self.commands.push(command);
        self
    }

    // ========== Transaction execution ==========

    /// Executing transactions
    pub fn execute(&mut self) -> RedissonResult<TransactionResult> {
        let start_time = Instant::now();
        let transaction_id = Uuid::new_v4().to_string();
        let mut retries = 0;
        let mut backoff_ms = self.transaction_config.initial_backoff_ms;

        loop {
            match self.try_execute_once(&transaction_id, start_time.elapsed()) {
                Ok((results, batches_executed)) => {
                    return Ok(TransactionResult {
                        success: true,
                        retries,
                        execution_time: start_time.elapsed(),
                        results,
                        watch_keys: self.watch_keys.clone(),
                        transaction_id: transaction_id.clone(),
                        batches_executed,
                    });
                }
                Err(e) => {
                    // Check if you need to retry
                    if retries >= self.transaction_config.max_retries {
                        return Err(e);
                    }

                    // Check for an optimistic lock violation
                    let should_retry = match &e {
                        RedissonError::RedisError(msg) => {
                            msg.to_string().contains("WATCH") || msg.to_string().contains("EXECABORT")
                        }
                        RedissonError::TransactionConflict => true,
                        _ => false,
                    };

                    if !should_retry {
                        return Err(e);
                    }

                    // Exponential backoff retry
                    retries += 1;
                    std::thread::sleep(Duration::from_millis(backoff_ms));
                    backoff_ms = (backoff_ms * 2).min(self.transaction_config.max_backoff_ms);

                    // The empty command is executed again
                    self.optimistic_lock_attempts += 1;
                    continue;
                }
            }
        }
    }

    /// Attempt to execute a transaction
    fn try_execute_once(
        &self,
        transaction_id: &str,
        elapsed: Duration,
    ) -> RedissonResult<(Vec<BatchResult>, usize)> {
        // Checking timeouts
        if let Some(timeout) = self.transaction_config.timeout {
            if elapsed > timeout {
                return Err(RedissonError::TimeoutError);
            }
        }

        let mut conn = self.manager.get_connection()?;

        // Set up monitoring (if needed)
        if !self.watch_keys.is_empty() && self.transaction_config.enable_watch {
            redis::cmd("WATCH")
                .arg(&self.watch_keys)
                .query::<()>(&mut conn)?;
        }

        // Start transaction (MULTI)
        redis::cmd("MULTI").query::<()>(&mut conn)?;

        // Execute commands in batches
        let mut all_results = Vec::new();
        let mut batches_executed = 0;
        let batch_config = self.get_batch_config();
        for chunk in self.commands.chunks(batch_config.max_batch_size) {
            let chunk_vec = chunk.iter().map(|cmd| cmd.box_clone()).collect::<Vec<_>>();

            if batch_config.enable_pipeline {
                // Use a batch processor for execution
                let results = self.batch_processor.query_batch(chunk_vec)?;
                all_results.extend(results);
            } else {
                // Execute one by one
                for cmd in chunk {
                    let results = self.batch_processor.query_batch(vec![cmd.box_clone()])?;
                    all_results.extend(results);
                }
            }
            batches_executed += 1;
        }

        // Commit transaction (EXEC)
        let exec_result: redis::Value = redis::cmd("EXEC").query(&mut conn)?;

        // Check if the transaction was interrupted
        if let Value::Nil = exec_result {
            return Err(RedissonError::TransactionConflict);
        }

        // For transactions, we need to parse the actual result from the result of EXEC
        let results = match exec_result {
            Value::Array(values) => {
                // EXEC returns an array of the results of each command
                self.parse_exec_results(&values)?
            }
            _ => {
                // Not an array. The transaction may have failed
                return Err(RedissonError::InvalidOperation("Transaction execution failed".to_string()));
            }
        };

        Ok((results, batches_executed))
    }

    /// Parse the result returned by EXEC
    fn parse_exec_results(&self, values: &[Value]) -> RedissonResult<Vec<BatchResult>> {
        let mut results = Vec::new();

        for (i, value) in values.iter().enumerate() {
            if i >= self.commands.len() {
                break;
            }

            let result = convert_value_to_batch_result(value.clone())?;
            results.push(result);
        }

        Ok(results)
    }

    /// Dropping transactions
    pub fn discard(&mut self) -> RedissonResult<()> {
        let mut conn = self.manager.get_connection()?;

        if !self.watch_keys.is_empty() {
            redis::cmd("UNWATCH").query::<()>(&mut conn)?;
        }

        // 发送 DISCARD 命令
        if !self.commands.is_empty() {
            redis::cmd("DISCARD").query::<()>(&mut conn)?;
        }

        Ok(())
    }

    /// Getting the number of commands
    pub fn command_count(&self) -> usize {
        self.commands.len()
    }

    /// Gets the number of monitored keys
    pub fn watch_count(&self) -> usize {
        self.watch_keys.len()
    }

    /// Clear transactions
    pub fn clear(&mut self) -> &mut Self {
        self.commands.clear();
        self.watch_keys.clear();
        self.read_only = false;
        self.optimistic_lock_attempts = 0;
        self
    }

    /// Gets the number of optimistic lock attempts
    pub fn optimistic_lock_attempts(&self) -> usize {
        self.optimistic_lock_attempts
    }

    /// Get batch processor statistics
    pub fn get_batch_stats(&self) -> BatchStats {
        self.batch_processor.get_stats()
    }
    pub fn get_batch_config(&self) -> &BatchConfig {
        self.batch_processor.get_batch_config()
    }
}

// ================ Synchronous transaction builder ================

/// Synchronous transaction builder
pub struct SyncTransactionBuilder {
    manager: Arc<SyncRedisConnectionManager>,
    config: TransactionConfig,
    batch_config: BatchConfig,
}

impl SyncTransactionBuilder {
    pub fn new(manager: Arc<SyncRedisConnectionManager>) -> Self {
        Self {
            manager,
            config: TransactionConfig::default(),
            batch_config: BatchConfig::default(),
        }
    }

    pub fn with_config(mut self, config: TransactionConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_batch_config(mut self, batch_config: BatchConfig) -> Self {
        self.batch_config = batch_config;
        self
    }

    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.config.max_retries = max_retries;
        self
    }

    pub fn enable_watch(mut self, enable: bool) -> Self {
        self.config.enable_watch = enable;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = Some(timeout);
        self
    }

    pub fn build(&self) -> SyncTransactionContext {
        SyncTransactionContext::with_config(
            self.manager.clone(),
            self.config.clone(),
            Some(self.batch_config.clone()),
        )
    }
}
