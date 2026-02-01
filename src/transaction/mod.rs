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
mod blocking;
mod non_blocking;

use std::time::Duration;
use redis::Value;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
pub use blocking::*;
pub use non_blocking::*;
use crate::{BatchResult, RedissonError, RedissonResult};


// ================ Universal conversion functions ================

/// Convert Redis Value to BatchResult
pub fn convert_value_to_batch_result(value: Value) -> RedissonResult<BatchResult> {
    BatchResult::from_redis_value(value)
}

// ================ Transaction execution result ================

/// Transaction execution result
#[derive(Debug)]
pub struct TransactionResult {
    pub success: bool,                // Was it successful?
    pub retries: u32,                 // Number of retries
    pub execution_time: Duration,     // Execution time
    pub results: Vec<BatchResult>,    // Use BatchResult
    pub watch_keys: Vec<String>,      // Monitored keys
    pub transaction_id: String,       // Transaction ID
    pub batches_executed: usize,      // The number of batches executed
}

impl TransactionResult {
    /// Get the result of a specific command
    pub fn get_result<T: DeserializeOwned>(&self, index: usize) -> RedissonResult<Option<T>> {
        if index >= self.results.len() {
            return Ok(None);
        }

        match &self.results[index] {
            BatchResult::String(s) => {
                let parsed: T = serde_json::from_str(s)?;
                Ok(Some(parsed))
            }
            BatchResult::Integer(i) => {
                // Try converting an integer to a generic type
                let s = i.to_string();
                let parsed: T = serde_json::from_str(&s)?;
                Ok(Some(parsed))
            }
            BatchResult::Float(f) => {
                let s = f.to_string();
                let parsed: T = serde_json::from_str(&s)?;
                Ok(Some(parsed))
            }
            BatchResult::Bool(b) => {
                let s = if *b { "true" } else { "false" };
                let parsed: T = serde_json::from_str(s)?;
                Ok(Some(parsed))
            }
            BatchResult::Nil => Ok(None),
            _ => Err(RedissonError::DeserializationError(
                format!("The result type {:? } deserialize", self.results[index])
            )),
        }
    }

    /// Gets the string result for a specific command
    pub fn get_string(&self, index: usize) -> RedissonResult<Option<String>> {
        if index >= self.results.len() {
            return Ok(None);
        }

        match &self.results[index] {
            BatchResult::String(s) => Ok(Some(s.clone())),
            BatchResult::Status(s) => Ok(Some(s.clone())),
            BatchResult::Integer(i) => Ok(Some(i.to_string())),
            BatchResult::Float(f) => Ok(Some(f.to_string())),
            BatchResult::Bool(b) => Ok(Some(b.to_string())),
            BatchResult::Nil => Ok(None),
            BatchResult::Raw(value) => {
                match value {
                    Value::SimpleString(s) => Ok(Some(s.clone())),
                    Value::BulkString(data) => {
                        String::from_utf8(data.clone())
                            .map(Some)
                            .map_err(|e| RedissonError::DeserializationError(e.to_string()))
                    }
                    Value::Int(i) => Ok(Some(i.to_string())),
                    Value::Okay => Ok(Some("OK".to_string())),
                    _ => Err(RedissonError::DeserializationError(
                        format!("Cannot convert from raw value to string:{:?}", value)
                    )),
                }
            }
            _ => Err(RedissonError::DeserializationError(
                format!("The result type {:? } Get string", self.results[index])
            )),
        }
    }

    /// Gets the integer result of a specific command
    pub fn get_i64(&self, index: usize) -> RedissonResult<Option<i64>> {
        if index >= self.results.len() {
            return Ok(None);
        }

        match &self.results[index] {
            BatchResult::Integer(i) => Ok(Some(*i)),
            BatchResult::String(s) => s.parse::<i64>()
                .map(Some)
                .map_err(|e| RedissonError::DeserializationError(e.to_string())),
            BatchResult::Raw(value) => {
                if let Value::Int(i) = value {
                    Ok(Some(*i))
                } else {
                    Err(RedissonError::DeserializationError(
                        format!("You can't get an integer from a raw value: {:?}", value)
                    ))
                }
            }
            BatchResult::Nil => Ok(None),
            _ => Err(RedissonError::DeserializationError(
                format!("The result type {:? } Get an integer", self.results[index])
            )),
        }
    }

    /// Retrieves the Boolean result of a specific command
    pub fn get_bool(&self, index: usize) -> RedissonResult<Option<bool>> {
        if index >= self.results.len() {
            return Ok(None);
        }

        match &self.results[index] {
            BatchResult::Bool(b) => Ok(Some(*b)),
            BatchResult::Integer(1) => Ok(Some(true)),
            BatchResult::Integer(0) => Ok(Some(false)),
            BatchResult::String(s) => match s.as_str() {
                "1" | "true" | "TRUE" | "yes" | "YES" => Ok(Some(true)),
                "0" | "false" | "FALSE" | "no" | "NO" => Ok(Some(false)),
                _ => Err(RedissonError::DeserializationError(
                    format!("Unable to convert a string to a Boolean: {}", s)
                )),
            },
            BatchResult::Raw(value) => {
                if let Value::Int(1) = value {
                    Ok(Some(true))
                } else if let Value::Int(0) = value {
                    Ok(Some(false))
                } else if let Value::SimpleString(s) = value {
                    match s.as_str() {
                        "OK" | "true" | "TRUE" | "yes" | "YES" => Ok(Some(true)),
                        _ => Ok(Some(false)),
                    }
                } else {
                    Err(RedissonError::DeserializationError(
                        format!("A Boolean value cannot be retrieved from a primitive value: {:?}", value)
                    ))
                }
            }
            BatchResult::Nil => Ok(None),
            _ => Err(RedissonError::DeserializationError(
                format!("The result type {:? } get a Boolean value", self.results[index])
            )),
        }
    }

    /// Check that all commands succeed
    pub fn all_success(&self) -> bool {
        for result in &self.results {
            match result {
                BatchResult::Status(s) if s == "OK" => continue,
                BatchResult::Integer(1) => continue, // Usually indicates success.
                BatchResult::Bool(true) => continue,
                BatchResult::Nil => continue, // For a read command, nil may indicate that the key does not exist
                BatchResult::Raw(Value::Okay) => continue,
                BatchResult::Raw(Value::SimpleString(s)) if s == "OK" => continue,
                BatchResult::Raw(Value::Int(1)) => continue,
                _ => {
                    // Check if the result is wrong
                    if let BatchResult::Error(_) = result {
                        return false;
                    }
                    // For other result types, it needs to be judged according to the command type
                }
            }
        }
        true
    }

    /// Getting the transaction ID
    pub fn id(&self) -> &str {
        &self.transaction_id
    }
}


/// 事务统计
#[derive(Clone, Debug)]
pub struct TransactionStats {
    pub total_batches: u64,
    pub total_commands: u64,
    pub total_executions: u64,
    pub total_success: u64,
    pub total_failures: u64,
    pub avg_batch_size: f64,
    pub avg_execution_time_ms: f64,
}

impl Default for TransactionStats {
    fn default() -> Self {
        Self {
            total_batches: 0,
            total_commands: 0,
            total_executions: 0,
            total_success: 0,
            total_failures: 0,
            avg_batch_size: 0.0,
            avg_execution_time_ms: 0.0,
        }
    }
}