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

use std::time::SystemTimeError;
use redis::{ParsingError, RedisError};
use thiserror::Error;

pub type RedissonResult<T> = std::result::Result<T, RedissonError>;

#[derive(Error, Debug)]
pub enum RedissonError {
    #[error("Redis error: {0}")]
    RedisError(#[from] RedisError),

    #[error("System time error: {0}")]
    TimeError(#[from] SystemTimeError),

    #[error("No redis servers provided")]
    NoServerError,

    #[error("Lock acquisition timeout")]
    TimeoutError,

    #[error("Transaction Conflict")]
    TransactionConflict,

    #[error("Lock has expired")]
    LockExpiredError,

    #[error("Failed to acquire lock")]
    LockAcquisitionError,

    #[error("Failed to release lock")]
    LockReleaseError,

    #[error("Failed to extend lock")]
    LockExtendError,

    #[error("circuit breaker error: {0}")]
    CircuitBreakerError(String),

    #[error("Connection pool error: {0}")]
    PoolError(String),

    #[error("Thread Task error: {0}")]
    ThreadError(String),

    #[error("Invalid configuration: {0}")]
    ConfigError(String),

    #[error("Async runtime error: {0}")]
    AsyncError(String),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Watchdog error: {0}")]
    WatchdogError(String),
}

impl From<r2d2::Error> for RedissonError {
    fn from(err: r2d2::Error) -> Self {
        RedissonError::PoolError(err.to_string())
    }
}

impl From<deadpool::managed::PoolError<RedisError>> for RedissonError {
    fn from(err: deadpool::managed::PoolError<RedisError>) -> Self {
        RedissonError::PoolError(err.to_string())
    }
}

impl From<deadpool::managed::BuildError> for RedissonError {
    fn from(err: deadpool::managed::BuildError) -> Self {
        RedissonError::PoolError(err.to_string())
    }
}

impl From<tokio::task::JoinError> for RedissonError {
    fn from(err: tokio::task::JoinError) -> Self {
        RedissonError::AsyncError(err.to_string())
    }
}

impl From<serde_json::Error> for RedissonError {
    fn from(err: serde_json::Error) -> Self {
        RedissonError::SerializationError(err.to_string())
    }
}

impl From<ParsingError> for RedissonError {
    fn from(err: ParsingError) -> Self {
        RedissonError::SerializationError(err.to_string())
    }
}
