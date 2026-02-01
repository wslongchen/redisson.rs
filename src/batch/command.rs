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

// ================ Specific command implementation ================

use std::time::Duration;
use redis::{Cmd, ToRedisArgs};
use crate::batch::command_builder::CommandBuilder;

/// SET command builder
#[derive(Clone, Debug)]
pub struct SetCommand {
    key: String,
    value: String,
    ttl: Option<Duration>,
    keep_ttl: bool,
}

impl SetCommand {
    pub fn new<K: ToString, V: ToString>(key: K, value: V) -> Self {
        Self {
            key: key.to_string(),
            value: value.to_string(),
            ttl: None,
            keep_ttl: false,
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    pub fn with_keep_ttl(mut self) -> Self {
        self.keep_ttl = true;
        self
    }
}

impl CommandBuilder for SetCommand {
    fn build(&self) -> Cmd {
        let mut cmd = Cmd::new();
        cmd.arg("SET");

        if self.keep_ttl {
            cmd.arg("KEEPTTL");
        }

        cmd.arg(&self.key).arg(&self.value);

        if let Some(ttl) = self.ttl {
            cmd.arg("EX").arg(ttl.as_secs() as i64);
        }

        cmd
    }

    fn needs_result(&self) -> bool {
        false
    }

    fn box_clone(&self) -> Box<dyn CommandBuilder> {
        Box::new(self.clone())
    }

    fn command_name(&self) -> &str {
        "SET"
    }

    fn keys(&self) -> Vec<String> {
        vec![self.key.clone()]
    }
}

/// GET command builder
#[derive(Clone, Debug)]
pub struct GetCommand {
    key: String,
}

impl GetCommand {
    pub fn new<K: ToString>(key: K) -> Self {
        Self {
            key: key.to_string(),
        }
    }
}

impl CommandBuilder for GetCommand {
    fn build(&self) -> Cmd {
        let mut cmd = Cmd::new();
        cmd.arg("GET").arg(&self.key);
        cmd
    }

    fn needs_result(&self) -> bool {
        true
    }

    fn box_clone(&self) -> Box<dyn CommandBuilder> {
        Box::new(self.clone())
    }

    fn command_name(&self) -> &str {
        "GET"
    }

    fn keys(&self) -> Vec<String> {
        vec![self.key.clone()]
    }
}

/// DEL command builder
#[derive(Clone, Debug)]
pub struct DelCommand {
    keys: Vec<String>,
}

impl DelCommand {
    pub fn new<K: ToString>(key: K) -> Self {
        Self {
            keys: vec![key.to_string()],
        }
    }

    pub fn multiple<K: ToString>(keys: &[K]) -> Self {
        Self {
            keys: keys.iter().map(|k| k.to_string()).collect(),
        }
    }
}

impl CommandBuilder for DelCommand {
    fn build(&self) -> Cmd {
        let mut cmd = Cmd::new();
        cmd.arg("DEL");
        for key in &self.keys {
            cmd.arg(key);
        }
        cmd
    }

    fn needs_result(&self) -> bool {
        false
    }

    fn box_clone(&self) -> Box<dyn CommandBuilder> {
        Box::new(self.clone())
    }

    fn command_name(&self) -> &str {
        "DEL"
    }

    fn keys(&self) -> Vec<String> {
        self.keys.clone()
    }
}

/// INCRBY command builder
#[derive(Clone, Debug)]
pub struct IncrByCommand {
    key: String,
    delta: i64,
}

impl IncrByCommand {
    pub fn new<K: ToString>(key: K, delta: i64) -> Self {
        Self {
            key: key.to_string(),
            delta,
        }
    }
}

impl CommandBuilder for IncrByCommand {
    fn build(&self) -> Cmd {
        let mut cmd = Cmd::new();
        cmd.arg("INCRBY").arg(&self.key).arg(self.delta);
        cmd
    }

    fn needs_result(&self) -> bool {
        true
    }

    fn box_clone(&self) -> Box<dyn CommandBuilder> {
        Box::new(self.clone())
    }

    fn command_name(&self) -> &str {
        "INCRBY"
    }

    fn keys(&self) -> Vec<String> {
        vec![self.key.clone()]
    }
}

/// The HSET command builder
#[derive(Clone, Debug)]
pub struct HSetCommand {
    key: String,
    field: String,
    value: String,
}

impl HSetCommand {
    pub fn new<K: ToString, F: ToString, V: ToString>(key: K, field: F, value: V) -> Self {
        Self {
            key: key.to_string(),
            field: field.to_string(),
            value: value.to_string(),
        }
    }
}

impl CommandBuilder for HSetCommand {
    fn build(&self) -> Cmd {
        let mut cmd = Cmd::new();
        cmd.arg("HSET").arg(&self.key).arg(&self.field).arg(&self.value);
        cmd
    }

    fn needs_result(&self) -> bool {
        false
    }

    fn box_clone(&self) -> Box<dyn CommandBuilder> {
        Box::new(self.clone())
    }

    fn command_name(&self) -> &str {
        "HSET"
    }

    fn keys(&self) -> Vec<String> {
        vec![self.key.clone()]
    }
}

/// The HGET command builder
#[derive(Clone, Debug)]
pub struct HGetCommand {
    key: String,
    field: String,
}

impl HGetCommand {
    pub fn new<K: ToString, F: ToString>(key: K, field: F) -> Self {
        Self {
            key: key.to_string(),
            field: field.to_string(),
        }
    }
}

impl CommandBuilder for HGetCommand {
    fn build(&self) -> Cmd {
        let mut cmd = Cmd::new();
        cmd.arg("HGET").arg(&self.key).arg(&self.field);
        cmd
    }

    fn needs_result(&self) -> bool {
        true
    }

    fn box_clone(&self) -> Box<dyn CommandBuilder> {
        Box::new(self.clone())
    }

    fn command_name(&self) -> &str {
        "HGET"
    }

    fn keys(&self) -> Vec<String> {
        vec![self.key.clone()]
    }
}

/// EXPIRE command builder
#[derive(Clone, Debug)]
pub struct ExpireCommand {
    key: String,
    seconds: i64,
}

impl ExpireCommand {
    pub fn new<K: ToString>(key: K, seconds: i64) -> Self {
        Self {
            key: key.to_string(),
            seconds,
        }
    }
}

impl CommandBuilder for ExpireCommand {
    fn build(&self) -> Cmd {
        let mut cmd = Cmd::new();
        cmd.arg("EXPIRE").arg(&self.key).arg(self.seconds);
        cmd
    }

    fn needs_result(&self) -> bool {
        false
    }

    fn box_clone(&self) -> Box<dyn CommandBuilder> {
        Box::new(self.clone())
    }

    fn command_name(&self) -> &str {
        "EXPIRE"
    }

    fn keys(&self) -> Vec<String> {
        vec![self.key.clone()]
    }
}

/// LPUSH command builder
#[derive(Clone, Debug)]
pub struct LPushCommand {
    key: String,
    values: Vec<String>,
}

impl LPushCommand {
    pub fn new<K: ToString, V: ToString>(key: K, value: V) -> Self {
        Self {
            key: key.to_string(),
            values: vec![value.to_string()],
        }
    }

    pub fn multiple<K: ToString, V: ToString>(key: K, values: &[V]) -> Self {
        Self {
            key: key.to_string(),
            values: values.iter().map(|v| v.to_string()).collect(),
        }
    }
}

impl CommandBuilder for LPushCommand {
    fn build(&self) -> Cmd {
        let mut cmd = Cmd::new();
        cmd.arg("LPUSH").arg(&self.key);
        for value in &self.values {
            cmd.arg(value);
        }
        cmd
    }

    fn needs_result(&self) -> bool {
        false
    }

    fn box_clone(&self) -> Box<dyn CommandBuilder> {
        Box::new(self.clone())
    }

    fn command_name(&self) -> &str {
        "LPUSH"
    }

    fn keys(&self) -> Vec<String> {
        vec![self.key.clone()]
    }
}

/// The SADD command builder
#[derive(Clone, Debug)]
pub struct SAddCommand {
    key: String,
    members: Vec<String>,
}

impl SAddCommand {
    pub fn new<K: ToString, M: ToString>(key: K, member: M) -> Self {
        Self {
            key: key.to_string(),
            members: vec![member.to_string()],
        }
    }

    pub fn multiple<K: ToString, M: ToString>(key: K, members: &[M]) -> Self {
        Self {
            key: key.to_string(),
            members: members.iter().map(|m| m.to_string()).collect(),
        }
    }
}

impl CommandBuilder for SAddCommand {
    fn build(&self) -> Cmd {
        let mut cmd = Cmd::new();
        cmd.arg("SADD").arg(&self.key);
        for member in &self.members {
            cmd.arg(member);
        }
        cmd
    }

    fn needs_result(&self) -> bool {
        false
    }

    fn box_clone(&self) -> Box<dyn CommandBuilder> {
        Box::new(self.clone())
    }

    fn command_name(&self) -> &str {
        "SADD"
    }

    fn keys(&self) -> Vec<String> {
        vec![self.key.clone()]
    }
}

/// Generic command builder (for unsupported commands)
#[derive(Clone, Debug)]
pub struct GenericCommand {
    args: Vec<String>,
    needs_result: bool,
    name: String,
}

impl GenericCommand {
    pub fn new<'a, T: ToRedisArgs + ToString>(args: &'a [T], needs_result: bool) -> Self {
        let name = if !args.is_empty() {
            args[0].to_string()
        } else {
            "UNKNOWN".to_string()
        };

        Self {
            args: args.iter().map(|arg| arg.to_string()).collect(),
            needs_result,
            name,
        }
    }
}

impl CommandBuilder for GenericCommand {
    fn build(&self) -> Cmd {
        let mut cmd = Cmd::new();
        for arg in &self.args {
            cmd.arg(arg);
        }
        cmd
    }

    fn needs_result(&self) -> bool {
        self.needs_result
    }

    fn box_clone(&self) -> Box<dyn CommandBuilder> {
        Box::new(self.clone())
    }

    fn command_name(&self) -> &str {
        &self.name
    }

    fn keys(&self) -> Vec<String> {
        // The generic command cannot determine which is the key
        Vec::new()
    }
}
