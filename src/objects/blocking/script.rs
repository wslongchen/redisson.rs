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
use crate::{BaseDistributedObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};

/// === RScript (Script execution) ===
pub struct RScript {
    base: BaseDistributedObject,
}

impl RScript {
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>,  name: String) -> Self {
        Self { base: BaseDistributedObject::new(connection_manager, name.to_string())}
    }

    pub fn eval<T>(
        &self,
        script: &str,
        keys: &[String],
        args: &[String],
    ) -> RedissonResult<T>
    where
        T: redis::FromRedisValue,
    {
        let mut conn = self.base.get_connection()?;
        let script_obj = redis::Script::new(script);
        let result = script_obj
            .key(keys)
            .arg(args)
            .invoke(&mut conn)?;
        Ok(result)
    }

    pub fn eval_sha<T>(
        &self,
        sha1: &str,
        keys: &[String],
        args: &[String],
    ) -> RedissonResult<T>
    where
        T: redis::FromRedisValue,
    {
        let mut conn = self.base.get_connection()?;

        let result: T = redis::cmd("EVALSHA")
            .arg(sha1)
            .arg(keys.len())
            .arg(keys)
            .arg(args)
            .query(&mut conn)?;

        Ok(result)
    }

    pub fn script_load(&self, script: &str) -> RedissonResult<String> {
        let mut conn = self.base.get_connection()?;
        let sha1: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(script)
            .query(&mut conn)?;

        Ok(sha1)
    }

    pub fn script_exists(&self, sha1: &str) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let exists: Vec<i32> = redis::cmd("SCRIPT")
            .arg("EXISTS")
            .arg(sha1)
            .query(&mut conn)?;

        Ok(exists.get(0).map_or(false, |&v| v == 1))
    }

    pub fn script_flush(&self) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        redis::cmd("SCRIPT")
            .arg("FLUSH")
            .query::<()>(&mut conn)?;
        Ok(())
    }

    pub fn script_kill(&self) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        redis::cmd("SCRIPT")
            .arg("KILL")
            .query::<()>(&mut conn)?;
        Ok(())
    }
}