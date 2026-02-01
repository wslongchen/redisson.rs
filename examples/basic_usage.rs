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
use std::time::Duration;
use redisson::{AsyncRedissonClient, RedissonClient, RedissonConfig, RedissonResult};

fn main() -> RedissonResult<()> {
    // 1. 创建配置
    let config = RedissonConfig::single_server("redis://127.0.0.1:6379")
        .with_pool_size(20)
        .with_watchdog_timeout(Duration::from_secs(30));

    // 2. 创建同步客户端
    let client = RedissonClient::new(config)?;

    // 3. 使用分布式数据结构
    let bucket = client.get_bucket::<String>("my_bucket");
    bucket.set(&"Hello World".to_string())?;
    let value: Option<String> = bucket.get()?;
    println!("Bucket value: {:?}", value);

    // 4. 使用分布式锁
    let lock = client.get_lock("my_lock");
    lock.lock()?;

    // 执行受保护的代码
    println!("Critical section accessed");

    lock.unlock()?;

    // 5. 使用Map
    let map = client.get_map::<String, i32>("my_map");
    map.put(&"key1".to_string(), &42)?;
    let map_value = map.get(&"key1".to_string())?;
    println!("Map value: {:?}", map_value);

    client.shutdown()?;
    Ok(())
}

// examples/async_usage.rs
#[tokio::main]
async fn async_main() -> RedissonResult<()> {
    // 1. 创建配置
    let config = RedissonConfig::single_server("redis://127.0.0.1:6379");

    // 2. 创建异步客户端
    let client = AsyncRedissonClient::new(config).await?;

    // 3. 使用异步分布式数据结构
    let bucket = client.get_bucket::<String>("async_bucket");
    bucket.set(&"Async Hello".to_string()).await?;
    let value = bucket.get().await?;
    println!("Async bucket value: {:?}", value);

    // 4. 使用异步分布式锁
    let lock = client.get_lock("async_lock");
    lock.lock().await?;

    // 执行受保护的异步代码
    println!("Async critical section accessed");

    lock.unlock().await?;

    client.shutdown().await?;
    Ok(())
}
