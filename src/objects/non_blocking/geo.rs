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
use serde::de::DeserializeOwned;
use serde::Serialize;
use async_trait::async_trait;
use crate::{RedissonResult, AsyncRedisConnectionManager, AsyncRedisConnection, AsyncBaseDistributedObject, AsyncRObject, AsyncRLockable, AsyncRLock, AsyncRFairLock, AsyncRObjectBase, GeoUnit, GeoRadiusOptions, GeoRadiusResult, GeoSort, AsyncRDelayedQueue, AsyncRSemaphore};


// === AsyncRGeo Asynchronous geospace ===
pub struct AsyncRGeo<V> {
    base: AsyncBaseDistributedObject,
    _marker: std::marker::PhantomData<V>,
}

impl<V> AsyncRGeo<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + Default + 'static,
{
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn add(&self, longitude: f64, latitude: f64, member: &V) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;
        let member_json = AsyncBaseDistributedObject::serialize(member)?;

        conn.execute_command::<()>(
            &mut redis::cmd("GEOADD")
                .arg(self.base.get_full_key())
                .arg(longitude)
                .arg(latitude)
                .arg(member_json)
        ).await?;

        Ok(())
    }

    pub async fn distance(&self, member1: &V, member2: &V, unit: GeoUnit) -> RedissonResult<Option<f64>> {
        let mut conn = self.base.get_connection().await?;
        let member1_json = AsyncBaseDistributedObject::serialize(member1)?;
        let member2_json = AsyncBaseDistributedObject::serialize(member2)?;

        let distance: Option<f64> = conn
            .execute_command(
                &mut redis::cmd("GEODIST")
                    .arg(self.base.get_full_key())
                    .arg(member1_json)
                    .arg(member2_json)
                    .arg(unit.as_str())
            )
            .await?;

        Ok(distance)
    }

    pub async fn radius(
        &self,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: GeoUnit,
        options: GeoRadiusOptions,
    ) -> RedissonResult<Vec<GeoRadiusResult<V>>> {
        let mut conn = self.base.get_connection().await?;

        let mut cmd = redis::cmd("GEORADIUS");
        cmd.arg(self.base.get_full_key())
            .arg(longitude)
            .arg(latitude)
            .arg(radius)
            .arg(unit.as_str());

        // Adding options
        if options.with_coord {
            cmd.arg("WITHCOORD");
        }
        if options.with_dist {
            cmd.arg("WITHDIST");
        }
        if options.with_hash {
            cmd.arg("WITHHASH");
        }
        if options.count.unwrap_or_default() > 0 {
            cmd.arg("COUNT").arg(options.count);
        }
        match options.sort {
            GeoSort::Asc => cmd.arg("ASC"),
            GeoSort::Desc => cmd.arg("DESC"),
        };

        let results: Vec<redis::Value> = cmd.query_async(&mut conn).await?;

        let mut radius_results = Vec::new();

        // Determine the parsing based on the number of options returned
        let has_options = options.with_coord || options.with_dist || options.with_hash;

        for result in results {
            if has_options {
                // If the option is set, an array is returned
                if let redis::Value::Array(items) = result {
                    let result_obj = self.parse_radius_item_with_options(&items, &options).await;
                    radius_results.push(result_obj);
                }
            } else {
                // If no options are set, a simple string or array may be returned
                let result_obj = match result {
                    redis::Value::BulkString(data) => {
                        // Only the member name is returned
                        let mut obj = GeoRadiusResult::default();
                        if let Ok(member_str) = String::from_utf8(data) {
                            obj.member = AsyncBaseDistributedObject::deserialize(&member_str).ok();
                        }
                        obj
                    }
                    redis::Value::SimpleString(member_str) => {
                        // A member of the simple string format
                        let mut obj = GeoRadiusResult::default();
                        obj.member = AsyncBaseDistributedObject::deserialize(&member_str).ok();
                        obj
                    }
                    _ => {
                        // In other formats, create empty results
                        GeoRadiusResult::default()
                    }
                };
                radius_results.push(result_obj);
            }
        }

        Ok(radius_results)
    }

    /// Resolves geographic radius query result items with options
    async fn parse_radius_item_with_options(
        &self,
        items: &[redis::Value],
        options: &GeoRadiusOptions,
    ) -> GeoRadiusResult<V> {
        let mut result_obj = GeoRadiusResult::default();
        let mut item_index = 0;

        // The first element is a member
        if item_index < items.len() {
            match &items[item_index] {
                redis::Value::BulkString(data) => {
                    if let Ok(member_str) = String::from_utf8(data.clone()) {
                        result_obj.member = AsyncBaseDistributedObject::deserialize(&member_str).ok();
                    }
                }
                redis::Value::SimpleString(member_str) => {
                    result_obj.member = AsyncBaseDistributedObject::deserialize(member_str).ok();
                }
                _ => {}
            }
            item_index += 1;
        }

        // The subsequent elements are parsed according to the options order
        let mut option_flags = Vec::new();

        // Determine the order of the options
        if options.with_dist {
            option_flags.push("dist");
        }
        if options.with_hash {
            option_flags.push("hash");
        }
        if options.with_coord {
            option_flags.push("coord");
        }

        for flag in option_flags {
            if item_index < items.len() {
                match flag {
                    "dist" => {
                        result_obj.distance = match &items[item_index] {
                            redis::Value::BulkString(data) => {
                                String::from_utf8_lossy(data).parse::<f64>().ok()
                            }
                            redis::Value::SimpleString(dist_str) => {
                                dist_str.parse::<f64>().ok()
                            }
                            redis::Value::Double(dist) => Some(*dist),
                            _ => None,
                        };
                        item_index += 1;
                    }
                    "hash" => {
                        result_obj.hash = match &items[item_index] {
                            redis::Value::Int(hash) => Some(*hash as u64),
                            redis::Value::BulkString(data) => {
                                String::from_utf8_lossy(data).parse::<u64>().ok()
                            }
                            _ => None,
                        };
                        item_index += 1;
                    }
                    "coord" => {
                        if let redis::Value::Array(coord_items) = &items[item_index] {
                            if coord_items.len() >= 2 {
                                let (lon, lat) = match (&coord_items[0], &coord_items[1]) {
                                    (redis::Value::BulkString(lon_data), redis::Value::BulkString(lat_data)) => {
                                        let lon_str = String::from_utf8_lossy(lon_data);
                                        let lat_str = String::from_utf8_lossy(lat_data);
                                        (lon_str.parse::<f64>().ok(), lat_str.parse::<f64>().ok())
                                    }
                                    (redis::Value::SimpleString(lon_str), redis::Value::SimpleString(lat_str)) => {
                                        (lon_str.parse::<f64>().ok(), lat_str.parse::<f64>().ok())
                                    }
                                    (redis::Value::Double(lon), redis::Value::Double(lat)) => {
                                        (Some(*lon), Some(*lat))
                                    }
                                    _ => (None, None),
                                };

                                if let (Some(lon_val), Some(lat_val)) = (lon, lat) {
                                    result_obj.coordinate = Some((lon_val, lat_val));
                                }
                            }
                        }
                        item_index += 1;
                    }
                    _ => {}
                }
            }
        }

        result_obj
    }
}



#[async_trait]
impl <V> AsyncRObject for AsyncRGeo<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + Default + 'static, {
    async fn delete(&self) -> RedissonResult<bool> {
        self.base.delete().await
    }
    async fn rename(&self, new_name: &str) -> RedissonResult<()> {
        self.base.rename(new_name).await
    }
    async fn is_exists(&self) -> RedissonResult<bool> {
        self.base.is_exists().await
    }
    async fn move_to_db(&self, db_index: i32) -> RedissonResult<bool> {
        self.base.move_to_db(db_index).await
    }
    async fn get_expire_time(&self) -> RedissonResult<Option<Duration>> {
        self.base.get_expire_time().await
    }
    async fn expire(&self, duration: Duration) -> RedissonResult<bool> {
        self.base.expire(duration).await
    }
    async fn expire_at(&self, timestamp: i64) -> RedissonResult<bool> {
        self.base.expire_at(timestamp).await
    }
    async fn clear_expire(&self) -> RedissonResult<bool> {
        self.base.clear_expire().await
    }
}



#[async_trait]
impl <V> AsyncRLockable for AsyncRGeo<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + Default + 'static, {
    fn get_lock(&self) -> AsyncRLock {
        AsyncRLock::new(
            self.base.connection_manager(),
            format!("{}:lock", self.base.get_full_key()),
            Duration::from_secs(30)
        )
    }

    fn get_fair_lock(&self) -> AsyncRFairLock {
        AsyncRFairLock::new(
            self.base.connection_manager(),
            format!("{}:fair_lock", self.base.get_full_key()),
            Duration::from_secs(30)
        )
    }

    async fn lock(&self) -> RedissonResult<()> {
        self.get_lock().lock().await
    }

    async fn try_lock(&self) -> RedissonResult<bool> {
        self.get_lock().try_lock().await
    }

    async fn try_lock_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        self.get_lock().try_lock_with_timeout(wait_time).await
    }

    async fn lock_lease(&self, lease_time: Duration) -> RedissonResult<()> {
        self.get_lock().lock_with_lease_time(lease_time).await
    }

    async fn unlock(&self) -> RedissonResult<bool> {
        self.get_lock().unlock().await
    }

    async fn force_unlock(&self) -> RedissonResult<bool> {
        self.get_lock().force_unlock().await
    }

    async fn is_locked(&self) -> RedissonResult<bool> {
        self.get_lock().is_locked().await
    }

    async fn is_held_by_current_thread(&self) -> bool {
        self.get_lock().is_held_by_current_thread().await
    }
}