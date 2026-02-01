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
use crate::{BaseDistributedObject, GeoRadiusOptions, GeoRadiusResult, GeoSort, GeoUnit, RFairLock, RLock, RLockable, RObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

/// === RGeo (Geographic space) ===
pub struct RGeo<V> {
    base: BaseDistributedObject,
    _marker: std::marker::PhantomData<V>,
}

impl<V: Serialize + DeserializeOwned + std::default::Default> RGeo<V> {
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: BaseDistributedObject::new(connection_manager, name),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn add(&self, longitude: f64, latitude: f64, member: &V) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        let member_json = BaseDistributedObject::serialize(member)?;

        redis::cmd("GEOADD")
            .arg(self.base.get_full_key())
            .arg(longitude)
            .arg(latitude)
            .arg(member_json)
            .query::<()>(&mut conn)?;

        Ok(())
    }

    pub fn distance(&self, member1: &V, member2: &V, unit: GeoUnit) -> RedissonResult<Option<f64>> {
        let mut conn = self.base.get_connection()?;
        let member1_json = BaseDistributedObject::serialize(member1)?;
        let member2_json = BaseDistributedObject::serialize(member2)?;

        let distance: Option<f64> = redis::cmd("GEODIST")
            .arg(self.base.get_full_key())
            .arg(member1_json)
            .arg(member2_json)
            .arg(unit.as_str())
            .query(&mut conn)?;

        Ok(distance)
    }

    pub fn radius(
        &self,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: GeoUnit,
        options: GeoRadiusOptions,
    ) -> RedissonResult<Vec<GeoRadiusResult<V>>> {
        let mut conn = self.base.get_connection()?;

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

        let results: Vec<redis::Value> = cmd.query(&mut conn)?;

        let mut radius_results = Vec::new();

        // Determine the parsing based on the number of options returned
        let has_options = options.with_coord || options.with_dist || options.with_hash;

        for result in results {
            if has_options {
                // If the option is set, an array is returned
                if let redis::Value::Array(items) = result {
                    let result_obj = self.parse_radius_item_with_options(&items, &options);
                    radius_results.push(result_obj);
                }
            } else {
                // If no options are set, a simple string or array may be returned
                let result_obj = match result {
                    redis::Value::BulkString(data) => {
                        // Only the member name is returned
                        let mut obj = GeoRadiusResult::default();
                        if let Ok(member_str) = String::from_utf8(data) {
                            obj.member = BaseDistributedObject::deserialize(&member_str).ok();
                        }
                        obj
                    }
                    redis::Value::SimpleString(member_str) => {
                        // A member of the simple string format
                        let mut obj = GeoRadiusResult::default();
                        obj.member = BaseDistributedObject::deserialize(&member_str).ok();
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
    fn parse_radius_item_with_options(
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
                        result_obj.member = BaseDistributedObject::deserialize(&member_str).ok();
                    }
                }
                redis::Value::SimpleString(member_str) => {
                    result_obj.member = BaseDistributedObject::deserialize(member_str).ok();
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

impl <V: Serialize + DeserializeOwned + std::default::Default> RObject for RGeo<V>
{
    fn get_name(&self) -> &str {
        self.base.get_name()
    }

    fn delete(&self) -> RedissonResult<bool> {
        self.base.delete()
    }

    fn rename(&self, new_name: &str) -> RedissonResult<()> {
        self.base.rename(new_name)
    }

    fn is_exists(&self) -> RedissonResult<bool> {
        self.base.is_exists()
    }

    fn move_to_db(&self, db_index: i32) -> RedissonResult<bool> {
        self.base.move_to_db(db_index)
    }

    fn get_expire_time(&self) -> RedissonResult<Option<Duration>> {
        self.base.get_expire_time()
    }

    fn expire(&self, duration: Duration) -> RedissonResult<bool> {
        self.base.expire(duration)
    }

    fn expire_at(&self, timestamp: i64) -> RedissonResult<bool> {
        self.base.expire_at(timestamp)
    }

    fn clear_expire(&self) -> RedissonResult<bool> {
        self.base.clear_expire()
    }
}

impl <V: Serialize + DeserializeOwned + std::default::Default> RLockable for RGeo<V>
{
    fn get_lock(&self) -> RLock {
        self.base.get_lock()
    }

    fn get_fair_lock(&self) -> RFairLock {
        self.base.get_fair_lock()
    }

    fn lock(&self) -> RedissonResult<()> {
        self.base.lock()
    }

    fn try_lock(&self) -> RedissonResult<bool> {
        self.base.try_lock()
    }

    fn try_lock_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        self.base.try_lock_timeout(wait_time)
    }

    fn lock_lease(&self, lease_time: Duration) -> RedissonResult<()> {
        self.base.lock_lease(lease_time)
    }

    fn unlock(&self) -> RedissonResult<bool> {
        self.base.unlock()
    }

    fn force_unlock(&self) -> RedissonResult<bool> {
        self.base.force_unlock()
    }

    fn is_locked(&self) -> RedissonResult<bool> {
        self.base.is_locked()
    }

    fn is_held_by_current_thread(&self) -> bool {
        self.base.is_held_by_current_thread()
    }
}