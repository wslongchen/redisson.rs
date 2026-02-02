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

/// The base class of all lock-enabled distributed objects
#[derive(Clone)]
pub struct LockableObject {
    pub(crate) name: String,
}

impl LockableObject {
    pub fn new(name: String) -> Self {
        Self {
            name: name.clone(),
        }
    }

    pub fn get_lock_name(&self) -> String {
        format!("{}:lock", self.get_full_key())
    }

    pub fn get_fair_lock_name(&self) -> String {
        format!("{}:fair_lock", self.get_full_key())
    }

    pub fn get_full_key(&self) -> &str {
        &self.name
    }
}