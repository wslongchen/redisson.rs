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

pub use blocking::*;
pub use non_blocking::*;




use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionType {
    Single,
    Cluster,
    Sentinel,
}


#[derive(Clone)]
pub struct CachedConnection {
    last_used: Instant,
    connection_type: ConnectionType,
}
