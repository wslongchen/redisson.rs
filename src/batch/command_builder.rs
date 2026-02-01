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

// ================ CommandBuilder Trait ================

use std::fmt::{Debug, Formatter};
use redis::Cmd;

/// Command Builder trait - Core abstraction
pub trait CommandBuilder: Send + Sync {
    /// Build Redis command
    fn build(&self) -> Cmd;

    /// Do we need results?
    fn needs_result(&self) -> bool;

    /// Cloning (for dynamic distribution)
    fn box_clone(&self) -> Box<dyn CommandBuilder>;

    /// Command name (for statistics and debugging)
    fn command_name(&self) -> &str;

    /// Get a list of keys (for caching key calculations)
    fn keys(&self) -> Vec<String>;

    fn cmd_to_string(&self) -> String {
        let cmd = self.build();
        let mut parts = Vec::new();

        // Get the command name
        parts.push(self.command_name().to_uppercase());

        // Add parameters
        for arg in cmd.args_iter().skip(1) {
            parts.push(format!("{:?}", arg));
        }
        parts.join(":")
    }
}

impl Clone for Box<dyn CommandBuilder> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

impl Debug for dyn CommandBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommandBuilder({})", self.cmd_to_string())
    }
}