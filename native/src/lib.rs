// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod errors;

use std::sync::OnceLock;

use datafusion::error::DataFusionError;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use tokio::runtime::Runtime;

use crate::errors::{try_unwrap_or_throw, JniResult};

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("failed to create Tokio runtime"))
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_createSessionContext<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        let ctx = SessionContext::new();
        Ok(Box::into_raw(Box::new(ctx)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_executeSql<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    sql: JString<'local>,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let sql_str: String = env.get_string(&sql)?.into();
        runtime().block_on(async {
            let df = ctx.sql(&sql_str).await?;
            df.collect().await?;
            Ok::<(), DataFusionError>(())
        })?;
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_closeSessionContext<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |_env| -> JniResult<()> {
        if handle != 0 {
            unsafe {
                drop(Box::from_raw(handle as *mut SessionContext));
            }
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_registerParquet<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    path: JString<'local>,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let name: String = env.get_string(&name)?.into();
        let path: String = env.get_string(&path)?.into();
        runtime().block_on(async {
            ctx.register_parquet(&name, &path, ParquetReadOptions::default())
                .await?;
            Ok::<(), datafusion::error::DataFusionError>(())
        })?;
        Ok(())
    })
}
