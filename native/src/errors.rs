use std::any::Any;
use std::error::Error;
use std::panic::{catch_unwind, AssertUnwindSafe};

use jni::JNIEnv;

pub type JniResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub fn try_unwrap_or_throw<T, F>(env: &mut JNIEnv, default: T, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> JniResult<T>,
{
    match catch_unwind(AssertUnwindSafe(|| f(env))) {
        Ok(Ok(value)) => value,
        Ok(Err(err)) => {
            throw_runtime_exception(env, &err.to_string());
            default
        }
        Err(panic) => {
            throw_runtime_exception(env, &panic_message(&panic));
            default
        }
    }
}

fn throw_runtime_exception(env: &mut JNIEnv, message: &str) {
    if env.exception_check().unwrap_or(false) {
        return;
    }
    let _ = env.throw_new("java/lang/RuntimeException", message);
}

fn panic_message(panic: &Box<dyn Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else {
        "rust panic with non-string payload".to_string()
    }
}
