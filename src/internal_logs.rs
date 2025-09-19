#![allow(unused_macros)]

/// Log an informational message for internal use.
///
/// # Usage
///
/// - Always requires a `message:` field.  
/// - Optional key-value pairs can be included after the message.
/// - When the `internal-logs` feature is enabled, this macro forwards directly
///   to [`tracing::info!`].
/// - When running tests (`#[cfg(test)]`), the message and fields are printed
///   to stdout (so `cargo test -- --nocapture` will show them).
/// - Otherwise, it compiles down to a no-op.
///
/// # Examples
/// ```rust
/// internal_info!(message: "worker started");
/// internal_info!(message: "upload finished", batch_id = 42, status = "ok");
/// ```
#[macro_export]
macro_rules! internal_info {
    (message: $msg:expr $(,)?) => {
        #[cfg(feature = "internal-logs")]
        {
            $crate::internal_logs::_private::info!("{}", $msg);
        }
        #[cfg(test)]
        {
            print!("info: message={}\n", $msg);
        }
        #[cfg(all(not(feature = "internal-logs"), not(test)))]
        {
            let _ = $msg;
        }
    };
    (message: $msg:expr, $($key:ident = $value:expr),+ $(,)?) => {
        #[cfg(feature = "internal-logs")]
        {
            $crate::internal_logs::_private::info!($($key = $value, )+ "{}", $msg);
        }
        #[cfg(test)]
        {
            print!("info: message={}", $msg);
            $(
                print!(", {}={}", stringify!($key), $value);
            )+
            print!("\n");
        }
        #[cfg(all(not(feature = "internal-logs"), not(test)))]
        {
            let _ = ($msg, $($value),+);
        }
    };
}

/// Log a warning message for internal use.
///
/// See [`internal_info!`] for details on behavior under features and tests.
///
/// # Examples
/// ```rust
/// internal_warn!(message: "slow response");
/// internal_warn!(message: "queue length high", queue = "fast", size = 123);
/// ```
#[macro_export]
macro_rules! internal_warn {
    (message: $msg:expr $(,)?) => {
        #[cfg(feature = "internal-logs")]
        {
            $crate::internal_logs::_private::warn!("{}", $msg);
        }
        #[cfg(test)]
        {
            print!("warn: message={}\n", $msg);
        }
        #[cfg(all(not(feature = "internal-logs"), not(test)))]
        {
            let _ = $msg;
        }
    };
    (message: $msg:expr, $($key:ident = $value:expr),+ $(,)?) => {
        #[cfg(feature = "internal-logs")]
        {
            $crate::internal_logs::_private::warn!($($key = $value, )+ "{}", $msg);
        }
        #[cfg(test)]
        {
            print!("warn: message={}", $msg);
            $(
                print!(", {}={}", stringify!($key), $value);
            )+
            print!("\n");
        }
        #[cfg(all(not(feature = "internal-logs"), not(test)))]
        {
            let _ = ($msg, $($value),+);
        }
    };
}

/// Log a debug message for internal use.
///
/// See [`internal_info!`] for details on behavior under features and tests.
///
/// # Examples
/// ```rust
/// internal_debug!(message: "retrying upload");
/// internal_debug!(message: format!("queued {}", n), queue = "fast");
/// ```
#[macro_export]
macro_rules! internal_debug {
    (message: $msg:expr $(,)?) => {
        #[cfg(feature = "internal-logs")]
        {
            $crate::internal_logs::_private::debug!("{}", $msg);
        }
        #[cfg(test)]
        {
            print!("debug: message={}\n", $msg);
        }
        #[cfg(all(not(feature = "internal-logs"), not(test)))]
        {
            let _ = $msg;
        }
    };
    (message: $msg:expr, $($key:ident = $value:expr),+ $(,)?) => {
        #[cfg(feature = "internal-logs")]
        {
            $crate::internal_logs::_private::debug!($($key = $value, )+ "{}", $msg);
        }
        #[cfg(test)]
        {
            print!("debug: message={}", $msg);
            $(
                print!(", {}={}", stringify!($key), $value);
            )+
            print!("\n");
        }
        #[cfg(all(not(feature = "internal-logs"), not(test)))]
        {
            let _ = ($msg, $($value),+);
        }
    };
}

/// Log an error message for internal use.
///
/// See [`internal_info!`] for details on behavior under features and tests.
///
/// # Examples
/// ```rust
/// internal_error!(message: "export failed");
/// internal_error!(message: format!("http {}", status), status = status, body = preview);
/// ```
#[macro_export]
macro_rules! internal_error {
    (message: $msg:expr $(,)?) => {
        #[cfg(feature = "internal-logs")]
        {
            $crate::internal_logs::_private::error!("{}", $msg);
        }
        #[cfg(test)]
        {
            print!("error: message={}\n", $msg);
        }
        #[cfg(all(not(feature = "internal-logs"), not(test)))]
        {
            let _ = $msg;
        }
    };
    (message: $msg:expr, $($key:ident = $value:expr),+ $(,)?) => {
        #[cfg(feature = "internal-logs")]
        {
            $crate::internal_logs::_private::error!($($key = $value, )+ "{}", $msg);
        }
        #[cfg(test)]
        {
            print!("error: message={}", $msg);
            $(
                print!(", {}={}", stringify!($key), $value);
            )+
            print!("\n");
        }
        #[cfg(all(not(feature = "internal-logs"), not(test)))]
        {
            let _ = ($msg, $($value),+);
        }
    };
}

#[doc(hidden)]
pub(crate) mod _private {
    #[cfg(feature = "internal-logs")]
    #[allow(unused_imports)]
    pub(crate) use tracing::{debug, error, info, warn}; // re-export
}
