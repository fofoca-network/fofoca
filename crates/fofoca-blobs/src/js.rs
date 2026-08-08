//! The one JS-error adapter the browser backends share.
//!
//! JS errors are not `std::error::Error`, so they cannot ride `?` into
//! `anyhow`. Rendering them at the boundary keeps every signature in the crate
//! identical across backends.

use anyhow::anyhow;
use wasm_bindgen::JsValue;

/// Render a `JsValue` rejection as an `anyhow::Error`, prefixed by what was
/// being attempted.
pub(crate) fn js_err(context: &str, error: &JsValue) -> anyhow::Error {
    anyhow!("{context}: {error:?}")
}
