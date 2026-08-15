//! Thin wrapper around a `reqwest::Client` bound to one base URL and one
//! `Authorization` header value, matching wal_tap.rs's convention: an empty
//! auth string means "send no Authorization header at all" rather than an
//! empty one.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::{Method, RequestBuilder};

use crate::security::redact_url;

#[derive(Clone)]
pub struct EsClient {
    client: reqwest::Client,
    base_url: String,
    auth: String,
}

impl EsClient {
    pub fn new(base_url: &str, auth: &str, timeout: Duration) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .context("building HTTP client")?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            auth: auth.to_string(),
        })
    }

    /// `base_url` with the path appended — for building request URLs.
    pub fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path.trim_start_matches('/'))
    }

    /// `base_url` safe to print: userinfo redacted (belt and braces — the CLI
    /// already refuses a URL with userinfo at startup).
    pub fn redacted_base_url(&self) -> String {
        redact_url(&self.base_url)
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        let req = self.client.request(method, self.url(path));
        if self.auth.is_empty() {
            req
        } else {
            req.header("Authorization", self.auth.clone())
        }
    }

    pub fn get(&self, path: &str) -> RequestBuilder {
        self.request(Method::GET, path)
    }

    pub fn post(&self, path: &str) -> RequestBuilder {
        self.request(Method::POST, path)
    }

    pub fn put(&self, path: &str) -> RequestBuilder {
        self.request(Method::PUT, path)
    }

    pub fn delete(&self, path: &str) -> RequestBuilder {
        self.request(Method::DELETE, path)
    }
}
