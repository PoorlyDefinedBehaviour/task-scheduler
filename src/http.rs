use std::collections::HashMap;

use async_trait::async_trait;

use crate::contracts::{self, Response};
use anyhow::{Context, Result};

pub struct ReqwestHttp {
    client: reqwest::Client,
}

impl ReqwestHttp {
    pub fn new() -> Self {
        let client = reqwest::Client::new();
        Self { client }
    }
}

#[async_trait]
impl contracts::Http for ReqwestHttp {
    #[tracing::instrument(skip_all, ret, err)]
    async fn post(
        &self,
        headers: &HashMap<String, String>,
        url: &str,
        body: Vec<u8>,
    ) -> Result<Response> {
        let mut req = self.client.post(url).body(body);

        for (k, v) in headers {
            req = req.header(k, v);
        }

        let res = req.send().await.context("sending http request")?;

        Ok(Response {
            status: res.status().as_u16(),
        })
    }
}
