// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod global_cookie_store;
mod http_client;
mod mysql_client;
mod ttc_client;

use std::borrow::Cow;
use std::fmt;

pub use http_client::HttpClient;
pub use mysql_client::MySQLClient;
use rand::Rng;
use rand::distributions::Alphanumeric;
use regex::Regex;
use sqllogictest::DBOutput;
pub use ttc_client::TTCClient;

use crate::error::Result;
use crate::util::ColumnType;

#[derive(Debug, Clone, Copy)]
pub enum QueryResultFormat {
    Json,
    #[allow(dead_code)]
    Arrow,
}

#[derive(Debug, Clone)]
pub enum ClientType {
    MySQL,
    Http,
    // Tcp Testing Container
    Ttc {
        image: String,
        port: u16,
        query_result_format: QueryResultFormat,
    },
    Hybird,
}

impl fmt::Display for ClientType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[allow(clippy::large_enum_variant)]
pub enum Client {
    MySQL(MySQLClient),
    Http(HttpClient),
    Ttc(TTCClient),
}

impl Client {
    pub async fn query(&mut self, sql: &str) -> Result<DBOutput<ColumnType>> {
        let sql = replace_placeholders(sql);
        match self {
            Client::MySQL(client) => client.query(&sql).await,
            Client::Http(client) => client.query(&sql).await,
            Client::Ttc(client) => client.query(&sql).await,
        }
    }

    pub fn enable_debug(&mut self) {
        match self {
            Client::MySQL(client) => client.debug = true,
            Client::Http(client) => client.debug = true,
            Client::Ttc(client) => client.debug = true,
        }
    }

    // Create sandbox tenant and create default database for the tenant
    pub async fn create_sandbox(&mut self) -> Result<()> {
        let sandbox_name: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
        self.query(format!("set sandbox_tenant = \'{sandbox_name}\'").as_str())
            .await?;
        self.query("create database if not exists default").await?;
        Ok(())
    }

    pub fn engine_name(&self) -> &str {
        match self {
            Client::MySQL(_) => "mysql",
            Client::Http(_) => "http",
            Client::Ttc(ttcclient) => ttcclient.image.as_str(),
        }
    }
}

fn replace_placeholders(input: &str) -> Cow<'_, str> {
    let with_rand = replace_rand_values(input);
    if !with_rand.contains("$PAIMON_WAREHOUSE") {
        return with_rand;
    }
    Cow::Owned(with_rand.replace("$PAIMON_WAREHOUSE", &resolve_paimon_warehouse()))
}

fn replace_rand_values(input: &str) -> Cow<'_, str> {
    let re = Regex::new(r"\$RAND_(\d+)_(\d+)").unwrap();
    re.replace_all(input, |caps: &regex::Captures| {
        let m: usize = caps[1].parse().unwrap();
        let n: usize = caps[2].parse().unwrap();
        let mut rng = rand::thread_rng();
        let rand_value = rng.gen_range(m..n);
        rand_value.to_string()
    })
}

/// Resolve like stateful Paimon shell tests:
/// `PAIMON_WAREHOUSE` / `PAIMON_WAREHOUSE_PATH` / `${TESTS_DATA_DIR}/paimon_warehouse`
/// / `<cwd>/tests/data/paimon_warehouse`.
fn resolve_paimon_warehouse() -> String {
    for key in ["PAIMON_WAREHOUSE", "PAIMON_WAREHOUSE_PATH"] {
        if let Ok(path) = std::env::var(key)
            && !path.is_empty()
        {
            return path;
        }
    }
    if let Ok(tests_data_dir) = std::env::var("TESTS_DATA_DIR")
        && !tests_data_dir.is_empty()
    {
        return format!("{tests_data_dir}/paimon_warehouse");
    }
    std::env::current_dir()
        .map(|cwd| cwd.join("tests/data/paimon_warehouse"))
        .unwrap_or_else(|_| std::path::PathBuf::from("tests/data/paimon_warehouse"))
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_placeholders_leaves_plain_sql() {
        let sql = "SELECT 1";
        assert!(matches!(replace_placeholders(sql), Cow::Borrowed("SELECT 1")));
    }

    #[test]
    fn resolve_paimon_warehouse_defaults_under_tests_data() {
        // Clear override env so default path is exercised when unset.
        // SAFETY: this unit test is the only code touching these keys here.
        unsafe {
            std::env::remove_var("PAIMON_WAREHOUSE");
            std::env::remove_var("PAIMON_WAREHOUSE_PATH");
            std::env::remove_var("TESTS_DATA_DIR");
        }
        let path = resolve_paimon_warehouse();
        assert!(
            path.ends_with("tests/data/paimon_warehouse"),
            "unexpected default warehouse path: {path}"
        );
    }
}
