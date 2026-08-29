use anyhow::{Context, Result, bail};
use std::env;
use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_CACHE_DIR: &str = "/tmp/lix-server-slatedb-cache";
const DEFAULT_DISK_CACHE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_BLOCK_CACHE_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_METADATA_CACHE_BYTES: u64 = 32 * 1024 * 1024;
// Each retained Lix owns an independent SlateDB runtime and in-memory cache
// allocation.
// Keep a useful single-operator working set warm. Cache byte budgets are split
// across this cap, so raising it does not raise the configured aggregate block,
// metadata, or disk-cache budgets. Larger deployments can still override it.
const DEFAULT_MAX_OPEN_LIXS: u64 = 32;
const DEFAULT_PROTOCOL_TIMEOUT_SECS: u64 = 60;
const DEFAULT_RECOVERY_CLOSE_TIMEOUT_SECS: u64 = 30;

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: String,
    pub internal_token: Option<String>,
    pub max_open_lixes: usize,
    pub protocol_timeout: Duration,
    pub recovery_close_timeout: Duration,
    pub(crate) storage: S3StorageConfig,
}

#[derive(Clone, Debug)]
pub(crate) struct S3StorageConfig {
    pub(crate) endpoint: String,
    pub(crate) bucket: String,
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: String,
    pub(crate) region: String,
    pub(crate) prefix: String,
    pub(crate) allow_http: bool,
    pub(crate) cache: SlateDBCacheConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SlateDBCacheConfig {
    pub(crate) root_folder: PathBuf,
    pub(crate) max_disk_cache_bytes: usize,
    pub(crate) block_cache_bytes: u64,
    pub(crate) metadata_cache_bytes: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| {
            let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
            format!("0.0.0.0:{port}")
        });
        let internal_token = Some(required_env("LIX_SERVER_INTERNAL_TOKEN")?);
        let max_open_lixes = usize::try_from(positive_u64_env(
            "LIX_SERVER_MAX_OPEN_LIXS",
            DEFAULT_MAX_OPEN_LIXS,
        )?)
        .context("LIX_SERVER_MAX_OPEN_LIXS does not fit this platform")?;
        let protocol_timeout = Duration::from_secs(positive_u64_env(
            "LIX_SERVER_PROTOCOL_TIMEOUT_SECS",
            DEFAULT_PROTOCOL_TIMEOUT_SECS,
        )?);
        let recovery_close_timeout = Duration::from_secs(positive_u64_env(
            "LIX_SERVER_RECOVERY_CLOSE_TIMEOUT_SECS",
            DEFAULT_RECOVERY_CLOSE_TIMEOUT_SECS,
        )?);

        let storage = S3StorageConfig {
            endpoint: required_env("S3_ENDPOINT")?,
            bucket: required_env("S3_BUCKET")?,
            access_key_id: required_env("S3_ACCESS_KEY_ID")?,
            secret_access_key: required_env("S3_SECRET_ACCESS_KEY")?,
            region: env::var("S3_REGION").unwrap_or_else(|_| "auto".to_string()),
            prefix: storage_prefix()?,
            allow_http: boolean_env("S3_ALLOW_HTTP", false)?,
            cache: SlateDBCacheConfig::from_env()?,
        };

        Ok(Self {
            bind_addr,
            internal_token,
            max_open_lixes,
            protocol_timeout,
            recovery_close_timeout,
            storage,
        })
    }
}

fn storage_prefix() -> Result<String> {
    let value = optional_nonempty_env("S3_PREFIX")?.unwrap_or_default();
    let normalized = value.trim_matches('/');
    if normalized.is_empty() {
        return Ok(String::new());
    }
    if normalized.split('/').any(|segment| {
        segment.is_empty()
            || segment == "."
            || segment == ".."
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    }) {
        bail!(
            "S3_PREFIX must contain slash-separated alphanumeric, hyphen, or underscore segments"
        );
    }
    Ok(normalized.to_string())
}

fn boolean_env(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) if value == "true" => Ok(true),
        Ok(value) if value == "false" => Ok(false),
        Ok(_) => bail!("{name} must be `true` or `false`"),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("read {name}")),
    }
}

impl SlateDBCacheConfig {
    fn from_env() -> Result<Self> {
        let root_folder =
            env::var("SLATEDB_CACHE_DIR").unwrap_or_else(|_| DEFAULT_CACHE_DIR.to_string());
        if root_folder.trim().is_empty() {
            bail!("SLATEDB_CACHE_DIR must not be empty");
        }
        let disk_cache_bytes =
            positive_u64_env("SLATEDB_CACHE_MAX_BYTES", DEFAULT_DISK_CACHE_BYTES)?;
        let max_disk_cache_bytes = usize::try_from(disk_cache_bytes)
            .context("SLATEDB_CACHE_MAX_BYTES does not fit this platform")?;

        Ok(Self {
            root_folder: PathBuf::from(root_folder),
            max_disk_cache_bytes,
            block_cache_bytes: positive_u64_env(
                "SLATEDB_BLOCK_CACHE_BYTES",
                DEFAULT_BLOCK_CACHE_BYTES,
            )?,
            metadata_cache_bytes: positive_u64_env(
                "SLATEDB_METADATA_CACHE_BYTES",
                DEFAULT_METADATA_CACHE_BYTES,
            )?,
        })
    }
}

fn positive_u64_env(name: &str, default: u64) -> Result<u64> {
    let value = match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .with_context(|| format!("{name} must be a positive integer"))?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error).with_context(|| format!("read {name}")),
    };
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(value)
}

fn required_env(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    if value.is_empty() {
        bail!("{name} is required");
    }
    Ok(value)
}

fn optional_nonempty_env(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.is_empty() => bail!("{name} must not be empty when set"),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {name}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn set_env(name: &str, value: &str) {
        // SAFETY: every environment-mutating test in this module holds env_lock.
        unsafe { env::set_var(name, value) };
    }

    fn remove_env(name: &str) {
        // SAFETY: every environment-mutating test in this module holds env_lock.
        unsafe { env::remove_var(name) };
    }

    #[test]
    fn from_env_configures_s3_without_a_lix_path() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear_server_env();
        set_s3_env();

        let config = Config::from_env().unwrap();

        assert_eq!(config.max_open_lixes, 32);
        assert_eq!(config.bind_addr, "0.0.0.0:8080");
        assert_eq!(config.internal_token.as_deref(), Some("test-token"));
        assert_eq!(config.storage.endpoint, "https://s3.example");
        assert_eq!(config.storage.bucket, "lix");
        assert_eq!(config.storage.region, "auto");
        assert_eq!(config.storage.prefix, "");
        assert!(!config.storage.allow_http);
        clear_server_env();
    }

    #[test]
    fn from_env_uses_platform_port_and_internal_token() {
        let _guard = env_lock().lock().unwrap();
        clear_server_env();
        set_s3_env();
        set_env("PORT", "4567");
        set_env("LIX_SERVER_INTERNAL_TOKEN", "test-token");

        let config = Config::from_env().unwrap();

        assert_eq!(config.bind_addr, "0.0.0.0:4567");
        assert_eq!(config.internal_token.as_deref(), Some("test-token"));
        clear_server_env();
    }

    #[test]
    fn from_env_rejects_empty_internal_token() {
        let _guard = env_lock().lock().unwrap();
        clear_server_env();
        set_env("LIX_SERVER_INTERNAL_TOKEN", "");

        let error = Config::from_env().unwrap_err();

        assert!(format!("{error:#}").contains("LIX_SERVER_INTERNAL_TOKEN is required"));
        clear_server_env();
    }

    #[test]
    fn from_env_requires_internal_token() {
        let _guard = env_lock().lock().unwrap();
        clear_server_env();

        let error = Config::from_env().unwrap_err();

        assert!(format!("{error:#}").contains("LIX_SERVER_INTERNAL_TOKEN is required"));
        clear_server_env();
    }

    #[test]
    fn from_env_configures_lix_capacity() {
        let _guard = env_lock().lock().unwrap();
        clear_server_env();
        set_s3_env();
        set_env("LIX_SERVER_MAX_OPEN_LIXS", "7");

        let config = Config::from_env().unwrap();

        assert_eq!(config.max_open_lixes, 7);
        clear_server_env();
    }

    #[test]
    fn from_env_configures_recovery_close_timeout() {
        let _guard = env_lock().lock().unwrap();
        clear_server_env();
        set_s3_env();
        set_env("LIX_SERVER_RECOVERY_CLOSE_TIMEOUT_SECS", "17");

        let config = Config::from_env().unwrap();

        assert_eq!(config.recovery_close_timeout, Duration::from_secs(17));
        clear_server_env();
    }

    #[test]
    fn from_env_rejects_zero_recovery_close_timeout() {
        let _guard = env_lock().lock().unwrap();
        clear_server_env();
        set_env("LIX_SERVER_INTERNAL_TOKEN", "test-token");
        set_env("LIX_SERVER_RECOVERY_CLOSE_TIMEOUT_SECS", "0");

        let error = Config::from_env().unwrap_err();

        assert!(
            format!("{error:#}")
                .contains("LIX_SERVER_RECOVERY_CLOSE_TIMEOUT_SECS must be greater than zero")
        );
        clear_server_env();
    }

    #[test]
    fn from_env_rejects_zero_lix_capacity() {
        let _guard = env_lock().lock().unwrap();
        clear_server_env();
        set_env("LIX_SERVER_INTERNAL_TOKEN", "test-token");
        set_env("LIX_SERVER_MAX_OPEN_LIXS", "0");

        let error = Config::from_env().unwrap_err();

        assert!(
            format!("{error:#}").contains("LIX_SERVER_MAX_OPEN_LIXS must be greater than zero")
        );
        clear_server_env();
    }

    #[test]
    fn from_env_configures_bounded_s3_cache_and_prefix() {
        let _guard = env_lock().lock().unwrap();
        clear_server_env();
        set_s3_env();
        set_env("S3_PREFIX", "/preview/pr_123/");
        set_env("S3_ALLOW_HTTP", "true");
        set_env("SLATEDB_CACHE_DIR", "/tmp/test-cache");
        set_env("SLATEDB_CACHE_MAX_BYTES", "1024");
        set_env("SLATEDB_BLOCK_CACHE_BYTES", "256");
        set_env("SLATEDB_METADATA_CACHE_BYTES", "64");

        let config = Config::from_env().unwrap();

        assert_eq!(config.storage.prefix, "preview/pr_123");
        assert!(config.storage.allow_http);
        assert_eq!(
            config.storage.cache,
            SlateDBCacheConfig {
                root_folder: PathBuf::from("/tmp/test-cache"),
                max_disk_cache_bytes: 1024,
                block_cache_bytes: 256,
                metadata_cache_bytes: 64,
            }
        );
        clear_server_env();
    }

    #[test]
    fn from_env_rejects_zero_cache_sizes() {
        let _guard = env_lock().lock().unwrap();
        clear_server_env();
        set_s3_env();
        set_env("SLATEDB_CACHE_MAX_BYTES", "0");

        let error = Config::from_env().unwrap_err();

        assert!(format!("{error:#}").contains("SLATEDB_CACHE_MAX_BYTES must be greater than zero"));
        clear_server_env();
    }

    #[test]
    fn from_env_rejects_unsafe_s3_prefix() {
        let _guard = env_lock().lock().unwrap();
        clear_server_env();
        set_s3_env();
        set_env("S3_PREFIX", "preview/../production");

        let error = Config::from_env().unwrap_err();

        assert!(format!("{error:#}").contains("S3_PREFIX"));
        clear_server_env();
    }

    fn set_s3_env() {
        set_env("LIX_SERVER_INTERNAL_TOKEN", "test-token");
        set_env("S3_ENDPOINT", "https://s3.example");
        set_env("S3_BUCKET", "lix");
        set_env("S3_ACCESS_KEY_ID", "access-key");
        set_env("S3_SECRET_ACCESS_KEY", "secret-key");
    }

    fn clear_server_env() {
        for name in [
            "BIND_ADDR",
            "PORT",
            "LIX_SERVER_INTERNAL_TOKEN",
            "LIX_SERVER_MAX_OPEN_LIXS",
            "LIX_SERVER_PROTOCOL_TIMEOUT_SECS",
            "LIX_SERVER_RECOVERY_CLOSE_TIMEOUT_SECS",
            "S3_ENDPOINT",
            "S3_BUCKET",
            "S3_ACCESS_KEY_ID",
            "S3_SECRET_ACCESS_KEY",
            "S3_REGION",
            "S3_PREFIX",
            "S3_ALLOW_HTTP",
            "SLATEDB_CACHE_DIR",
            "SLATEDB_CACHE_MAX_BYTES",
            "SLATEDB_BLOCK_CACHE_BYTES",
            "SLATEDB_METADATA_CACHE_BYTES",
        ] {
            remove_env(name);
        }
    }
}
