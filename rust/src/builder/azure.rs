use std::collections::HashMap;

use super::BuilderError;
use crate::DeltaResult;

use object_store::azure::MicrosoftAzureBuilder;
use once_cell::sync::Lazy;

#[derive(PartialEq, Eq)]
enum AzureConfigKey {
    AccountKey,
    AccountName,
    ClientId,
    ClientSecret,
    AuthorityId,
    SasKey,
    UseEmulator,
    AllowHttp,
}

impl AzureConfigKey {
    fn get_from_env(&self) -> Option<String> {
        for (key, value) in ALIAS_MAP.iter() {
            if value == self {
                if let Ok(val) = std::env::var(key.to_ascii_uppercase()) {
                    return Some(val);
                }
            }
        }
        None
    }
}

static ALIAS_MAP: Lazy<HashMap<&'static str, AzureConfigKey>> = Lazy::new(|| {
    HashMap::from([
        // access key
        ("azure_storage_account_key", AzureConfigKey::AccountKey),
        ("azure_storage_access_key", AzureConfigKey::AccountKey),
        ("azure_storage_master_key", AzureConfigKey::AccountKey),
        ("azure_storage_key", AzureConfigKey::AccountKey),
        ("account_key", AzureConfigKey::AccountKey),
        ("access_key", AzureConfigKey::AccountKey),
        // sas key
        ("azure_storage_sas_token", AzureConfigKey::SasKey),
        ("azure_storage_sas_key", AzureConfigKey::SasKey),
        ("sas_token", AzureConfigKey::SasKey),
        ("sas_key", AzureConfigKey::SasKey),
        // account name
        ("azure_storage_account_name", AzureConfigKey::AccountName),
        ("account_name", AzureConfigKey::AccountName),
        // client id
        ("azure_storage_client_id", AzureConfigKey::ClientId),
        ("azure_client_id", AzureConfigKey::ClientId),
        ("client_id", AzureConfigKey::ClientId),
        // client secret
        ("azure_storage_client_secret", AzureConfigKey::ClientSecret),
        ("azure_client_secret", AzureConfigKey::ClientSecret),
        ("client_secret", AzureConfigKey::ClientSecret),
        // authority id
        ("azure_storage_tenant_id", AzureConfigKey::AuthorityId),
        ("azure_storage_authority_id", AzureConfigKey::AuthorityId),
        ("azure_tenant_id", AzureConfigKey::AuthorityId),
        ("azure_authority_id", AzureConfigKey::AuthorityId),
        ("tenant_id", AzureConfigKey::AuthorityId),
        ("authority_id", AzureConfigKey::AuthorityId),
        // use emulator
        ("azure_storage_use_emulator", AzureConfigKey::UseEmulator),
        ("object_store_use_emulator", AzureConfigKey::UseEmulator),
        ("use_emulator", AzureConfigKey::UseEmulator),
        // AllowHttp
        ("azure_storage_allow_http", AzureConfigKey::AllowHttp),
        ("object_store_allow_http", AzureConfigKey::AllowHttp),
        ("allow_http", AzureConfigKey::AllowHttp),
    ])
});

pub struct AzureConfig {
    account_key: Option<String>,
    account_name: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    authority_id: Option<String>,
    sas_key: Option<String>,
    use_emulator: Option<bool>,
    allow_http: Option<bool>,
}

impl AzureConfig {
    fn new(options: &HashMap<String, String>) -> Self {
        let mut account_key = None;
        let mut account_name = None;
        let mut client_id = None;
        let mut client_secret = None;
        let mut authority_id = None;
        let mut sas_key = None;
        let mut use_emulator = None;
        let mut allow_http = None;

        for (raw, value) in options {
            if let Some(key) = ALIAS_MAP.get(&*raw.to_ascii_lowercase()) {
                match key {
                    AzureConfigKey::AccountKey => account_key = Some(value.clone()),
                    AzureConfigKey::AccountName => account_name = Some(value.clone()),
                    AzureConfigKey::ClientId => client_id = Some(value.clone()),
                    AzureConfigKey::ClientSecret => client_secret = Some(value.clone()),
                    AzureConfigKey::AuthorityId => authority_id = Some(value.clone()),
                    AzureConfigKey::SasKey => sas_key = Some(value.clone()),
                    AzureConfigKey::UseEmulator => use_emulator = parse_boolean(value),
                    AzureConfigKey::AllowHttp => allow_http = parse_boolean(value),
                }
            }
        }

        Self {
            account_key,
            account_name,
            client_id,
            client_secret,
            authority_id,
            sas_key,
            use_emulator,
            allow_http,
        }
    }

    /// Check all options if a valid builder can be generated, if not, check if configuration
    /// can be read from the environment.
    pub fn get_builder(options: &HashMap<String, String>) -> DeltaResult<MicrosoftAzureBuilder> {
        let config = Self::new(options);

        let mut builder = MicrosoftAzureBuilder::default();
        if let Some(account) = config.account_name {
            builder = builder.with_account(account);
        } else {
            builder = builder.with_account(
                AzureConfigKey::AccountName
                    .get_from_env()
                    .ok_or_else(|| BuilderError::Required("AZURE_STORAGE_ACCOUNT".into()))?,
            );
        }

        if let Some(use_emulator) = config.use_emulator {
            builder = builder.with_use_emulator(use_emulator);
        } else if let Some(val) = AzureConfigKey::UseEmulator.get_from_env() {
            if let Some(use_emulator) = parse_boolean(&val) {
                builder = builder.with_use_emulator(use_emulator);
            }
        }

        let allow_http = config.allow_http.map(Some).unwrap_or_else(|| {
            AzureConfigKey::AllowHttp
                .get_from_env()
                .and_then(|val| parse_boolean(&val))
        });
        if let Some(allow) = allow_http {
            builder = builder.with_allow_http(allow);
        }

        if let Some(key) = config.account_key {
            builder = builder.with_access_key(key);
            return Ok(builder);
        }

        if let Some(sas) = config.sas_key {
            let query_pairs = split_sas(&sas)?;
            builder = builder.with_sas_authorization(query_pairs);
            return Ok(builder);
        }

        if let (Some(client_id), Some(client_secret), Some(tenant_id)) = (
            config.client_id.clone(),
            config.client_secret.clone(),
            config.authority_id.clone(),
        ) {
            builder = builder.with_client_secret_authorization(client_id, client_secret, tenant_id);
            return Ok(builder);
        }

        if let Some(key) = AzureConfigKey::AccountKey.get_from_env() {
            builder = builder.with_access_key(key);
            return Ok(builder);
        }

        let client_id = config
            .client_id
            .map(Some)
            .unwrap_or_else(|| AzureConfigKey::ClientId.get_from_env());
        let client_secret = config
            .client_secret
            .map(Some)
            .unwrap_or_else(|| AzureConfigKey::ClientSecret.get_from_env());
        let authority_id = config
            .authority_id
            .map(Some)
            .unwrap_or_else(|| AzureConfigKey::AuthorityId.get_from_env());

        if let (Some(client_id), Some(client_secret), Some(tenant_id)) =
            (client_id, client_secret, authority_id)
        {
            builder = builder.with_client_secret_authorization(client_id, client_secret, tenant_id);
            return Ok(builder);
        }

        if let Some(sas) = AzureConfigKey::SasKey.get_from_env() {
            let query_pairs = split_sas(&sas)?;
            builder = builder.with_sas_authorization(query_pairs);
            return Ok(builder);
        }

        Err(BuilderError::MissingCredential.into())
    }
}

fn parse_boolean(term: &str) -> Option<bool> {
    match term.to_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Some(true),
        "false" | "0" | "no" | "n" => Some(false),
        _ => None,
    }
}

fn split_sas(sas: &str) -> Result<Vec<(String, String)>, BuilderError> {
    let kv_str_pairs = sas
        .trim_start_matches('?')
        .split('&')
        .filter(|s| !s.chars().all(char::is_whitespace));
    let mut pairs = Vec::new();
    for kv_pair_str in kv_str_pairs {
        let (k, v) = kv_pair_str
            .trim()
            .split_once('=')
            .ok_or(BuilderError::MissingCredential)?;
        pairs.push((k.into(), v.into()))
    }
    Ok(pairs)
}
