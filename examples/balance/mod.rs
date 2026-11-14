// SPDX-License-Identifier: GPL-3.0-or-later

use std::hash::Hasher;

use anyhow::{Context, Result, bail, ensure};
use blake2::{Blake2b, Blake2b512, Digest, digest::consts::U16};
use frame_metadata::{
    RuntimeMetadata,
    v14::{StorageEntryModifier, StorageEntryType, StorageHasher},
};
use scale_info::{PortableRegistry, TypeDef, TypeDefPrimitive, form::PortableForm};
use scale_value::{Composite, Value, ValueDef};
use serde_json::{Map, Value as Json};
use twox_hash::XxHash64;

pub fn bytes(value: &str) -> Result<Vec<u8>> {
    hex::decode(
        value
            .strip_prefix("0x")
            .context("expected 0x-prefixed hex")?,
    )
    .context("invalid hex")
}

/// SS58 is only an account encoding: its network prefix does not select a chain.
pub fn account_id(address: &str) -> Result<[u8; 32]> {
    if address.starts_with("0x") {
        ensure!(
            address.len() == 66,
            "hex address must contain exactly 32 bytes"
        );
        let mut account = [0; 32];
        hex::decode_to_slice(&address[2..], &mut account).context("invalid account hex")?;
        return Ok(account);
    }
    ensure!(address.len() <= 64, "SS58 address is too long");
    let decoded = bs58::decode(address)
        .into_vec()
        .context("invalid SS58 base58")?;
    let first = *decoded.first().context("empty SS58 address")?;
    let (prefix_len, network) = match first {
        0..=63 => (1, u16::from(first)),
        64..=127 => {
            let second = *decoded.get(1).context("truncated SS58 prefix")?;
            let network = (u16::from(first & 0x3f) << 2)
                | u16::from(second >> 6)
                | (u16::from(second & 0x3f) << 8);
            ensure!(network >= 64, "noncanonical SS58 network prefix");
            (2, network)
        }
        _ => bail!("unsupported SS58 prefix"),
    };
    ensure!(
        network != 46 && network != 47,
        "reserved SS58 network prefix"
    );
    ensure!(
        decoded.len() == prefix_len + 34,
        "SS58 address must encode AccountId32, not an account index"
    );
    let payload_len = prefix_len + 32;
    let mut hash = Blake2b512::new();
    hash.update(b"SS58PRE");
    hash.update(&decoded[..payload_len]);
    ensure!(
        decoded[payload_len..] == hash.finalize()[..2],
        "invalid SS58 checksum"
    );
    decoded[prefix_len..payload_len]
        .try_into()
        .context("invalid AccountId32")
}

pub struct AccountSchema {
    types: PortableRegistry,
    value_type: u32,
    balance_fields: Vec<String>,
    pub storage_key: String,
    pub metadata_version: u32,
}

impl AccountSchema {
    /// Metadata_metadata returns a SCALE-encoded OpaqueMetadata (a byte vector).
    pub fn from_runtime_output(output: &str, account: &[u8; 32]) -> Result<Self> {
        let metadata = crate::shared::metadata::decode(output)?;
        let version = metadata.1.version();
        // Storage entry metadata has the same key/value representation in these versions.
        // Move out only System storage and the registry; discard all unrelated metadata.
        macro_rules! extract {
            ($metadata:expr) => {{
                let metadata = $metadata;
                let mut system = metadata.pallets.into_iter().filter(|p| p.name == "System");
                let pallet = system.next().context("metadata has no System pallet")?;
                ensure!(system.next().is_none(), "duplicate System pallets");
                let storage = pallet.storage.context("System has no storage metadata")?;
                let mut entries = storage.entries.into_iter().filter(|e| e.name == "Account");
                let entry = entries.next().context("metadata has no System.Account")?;
                ensure!(entries.next().is_none(), "duplicate System.Account entries");
                ensure!(
                    matches!(entry.modifier, StorageEntryModifier::Default),
                    "System.Account is not default-valued storage"
                );
                (metadata.types, storage.prefix, entry.ty, entry.default)
            }};
        }
        let (types, prefix, entry_type, default) = match metadata.1 {
            RuntimeMetadata::V14(metadata) => extract!(metadata),
            RuntimeMetadata::V15(metadata) => extract!(metadata),
            RuntimeMetadata::V16(metadata) => extract!(metadata),
            _ => bail!("unsupported runtime metadata version {version}; only V14–V16 are accepted"),
        };
        let schema = Self::new(types, &prefix, entry_type, account, version)?;
        let default = schema.decode(Some(&default))?;
        ensure!(
            default.values().all(|v| v.as_str() == Some("0")),
            "System.Account metadata default has nonzero balances"
        );
        Ok(schema)
    }

    fn new(
        types: PortableRegistry,
        prefix: &str,
        entry_type: StorageEntryType<PortableForm>,
        account: &[u8; 32],
        metadata_version: u32,
    ) -> Result<Self> {
        let (key_type, value_type) = match entry_type {
            StorageEntryType::Map {
                hashers,
                key,
                value,
            } => {
                ensure!(
                    hashers == [StorageHasher::Blake2_128Concat],
                    "System.Account requires a single Blake2_128Concat hasher"
                );
                (key.id, value.id)
            }
            _ => bail!("System.Account is not a storage map"),
        };
        ensure!(
            is_account_id(&types, key_type, 0),
            "System.Account key is not a SCALE AccountId32"
        );
        // Accept only bounded fixed-width numeric structures, never vectors, enums or
        // recursive types. Decode by metadata, not offsets or a hardcoded AccountInfo.
        validate_fixed_type(&types, value_type, 0, &mut 128)?;
        let account_fields = named_fields(&types, value_type)?;
        let data_type = account_fields
            .iter()
            .find(|f| f.name.as_deref() == Some("data"))
            .context("System.Account has no named data field")?
            .ty
            .id;
        let fields = named_fields(&types, data_type)?;
        let names: Vec<_> = fields.iter().map(|f| f.name.as_deref().unwrap()).collect();
        ensure!(
            names.contains(&"free") && names.contains(&"reserved"),
            "AccountData lacks free/reserved balances"
        );
        let modern = names.contains(&"frozen");
        let legacy = names.contains(&"misc_frozen") && names.contains(&"fee_frozen");
        ensure!(
            modern != legacy,
            "AccountData must have frozen OR misc_frozen/fee_frozen balances"
        );
        for name in &names {
            ensure!(
                matches!(*name, "free" | "reserved" | "flags")
                    || (modern && *name == "frozen")
                    || (legacy && matches!(*name, "misc_frozen" | "fee_frozen")),
                "unsupported AccountData field {name}"
            );
        }
        let balance_fields: Vec<String> = fields
            .iter()
            .filter(|f| f.name.as_deref() != Some("flags"))
            .map(|f| {
                ensure!(
                    matches!(
                        types.resolve(f.ty.id).map(|t| &t.type_def),
                        Some(TypeDef::Primitive(TypeDefPrimitive::U128))
                    ),
                    "balance {} is not an unsigned 128-bit base-unit value",
                    f.name.as_deref().unwrap()
                );
                Ok(f.name.clone().unwrap())
            })
            .collect::<Result<_>>()?;
        let mut key = [0; 80];
        for (component_index, component) in [prefix.as_bytes(), b"Account".as_slice()]
            .into_iter()
            .enumerate()
        {
            for seed in [0, 1] {
                let mut hash = XxHash64::with_seed(seed);
                hash.write(component);
                let offset = component_index * 16 + seed as usize * 8;
                key[offset..offset + 8].copy_from_slice(&hash.finish().to_le_bytes());
            }
        }
        key[32..48].copy_from_slice(&Blake2b::<U16>::digest(account));
        key[48..].copy_from_slice(account);
        Ok(Self {
            types,
            value_type,
            balance_fields,
            storage_key: format!("0x{}", hex::encode(key)),
            metadata_version,
        })
    }

    pub fn decode(&self, encoded: Option<&[u8]>) -> Result<Map<String, Json>> {
        let mut balances = Map::new();
        let Some(mut input) = encoded else {
            for field in &self.balance_fields {
                balances.insert(field.clone(), Json::String("0".into()));
            }
            return Ok(balances);
        };
        ensure!(
            input.len() <= 4096,
            "System.Account value exceeds 4096 bytes"
        );
        let value = scale_value::scale::decode_as_type(&mut input, self.value_type, &self.types)
            .context("System.Account does not match verified metadata")?;
        ensure!(input.is_empty(), "trailing bytes in System.Account value");
        let data = field(&value, "data")?;
        for name in &self.balance_fields {
            let amount = field(data, name)?
                .as_u128()
                .context("decoded balance is not u128")?;
            balances.insert(name.clone(), Json::String(amount.to_string()));
        }
        Ok(balances)
    }
}

fn named_fields(types: &PortableRegistry, id: u32) -> Result<&[scale_info::Field<PortableForm>]> {
    let TypeDef::Composite(composite) = &types
        .resolve(id)
        .context("unresolved metadata type")?
        .type_def
    else {
        bail!("expected a named AccountInfo/AccountData struct");
    };
    for (index, field) in composite.fields.iter().enumerate() {
        let name = field
            .name
            .as_deref()
            .context("unnamed AccountInfo/AccountData field")?;
        ensure!(
            !composite.fields[..index]
                .iter()
                .any(|f| f.name.as_deref() == Some(name)),
            "duplicate metadata field {name}"
        );
    }
    Ok(&composite.fields)
}

fn is_account_id(types: &PortableRegistry, id: u32, depth: usize) -> bool {
    if depth > 8 {
        return false;
    }
    match types.resolve(id).map(|t| &t.type_def) {
        Some(TypeDef::Array(array)) if array.len == 32 => matches!(
            types.resolve(array.type_param.id).map(|t| &t.type_def),
            Some(TypeDef::Primitive(TypeDefPrimitive::U8))
        ),
        Some(TypeDef::Composite(composite)) if composite.fields.len() == 1 => {
            is_account_id(types, composite.fields[0].ty.id, depth + 1)
        }
        _ => false,
    }
}

fn validate_fixed_type(
    types: &PortableRegistry,
    id: u32,
    depth: usize,
    remaining: &mut usize,
) -> Result<()> {
    ensure!(
        depth <= 8 && *remaining > 0,
        "recursive, oversized or deeply nested System.Account type"
    );
    *remaining -= 1;
    match &types
        .resolve(id)
        .context("unresolved System.Account type")?
        .type_def
    {
        TypeDef::Primitive(
            TypeDefPrimitive::U8
            | TypeDefPrimitive::U16
            | TypeDefPrimitive::U32
            | TypeDefPrimitive::U64
            | TypeDefPrimitive::U128,
        ) => Ok(()),
        TypeDef::Composite(composite) => {
            ensure!(
                composite.fields.len() <= 16,
                "too many System.Account fields"
            );
            for field in &composite.fields {
                validate_fixed_type(types, field.ty.id, depth + 1, remaining)?;
            }
            Ok(())
        }
        _ => bail!("unsupported System.Account type; expected fixed-width unsigned fields"),
    }
}

fn field<'a, T>(value: &'a Value<T>, name: &str) -> Result<&'a Value<T>> {
    let ValueDef::Composite(Composite::Named(fields)) = &value.value else {
        bail!("decoded value is not a named struct");
    };
    fields
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
        .with_context(|| format!("decoded value has no {name} field"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parity_scale_codec::Encode;
    use scale_info::{Registry, TypeInfo, meta_type};

    #[derive(Encode, TypeInfo)]
    struct Account {
        data: Balances,
        nonce: u32,
    }
    // Deliberately nonstandard field order proves that decoding uses metadata.
    #[derive(Encode, TypeInfo)]
    struct Balances {
        frozen: u128,
        free: u128,
        reserved: u128,
    }

    fn schema() -> AccountSchema {
        let mut registry = Registry::new();
        let key = registry.register_type(&meta_type::<[u8; 32]>());
        let value = registry.register_type(&meta_type::<Account>());
        AccountSchema::new(
            registry.into(),
            "System",
            StorageEntryType::Map {
                hashers: vec![StorageHasher::Blake2_128Concat],
                key,
                value,
            },
            &[0; 32],
            14,
        )
        .unwrap()
    }

    #[test]
    fn present_absent_and_invalid_scale() {
        let schema = schema();
        let encoded = Account {
            data: Balances {
                free: u128::MAX,
                reserved: 9,
                frozen: 7,
            },
            nonce: 42,
        }
        .encode();
        let balances = schema.decode(Some(&encoded)).unwrap();
        assert_eq!(balances["free"], u128::MAX.to_string());
        assert_eq!(balances["reserved"], "9");
        assert_eq!(balances["frozen"], "7");
        assert!(schema.decode(None).unwrap().values().all(|v| v == "0"));
        assert!(schema.decode(Some(&[])).is_err());
        assert!(schema.decode(Some(&encoded[..encoded.len() - 1])).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(schema.decode(Some(&trailing)).is_err());
        assert!(
            schema
                .storage_key
                .starts_with("0x26aa394eea5630e07c48ae0c9558cef7b99d880ec681799c0cf30e8886371da9")
        );
    }

    #[test]
    fn rejects_incompatible_metadata() {
        let mut registry = Registry::new();
        let key = registry.register_type(&meta_type::<[u8; 32]>());
        let value = registry.register_type(&meta_type::<Account>());
        assert!(
            AccountSchema::new(
                registry.into(),
                "System",
                StorageEntryType::Map {
                    hashers: vec![StorageHasher::Twox64Concat],
                    key,
                    value,
                },
                &[0; 32],
                14
            )
            .is_err()
        );
    }

    #[test]
    fn metadata_envelope_and_default_are_verified() {
        use frame_metadata::RuntimeMetadataPrefixed;
        use frame_metadata::v14::{
            ExtrinsicMetadata, PalletMetadata, PalletStorageMetadata, RuntimeMetadataV14,
            StorageEntryMetadata,
        };
        let output = |free| {
            let metadata = RuntimeMetadataV14::new(
                vec![PalletMetadata {
                    name: "System",
                    storage: Some(PalletStorageMetadata {
                        prefix: "System",
                        entries: vec![StorageEntryMetadata {
                            name: "Account",
                            modifier: StorageEntryModifier::Default,
                            ty: StorageEntryType::Map {
                                hashers: vec![StorageHasher::Blake2_128Concat],
                                key: meta_type::<[u8; 32]>(),
                                value: meta_type::<Account>(),
                            },
                            default: Account {
                                data: Balances {
                                    free,
                                    reserved: 0,
                                    frozen: 0,
                                },
                                nonce: 0,
                            }
                            .encode(),
                            docs: vec![],
                        }],
                    }),
                    calls: None,
                    event: None,
                    constants: vec![],
                    error: None,
                    index: 0,
                }],
                ExtrinsicMetadata {
                    ty: meta_type::<()>(),
                    version: 4,
                    signed_extensions: vec![],
                },
                meta_type::<()>(),
            );
            let prefixed: RuntimeMetadataPrefixed = metadata.into();
            format!("0x{}", hex::encode(prefixed.encode().encode()))
        };
        let schema = AccountSchema::from_runtime_output(&output(0), &[0; 32]).unwrap();
        assert_eq!(schema.metadata_version, 14);
        assert!(AccountSchema::from_runtime_output(&output(1), &[0; 32]).is_err());
        assert!(AccountSchema::from_runtime_output("0x00", &[0; 32]).is_err());
        assert!(AccountSchema::from_runtime_output(&(output(0) + "00"), &[0; 32]).is_err());
    }

    #[test]
    fn legacy_frozen_fields_are_not_conflated() {
        #[derive(Encode, TypeInfo)]
        struct LegacyData {
            free: u128,
            reserved: u128,
            misc_frozen: u128,
            fee_frozen: u128,
        }
        #[derive(Encode, TypeInfo)]
        struct LegacyAccount {
            nonce: u32,
            data: LegacyData,
        }
        let mut registry = Registry::new();
        let key = registry.register_type(&meta_type::<[u8; 32]>());
        let value = registry.register_type(&meta_type::<LegacyAccount>());
        let schema = AccountSchema::new(
            registry.into(),
            "System",
            StorageEntryType::Map {
                hashers: vec![StorageHasher::Blake2_128Concat],
                key,
                value,
            },
            &[0; 32],
            14,
        )
        .unwrap();
        let account = LegacyAccount {
            nonce: 0,
            data: LegacyData {
                free: 10,
                reserved: 1,
                misc_frozen: 7,
                fee_frozen: 3,
            },
        };
        let balances = schema.decode(Some(&account.encode())).unwrap();
        assert_eq!(balances["misc_frozen"], "7");
        assert_eq!(balances["fee_frozen"], "3");
        assert!(!balances.contains_key("frozen"));
    }

    fn ss58(prefix: &[u8], account: &[u8; 32]) -> String {
        let mut bytes = prefix.to_vec();
        bytes.extend_from_slice(account);
        let mut hash = Blake2b512::new();
        hash.update(b"SS58PRE");
        hash.update(&bytes);
        bytes.extend_from_slice(&hash.finalize()[..2]);
        bs58::encode(bytes).into_string()
    }

    #[test]
    fn account_encodings_and_checksums() {
        let alice =
            account_id("0xd43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d")
                .unwrap();
        assert_eq!(account_id(&ss58(&[42], &alice)).unwrap(), alice);
        // 1000 is a canonical two-byte prefix: [0x7a, 0x03].
        assert_eq!(account_id(&ss58(&[0x7a, 0x03], &alice)).unwrap(), alice);
        assert!(account_id(&ss58(&[46], &alice)).is_err());
        assert!(account_id(&ss58(&[0x40, 0], &alice)).is_err());
        let encoded = ss58(&[42], &alice);
        let mut corrupt = bs58::decode(encoded).into_vec().unwrap();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(account_id(&bs58::encode(corrupt).into_string()).is_err());
        for bad in ["", "0x00", "0xzz", "not an address", "11111"] {
            assert!(account_id(bad).is_err(), "accepted {bad}");
        }
    }
}
