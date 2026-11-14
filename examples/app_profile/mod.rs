// SPDX-License-Identifier: GPL-3.0-or-later

//! Two intentionally narrow domain reads, not a general metadata SDK.
//! Schema evidence (not a hardcoded value or runtime-version assumption):
//! https://github.com/paritytech/polkadot-bulletin-chain/blob/ec88c0f52ee4a09213cc0cc105390a190cd41568/pallets/transaction-storage/src/lib.rs#L794-L800
//! https://github.com/paritytech/individuality-community/blob/3c4c1de7ec86804165dd5e5008d4acfe4ba731bd/pallets/members/src/lib.rs#L166-L169
//! Runtime aliases are TransactionStorage (bulletin-paseo/src/lib.rs) and
//! Members (next-people-paseo/src/lib.rs) at those same revisions.

use std::hash::Hasher;

use anyhow::{Context, Result, bail, ensure};
use frame_metadata::{
    RuntimeMetadata,
    v14::{StorageEntryModifier, StorageEntryType},
};
use scale_info::{PortableRegistry, TypeDef, TypeDefPrimitive};
use serde_json::{Value, json};
use twox_hash::XxHash64;

pub struct ScalarRead {
    types: PortableRegistry,
    value_type: u32,
    type_name: &'static str,
    source: Source,
    pub metadata_version: u32,
}

enum Source {
    Storage { key: String, default: Vec<u8> },
    Constant(Vec<u8>),
}

impl ScalarRead {
    pub fn retention(output: &str) -> Result<Self> {
        Self::from_metadata(output, "TransactionStorage", "RetentionPeriod", false)
    }

    pub fn old_root_retention(output: &str) -> Result<Self> {
        Self::from_metadata(output, "Members", "OldRootRetentionDuration", true)
    }

    fn from_metadata(output: &str, pallet_name: &str, item: &str, constant: bool) -> Result<Self> {
        let metadata = crate::shared::metadata::decode(output)?;
        let metadata_version = metadata.1.version();
        macro_rules! extract {
            ($metadata:expr) => {{
                let metadata = $metadata;
                let mut pallets = metadata
                    .pallets
                    .into_iter()
                    .filter(|p| p.name == pallet_name);
                let pallet = pallets
                    .next()
                    .with_context(|| format!("no {pallet_name} pallet"))?;
                ensure!(pallets.next().is_none(), "duplicate {pallet_name} pallets");
                let (value_type, source) = if constant {
                    let mut constants = pallet.constants.into_iter().filter(|c| c.name == item);
                    let entry = constants
                        .next()
                        .with_context(|| format!("no {pallet_name}.{item} constant"))?;
                    ensure!(constants.next().is_none(), "duplicate constant {item}");
                    (entry.ty.id, Source::Constant(entry.value))
                } else {
                    let storage = pallet.storage.context("pallet has no storage metadata")?;
                    let mut entries = storage.entries.into_iter().filter(|e| e.name == item);
                    let entry = entries
                        .next()
                        .with_context(|| format!("no {pallet_name}.{item} storage"))?;
                    ensure!(entries.next().is_none(), "duplicate storage {item}");
                    ensure!(
                        matches!(entry.modifier, StorageEntryModifier::Default),
                        "expected default-valued {item}"
                    );
                    let StorageEntryType::Plain(value_type) = entry.ty else {
                        bail!("expected plain storage {pallet_name}.{item}");
                    };
                    (
                        value_type.id,
                        Source::Storage {
                            key: storage_key(&storage.prefix, item),
                            default: entry.default,
                        },
                    )
                };
                (metadata.types, value_type, source)
            }};
        }
        let (types, value_type, source) = match metadata.1 {
            RuntimeMetadata::V14(m) => extract!(m),
            RuntimeMetadata::V15(m) => extract!(m),
            RuntimeMetadata::V16(m) => extract!(m),
            _ => bail!("unsupported metadata version {metadata_version}; expected V14–V16"),
        };
        let type_name = unsigned_type(&types, value_type)?;
        let schema = Self {
            types,
            value_type,
            type_name,
            source,
            metadata_version,
        };
        // Validate the metadata default even if the queried storage is present.
        if let Source::Storage { default, .. } = &schema.source {
            schema.unsigned(default)?;
        }
        Ok(schema)
    }

    pub fn storage_key(&self) -> Option<&str> {
        match &self.source {
            Source::Storage { key, .. } => Some(key),
            Source::Constant(_) => None,
        }
    }

    /// Absence is a verified storage result, never an error fallback.
    /// Constants have no storage presence and never masquerade as storage proofs.
    pub fn decode(&self, stored: Option<&[u8]>) -> Result<Value> {
        let (encoded, presence) = match &self.source {
            Source::Storage { default, .. } => match stored {
                Some(value) => (value, "present"),
                None => (default.as_slice(), "default"),
            },
            Source::Constant(value) => {
                ensure!(
                    stored.is_none(),
                    "a metadata constant cannot have a storage value"
                );
                (value.as_slice(), "constant")
            }
        };
        let value = self.unsigned(encoded)?;
        Ok(
            json!({"presence": presence, "result": {"type": self.type_name, "value": value.to_string()}}),
        )
    }

    fn unsigned(&self, encoded: &[u8]) -> Result<u128> {
        ensure!(encoded.len() <= 16, "unsigned scalar exceeds 16 bytes");
        let mut input = encoded;
        let value = scale_value::scale::decode_as_type(&mut input, self.value_type, &self.types)
            .context("domain value does not match verified metadata")?;
        ensure!(input.is_empty(), "trailing bytes in domain value");
        value.as_u128().context("domain value is not unsigned")
    }
}

fn unsigned_type(types: &PortableRegistry, id: u32) -> Result<&'static str> {
    match types.resolve(id).map(|t| &t.type_def) {
        Some(TypeDef::Primitive(TypeDefPrimitive::U8)) => Ok("u8"),
        Some(TypeDef::Primitive(TypeDefPrimitive::U16)) => Ok("u16"),
        Some(TypeDef::Primitive(TypeDefPrimitive::U32)) => Ok("u32"),
        Some(TypeDef::Primitive(TypeDefPrimitive::U64)) => Ok("u64"),
        Some(TypeDef::Primitive(TypeDefPrimitive::U128)) => Ok("u128"),
        _ => bail!("unsupported domain type; expected a fixed-width unsigned scalar"),
    }
}

fn storage_key(prefix: &str, item: &str) -> String {
    let mut key = [0; 32];
    for (index, component) in [prefix, item].into_iter().enumerate() {
        for seed in [0, 1] {
            let mut hash = XxHash64::with_seed(seed);
            hash.write(component.as_bytes());
            let offset = index * 16 + seed as usize * 8;
            key[offset..offset + 8].copy_from_slice(&hash.finish().to_le_bytes());
        }
    }
    format!("0x{}", hex::encode(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parity_scale_codec::Encode;
    use scale_info::{Registry, meta_type};

    fn scalar(source: Source) -> ScalarRead {
        let mut registry = Registry::new();
        let value_type = registry.register_type(&meta_type::<u128>()).id;
        let types = registry.into();
        ScalarRead {
            type_name: unsigned_type(&types, value_type).unwrap(),
            types,
            value_type,
            source,
            metadata_version: 14,
        }
    }

    #[test]
    fn exact_present_and_nonzero_metadata_default() {
        let schema = scalar(Source::Storage {
            key: String::new(),
            default: 17u128.encode(),
        });
        let absent = schema.decode(None).unwrap();
        assert_eq!(absent["presence"], "default");
        assert_eq!(absent["result"]["value"], "17");
        let present = schema.decode(Some(&u128::MAX.encode())).unwrap();
        assert_eq!(present["presence"], "present");
        assert_eq!(present["result"]["type"], "u128");
        assert_eq!(present["result"]["value"], u128::MAX.to_string());
        assert!(schema.decode(Some(&[])).is_err());
        assert!(schema.decode(Some(&[0; 17])).is_err());
    }

    #[test]
    fn constant_is_not_storage_and_rejects_trailing_bytes() {
        let schema = scalar(Source::Constant(42u128.encode()));
        assert!(schema.storage_key().is_none());
        assert_eq!(schema.decode(None).unwrap()["presence"], "constant");
        assert!(schema.decode(Some(&42u128.encode())).is_err());
        let mut registry = Registry::new();
        let value_type = registry.register_type(&meta_type::<u32>()).id;
        let types = registry.into();
        let schema = ScalarRead {
            types,
            value_type,
            type_name: "u32",
            source: Source::Constant(vec![0; 5]),
            metadata_version: 14,
        };
        assert!(schema.decode(None).is_err());
    }

    #[test]
    fn rejects_variable_or_signed_domain_types() {
        let mut registry = Registry::new();
        let signed = registry.register_type(&meta_type::<i64>()).id;
        let sequence = registry.register_type(&meta_type::<Vec<u8>>()).id;
        let types = registry.into();
        assert!(unsigned_type(&types, signed).is_err());
        assert!(unsigned_type(&types, sequence).is_err());
    }
}
