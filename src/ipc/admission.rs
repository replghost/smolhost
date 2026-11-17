// SPDX-License-Identifier: GPL-3.0-or-later

use std::borrow::Cow;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, value::RawValue};

use super::{MAX_PENDING_REQUESTS, MAX_SUBSCRIPTIONS, rpc_error};

pub(super) struct Budget {
    next_id: Option<u64>,
    pending: Vec<Pending>,
    subscriptions: Vec<Subscription>,
}

struct Pending {
    id: u64,
    client_id: Option<Box<RawValue>>,
    operation: Operation,
}

#[derive(Clone, Copy)]
enum Operation {
    Ordinary,
    Follow,
    Unfollow(Option<u64>),
}

struct Subscription {
    // The creating request's never-reused ID also identifies this incarnation.
    generation: u64,
    name: String,
}

impl Budget {
    pub(super) fn new() -> Self {
        Self {
            next_id: Some(0),
            pending: Vec::with_capacity(MAX_PENDING_REQUESTS as usize),
            subscriptions: Vec::with_capacity(MAX_SUBSCRIPTIONS as usize),
        }
    }

    pub(super) fn admit(
        &mut self,
        mut request: Value,
        wire: &str,
    ) -> Result<String, Option<String>> {
        // The caller has already validated the envelope and allowed method.
        // Value falls back to f64 for large integers and decimal IDs. Preserve
        // their original JSON rather than rounding the client's correlation key.
        // Native integers, strings and null need no second scan of the request.
        let numeric_id = if request.get("id").is_some_and(Value::is_f64) {
            Some(
                serde_json::from_str::<NumericClientId<'_>>(wire)
                    .map_err(|_| {
                        Some(rpc_error(Value::Null, -32600, "Invalid Request").to_string())
                    })?
                    .id,
            )
        } else {
            None
        };
        let method = request["method"].as_str().unwrap();
        let operation = match method {
            "chainHead_v1_follow" => Operation::Follow,
            "chainHead_v1_unfollow" => {
                // smoldot 6.0.0 methods.rs accepts [id] or {"followSubscription": id}.
                let name = request
                    .get("params")
                    .and_then(|params| match params {
                        Value::Array(params) => params.first(),
                        Value::Object(params) => params.get("followSubscription"),
                        _ => None,
                    })
                    .and_then(Value::as_str);
                Operation::Unfollow(name.and_then(|name| {
                    self.subscriptions
                        .iter()
                        .find(|sub| sub.name == name)
                        .map(|sub| sub.generation)
                }))
            }
            _ => Operation::Ordinary,
        };
        let rejection = if self.next_id.is_none() {
            Some(("Request ID space exhausted", None))
        } else if self.pending.len() == MAX_PENDING_REQUESTS as usize {
            Some((
                "Too many pending RPC requests",
                Some(("pending_requests", MAX_PENDING_REQUESTS)),
            ))
        } else if matches!(operation, Operation::Follow)
            && self.subscriptions.len()
                + self
                    .pending
                    .iter()
                    .filter(|p| matches!(p.operation, Operation::Follow))
                    .count()
                >= MAX_SUBSCRIPTIONS as usize
        {
            Some((
                "Too many RPC subscriptions",
                Some(("subscriptions", MAX_SUBSCRIPTIONS)),
            ))
        } else {
            None
        };
        if let Some((message, quota)) = rejection {
            return Err(request.as_object_mut().unwrap().remove("id").map(|id| {
                let mut error = rpc_error(id, -32000, message);
                if let Some((resource, limit)) = quota {
                    error["error"]["data"] =
                        serde_json::json!({"resource": resource, "limit": limit});
                }
                match numeric_id {
                    Some(id) => format!(
                        r#"{{"jsonrpc":"2.0","id":{},"error":{}}}"#,
                        id.get(),
                        error["error"],
                    ),
                    None => error.to_string(),
                }
            }));
        }

        let id = self.next_id.unwrap();
        self.next_id = id.checked_add(1);
        let original_id = request
            .as_object_mut()
            .unwrap()
            .insert("id".into(), id.into());
        let client_id = original_id.map(|original| match numeric_id {
            Some(id) => id.to_owned(),
            None => {
                serde_json::value::to_raw_value(&original).expect("validated request ID serializes")
            }
        });
        // Even notifications get a response upstream, so every slot can be retired.
        self.pending.push(Pending {
            id,
            client_id,
            operation,
        });
        Ok(request.to_string())
    }

    pub(super) fn response(&mut self, mut response: String) -> Result<Option<String>> {
        let envelope: Envelope<'_> =
            serde_json::from_str(&response).context("Invalid upstream JSON-RPC envelope")?;
        let Some(raw_id) = envelope.id else {
            ensure!(
                envelope.result.is_none() && envelope.error.is_none(),
                "Upstream response has no ID"
            );
            let method: JsonString<'_> = serde_json::from_str(
                envelope
                    .method
                    .context("Upstream notification has no method")?
                    .get(),
            )?;
            if method.0 == "chainHead_v1_followEvent" {
                let params: FollowParams<'_> = serde_json::from_str(
                    envelope
                        .params
                        .context("Follow event has no parameters")?
                        .get(),
                )?;
                if params.result.event.0 == "stop"
                    && let Some(index) = self
                        .subscriptions
                        .iter()
                        .position(|sub| sub.name == params.subscription.0)
                {
                    self.subscriptions.swap_remove(index);
                }
            }
            return Ok(Some(response));
        };
        ensure!(
            envelope.method.is_none(),
            "Upstream response also has a method"
        );
        ensure!(
            envelope.result.is_some() != envelope.error.is_some(),
            "Upstream response needs exactly one result or error"
        );
        let id: u64 = serde_json::from_str(raw_id.get()).context("Invalid upstream response ID")?;
        let index = self
            .pending
            .iter()
            .position(|pending| pending.id == id)
            .context("Unknown or duplicate upstream response ID")?;

        // Inspect only lifecycle metadata. Large results remain borrowed raw JSON.
        if let Some(result) = envelope.result {
            match self.pending[index].operation {
                Operation::Follow => {
                    let name: JsonString<'_> = serde_json::from_str(result.get())
                        .context("Invalid follow subscription ID")?;
                    ensure!(
                        !self.subscriptions.iter().any(|sub| sub.name == name.0),
                        "Duplicate active follow subscription ID"
                    );
                    self.subscriptions.push(Subscription {
                        generation: id,
                        name: name.0.into_owned(),
                    });
                }
                Operation::Unfollow(generation) => {
                    ensure!(result.get() == "null", "Invalid unfollow response");
                    // A delayed unfollow must not retire a later reuse of the same name.
                    if let Some(generation) = generation
                        && let Some(index) = self
                            .subscriptions
                            .iter()
                            .position(|sub| sub.generation == generation)
                    {
                        self.subscriptions.swap_remove(index);
                    }
                }
                Operation::Ordinary => {}
            }
        }
        let pending = self.pending.swap_remove(index);
        let Some(client_id) = pending.client_id else {
            return Ok(None);
        };
        // RawValue points into this String; byte offsets also work after Unicode.
        let start = raw_id.get().as_ptr() as usize - response.as_ptr() as usize;
        let end = start + raw_id.get().len();
        response.replace_range(start..end, client_id.get());
        Ok(Some(response))
    }
}

#[derive(Deserialize)]
struct NumericClientId<'a> {
    #[serde(borrow)]
    id: &'a RawValue,
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(default, borrow, deserialize_with = "present_raw")]
    id: Option<&'a RawValue>,
    #[serde(default, borrow, deserialize_with = "present_raw")]
    result: Option<&'a RawValue>,
    #[serde(default, borrow, deserialize_with = "present_raw")]
    error: Option<&'a RawValue>,
    #[serde(borrow)]
    method: Option<&'a RawValue>,
    #[serde(borrow)]
    params: Option<&'a RawValue>,
}

// Unlike Option's normal deserializer, retain a present JSON null.
fn present_raw<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(transparent)]
struct JsonString<'a>(#[serde(borrow)] Cow<'a, str>);

#[derive(Deserialize)]
struct FollowParams<'a> {
    #[serde(borrow)]
    subscription: JsonString<'a>,
    #[serde(borrow)]
    result: FollowEvent<'a>,
}

#[derive(Deserialize)]
struct FollowEvent<'a> {
    #[serde(borrow)]
    event: JsonString<'a>,
}

#[cfg(test)]
mod tests;
