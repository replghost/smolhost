// SPDX-License-Identifier: GPL-3.0-or-later

use serde_json::json;

use super::*;

fn request(id: Value) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "method":"chainSpec_v1_chainName", "params":[]})
}

fn follow(id: Value) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "method":"chainHead_v1_follow", "params":[false]})
}

fn unfollow(params: Value) -> Value {
    json!({"jsonrpc":"2.0", "id":"unfollow", "method":"chainHead_v1_unfollow", "params":params})
}

fn admit(budget: &mut Budget, request: Value) -> Result<String, Option<String>> {
    let wire = request.to_string();
    budget.admit(request, &wire)
}

fn admitted(budget: &mut Budget, request: Value) -> u64 {
    let original = request.clone();
    let forwarded: Value = serde_json::from_str(&admit(budget, request).unwrap()).unwrap();
    assert_eq!(forwarded["method"], original["method"]);
    assert_eq!(forwarded["params"], original["params"]);
    forwarded["id"].as_u64().unwrap()
}

fn reply(budget: &mut Budget, id: u64, result: Value) -> Option<Value> {
    budget
        .response(json!({"jsonrpc":"2.0", "id":id, "result":result}).to_string())
        .unwrap()
        .map(|text| serde_json::from_str(&text).unwrap())
}

fn error(budget: &mut Budget, id: u64) -> Option<Value> {
    budget.response(json!({"jsonrpc":"2.0", "id":id, "error":{"code":-32602,"message":"upstream rejection"}}).to_string())
        .unwrap().map(|text| serde_json::from_str(&text).unwrap())
}

fn rejected(budget: &mut Budget, request: Value) -> Value {
    let id = request["id"].clone();
    let error: Value = serde_json::from_str(&admit(budget, request).unwrap_err().unwrap()).unwrap();
    assert_eq!(error["id"], id);
    assert_eq!(error["error"]["code"], -32000);
    error
}

fn event(budget: &mut Budget, method: &str, subscription: &str, event: &str) {
    let message = json!({"jsonrpc":"2.0", "method":method,
        "params":{"subscription":subscription,"result":{"event":event}}})
    .to_string();
    assert_eq!(budget.response(message.clone()).unwrap(), Some(message));
}

fn full_subscriptions(budget: &mut Budget) {
    for i in 0..MAX_SUBSCRIPTIONS {
        let id = admitted(budget, follow(json!(i)));
        assert_eq!(
            reply(budget, id, json!(format!("sub-{i}"))).unwrap()["id"],
            i
        );
    }
    let rejection = rejected(budget, follow(json!("full")));
    assert_eq!(
        rejection["error"]["data"],
        json!({"resource":"subscriptions","limit":MAX_SUBSCRIPTIONS})
    );
}

#[test]
fn pending_overflow_retires_one_slot_without_reusing_ids_or_growing_storage() {
    let mut budget = Budget::new();
    let pending_capacity = budget.pending.capacity();
    let subscription_capacity = budget.subscriptions.capacity();
    let mut ids = Vec::new();
    for i in 0..MAX_PENDING_REQUESTS {
        ids.push(admitted(&mut budget, request(json!(i))));
    }
    for _ in 0..100 {
        let rejection = rejected(&mut budget, request(json!("overflow")));
        assert_eq!(
            rejection["error"]["data"],
            json!({"resource":"pending_requests","limit":MAX_PENDING_REQUESTS})
        );
    }
    let retired = ids.remove(7);
    assert_eq!(reply(&mut budget, retired, json!("done")).unwrap()["id"], 7);
    let replacement = admitted(&mut budget, request(json!("replacement")));
    assert!(replacement > *ids.last().unwrap());
    rejected(&mut budget, request(Value::Null));
    assert!(
        budget
            .response(json!({"id":retired,"result":null}).to_string())
            .is_err()
    );
    rejected(
        &mut budget,
        request(json!("duplicate did not free capacity")),
    );
    assert!(
        budget
            .response(json!({"id":u64::MAX,"result":null}).to_string())
            .is_err()
    );
    rejected(&mut budget, request(json!("unknown did not free capacity")));
    for id in ids {
        reply(&mut budget, id, Value::Null);
    }
    reply(&mut budget, replacement, Value::Null);
    for i in 0..100 {
        let id = admitted(&mut budget, request(json!(i)));
        assert!(id > replacement);
        reply(&mut budget, id, Value::Null);
    }
    assert_eq!(budget.pending.capacity(), pending_capacity);
    assert_eq!(budget.subscriptions.capacity(), subscription_capacity);
}

#[test]
fn duplicate_null_numeric_and_string_client_ids_survive_out_of_order_replies() {
    let mut budget = Budget::new();
    let client_ids = [
        json!(7),
        json!(7),
        Value::Null,
        json!("7"),
        json!("雪\"\\"),
        Value::Null,
    ];
    let ids: Vec<_> = client_ids
        .iter()
        .map(|id| admitted(&mut budget, request(id.clone())))
        .collect();
    for pair in ids.windows(2) {
        assert!(pair[1] > pair[0]);
    }
    for index in [5, 1, 4, 0, 3, 2] {
        let response = if index % 2 == 0 {
            reply(&mut budget, ids[index], json!(index)).unwrap()
        } else {
            error(&mut budget, ids[index]).unwrap()
        };
        assert_eq!(response["id"], client_ids[index]);
        if index % 2 == 0 {
            assert_eq!(response["result"], index);
        } else {
            assert_eq!(response["error"]["code"], -32602);
        }
    }
}

#[test]
fn notifications_have_internal_ids_and_suppressed_replies_but_consume_capacity() {
    let mut budget = Budget::new();
    let notification = json!({"jsonrpc":"2.0","method":"chainSpec_v1_chainName","params":[]});
    let ids: Vec<_> = (0..MAX_PENDING_REQUESTS)
        .map(|_| admitted(&mut budget, notification.clone()))
        .collect();
    assert_eq!(admit(&mut budget, notification.clone()), Err(None));
    rejected(&mut budget, request(json!("full")));
    assert_eq!(reply(&mut budget, ids[0], json!("invisible")), None);
    let request_id = admitted(&mut budget, request(Value::Null));
    assert_eq!(admit(&mut budget, notification.clone()), Err(None));
    assert_eq!(error(&mut budget, ids[1]), None);
    let notification_id = admitted(&mut budget, notification);
    assert!(notification_id > request_id);
    assert_eq!(
        reply(&mut budget, request_id, Value::Null).unwrap()["id"],
        Value::Null
    );
}

#[test]
fn exhausted_counter_fails_closed_after_last_id_is_retired() {
    let mut budget = Budget::new();
    budget.next_id = Some(u64::MAX);
    let id = admitted(&mut budget, request(json!("last")));
    assert_eq!(id, u64::MAX);
    let rejection = rejected(&mut budget, request(json!("exhausted")));
    assert!(rejection["error"].get("data").is_none());
    reply(&mut budget, id, Value::Null);
    rejected(&mut budget, request(Value::Null));
    assert_eq!(
        admit(&mut budget, json!({"jsonrpc":"2.0","method":"rpc_methods"})),
        Err(None)
    );
}

#[test]
fn pending_and_active_follows_share_the_subscription_budget() {
    let mut budget = Budget::new();
    let ids: Vec<_> = (0..MAX_SUBSCRIPTIONS)
        .map(|i| admitted(&mut budget, follow(json!(i))))
        .collect();
    let rejection = rejected(&mut budget, follow(json!("pending full")));
    assert_eq!(
        rejection["error"]["data"],
        json!({"resource":"subscriptions","limit":MAX_SUBSCRIPTIONS})
    );
    reply(&mut budget, ids[3], json!("active"));
    let rejection = rejected(&mut budget, follow(json!("pending plus active full")));
    assert_eq!(
        rejection["error"]["data"],
        json!({"resource":"subscriptions","limit":MAX_SUBSCRIPTIONS})
    );
    let ordinary = admitted(&mut budget, request(json!("ordinary still allowed")));
    reply(&mut budget, ordinary, Value::Null);
    let failure = error(&mut budget, ids[1]).unwrap();
    assert_eq!(failure["id"], 1);
    assert_eq!(failure["error"]["code"], -32602);
    admitted(&mut budget, follow(json!("released failed reservation")));
    rejected(&mut budget, follow(json!("full again")));
}

#[test]
fn notification_follows_reserve_and_retire_like_requests() {
    let mut budget = Budget::new();
    let mut notification = follow(Value::Null);
    notification.as_object_mut().unwrap().remove("id");
    let ids: Vec<_> = (0..MAX_SUBSCRIPTIONS)
        .map(|_| admitted(&mut budget, notification.clone()))
        .collect();
    assert_eq!(admit(&mut budget, notification.clone()), Err(None));
    assert_eq!(reply(&mut budget, ids[0], json!("hidden-follow")), None);
    assert_eq!(admit(&mut budget, notification.clone()), Err(None));
    assert_eq!(error(&mut budget, ids[1]), None);
    let retry = admitted(&mut budget, notification.clone());
    assert_eq!(reply(&mut budget, retry, json!("another")), None);
    event(
        &mut budget,
        "chainHead_v1_followEvent",
        "hidden-follow",
        "stop",
    );
    admitted(&mut budget, notification);
}

#[test]
fn successful_positional_and_named_unfollows_release_only_after_reply() {
    for params in [json!(["sub-0"]), json!({"followSubscription":"sub-0"})] {
        let mut budget = Budget::new();
        full_subscriptions(&mut budget);
        let id = admitted(&mut budget, unfollow(params));
        rejected(&mut budget, follow(json!("unfollow is still pending")));
        assert_eq!(
            reply(&mut budget, id, Value::Null).unwrap()["id"],
            "unfollow"
        );
        admitted(&mut budget, follow(json!("one slot released")));
        rejected(&mut budget, follow(json!("only one slot released")));
    }
}

#[test]
fn failed_and_unknown_unfollows_do_not_release_subscriptions() {
    let mut budget = Budget::new();
    full_subscriptions(&mut budget);
    let id = admitted(&mut budget, unfollow(json!(["sub-0"])));
    assert!(error(&mut budget, id).unwrap().get("error").is_some());
    rejected(&mut budget, follow(json!("failed unfollow")));
    for params in [
        json!(["unknown"]),
        json!({"subscription":"sub-0"}),
        json!([]),
    ] {
        let id = admitted(&mut budget, unfollow(params));
        reply(&mut budget, id, Value::Null);
        rejected(&mut budget, follow(json!("unknown unfollow")));
    }
}

#[test]
fn malformed_unfollow_success_does_not_release_a_subscription() {
    let mut budget = Budget::new();
    full_subscriptions(&mut budget);
    let id = admitted(&mut budget, unfollow(json!(["sub-0"])));
    for result in [json!(false), json!(true), json!("null"), json!({})] {
        assert!(
            budget
                .response(json!({"jsonrpc":"2.0", "id":id, "result":result}).to_string())
                .is_err()
        );
        rejected(&mut budget, follow(json!("subscription remains active")));
    }
    reply(&mut budget, id, Value::Null);
    admitted(
        &mut budget,
        follow(json!("acknowledged unfollow releases capacity")),
    );
}

#[test]
fn stop_releases_only_matching_follow_subscription_once() {
    let mut budget = Budget::new();
    full_subscriptions(&mut budget);
    for (method, name, kind) in [
        ("another_subscription", "sub-0", "stop"),
        ("chainHead_v1_followEvent", "unknown", "stop"),
        ("chainHead_v1_followEvent", "sub-0", "initialized"),
        ("chainHead_v1_followEvent", "sub-0", "operationStorageDone"),
    ] {
        event(&mut budget, method, name, kind);
        rejected(&mut budget, follow(json!("not stopped")));
    }
    event(&mut budget, "chainHead_v1_followEvent", "sub-0", "stop");
    admitted(&mut budget, follow(json!("replacement")));
    event(&mut budget, "chainHead_v1_followEvent", "sub-0", "stop");
    rejected(&mut budget, follow(json!("duplicate stop")));
}

#[test]
fn delayed_unfollow_cannot_release_a_reused_subscription_name() {
    let mut budget = Budget::new();
    full_subscriptions(&mut budget);
    let old_unfollow = admitted(&mut budget, unfollow(json!(["sub-0"])));
    event(&mut budget, "chainHead_v1_followEvent", "sub-0", "stop");
    let replacement = admitted(&mut budget, follow(json!("new incarnation")));
    reply(&mut budget, replacement, json!("sub-0"));
    reply(&mut budget, old_unfollow, Value::Null);
    rejected(
        &mut budget,
        follow(json!("new incarnation is still active")),
    );
    let new_unfollow = admitted(&mut budget, unfollow(json!({"followSubscription":"sub-0"})));
    reply(&mut budget, new_unfollow, Value::Null);
    admitted(&mut budget, follow(json!("new incarnation removed")));
}

#[test]
fn unfollow_of_unknown_name_cannot_release_a_later_subscription() {
    let mut budget = Budget::new();
    let old_unfollow = admitted(&mut budget, unfollow(json!(["sub-0"])));
    full_subscriptions(&mut budget);
    reply(&mut budget, old_unfollow, Value::Null);
    rejected(
        &mut budget,
        follow(json!("unknown name was not an incarnation")),
    );
}

#[test]
fn response_rewrites_only_id_bytes_in_large_unicode_payload() {
    let mut budget = Budget::new();
    let client_id = json!("client 雪\"\\");
    let id = admitted(&mut budget, request(client_id.clone()));
    let prefix = format!(
        "{{ \"jsonrpc\":\"2.0\", \"result\":{{\"雪\": [1.2300, 1e+03, \"\\u0061\", \"{}\"]}}, \"id\" : ",
        "λ".repeat(128 * 1024)
    );
    let suffix = ", \"extra\" : {\"id\": 17} }\n";
    let upstream = format!("{prefix}{id}{suffix}");
    let expected = format!("{prefix}{client_id}{suffix}");
    assert_eq!(budget.response(upstream).unwrap().unwrap(), expected);
}

#[test]
fn error_payload_and_nested_id_are_preserved_verbatim() {
    let mut budget = Budget::new();
    let id = admitted(&mut budget, request(Value::Null));
    let prefix =
        "{\"error\": {\"code\":-32000,\"message\":\"雪\",\"data\":{\"id\":5,\"n\":1.00}},\"id\":";
    assert_eq!(
        budget.response(format!("{prefix}{id}}}")).unwrap().unwrap(),
        format!("{prefix}null}}")
    );
}

#[test]
fn escaped_lifecycle_strings_are_decoded_without_rewriting_notifications() {
    let mut budget = Budget::new();
    let id = admitted(&mut budget, follow(json!("escaped")));
    budget
        .response(format!(r#"{{"id":{id},"result":"sub-\u0030"}}"#))
        .unwrap();
    for i in 1..MAX_SUBSCRIPTIONS {
        let id = admitted(&mut budget, follow(json!(i)));
        reply(&mut budget, id, json!(format!("sub-{i}")));
    }
    rejected(&mut budget, follow(json!("full")));
    let notification = r#"{"jsonrpc":"2.0","method":"chainHead_v1_follow\u0045vent","params":{"subscription":"sub-\u0030","result":{"event":"st\u006fp"}}}"#.to_string();
    assert_eq!(
        budget.response(notification.clone()).unwrap(),
        Some(notification)
    );
    admitted(&mut budget, follow(json!("decoded stop released slot")));
}

#[test]
fn invalid_response_envelopes_never_retire_pending_requests() {
    let mut budget = Budget::new();
    let ids: Vec<_> = (0..MAX_PENDING_REQUESTS)
        .map(|i| admitted(&mut budget, request(json!(i))))
        .collect();
    for malformed in [
        format!(r#"{{"id":"{}","result":null}}"#, ids[0]),
        "{\"id\":null,\"result\":null}".into(),
        format!(r#"{{"id":{0},"id":{0},"result":null}}"#, ids[0]),
        format!(r#"{{"id":{},"result":null,"error":{{}}}}"#, ids[0]),
        format!(r#"{{"id":{}}}"#, ids[0]),
        "{\"result\":null}".into(),
    ] {
        assert!(budget.response(malformed).is_err());
        rejected(&mut budget, request(json!("still full")));
    }
    reply(&mut budget, ids[0], Value::Null);
    admitted(&mut budget, request(json!("valid response releases slot")));
}

#[test]
fn duplicate_active_subscription_id_is_fatal_without_losing_reservation() {
    let mut budget = Budget::new();
    full_subscriptions(&mut budget);
    event(&mut budget, "chainHead_v1_followEvent", "sub-0", "stop");
    let id = admitted(&mut budget, follow(json!("duplicate")));
    assert!(
        budget
            .response(json!({"id":id,"result":"sub-1"}).to_string())
            .is_err()
    );
    rejected(&mut budget, follow(json!("reservation remains")));
}

#[test]
fn notification_unfollow_suppresses_reply_without_skipping_lifecycle_updates() {
    let mut budget = Budget::new();
    full_subscriptions(&mut budget);
    let mut notification = unfollow(json!({"followSubscription":"sub-0"}));
    notification.as_object_mut().unwrap().remove("id");
    let failed = admitted(&mut budget, notification.clone());
    assert_eq!(error(&mut budget, failed), None);
    rejected(&mut budget, follow(json!("failed notification unfollow")));
    let succeeded = admitted(&mut budget, notification);
    assert_eq!(reply(&mut budget, succeeded, Value::Null), None);
    admitted(
        &mut budget,
        follow(json!("successful notification unfollow")),
    );
    rejected(&mut budget, follow(json!("released exactly one slot")));
}

#[test]
fn numeric_client_ids_retain_precision_beyond_native_numbers() {
    for client_id in [
        "18446744073709551617",
        "-18446744073709551617",
        "1.234567890123456789",
    ] {
        let mut budget = Budget::new();
        let wire = format!(
            r#"{{"jsonrpc":"2.0","id":{client_id},"method":"chainSpec_v1_chainName","params":[]}}"#
        );
        let parsed: Value = serde_json::from_str(&wire).unwrap();
        let forwarded: Value = serde_json::from_str(&budget.admit(parsed, &wire).unwrap()).unwrap();
        let response = budget
            .response(json!({"jsonrpc":"2.0","id":forwarded["id"],"result":"chain"}).to_string())
            .unwrap()
            .unwrap();
        let envelope: Envelope<'_> = serde_json::from_str(&response).unwrap();
        assert_eq!(envelope.id.unwrap().get(), client_id);
        for index in 0..MAX_PENDING_REQUESTS {
            admitted(&mut budget, request(json!(index)));
        }
        let parsed: Value = serde_json::from_str(&wire).unwrap();
        let rejection = budget.admit(parsed, &wire).unwrap_err().unwrap();
        let envelope: Envelope<'_> = serde_json::from_str(&rejection).unwrap();
        assert_eq!(envelope.id.unwrap().get(), client_id);
        let error: Value = serde_json::from_str(envelope.error.unwrap().get()).unwrap();
        assert_eq!(error["data"]["resource"], "pending_requests");
    }
}
