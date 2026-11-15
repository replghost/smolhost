// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;
use futures_util::{SinkExt, StreamExt};
use smolhost::client::Connection;
use tokio::net::UnixListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

const BLOCK: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

#[tokio::test]
async fn interrupted_reads_never_publish_partial_storage_or_retry_the_operation() {
    for (event, expected) in [
        (
            json!({"event":"operationInaccessible", "operationId":"op-1"}),
            Some(Interruption::OperationInaccessible),
        ),
        (
            json!({"event":"operationError", "operationId":"op-1",
                "error":"operationInaccessible"}),
            None,
        ),
        (
            json!({"event":"operationInaccessible", "operationId":"wrong"}),
            None,
        ),
        (json!({"event":"stop"}), Some(Interruption::FollowStopped)),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rpc.sock");
        let listener = UnixListener::bind(&path).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            let (connection, mut peer) =
                tokio::join!(Connection::chain(&path, "test-chain"), async {
                    let (stream, _) = listener.accept().await.unwrap();
                    accept_async(stream).await.unwrap()
                },);
            let mut session = Session::new(RpcClient::new(connection.unwrap()));
            session.subscription = Some("s".to_owned());
            session.pinned.insert(BLOCK.to_owned());
            session.pending.push_back(BLOCK.to_owned());
            let (error, ()) = tokio::join!(
                async { session.storage(BLOCK, "0x01", "value").await.unwrap_err() },
                async {
                    let request: Value = serde_json::from_str(
                        &peer.next().await.unwrap().unwrap().into_text().unwrap(),
                    )
                    .unwrap();
                    assert_eq!(request["method"], "chainHead_v1_storage");
                    peer.send(Message::Text(
                        json!({"jsonrpc":"2.0", "id":request["id"], "result":{
                            "result":"started", "operationId":"op-1", "discardedItems":0
                        }})
                        .to_string()
                        .into(),
                    ))
                    .await
                    .unwrap();
                    for result in [
                        json!({"event":"operationStorageItems", "operationId":"op-1",
                            "items":[{"key":"0x01", "value":"0x01000000"}]}),
                        event,
                    ] {
                        peer.send(Message::Text(
                            json!({"jsonrpc":"2.0", "method":"chainHead_v1_followEvent",
                                "params":{"subscription":"s", "result":result}})
                            .to_string()
                            .into(),
                        ))
                        .await
                        .unwrap();
                    }
                },
            );
            assert_eq!(error.downcast_ref::<Interruption>().copied(), expected);
            if expected == Some(Interruption::FollowStopped) {
                assert!(session.subscription.is_none());
                assert!(session.operation.is_none());
                assert!(session.pinned.is_empty());
                assert!(session.pending.is_empty());
            } else if expected == Some(Interruption::OperationInaccessible) {
                assert!(session.operation.is_none());
                assert!(session.pinned.contains(BLOCK));
            }
        })
        .await
        .expect("interrupted operation did not terminate");
    }
}
