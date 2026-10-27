#![cfg(feature = "consensus")]
use openraft::BasicNode;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use substrate_liquid::{
    consensus::{AuthenticatedPeerTransport, ConsensusNode, ConsensusOptions},
    paged::{PagedDatabase, PagedOptions, PagedReplica},
    *,
};

#[derive(Default)]
struct Router {
    nodes: Mutex<BTreeMap<u64, Peer>>,
    blocked: Mutex<BTreeSet<(u64, u64)>>,
}
const ROUTER_REQUESTS: u32 = 64;
struct Peer {
    node: Weak<ConsensusNode>,
    active: Arc<tokio::sync::Semaphore>,
}
struct ActivePeer {
    // Field drop order releases Node even when a transport future is cancelled.
    node: Arc<ConsensusNode>,
    _active: tokio::sync::OwnedSemaphorePermit,
}
struct Endpoint {
    id: u64,
    router: Arc<Router>,
}
#[async_trait::async_trait]
impl AuthenticatedPeerTransport for Endpoint {
    async fn request(&self, target: u64, bytes: Vec<u8>) -> std::result::Result<Vec<u8>, String> {
        if self
            .router
            .blocked
            .lock()
            .unwrap()
            .contains(&(self.id, target))
        {
            return Err("partition".into());
        }
        let request = {
            let peers = self.router.nodes.lock().unwrap();
            let peer = peers.get(&target).ok_or("peer offline")?;
            let active = peer
                .active
                .clone()
                .try_acquire_owned()
                .map_err(|_| "peer busy")?;
            let node = peer.node.upgrade().ok_or("peer offline")?;
            ActivePeer {
                node,
                _active: active,
            }
        };
        let response = request
            .node
            .handle_rpc(self.id, &bytes)
            .await
            .map_err(|e| e.to_string());
        drop(request);
        response
    }
}
impl Router {
    fn isolate(&self, id: u64, isolated: bool) {
        let mut blocked = self.blocked.lock().unwrap();
        for other in 1..=3 {
            if other != id {
                for pair in [(id, other), (other, id)] {
                    if isolated {
                        blocked.insert(pair);
                    } else {
                        blocked.remove(&pair);
                    }
                }
            }
        }
    }
}
fn options() -> PagedOptions {
    PagedOptions {
        cache_bytes: 1024 * 1024,
        decoded_cache_bytes: 0,
        ..Default::default()
    }
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}
struct Cluster {
    directory: std::path::PathBuf,
    _temporary: Option<tempfile::TempDir>,
    router: Arc<Router>,
    nodes: BTreeMap<u64, Arc<ConsensusNode>>,
    opts: ConsensusOptions,
}
impl Cluster {
    async fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        Self::at(temporary.path().to_path_buf(), Some(temporary)).await
    }
    async fn at(directory: std::path::PathBuf, temporary: Option<tempfile::TempDir>) -> Self {
        Self::at_with_options(
            directory,
            temporary,
            ConsensusOptions {
                heartbeat_ms: 50,
                election_timeout_min_ms: 150,
                election_timeout_max_ms: 300,
                request_timeout: Duration::from_millis(700),
                ..Default::default()
            },
        )
        .await
    }
    async fn at_with_options(
        directory: std::path::PathBuf,
        temporary: Option<tempfile::TempDir>,
        opts: ConsensusOptions,
    ) -> Self {
        let source = directory.join("source");
        {
            let db = PagedDatabase::open(&source, options()).unwrap();
            let mut tx = db.admin().transaction().unwrap();
            tx.install(Module::new(
                "app",
                1,
                vec![],
                vec![
                    Relation::new("app.Item", vec![Role::new("name", DataType::Text)])
                        .unique(&["name"]),
                ],
            ))
            .unwrap();
            tx.grant("writer", "app.Item", Grant::read_write()).unwrap();
            tx.commit().unwrap();
            db.enable_replication().unwrap();
        }
        for id in 1..=3 {
            PagedReplica::create_from_file(
                &source,
                directory.join(format!("state-{id}")),
                options(),
            )
            .unwrap();
        }
        let mut cluster = Self {
            directory,
            _temporary: temporary,
            router: Arc::default(),
            nodes: BTreeMap::new(),
            opts,
        };
        for id in 1..=3 {
            cluster.start(id).await;
        }
        cluster.nodes[&1].initialize().await.unwrap();
        cluster.leader(None).await;
        cluster
    }
    async fn start(&mut self, id: u64) {
        let replica =
            PagedReplica::open(self.directory.join(format!("state-{id}")), options()).unwrap();
        let peers = (1..=3)
            .map(|id| (id, BasicNode::new(format!("test-peer-{id}"))))
            .collect();
        let node = Arc::new(
            ConsensusNode::open(
                id,
                replica,
                self.directory.join(format!("log-{id}")),
                peers,
                Arc::new(Endpoint {
                    id,
                    router: self.router.clone(),
                }),
                self.opts,
            )
            .await
            .unwrap(),
        );
        self.router.nodes.lock().unwrap().insert(
            id,
            Peer {
                node: Arc::downgrade(&node),
                active: Arc::new(tokio::sync::Semaphore::new(ROUTER_REQUESTS as usize)),
            },
        );
        self.nodes.insert(id, node);
    }
    async fn stop(&mut self, id: u64) {
        let peer = self.router.nodes.lock().unwrap().remove(&id).unwrap();
        let node = self.nodes.remove(&id).unwrap();
        node.shutdown().await.unwrap();
        // Removing the route prevents new references; drain already accepted
        // RPCs so their Arc<Node> drops before opening the same database file.
        let _drained = peer.active.acquire_many(ROUTER_REQUESTS).await.unwrap();
        drop(node);
    }
    async fn leader(&self, excluded: Option<u64>) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            for (&id, node) in &self.nodes {
                if Some(id) != excluded
                    && node.current_leader() == Some(id)
                    && node.read("writer", |_| Ok(())).await.is_ok()
                {
                    return id;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no majority leader elected"
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }
    async fn await_applied(&self, leader: u64) {
        let applied = self.nodes[&leader].last_applied_log_index().unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while self.nodes.values().any(|node| {
            node.last_applied_log_index()
                .is_none_or(|index| index < applied)
        }) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "followers did not apply the acknowledged backlog"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    async fn close(mut self) {
        for id in self.nodes.keys().copied().collect::<Vec<_>>() {
            self.stop(id).await;
        }
    }
}
#[test]
fn quorum_partition_stale_leader_rejoin_and_durable_restart() {
    runtime().block_on(async {
        let mut cluster = Cluster::new().await;
        let old = cluster.leader(None).await;
        let (first, _) = cluster.nodes[&old]
            .transact("writer", |tx| {
                tx.insert("app.Item", vec!["acknowledged".into()])
            })
            .await
            .unwrap();
        cluster.router.isolate(old, true);
        let leader = cluster.leader(Some(old)).await;
        assert!(
            cluster.nodes[&old]
                .read("writer", |_| Ok(()))
                .await
                .is_err()
        );
        assert!(
            cluster.nodes[&old]
                .transact("writer", |tx| tx
                    .insert("app.Item", vec!["minority".into()]))
                .await
                .is_err()
        );
        assert!(
            cluster.nodes[&leader]
                .read("writer", |read| read.get("app.Item", first))
                .await
                .unwrap()
                .is_some()
        );
        let (second, _) = cluster.nodes[&leader]
            .transact("writer", |tx| {
                tx.insert("app.Item", vec!["after-failover".into()])
            })
            .await
            .unwrap();
        cluster.router.isolate(old, false);
        cluster.await_applied(leader).await;
        // All disks are closed/reopened, including votes, membership and applied markers.
        for id in 1..=3 {
            cluster.stop(id).await;
        }
        for id in 1..=3 {
            cluster.start(id).await;
        }
        let leader = cluster.leader(None).await;
        cluster.nodes[&leader]
            .read("writer", |snapshot| {
                assert!(snapshot.get("app.Item", first)?.is_some());
                assert!(snapshot.get("app.Item", second)?.is_some());
                assert!(
                    snapshot
                        .lookup_page("app.Item", "name", &"minority".into(), None, 8)?
                        .rows
                        .is_empty()
                );
                Ok(())
            })
            .await
            .unwrap();
        cluster.close().await;
    });
}
#[test]
fn majority_loss_never_acknowledges_and_peer_auth_fails_closed() {
    runtime().block_on(async {
        let cluster = Cluster::new().await;
        let leader = cluster.leader(None).await;
        assert!(matches!(
            cluster.nodes[&leader].handle_rpc(99, b"{}").await,
            Err(Error::Denied)
        ));
        for id in 1..=3 {
            cluster.router.isolate(id, true);
        }
        assert!(
            cluster.nodes[&leader]
                .transact("writer", |tx| tx
                    .insert("app.Item", vec!["no-quorum".into()]))
                .await
                .is_err()
        );
        for id in 1..=3 {
            cluster.router.isolate(id, false);
        }
        let leader = cluster.leader(None).await;
        assert!(
            cluster.nodes[&leader]
                .read("writer", |read| read.lookup_page(
                    "app.Item",
                    "name",
                    &"no-quorum".into(),
                    None,
                    8
                ))
                .await
                .unwrap()
                .rows
                .is_empty()
        );
        cluster.close().await;
    });
}
#[test]
fn committed_revocation_is_preserved_after_election() {
    runtime().block_on(async {
        let mut cluster = Cluster::new().await;
        let leader = cluster.leader(None).await;
        cluster.nodes[&leader]
            .transact_admin(|tx| tx.revoke("writer", "app.Item"))
            .await
            .unwrap();
        cluster.stop(leader).await;
        let next = cluster.leader(Some(leader)).await;
        assert!(matches!(
            cluster.nodes[&next]
                .transact("writer", |tx| tx.insert("app.Item", vec!["denied".into()]))
                .await,
            Err(Error::Denied)
        ));
        cluster.close().await;
    });
}

#[test]
fn consensus_crash_writer() {
    let Some(directory) = std::env::var_os("SUBSTRATE_CONSENSUS_CRASH_DIR") else {
        return;
    };
    runtime().block_on(async {
        let cluster = Cluster::at(directory.into(), None).await;
        let leader = cluster.leader(None).await;
        cluster.nodes[&leader]
            .transact("writer", |tx| {
                tx.insert_with_id(
                    "app.Item",
                    Id::from_u128(4242),
                    vec!["durably-acknowledged".into()],
                )
            })
            .await
            .unwrap();
        // Skip every database/runtime destructor immediately after durable acknowledgement.
        std::process::exit(86);
    });
}
#[test]
fn acknowledged_quorum_write_survives_abrupt_process_exit() {
    let temporary = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "consensus_crash_writer", "--nocapture"])
        .env("SUBSTRATE_CONSENSUS_CRASH_DIR", temporary.path())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(86));
    runtime().block_on(async {
        let mut cluster = Cluster {
            directory: temporary.path().to_path_buf(),
            _temporary: None,
            router: Arc::default(),
            nodes: BTreeMap::new(),
            opts: ConsensusOptions::default(),
        };
        for id in 1..=3 {
            cluster.start(id).await;
        }
        let leader = cluster.leader(None).await;
        assert!(
            cluster.nodes[&leader]
                .read("writer", |read| read.get("app.Item", Id::from_u128(4242)))
                .await
                .unwrap()
                .is_some()
        );
        cluster.close().await;
    });
}
#[test]
fn final_read_barrier_rejects_a_mid_read_partition() {
    runtime().block_on(async {
        let cluster = Cluster::new().await;
        let leader = cluster.leader(None).await;
        let result = cluster.nodes[&leader]
            .read("writer", |read| {
                let rows = read.scan_page("app.Item", None, 8)?;
                cluster.router.isolate(leader, true);
                Ok(rows)
            })
            .await;
        assert!(result.is_err());
        cluster.router.isolate(leader, false);
        cluster.close().await;
    });
}

#[test]
fn mismatched_bootstrap_cursor_is_rejected_before_voting() {
    runtime().block_on(async {
    let cluster = Cluster::new().await;
    let source = PagedDatabase::open(cluster.directory.join("source"), options()).unwrap();
    let mut genesis = source.replication_cursor().unwrap(); genesis.revision += 1;
    let request = serde_json::json!({"cluster":genesis,"request":{"Vote":{"vote":openraft::Vote::new(100, 2),"last_log_id":null}}});
    assert!(matches!(cluster.nodes[&1].handle_rpc(2, &serde_json::to_vec(&request).unwrap()).await, Err(Error::Denied)));
    drop(source); cluster.close().await;
});
}
#[test]
fn configured_log_capacity_fails_closed_without_unbounded_growth() {
    runtime().block_on(async {
        let temporary = tempfile::tempdir().unwrap();
        let opts = ConsensusOptions {
            max_log_bytes: 1024 * 1024,
            request_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let cluster = Cluster::at_with_options(temporary.path().to_path_buf(), None, opts).await;
        let leader = cluster.leader(None).await;
        let mut acknowledged = 0;
        for i in 0..8 {
            let text = format!("{i}{}", "x".repeat(64 * 1024));
            match cluster.nodes[&leader]
                .transact("writer", |tx| {
                    tx.insert("app.Item", vec![Value::Text(text)])
                })
                .await
            {
                Ok(_) => acknowledged += 1,
                Err(_) => break,
            }
        }
        assert!(
            acknowledged > 0 && acknowledged < 8,
            "hard log budget must stop writes"
        );
        cluster.close().await;
    });
}

#[test]
fn large_committed_backlog_replays_in_bounded_recovery_chunks() {
    runtime().block_on(async {
        let temporary = tempfile::tempdir().unwrap();
        let opts = ConsensusOptions {
            election_timeout_min_ms: 1000,
            election_timeout_max_ms: 2000,
            ..Default::default()
        };
        let mut cluster =
            Cluster::at_with_options(temporary.path().to_path_buf(), None, opts).await;
        let leader = cluster.leader(None).await;
        for i in 0..40 {
            let value = format!("{i:02}{}", "z".repeat(250 * 1024));
            cluster.nodes[&leader]
                .transact("writer", |tx| {
                    tx.insert("app.Item", vec![Value::Text(value)])
                })
                .await
                .unwrap();
        }
        // Wait until every follower has the committed log, then rebuild node 2's
        // application state from the retained genesis plus its durable Raft log.
        cluster.await_applied(leader).await;
        for id in 1..=3 {
            cluster.stop(id).await;
        }
        let state = cluster.directory.join("state-2");
        std::fs::remove_file(&state).unwrap();
        PagedReplica::create_from_file(cluster.directory.join("source"), &state, options())
            .unwrap();
        for id in 1..=3 {
            cluster.start(id).await;
        }
        let leader = cluster.leader(None).await;
        assert_eq!(
            cluster.nodes[&leader]
                .read("writer", |read| {
                    let mut cursor = None;
                    let mut count = 0;
                    loop {
                        let page = read.scan_page("app.Item", cursor, 64)?;
                        count += page.rows.len();
                        cursor = page.next;
                        if cursor.is_none() {
                            break;
                        }
                    }
                    Ok(count)
                })
                .await
                .unwrap(),
            40
        );
        let path = cluster.directory.clone();
        for id in 1..=3 {
            cluster.stop(id).await;
        }
        let restored = PagedReplica::open(path.join("state-2"), options()).unwrap();
        let snapshot = restored.snapshot_eventual("writer").unwrap();
        let mut cursor = None;
        let mut count = 0;
        loop {
            let page = snapshot.scan_page("app.Item", cursor, 64).unwrap();
            count += page.rows.len();
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(count, 40);
        drop(snapshot);
        drop(restored);
        cluster.close().await;
    });
}
