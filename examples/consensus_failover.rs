//! cargo run --features consensus --example consensus_failover
//! Identity-bound in-process transport for a local demonstration, not a remote server.
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
struct Mesh {
    nodes: Mutex<BTreeMap<u64, Weak<ConsensusNode>>>,
    isolated: Mutex<BTreeSet<u64>>,
}
struct Peer {
    id: u64,
    mesh: Arc<Mesh>,
}
#[async_trait::async_trait]
impl AuthenticatedPeerTransport for Peer {
    async fn request(&self, target: u64, bytes: Vec<u8>) -> std::result::Result<Vec<u8>, String> {
        {
            let isolated = self.mesh.isolated.lock().map_err(|e| e.to_string())?;
            if isolated.contains(&self.id) || isolated.contains(&target) {
                return Err("simulated network partition".into());
            }
        }
        let node = self
            .mesh
            .nodes
            .lock()
            .map_err(|e| e.to_string())?
            .get(&target)
            .and_then(Weak::upgrade)
            .ok_or("peer unavailable")?;
        node.handle_rpc(self.id, &bytes)
            .await
            .map_err(|e| e.to_string())
    }
}
async fn leader(nodes: &BTreeMap<u64, Arc<ConsensusNode>>, excluded: Option<u64>) -> Result<u64> {
    let until = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        for (&id, node) in nodes {
            if Some(id) != excluded
                && node.current_leader() == Some(id)
                && node.read("app", |_| Ok(())).await.is_ok()
            {
                return Ok(id);
            }
        }
        if tokio::time::Instant::now() >= until {
            return Err(Error::Storage("no majority leader elected".into()));
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?; let source = directory.path().join("genesis.redb");
        let options = PagedOptions { cache_bytes: 1024*1024, ..Default::default() };
        { let db = PagedDatabase::open(&source, options)?; let mut tx = db.admin().transaction()?;
          tx.install(Module::new("shared", 1, vec![], vec![Relation::new("shared.Person", vec![Role::new("name", DataType::Text)])]))?;
          tx.grant("app", "shared.Person", Grant::read_write())?; tx.commit()?; db.enable_replication()?; }
        let mesh = Arc::<Mesh>::default(); let mut nodes = BTreeMap::new();
        let peers: BTreeMap<_, _> = (1..=3).map(|id| (id, BasicNode::new(format!("local-peer-{id}")))).collect();
        for id in 1..=3 {
            let replica = PagedReplica::create_from_file(&source, directory.path().join(format!("state-{id}.redb")), options)?;
            let node = Arc::new(ConsensusNode::open(id, replica, directory.path().join(format!("raft-{id}.redb")), peers.clone(), Arc::new(Peer { id, mesh: mesh.clone() }), ConsensusOptions { request_timeout: Duration::from_secs(1), ..Default::default() }).await?);
            mesh.nodes.lock().unwrap().insert(id, Arc::downgrade(&node)); nodes.insert(id, node);
        }
        nodes[&1].initialize().await?;
        let old = leader(&nodes, None).await?;
        let (person, revision) = nodes[&old].transact("app", |tx| tx.insert("shared.Person", vec!["Ada".into()])).await?;
        println!("Leader {old} durably committed revision {revision} through a quorum");
        mesh.isolated.lock().unwrap().insert(old);
        let next = leader(&nodes, Some(old)).await?;
        assert!(nodes[&old].transact("app", |tx| tx.insert("shared.Person", vec!["must-not-commit".into()])).await.is_err());
        assert!(nodes[&next].read("app", |read| read.get("shared.Person", person)).await?.is_some());
        let (_, revision) = nodes[&next].transact("app", |tx| tx.update("shared.Person", person, vec!["Ada Lovelace".into()])).await?;
        println!("Majority elected leader {next}; stable identity survived and revision {revision} committed");
        mesh.isolated.lock().unwrap().remove(&old);
        for node in nodes.values() { node.shutdown().await?; }
        println!("Demo uses local peers. Production transport and log compaction remain separate milestones.");
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}
