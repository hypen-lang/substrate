//! Host-managed, durable, scope-bound credentials. Only token digests are stored.
use crate::{
    storage::{Key, State},
    *,
};
use ring::rand::{SecureRandom, SystemRandom};
use sha2::{Digest, Sha256};
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};
use subtle::ConstantTimeEq;

pub(crate) const TYPE_ID: Id = Id(uuid::Uuid::from_u128(0x6c697175696400000000000000617574));
const FAMILY: &str = "liquid_auth.Credential";
/// Exclusive Unix-second deadlines are bounded to year 9999 (exact in JavaScript).
pub(crate) fn valid_deadline(t: u64) -> bool {
    t > 0 && t <= 253402300799
}
pub(crate) fn check_deadline(t: Option<u64>, now: u64) -> Result<()> {
    if t.is_some_and(|t| !valid_deadline(t) || t <= now) {
        Err(Error::Denied)
    } else {
        Ok(())
    }
}
pub(crate) fn earliest(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}
pub(crate) fn deadline_within(child: Option<u64>, parent: Option<u64>) -> bool {
    parent.is_none_or(|p| child.is_some_and(|c| c <= p))
}
#[derive(Default)]
pub(crate) struct Clock {
    last: AtomicU64,
    #[cfg(test)]
    manual: AtomicU64,
}
impl Clock {
    pub(crate) fn now(&self) -> u64 {
        #[cfg(test)]
        if self.manual.load(Ordering::SeqCst) != 0 {
            return self.manual.load(Ordering::SeqCst);
        }
        // Fail closed on unavailable/pre-epoch time; never move backwards in a process.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(u64::MAX);
        self.observe(now)
    }
    fn observe(&self, now: u64) -> u64 {
        // Most fences observe the same Unix second. A read preserves the same
        // monotonic linearization without taking write ownership of the clock's
        // cache line on every concurrent credential check.
        let last = self.last.load(Ordering::SeqCst);
        if now <= last {
            last
        } else {
            self.last.fetch_max(now, Ordering::SeqCst).max(now)
        }
    }
    #[cfg(test)]
    pub(crate) fn set(&self, now: u64) {
        self.manual.store(now, Ordering::SeqCst);
    }
}
#[derive(Clone)]
pub(crate) struct Credential {
    pub principal: String,
    pub capability: Id,
    pub expires_at: u64,
    pub revoked: bool,
    digest: String,
}
/// Secret returned only at issuance/rotation; deliberately no Debug/Serialize.
pub struct IssuedCredential {
    pub id: Id,
    pub token: String,
    pub expires_at: u64,
}
pub(crate) fn module() -> &'static Module {
    static MODULE: OnceLock<Module> = OnceLock::new();
    MODULE.get_or_init(|| {
        Module::new(
            "liquid_auth",
            1,
            vec![],
            vec![Relation {
                id: TYPE_ID,
                name: FAMILY.into(),
                roles: vec![
                    Role::new("principal", DataType::Text),
                    Role::new("digest", DataType::Text),
                    Role::new("capability", DataType::Text),
                    Role::new("expires_at", DataType::Int),
                    Role::new("revoked", DataType::Bool),
                ],
                unique: vec![],
                checks: vec![],
            }],
        )
    })
}
pub(crate) fn compile(state: &State, row: &Row) -> Result<Credential> {
    if state
        .modules
        .get(&("liquid_auth".into(), 1))
        .map(AsRef::as_ref)
        != Some(module())
    {
        return Err(Error::Denied);
    }
    let [
        Value::Text(principal),
        Value::Text(digest),
        Value::Text(cap),
        Value::Int(end),
        Value::Bool(revoked),
    ] = row.values.as_ref()
    else {
        return Err(Error::Denied);
    };
    if principal.is_empty()
        || principal.len() > 128
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        || !valid_deadline(*end as u64)
    {
        return Err(Error::Denied);
    }
    Ok(Credential {
        principal: principal.clone(),
        digest: digest.clone(),
        capability: Id(uuid::Uuid::parse_str(cap).map_err(|_| Error::Denied)?),
        expires_at: *end as u64,
        revoked: *revoked,
    })
}
fn digest(token: &str) -> String {
    format!(
        "{:x}",
        Sha256::digest(format!("substrate-credential-v1:{token}"))
    )
}
fn generate() -> Result<(Id, String)> {
    let id = Id::new();
    let mut secret = [0u8; 32];
    SystemRandom::new()
        .fill(&mut secret)
        .map_err(|_| Error::Storage("secure randomness unavailable".into()))?;
    let encoded: String = secret.iter().map(|b| format!("{b:02x}")).collect();
    Ok((id, format!("s1.{id}.{encoded}")))
}
impl Transaction {
    fn store_credential(
        &mut self,
        id: Id,
        token: &str,
        principal: &str,
        capability: Id,
        end: u64,
    ) -> Result<()> {
        let snapshot = self.snapshot();
        let (cap, keys) =
            crate::capability::resolve(&snapshot.state, principal, capability, self.db_now())?;
        if !deadline_within(Some(end), cap.record.expires_at) {
            return Err(Error::Denied);
        }
        for key in keys {
            self.track_authority_key(key);
        }
        self.expire_at(Some(end));
        if !snapshot
            .state
            .modules
            .contains_key(&("liquid_auth".into(), 1))
        {
            self.install(module().clone())?;
        }
        if self.count_internal_family(FAMILY)? >= 8192 {
            return Err(Error::Constraint("credential ledger quota exceeded".into()));
        }
        let values = vec![
            Value::Text(principal.into()),
            Value::Text(digest(token)),
            Value::Text(capability.to_string()),
            Value::Int(end as i64),
            Value::Bool(false),
        ];
        compile(
            &self.snapshot().state,
            &Row {
                id,
                values: values.clone().into_boxed_slice(),
            },
        )?;
        self.insert_with_id(FAMILY, id, values).map(|_| ())
    }
    fn revoke_credential_row(&mut self, id: Id) -> Result<()> {
        let row = self.get(FAMILY, id)?.ok_or(Error::Denied)?;
        let mut values = row.values.to_vec();
        values[4] = Value::Bool(true);
        self.update(FAMILY, id, values)
    }
}
impl Database {
    /// Host-only. Every remote credential is bound to one principal and capability.
    pub fn issue_credential(
        &self,
        principal: &str,
        capability: Id,
        expires_at: u64,
    ) -> Result<IssuedCredential> {
        check_deadline(Some(expires_at), self.clock.now())?;
        let (id, token) = generate()?;
        self.admin().transact(RetryPolicy::default(), |tx| {
            tx.store_credential(id, &token, principal, capability, expires_at)
        })?;
        Ok(IssuedCredential {
            id,
            token,
            expires_at,
        })
    }
    /// Atomic, immediate cutover. Scope and principal cannot change during rotation.
    pub fn rotate_credential(&self, old: Id, expires_at: u64) -> Result<IssuedCredential> {
        check_deadline(Some(expires_at), self.clock.now())?;
        let (id, token) = generate()?;
        self.admin().transact(RetryPolicy::default(), |tx| {
            tx.track_authority_key(Key::Row(TYPE_ID, old));
            let snapshot = tx.snapshot();
            let c = snapshot.state.credentials.get(&old).ok_or(Error::Denied)?;
            if c.revoked {
                return Err(Error::Denied);
            }
            tx.store_credential(id, &token, &c.principal, c.capability, expires_at)?;
            tx.revoke_credential_row(old)
        })?;
        Ok(IssuedCredential {
            id,
            token,
            expires_at,
        })
    }
    pub fn revoke_credential(&self, id: Id) -> Result<()> {
        self.admin()
            .transact(RetryPolicy::default(), |tx| tx.revoke_credential_row(id))
    }
    /// Authenticate once, but retain live credential and capability checks on every operation.
    pub fn authenticate(&self, token: &str) -> Result<Session> {
        if token.len() != 104 || !token.is_ascii() || !token.starts_with("s1.") {
            return Err(Error::Denied);
        }
        let id: Id = Id(uuid::Uuid::parse_str(&token[3..39]).map_err(|_| Error::Denied)?);
        if token.as_bytes()[39] != b'.' || !token.as_bytes()[40..].iter().all(u8::is_ascii_hexdigit)
        {
            return Err(Error::Denied);
        }
        let snapshot = self.admin().snapshot();
        let c = snapshot.state.credentials.get(&id).ok_or(Error::Denied)?;
        if c.revoked
            || self.clock.now() >= c.expires_at
            || !bool::from(c.digest.as_bytes().ct_eq(digest(token).as_bytes()))
        {
            return Err(Error::Denied);
        }
        let mut session = self.session(&c.principal);
        session.select_capability(c.capability);
        session.credential = Some(id);
        session.snapshot().check_live()?;
        Ok(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn observed_clock_never_rewinds_or_recovers_from_failed_time() {
        let clock = Clock::default();
        assert_eq!(clock.observe(100), 100);
        assert_eq!(clock.observe(99), 100);
        assert_eq!(clock.observe(100), 100);
        assert_eq!(clock.observe(101), 101);
        assert_eq!(clock.observe(u64::MAX), u64::MAX);
        assert_eq!(clock.observe(102), u64::MAX);
    }
    #[test]
    fn concurrent_clock_observations_preserve_completed_advances() {
        let clock = std::sync::Arc::new(Clock::default());
        let ready = std::sync::Arc::new(std::sync::Barrier::new(8));
        let workers: Vec<_> = (0..8)
            .map(|worker| {
                let clock = clock.clone();
                let ready = ready.clone();
                std::thread::spawn(move || {
                    ready.wait();
                    let mut previous = 0;
                    for step in 0..1000 {
                        let supplied = worker * 1000 + step;
                        let observed = clock.observe(supplied);
                        assert!(observed >= supplied && observed >= previous);
                        previous = clock.observe(supplied / 2);
                        assert!(previous >= observed);
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(clock.observe(0), 7999);
    }
    use crate::{
        capability::{CapabilityScope, DelegatedCapability, RelationCapability},
        schema_control::*,
    };
    fn fixture(db: &Database, end: Option<u64>) -> (DelegatedCapability, Id) {
        db.clock.set(100);
        let mut tx = db.admin().transaction();
        tx.install(demo::core()).unwrap();
        let ada = tx.insert("core.Person", vec!["Ada".into()]).unwrap();
        tx.insert("core.Person", vec!["Bob".into()]).unwrap();
        tx.grant("owner", "core.Person", Grant::read_write())
            .unwrap();
        tx.commit().unwrap();
        db.configure_namespace(NamespacePolicy {
            namespace: "app".into(),
            owner: "owner".into(),
            enabled: true,
            auto_approve: true,
            quotas: SchemaQuotas::default(),
        })
        .unwrap();
        let scope = CapabilityScope {
            relations: vec![RelationCapability {
                relation: db.admin().snapshot().schema("core.Person").unwrap().id,
                grant: Grant {
                    filter: RowFilter::Equals {
                        role: "name".into(),
                        value: "Ada".into(),
                    },
                    ..Grant::read_write()
                },
            }],
            schema_namespace: Some("app".into()),
            delegations: 2,
        };
        (
            db.issue_capability_until(Id::new(), "owner", scope, end)
                .unwrap(),
            ada,
        )
    }
    #[test]
    fn credentials_are_scope_bound_and_never_store_secrets() {
        let db = Database::memory();
        let (cap, _) = fixture(&db, Some(200));
        let cred = db.issue_credential("owner", cap.id, 150).unwrap();
        let s = db.authenticate(&cred.token).unwrap();
        assert_eq!(s.snapshot().scan("core.Person").unwrap().len(), 1);
        assert!(s.snapshot().schema(FAMILY).is_err());
        assert!(
            s.transaction()
                .insert("core.Person", vec!["Eve".into()])
                .is_err()
        );
        let record = db.admin().snapshot().get(FAMILY, cred.id).unwrap().unwrap();
        assert!(!format!("{record:?}").contains(&cred.token));
        let different = db.issue_capability(Id::new(), "owner", cap.scope).unwrap();
        assert!(s.with_capability(different.id).is_err());
        assert!(db.issue_credential("stranger", cap.id, 150).is_err());
        assert!(db.issue_credential("owner", cap.id, 201).is_err());
    }
    #[test]
    fn malformed_and_wrong_secrets_are_denied_without_panics() {
        let db = Database::memory();
        let (cap, _) = fixture(&db, None);
        let cred = db.issue_credential("owner", cap.id, 150).unwrap();
        for token in [
            String::new(),
            "é".repeat(52),
            cred.token.replace("s1.", "s2."),
            format!("{}{}", &cred.token[..40], "0".repeat(64)),
        ] {
            assert!(matches!(db.authenticate(&token), Err(Error::Denied)));
        }
    }
    #[test]
    fn rotation_fences_retained_native_sql_runtime_and_staged_writes() {
        let db = Database::memory();
        let (cap, ada) = fixture(&db, None);
        let old = db.issue_credential("owner", cap.id, 150).unwrap();
        let s = db.authenticate(&old.token).unwrap();
        let snap = s.snapshot();
        let sql = s.snapshot().sql().unwrap();
        let mut tx = s.transaction();
        tx.update("core.Person", ada, vec!["Ada".into()]).unwrap();
        let new = db.rotate_credential(old.id, 160).unwrap();
        assert!(db.authenticate(&old.token).is_err());
        assert!(snap.scan("core.Person").is_err());
        assert!(snap.describe("core.Person").is_err());
        assert!(sql.query("SELECT * FROM core.Person", &[]).is_err());
        assert!(tx.commit().is_err());
        assert!(s.schema_namespaces().is_err());
        assert!(
            s.delegate_capability(
                cap.id,
                Id::new(),
                "child",
                CapabilityScope {
                    delegations: 1,
                    ..cap.scope.clone()
                }
            )
            .is_err()
        );
        assert!(s.revoke_capability(cap.id).is_err());
        assert_eq!(
            db.authenticate(&new.token)
                .unwrap()
                .snapshot()
                .scan("core.Person")
                .unwrap()
                .len(),
            1
        );
        assert!(db.rotate_credential(old.id, 170).is_err());
    }
    #[test]
    fn credential_expiry_is_exclusive_and_fences_pending_schema_install() {
        let db = Database::memory();
        let (cap, _) = fixture(&db, None);
        let cred = db.issue_credential("owner", cap.id, 150).unwrap();
        let s = db.authenticate(&cred.token).unwrap();
        let snap = s.snapshot();
        let mut install = db.admin().transaction();
        install.bind_schema_session(&s, Some("app")).unwrap();
        install
            .install(Module::new(
                "app",
                1,
                vec![],
                vec![Relation::new("app.Note", vec![])],
            ))
            .unwrap();
        db.clock.set(149);
        assert!(snap.scan("core.Person").is_ok());
        db.clock.set(150);
        assert!(snap.scan("core.Person").is_err());
        assert!(install.commit().is_err());
        assert!(db.authenticate(&cred.token).is_err());
        assert!(db.admin().snapshot().schema("app.Note").is_err());
    }
    #[test]
    fn capability_expiry_reaches_retained_handles_and_inherited_children() {
        let db = Database::memory();
        let (cap, ada) = fixture(&db, Some(200));
        let owner = db.session("owner");
        let scope = CapabilityScope {
            delegations: 1,
            ..cap.scope.clone()
        };
        assert!(
            owner
                .delegate_capability_until(cap.id, Id::new(), "child", scope.clone(), Some(201))
                .is_err()
        );
        let child = owner
            .delegate_capability(cap.id, Id::new(), "child", scope)
            .unwrap();
        assert_eq!(child.expires_at, Some(200));
        let s = db.session("child").with_capability(child.id).unwrap();
        let snap = s.snapshot();
        let sql = s.snapshot().sql().unwrap();
        let mut tx = s.transaction();
        tx.update("core.Person", ada, vec!["Ada".into()]).unwrap();
        db.clock.set(200);
        assert!(snap.scan("core.Person").is_err());
        assert!(sql.query("SELECT * FROM core.Person", &[]).is_err());
        assert!(tx.commit().is_err());
        assert!(s.with_capability(child.id).is_err());
    }
    #[test]
    fn revoking_parent_or_credential_preserves_live_revocation() {
        for root in [false, true] {
            let db = Database::memory();
            let (cap, _) = fixture(&db, None);
            let c = db.issue_credential("owner", cap.id, 150).unwrap();
            let s = db.authenticate(&c.token).unwrap();
            let snapshot = s.snapshot();
            if root {
                db.revoke_capability(cap.id).unwrap();
            } else {
                db.revoke_credential(c.id).unwrap();
            }
            assert!(snapshot.scan("core.Person").is_err());
            assert!(db.authenticate(&c.token).is_err());
        }
    }
    #[test]
    fn rotation_and_expiry_survive_checkpoint_and_log_recovery() {
        for checkpoint in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let db = Database::open(&path).unwrap();
            let (cap, _) = fixture(&db, Some(200));
            let old = db.issue_credential("owner", cap.id, 150).unwrap();
            let new = db.rotate_credential(old.id, 190).unwrap();
            if checkpoint {
                db.checkpoint().unwrap();
            }
            drop(db);
            let db = Database::open(&path).unwrap();
            db.clock.set(100);
            assert!(db.authenticate(&old.token).is_err());
            assert!(db.authenticate(&new.token).is_ok());
            db.clock.set(190);
            assert!(db.authenticate(&new.token).is_err());
        }
    }
    #[test]
    fn v1_capability_records_remain_compatible_and_unknown_versions_fail_closed() {
        let db = Database::memory();
        let (mut cap, _) = fixture(&db, None);
        cap.format = 1;
        let row = Row {
            id: cap.id,
            values: vec![
                Value::Text(serde_json::to_string(&cap).unwrap()),
                Value::Bool(false),
            ]
            .into_boxed_slice(),
        };
        assert!(!serde_json::to_string(&cap).unwrap().contains("expires_at"));
        assert!(crate::capability::compile(&db.admin().snapshot().state, &row).is_ok());
        for (format, expiry) in [(1, Some(200)), (0, None), (3, None), (2, Some(0))] {
            cap.format = format;
            cap.expires_at = expiry;
            let row = Row {
                id: cap.id,
                values: vec![
                    Value::Text(serde_json::to_string(&cap).unwrap()),
                    Value::Bool(false),
                ]
                .into_boxed_slice(),
            };
            assert!(crate::capability::compile(&db.admin().snapshot().state, &row).is_err());
        }
    }
    #[test]
    fn deadline_validation_rejects_past_zero_and_inexact_js_ranges() {
        let db = Database::memory();
        let (cap, _) = fixture(&db, None);
        for end in [0, 99, 100, 253402300800, u64::MAX] {
            assert!(db.issue_credential("owner", cap.id, end).is_err());
            assert!(
                db.issue_capability_until(Id::new(), "owner", cap.scope.clone(), Some(end))
                    .is_err()
            );
        }
    }
    #[test]
    fn concurrent_rotations_have_one_winner_and_no_extra_active_credentials() {
        let db = Database::memory();
        let (cap, _) = fixture(&db, None);
        let old = db.issue_credential("owner", cap.id, 150).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let mut workers = vec![];
        for _ in 0..2 {
            let db = db.clone();
            let barrier = barrier.clone();
            let id = old.id;
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                db.rotate_credential(id, 190)
            }));
        }
        barrier.wait();
        let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert!(db.authenticate(&old.token).is_err());
        let winner = results.into_iter().find_map(Result::ok).unwrap();
        assert!(db.authenticate(&winner.token).is_ok());
        assert_eq!(db.admin().snapshot().state.credentials.len(), 2);
    }
    #[test]
    fn authenticated_schema_workflow_rechecks_rotation_and_does_not_expand_scope() {
        let db = Database::memory();
        let (cap, _) = fixture(&db, Some(200));
        let old = db.issue_credential("owner", cap.id, 150).unwrap();
        let session = db.authenticate(&old.token).unwrap();
        let proposal = session
            .propose_schema(
                Id::new(),
                Module::new(
                    "app",
                    1,
                    vec![],
                    vec![Relation::new(
                        "app.Note",
                        vec![Role::new("text", DataType::Text)],
                    )],
                ),
            )
            .unwrap();
        let new = db.rotate_credential(old.id, 160).unwrap();
        assert!(
            session
                .apply_schema_proposal(proposal.id, &proposal.plan.digest)
                .is_err()
        );
        let session = db.authenticate(&new.token).unwrap();
        session
            .apply_schema_proposal(proposal.id, &proposal.plan.digest)
            .unwrap();
        assert!(db.admin().snapshot().schema("app.Note").is_ok());
        assert!(session.snapshot().schema("app.Note").is_err());
    }
    #[test]
    fn a_shorter_child_expires_without_revoking_parent_or_sibling() {
        let db = Database::memory();
        let (cap, _) = fixture(&db, Some(200));
        let scope = CapabilityScope {
            delegations: 1,
            ..cap.scope.clone()
        };
        let owner = db.session("owner");
        let short = owner
            .delegate_capability_until(cap.id, Id::new(), "short", scope.clone(), Some(120))
            .unwrap();
        let long = owner
            .delegate_capability(cap.id, Id::new(), "long", scope)
            .unwrap();
        let short = db
            .session("short")
            .with_capability(short.id)
            .unwrap()
            .snapshot();
        db.clock.set(120);
        assert!(short.scan("core.Person").is_err());
        assert!(
            owner
                .with_capability(cap.id)
                .unwrap()
                .snapshot()
                .scan("core.Person")
                .is_ok()
        );
        assert!(
            db.session("long")
                .with_capability(long.id)
                .unwrap()
                .snapshot()
                .scan("core.Person")
                .is_ok()
        );
    }
    #[test]
    fn credential_crash_child() {
        let Ok(path) = std::env::var("SUBSTRATE_AUTH_CRASH_PATH") else {
            return;
        };
        let db = Database::open(path).unwrap();
        db.clock.set(100);
        let id =
            Id(uuid::Uuid::parse_str(&std::env::var("SUBSTRATE_AUTH_CRASH_ID").unwrap()).unwrap());
        db.rotate_credential(id, 190).unwrap();
        db.checkpoint().unwrap();
    }
    #[test]
    fn credential_rotation_is_atomic_across_process_crashes() {
        for point in [
            "append_before_commit",
            "append_after_commit",
            "checkpoint_before_commit",
            "checkpoint_after_commit",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let db = Database::open(&path).unwrap();
            let (cap, _) = fixture(&db, Some(200));
            let old = db.issue_credential("owner", cap.id, 150).unwrap();
            drop(db);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "auth::tests::credential_crash_child"])
                .env("SUBSTRATE_AUTH_CRASH_PATH", &path)
                .env("SUBSTRATE_AUTH_CRASH_ID", old.id.to_string())
                .env("SUBSTRATE_TEST_CRASH_POINT", point)
                .output()
                .unwrap()
                .status;
            assert_eq!(status.code(), Some(91));
            let db = Database::open(&path).unwrap();
            db.clock.set(100);
            let before = point == "append_before_commit";
            assert_eq!(db.authenticate(&old.token).is_ok(), before);
            let snapshot = db.admin().snapshot();
            let ledger = &snapshot.state.credentials;
            assert_eq!(ledger.len(), if before { 1 } else { 2 });
            assert_eq!(ledger.values().filter(|c| !c.revoked).count(), 1);
            assert!(
                ledger
                    .values()
                    .all(|c| c.capability == cap.id && c.principal == "owner")
            );
        }
    }
    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(96))]
        #[test]
        fn arbitrary_tokens_never_authenticate_or_panic(token in ".{0,150}") {
            let db=Database::memory();proptest::prop_assert!(db.authenticate(&token).is_err());
        }
        #[test]
        fn expiry_boundary_never_extends_authority(end in 101u64..10000, offset in 0u64..100) {
            let db=Database::memory();let(cap,_) = fixture(&db,Some(end));let c=db.issue_credential("owner",cap.id,end).unwrap();
            let s=db.authenticate(&c.token).unwrap();let snap=s.snapshot();db.clock.set(end+offset);
            proptest::prop_assert!(snap.scan("core.Person").is_err());proptest::prop_assert!(db.authenticate(&c.token).is_err());
        }
    }
}
