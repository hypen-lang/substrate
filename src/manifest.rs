//! Reproducible schema packages. Lock once, check in IDs, then generate clients.
use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SchemaManifest {
    pub format: u32,
    pub modules: Vec<Module>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Draft {
    format: u32,
    modules: Vec<DraftModule>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DraftModule {
    name: String,
    version: u64,
    imports: Vec<(String, u64)>,
    relations: Vec<DraftRelation>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DraftRelation {
    #[serde(default)]
    id: Option<Id>,
    name: String,
    roles: Vec<Role>,
    #[serde(default)]
    unique: Vec<Vec<String>>,
    #[serde(default)]
    checks: Vec<Check>,
}
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct SchemaPlan {
    pub installed: Vec<String>,
    pub additions: Vec<String>,
}
fn invalid(message: impl Into<String>) -> Error {
    Error::Schema(message.into())
}
impl SchemaManifest {
    /// A locked file must carry every relation ID. Missing IDs are never regenerated.
    pub fn from_json(json: &str) -> Result<Self> {
        Self::parse(json, None, true)
    }
    /// Allocate IDs for new definitions only. All previous versions must be retained
    /// unchanged, including their imports and layouts. Re-locking is deterministic.
    pub fn lock(json: &str, previous: Option<&Self>) -> Result<Self> {
        Self::parse(json, previous, false)
    }
    fn parse(json: &str, previous: Option<&Self>, locked: bool) -> Result<Self> {
        let draft: Draft = serde_json::from_str(json).map_err(|e| invalid(e.to_string()))?;
        if !matches!(draft.format, 1 | 2) {
            return Err(invalid("unsupported schema manifest format"));
        }
        if let Some(old) = previous {
            old.validate()?;
        }
        let old_defs: BTreeMap<_, _> = previous
            .into_iter()
            .flat_map(|m| &m.modules)
            .flat_map(|m| &m.relations)
            .map(|r| (r.name.as_str(), r))
            .collect();
        let mut modules = Vec::new();
        for m in draft.modules {
            let mut relations = Vec::new();
            for r in m.relations {
                if locked && r.id.is_none() {
                    return Err(invalid(format!(
                        "locked relation {} is missing its ID",
                        r.name
                    )));
                }
                let prior = old_defs.get(r.name.as_str());
                let id = r.id.or_else(|| prior.map(|r| r.id)).unwrap_or_default();
                relations.push(Relation {
                    id,
                    name: r.name,
                    roles: r.roles,
                    unique: r.unique,
                    checks: r.checks,
                });
            }
            relations.sort_by(|a, b| a.name.cmp(&b.name));
            let mut imports = m.imports;
            imports.sort();
            modules.push(Module::new(&m.name, m.version, imports, relations));
        }
        let manifest = Self {
            format: draft.format,
            modules,
        }
        .canonical()?;
        if let Some(previous) = previous {
            let current: BTreeMap<_, _> = manifest
                .modules
                .iter()
                .map(|m| ((m.name.as_str(), m.version), m))
                .collect();
            for old in &previous.canonical()?.modules {
                if current.get(&(old.name.as_str(), old.version)).copied() != Some(old) {
                    return Err(invalid(format!(
                        "existing module {}@{} must remain unchanged; add a new version/family",
                        old.name, old.version
                    )));
                }
            }
        }
        manifest.validate()?;
        Ok(manifest)
    }
    pub fn canonical(&self) -> Result<Self> {
        if !matches!(self.format, 1 | 2)
            || (self.format == 1
                && self
                    .modules
                    .iter()
                    .flat_map(|m| &m.relations)
                    .any(|r| !r.checks.is_empty()))
        {
            return Err(invalid("unsupported schema manifest format"));
        }
        // The old algorithm repeatedly scanned/removed from the remaining sorted
        // vector. Kahn's algorithm with the smallest ready index produces exactly
        // the same lexicographically-first-ready dependency order in O((V + E) log V),
        // plus the unchanged import/relation sorting and payload clone costs.
        let mut modules = self.modules.clone();
        modules.sort_by(|a, b| (&a.name, a.version).cmp(&(&b.name, b.version)));
        let mut keys = BTreeMap::new();
        for (index, module) in modules.iter().enumerate() {
            if keys
                .insert((module.name.clone(), module.version), index)
                .is_some()
            {
                return Err(invalid("duplicate module version"));
            }
        }
        let invalid_dependencies =
            || invalid("missing/cyclic imports or nonconsecutive module versions");
        let mut indegree = vec![0usize; modules.len()];
        let mut dependents = vec![Vec::new(); modules.len()];
        let mut ready = BTreeSet::new();
        for (index, module) in modules.iter().enumerate() {
            if module.version == 0 {
                return Err(invalid_dependencies());
            }
            // Duplicates remain in the declaration for the semantic validator;
            // they represent only one dependency edge in the ordering graph.
            let mut dependencies = BTreeSet::new();
            for key in &module.imports {
                dependencies.insert(*keys.get(key).ok_or_else(invalid_dependencies)?);
            }
            if module.version > 1 {
                let previous = (module.name.clone(), module.version - 1);
                dependencies.insert(*keys.get(&previous).ok_or_else(invalid_dependencies)?);
            }
            indegree[index] = dependencies.len();
            if dependencies.is_empty() {
                ready.insert(index);
            }
            for dependency in dependencies {
                dependents[dependency].push(index);
            }
        }
        let mut pending: Vec<_> = modules.into_iter().map(Some).collect();
        let mut ordered = Vec::with_capacity(pending.len());
        while let Some(index) = ready.pop_first() {
            let mut module = pending[index].take().expect("ready module emitted once");
            module.imports.sort();
            module.relations.sort_by(|a, b| a.name.cmp(&b.name));
            ordered.push(module);
            for &dependent in &dependents[index] {
                indegree[dependent] -= 1;
                if indegree[dependent] == 0 {
                    ready.insert(dependent);
                }
            }
        }
        if ordered.len() != pending.len() {
            return Err(invalid_dependencies());
        }
        Ok(Self {
            format: self.format,
            modules: ordered,
        })
    }
    pub fn validate(&self) -> Result<()> {
        let db = Database::memory();
        let mut tx = db.admin().transaction();
        for module in self.canonical()?.modules {
            tx.install(module)?;
        }
        tx.commit()?;
        Ok(())
    }
    /// Versioned canonical contract hash, independent of JSON object ordering.
    pub fn fingerprint(&self) -> Result<String> {
        self.validate()?;
        Ok(contract_hash(
            b"LiquidManifest/1\0",
            &serde_json::to_vec(&self.canonical()?).map_err(|e| invalid(e.to_string()))?,
        ))
    }
    /// Select exact module versions and their transitive imports/earlier revisions.
    /// Unrelated installed modules cannot change this generated contract.
    pub fn select(&self, roots: &[(String, u64)]) -> Result<Self> {
        self.validate()?;
        let modules: BTreeMap<_, _> = self
            .modules
            .iter()
            .map(|m| ((m.name.as_str(), m.version), m))
            .collect();
        let mut wanted = BTreeSet::new();
        let mut pending = roots.to_vec();
        while let Some(key) = pending.pop() {
            if !wanted.insert(key.clone()) {
                continue;
            }
            let module = modules
                .get(&(key.0.as_str(), key.1))
                .ok_or_else(|| invalid("selected module version is not installed"))?;
            pending.extend(module.imports.iter().cloned());
            if module.version > 1 {
                pending.push((module.name.clone(), module.version - 1));
            }
        }
        Self {
            format: if self
                .modules
                .iter()
                .filter(|m| wanted.contains(&(m.name.clone(), m.version)))
                .flat_map(|m| &m.relations)
                .any(|r| !r.checks.is_empty())
            {
                2
            } else {
                1
            },
            modules: self
                .modules
                .iter()
                .filter(|m| wanted.contains(&(m.name.clone(), m.version)))
                .cloned()
                .collect(),
        }
        .canonical()
    }
    pub fn to_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string_pretty(&self.canonical()?)
            .map(|s| s + "\n")
            .map_err(|e| invalid(e.to_string()))
    }
}
impl Snapshot {
    /// Host-only export; never discloses other applications' schema through a session.
    pub fn manifest(&self) -> Result<SchemaManifest> {
        if self.principal.is_some() {
            return Err(Error::Denied);
        }
        SchemaManifest {
            format: if self.state.relations.values().any(|r| !r.checks.is_empty()) {
                2
            } else {
                1
            },
            // Mutable visibility policy is engine metadata, not part of the
            // immutable application schema contract or generated clients.
            modules: self
                .state
                .modules
                .values()
                .filter(|m| m.name != "liquid_visibility")
                .map(|m| (**m).clone())
                .collect(),
        }
        .canonical()
    }
    pub fn plan_manifest(&self, manifest: &SchemaManifest) -> Result<SchemaPlan> {
        if self.principal.is_some() {
            return Err(Error::Denied);
        }
        manifest.validate()?;
        let mut plan = SchemaPlan {
            installed: vec![],
            additions: vec![],
        };
        for module in manifest.canonical()?.modules {
            let name = format!("{}@{}", module.name, module.version);
            if let Some(old) = self
                .state
                .modules
                .get(&(module.name.clone(), module.version))
            {
                let mut old = (**old).clone();
                old.relations.sort_by(|a, b| a.name.cmp(&b.name));
                old.imports.sort();
                if old != module {
                    return Err(invalid(format!("installed {name} differs from manifest")));
                }
                plan.installed.push(name);
            } else {
                plan.additions.push(name);
            }
        }
        // Also validate additions against the actual catalog, not just the bundle.
        let mut combined = self.manifest()?;
        let mut combined_keys: BTreeSet<_> = combined
            .modules
            .iter()
            .map(|m| (m.name.clone(), m.version))
            .collect();
        for module in &manifest.modules {
            if combined_keys.insert((module.name.clone(), module.version)) {
                combined.modules.push(module.clone());
            }
        }
        combined.format = combined.format.max(manifest.format);
        combined.validate()?;
        Ok(plan)
    }
}

fn contract_hash(domain: &[u8], bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(bytes);
    format!("{:x}", hash.finalize())
}
impl Relation {
    /// Layout contract, including names, stable ID, codecs and constraints.
    pub fn fingerprint(&self) -> String {
        contract_hash(
            if self.checks.is_empty() {
                b"LiquidRelation/1\0"
            } else {
                b"LiquidRelation/2\0"
            },
            &serde_json::to_vec(self).expect("typed schema is serializable"),
        )
    }
}
