//! Mutable schema visibility, separate from immutable relation declarations.
//! Absence of a policy is the legacy grant-only behavior.
use crate::*;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::OnceLock};

pub(crate) const ANONYMOUS: &str = "\0anonymous";
pub(crate) const HOST: &str = "\0host";
pub(crate) const TYPE_ID: Id = Id::from_u128(0x6c697175696400000000007669736962);
pub(crate) const FAMILY: &str = "liquid_visibility.Policy";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    Public,
    #[default]
    Authenticated,
    Private,
}
impl Visibility {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Authenticated => "authenticated",
            Self::Private => "private",
        }
    }
}
#[derive(Clone)]
pub(crate) struct Policy {
    pub row_id: Id,
    pub relation: Id,
    pub owner: String,
    pub visibility: Visibility,
}
pub(crate) fn protected(namespace: &str) -> bool {
    [
        "liquid_control",
        "liquid_security",
        "liquid_auth",
        "liquid_views",
        "liquid_view_catalog",
        "liquid_visibility",
    ]
    .iter()
    .any(|n| namespace.eq_ignore_ascii_case(n))
}
pub(crate) fn module() -> &'static Module {
    static MODULE: OnceLock<Module> = OnceLock::new();
    MODULE.get_or_init(|| {
        Module::new(
            "liquid_visibility",
            1,
            vec![],
            vec![Relation {
                id: TYPE_ID,
                name: FAMILY.into(),
                roles: vec![
                    Role::new("relation", DataType::Text),
                    Role::new("owner", DataType::Text),
                    Role::new("visibility", DataType::Text),
                ],
                unique: vec![vec!["relation".into()]],
                checks: vec![],
            }],
        )
    })
}
pub(crate) fn normalize(
    module: &Module,
    mut supplied: BTreeMap<String, Visibility>,
) -> Result<BTreeMap<String, Visibility>> {
    let mut result = BTreeMap::new();
    for relation in &module.relations {
        result.insert(
            relation.name.clone(),
            supplied.remove(&relation.name).unwrap_or_default(),
        );
    }
    if !supplied.is_empty() {
        return Err(Error::Schema(
            "visibility names must belong to the new module".into(),
        ));
    }
    Ok(result)
}
pub(crate) fn compile(state: &crate::storage::State, row: &Row) -> Result<Policy> {
    if state
        .modules
        .get(&("liquid_visibility".into(), 1))
        .map(AsRef::as_ref)
        != Some(module())
    {
        return Err(Error::Denied);
    }
    let [Value::Text(relation), Value::Text(owner), Value::Text(mode)] = row.values.as_ref() else {
        return Err(Error::Denied);
    };
    let id = Id(uuid::Uuid::parse_str(relation).map_err(|_| Error::Denied)?);
    if id.to_string() != *relation || owner.is_empty() || owner.len() > 128 || owner == ANONYMOUS {
        return Err(Error::Denied);
    }
    let def = state.relations.get(&id).ok_or(Error::Denied)?;
    if protected(def.name.split_once('.').ok_or(Error::Denied)?.0) {
        return Err(Error::Denied);
    }
    let visibility = match mode.as_str() {
        "public" => Visibility::Public,
        "authenticated" => Visibility::Authenticated,
        "private" => Visibility::Private,
        _ => return Err(Error::Denied),
    };
    Ok(Policy {
        row_id: row.id,
        relation: id,
        owner: owner.clone(),
        visibility,
    })
}
