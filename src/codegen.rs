//! Deterministic source generation from the validated locked manifest.
use crate::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
};
pub struct GeneratedCode {
    pub rust: String,
    pub typescript: String,
}
fn rust_name(name: &str) -> String {
    if ["self", "Self", "super", "crate", "_"].contains(&name) {
        format!("r#schema_{name}")
    } else {
        format!("r#{name}")
    }
}
fn path(name: &str) -> String {
    let (m, r) = name.split_once('.').expect("validated name");
    format!("super::{}::{}", rust_name(m), rust_name(r))
}
fn ts_name(name: &str) -> String {
    name.replace('.', "_")
}
// A relation exports a descriptor/type pair and three companion types. Treat
// the whole group as one name when deciding whether a short alias is unambiguous.
fn ts_group(name: &str) -> [String; 4] {
    [
        name.into(),
        format!("{name}Insert"),
        format!("{name}Update"),
        format!("{name}Read"),
    ]
}
fn ts_reserved() -> BTreeSet<String> {
    let mut reserved: BTreeSet<String> = "await break case catch class const continue debugger default delete do else enum export extends false finally for function if implements import in instanceof interface let new null package private protected public return static super switch this throw true try typeof var void while with yield abstract any as asserts async bigint boolean constructor declare from get infer is keyof module namespace never number object of readonly require set string symbol type undefined unique unknown using global intrinsic out override satisfies Array BigInt Boolean Error Map Number Object Partial Promise Readonly Record Set String Uint8Array CONTRACT_FINGERPRINT"
        .split_whitespace().map(str::to_owned).collect();
    // Keep aliases clear of every generated validation/descriptor helper, including
    // private helpers, so changes to the shared preamble cannot break generation.
    for source in [
        include_str!("../sdk/typescript/validation.ts"),
        include_str!("../sdk/typescript/generated-preamble.ts"),
    ] {
        for line in source.lines() {
            let line = line.strip_prefix("export ").unwrap_or(line);
            for prefix in ["interface ", "type ", "class ", "function ", "const "] {
                if let Some(rest) = line.strip_prefix(prefix) {
                    let name = rest
                        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                        .next()
                        .unwrap();
                    reserved.insert(name.into());
                }
            }
        }
    }
    reserved
}
fn ts_aliases(modules: &BTreeMap<String, Vec<Relation>>) -> Result<BTreeMap<String, String>> {
    let mut occupied = ts_reserved();
    let mut candidates = BTreeMap::<String, usize>::new();
    for relation in modules.values().flatten() {
        for symbol in ts_group(&ts_name(&relation.name)) {
            if !occupied.insert(symbol) {
                return Err(Error::Schema(
                    "generated TypeScript name collision; choose distinct names".into(),
                ));
            }
        }
        let local = relation.name.split_once('.').unwrap().1;
        for symbol in ts_group(local) {
            *candidates.entry(symbol).or_default() += 1;
        }
    }
    Ok(modules
        .values()
        .flatten()
        .filter_map(|relation| {
            let local = relation.name.split_once('.').unwrap().1;
            ts_group(local)
                .iter()
                .all(|name| !occupied.contains(name) && candidates[name] == 1)
                .then(|| (relation.name.clone(), local.to_owned()))
        })
        .collect())
}
fn rust_type(t: &DataType) -> String {
    match t {
        DataType::Nullable(inner) => format!("Option<{}>", rust_type(inner)),
        DataType::Float => "substrate_liquid::FiniteFloat".into(),
        DataType::Bool => "bool".into(),
        DataType::Text => "String".into(),
        DataType::Bytes => "Vec<u8>".into(),
        DataType::Ref(n) => format!("TypedId<{}>", path(n)),
        _ => "i64".into(),
    }
}
fn ts_type(t: &DataType) -> String {
    match t {
        DataType::Nullable(inner) => format!("{} | null", ts_type(inner)),
        DataType::Float => "number".into(),
        DataType::Bool => "boolean".into(),
        DataType::Text => "string".into(),
        DataType::Bytes => "Uint8Array".into(),
        DataType::Ref(n) => format!("EntityId<{n:?}>"),
        _ => "bigint".into(),
    }
}
fn variant(t: &DataType) -> &'static str {
    match t {
        DataType::Nullable(inner) => variant(inner),
        DataType::Float => "Float",
        DataType::Bool => "Bool",
        DataType::Int => "Int",
        DataType::Text => "Text",
        DataType::Ref(_) => "Ref",
        DataType::Decimal { .. } => "Decimal",
        DataType::Timestamp => "Timestamp",
        DataType::Bytes => "Bytes",
    }
}
fn rust_encode(t: &DataType, expression: &str) -> String {
    match t {
        DataType::Nullable(inner) => format!(
            "match {expression} {{ Some(value) => {}, None => Value::Null }}",
            rust_encode(inner, "value")
        ),
        DataType::Ref(_) => format!("Value::Ref({expression}.raw())"),
        _ => format!("Value::{}({expression})", variant(t)),
    }
}
fn rust_decode(t: &DataType, expression: &str, error: &str) -> String {
    match t {
        DataType::Nullable(inner) => format!(
            "match {expression} {{ Some(Value::Null) => None, value => Some({}) }}",
            rust_decode(inner, "value", error)
        ),
        _ => format!(
            "match {expression} {{Some(Value::{}(value))=>{}, _=>return Err(Error::Constraint({error:?}.into()))}}",
            variant(t),
            if matches!(t, DataType::Ref(_)) {
                "TypedId::from_raw(value)"
            } else {
                "value"
            }
        ),
    }
}
impl SchemaManifest {
    pub fn generate(&self) -> Result<GeneratedCode> {
        self.validate()?;
        let mut modules: BTreeMap<String, Vec<Relation>> = BTreeMap::new();
        for module in self.canonical()?.modules {
            modules
                .entry(module.name)
                .or_default()
                .extend(module.relations);
        }
        let mut rust =
            String::from("// Generated by Substrate from a locked manifest. Do not edit.\n");
        let mut ts = String::from(include_str!("../sdk/typescript/validation.ts"));
        ts.push_str(
            &include_str!("../sdk/typescript/generated-preamble.ts")
                .lines()
                .filter(|l| !l.starts_with("import "))
                .collect::<Vec<_>>()
                .join("\n"),
        );
        writeln!(
            ts,
            "\nexport const CONTRACT_FINGERPRINT: string = {:?};",
            self.fingerprint()?
        )
        .unwrap();
        let aliases = ts_aliases(&modules)?;
        let mut rust_modules = BTreeSet::new();
        for (namespace, mut relations) in modules {
            if !rust_modules.insert(rust_name(&namespace)) {
                return Err(Error::Schema("generated Rust module name collision".into()));
            }
            relations.sort_by(|a, b| a.name.cmp(&b.name));
            writeln!(rust,"#[rustfmt::skip]\n#[allow(non_snake_case, non_camel_case_types, unused_imports, unused_mut)]\npub mod {} {{\nuse substrate_liquid::{{Record, Fields, Value, Row, Id, TypedId, Result, Error}};",rust_name(&namespace)).unwrap();
            let mut rust_symbols = BTreeSet::new();
            for def in relations {
                let local = def.name.split_once('.').unwrap().1;
                let rn = rust_name(local);
                let tn = ts_name(&def.name);
                if [
                    "Record",
                    "Fields",
                    "Value",
                    "Row",
                    "Id",
                    "TypedId",
                    "Result",
                    "Error",
                    "Vec",
                    "String",
                    "i64",
                    "bool",
                    "FiniteFloat",
                    "Option",
                ]
                .contains(&local)
                    || !rust_symbols.insert(rn.clone())
                {
                    return Err(Error::Schema(
                        "generated type name collision; choose distinct names".into(),
                    ));
                }
                let mut fields = BTreeSet::new();
                writeln!(
                    rust,
                    "#[derive(Clone, Debug, PartialEq, Eq)]\npub struct {rn} {{"
                )
                .unwrap();
                writeln!(ts, "export interface {tn} {{").unwrap();
                for role in &def.roles {
                    if !fields.insert(rust_name(&role.name)) {
                        return Err(Error::Schema("generated field name collision".into()));
                    }
                    if let DataType::Decimal { scale } = role.data_type {
                        writeln!(rust, "/// Exact minor units; decimal scale {scale}.").unwrap();
                        writeln!(ts, "/** Exact minor units; decimal scale {scale}. */").unwrap();
                    }
                    writeln!(
                        rust,
                        "pub {}: {},",
                        rust_name(&role.name),
                        rust_type(&role.data_type)
                    )
                    .unwrap();
                    writeln!(ts, "{}: {};", role.name, ts_type(&role.data_type)).unwrap();
                }
                writeln!(rust,"}}\nimpl Record for {rn} {{\nconst RELATION: &'static str = {:?};\nconst TYPE_ID: Id = Id::from_u128({});\nconst FINGERPRINT: Option<&'static str> = Some({:?});\nfn fields(self) -> Fields {{\n[",def.name,def.id.0.as_u128(),def.fingerprint()).unwrap();
                for role in &def.roles {
                    let field = rust_name(&role.name);
                    let value = rust_encode(&role.data_type, &format!("self.{field}"));
                    writeln!(rust, "({:?}.into(),{value}),", role.name).unwrap();
                }
                writeln!(rust,"] .into_iter().collect()\n}}\nfn decode(row:Row)->Result<Self> {{\nif row.values.len()!={} {{return Err(Error::Constraint(\"generated row arity mismatch\".into()));}}\nlet mut values=row.values.into_vec().into_iter();\nOk(Self {{",def.roles.len()).unwrap();
                for role in &def.roles {
                    let decoded = rust_decode(
                        &role.data_type,
                        "values.next()",
                        &format!("invalid {}.{}", def.name, role.name),
                    );
                    writeln!(rust, "{}:{decoded},", rust_name(&role.name)).unwrap();
                }
                rust.push_str("})\n}\n}\n");
                writeln!(ts,"}}\nexport type {tn}Insert = {tn};\nexport type {tn}Update = Partial<{tn}>;\nexport type {tn}Read = {{ id: EntityId<{:?}>; data: {tn} }};\nexport const {tn}: Descriptor<{tn}, {:?}> = defineRecord({:?}, {:?}, {:?}, {{",def.name,def.name,def.name,def.id.to_string(),def.fingerprint()).unwrap();
                for role in &def.roles {
                    writeln!(
                        ts,
                        "[{:?}]: {{kind: {:?}{}}},",
                        role.name,
                        variant(&role.data_type),
                        {
                            let mut details = match role.data_type.base_type() {
                                DataType::Ref(target) => format!(",target: {target:?}"),
                                DataType::Decimal { scale } => format!(",scale: {scale}"),
                                _ => String::new(),
                            };
                            if role.data_type.is_nullable() {
                                details.push_str(",nullable: true");
                            }
                            if role.on_delete == OnDelete::Cascade {
                                details.push_str(",onDelete: \"cascade\"");
                            }
                            if role.deferrable {
                                details.push_str(",deferrable: true");
                            }
                            let checks: Vec<_> = def
                                .checks
                                .iter()
                                .filter(|c| c.role() == role.name)
                                .map(|c| match c {
                                    Check::IntRange { min, max, .. } => {
                                        format!("{{kind: \"IntRange\", min: {min}n, max: {max}n}}")
                                    }
                                    Check::ByteLength { min, max, .. } => {
                                        format!("{{kind: \"ByteLength\", min: {min}, max: {max}}}")
                                    }
                                })
                                .collect();
                            if !checks.is_empty() {
                                write!(details, ", checks: [{}]", checks.join(",")).unwrap();
                            }
                            details
                        }
                    )
                    .unwrap();
                }
                ts.push_str("});\n");
                if let Some(alias) = aliases.get(&def.name) {
                    writeln!(ts, "// Unambiguous short name; the qualified export remains stable.\nexport {{ {tn} as {alias} }};\nexport type {alias}Insert = {tn}Insert;\nexport type {alias}Update = {tn}Update;\nexport type {alias}Read = {tn}Read;").unwrap();
                }
            }
            rust.push_str("}\n");
        }
        Ok(GeneratedCode {
            rust,
            typescript: ts,
        })
    }
}
