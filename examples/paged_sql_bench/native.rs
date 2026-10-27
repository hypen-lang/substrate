//! Public native API controls. These never construct SQL or access engine
//! internals; the authenticated reader and pinned snapshot match SQL controls.
use super::{AnyResult, Db, S, Suite, id};
use substrate_liquid::{Row, Snapshot, Value, paged::PagedSnapshot};

pub(super) enum NativeSnapshot {
    Resident(Snapshot),
    Paged(PagedSnapshot),
}
impl NativeSnapshot {
    pub(super) fn new(db: &Db) -> AnyResult<Self> {
        match db {
            Db::Resident(db) => Ok(Self::Resident(db.session("reader").snapshot())),
            Db::Paged(db, _) => Ok(Self::Paged(db.session("reader")?.snapshot()?)),
            Db::Sqlite(_) => Err("native mode requires Substrate storage".into()),
        }
    }
    fn get(&self, name: &str, id: substrate_liquid::Id) -> AnyResult<Option<Row>> {
        Ok(match self {
            Self::Resident(snapshot) => snapshot.get(name, id)?,
            Self::Paged(snapshot) => snapshot.get(name, id)?,
        })
    }
    fn lookup_unique(&self, name: &str, role: &str, value: &Value) -> AnyResult<Option<Row>> {
        let mut rows = match self {
            Self::Resident(snapshot) => snapshot.lookup(name, role, value)?,
            Self::Paged(snapshot) => {
                let page = snapshot.lookup_page(name, role, value, None, 2)?;
                if page.next.is_some() {
                    return Err("unique benchmark lookup unexpectedly returned another page".into());
                }
                page.rows
            }
        };
        if rows.len() > 1 {
            return Err("unique benchmark lookup returned duplicate rows".into());
        }
        Ok(rows.pop())
    }
    fn visit(
        &self,
        equality: Option<i64>,
        rows: usize,
        mut visitor: impl FnMut(&Row) -> AnyResult<()>,
    ) -> AnyResult<()> {
        match self {
            Self::Resident(snapshot) => {
                // Today's public resident API eagerly materializes the result.
                // This allocation is intentionally included and reported.
                let rows = if let Some(value) = equality {
                    snapshot.lookup("app.Score", "score", &Value::Int(value))?
                } else {
                    snapshot.scan("app.Score")?
                };
                for row in rows {
                    visitor(&row)?;
                }
            }
            Self::Paged(snapshot) => {
                let mut after = None;
                for _ in 0..=rows {
                    let page = if let Some(value) = equality {
                        snapshot.lookup_page(
                            "app.Score",
                            "score",
                            &Value::Int(value),
                            after,
                            256,
                        )?
                    } else {
                        snapshot.scan_page("app.Score", after, 256)?
                    };
                    for row in &page.rows {
                        visitor(row)?;
                    }
                    let Some(next) = page.next else { return Ok(()) };
                    if after.is_some_and(|old| next <= old) {
                        return Err("native pagination failed to advance".into());
                    }
                    after = Some(next);
                }
                return Err("native pagination exceeded fixture source bound".into());
            }
        }
        Ok(())
    }
    fn aggregate(&self, case: &str, rows: usize, suite: Suite) -> AnyResult<Vec<Vec<S>>> {
        let grouped = matches!(case, "aggregate_grouped" | "aggregate_filtered_grouped");
        let equality = (case == "aggregate_equality").then_some((rows / 2) as i64 * 10);
        let lower = match case {
            "aggregate_range" | "aggregate_filtered_grouped" => Some((rows / 2) as i64 * 10),
            "aggregate_broad" => Some((rows / 10) as i64 * 10),
            _ => None,
        };
        let upper = (case == "aggregate_range").then_some((rows / 2 + rows / 100) as i64 * 10);
        let mut groups = std::collections::BTreeMap::<i64, (i64, i128)>::new();
        if !grouped {
            groups.insert(0, (0, 0));
        }
        let mut visited = 0usize;
        self.visit(equality, rows, |row| {
            visited = visited
                .checked_add(1)
                .ok_or("native source count overflow")?;
            if visited > rows {
                return Err("native source rows exceed fixture bound".into());
            }
            let Some(Value::Int(score)) = row.values.get(1) else {
                return Err("invalid native score field".into());
            };
            if lower.is_some_and(|n| *score < n) || upper.is_some_and(|n| *score >= n) {
                return Ok(());
            }
            let key = if grouped {
                let Some(Value::Int(bucket)) = row.values.get(2) else {
                    return Err("invalid native bucket field".into());
                };
                if !(0..suite.buckets() as i64).contains(bucket) {
                    return Err("native group exceeds fixture bounds".into());
                }
                *bucket
            } else {
                0
            };
            let state = groups.entry(key).or_default();
            state.0 = state.0.checked_add(1).ok_or("native COUNT overflow")?;
            state.1 = state
                .1
                .checked_add(i128::from(*score))
                .ok_or("native SUM overflow")?;
            Ok(())
        })?;
        groups
            .into_iter()
            .map(|(key, (count, sum))| {
                let mut row = Vec::with_capacity(if grouped { 3 } else { 2 });
                if grouped {
                    row.push(S::Integer(key));
                }
                row.push(S::Integer(count));
                row.push(if count == 0 {
                    S::Null
                } else {
                    S::Integer(i64::try_from(sum)?)
                });
                Ok(row)
            })
            .collect()
    }
    pub(super) fn read(
        &self,
        case: &str,
        n: usize,
        rows: usize,
        suite: Suite,
    ) -> AnyResult<Vec<Vec<S>>> {
        let index = if case.starts_with("changing_parameters") {
            (n * 7919) % rows
        } else {
            rows / 2
        };
        match case {
            "point" => self
                .get("core.Person", id(index))?
                .map(person_name)
                .transpose()
                .map(|row| row.into_iter().collect()),
            "equality" | "changing_parameters" | "changing_parameters_warm" => self
                .lookup_unique("core.Person", "number", &Value::Int(index as i64))?
                .map(person_name)
                .transpose()
                .map(|row| row.into_iter().collect()),
            "selective_join" => {
                let Some(person) =
                    self.lookup_unique("core.Person", "number", &Value::Int(index as i64))?
                else {
                    return Ok(vec![]);
                };
                let Some(score) =
                    self.lookup_unique("app.Score", "person", &Value::Ref(person.id))?
                else {
                    return Ok(vec![]);
                };
                let Some(Value::Int(score)) = score.values.get(1) else {
                    return Err("invalid native score row".into());
                };
                let mut row = person_name(person)?;
                row.push(S::Integer(*score));
                Ok(vec![row])
            }
            "aggregate"
            | "aggregate_equality"
            | "aggregate_range"
            | "aggregate_broad"
            | "aggregate_grouped"
            | "aggregate_filtered_grouped" => self.aggregate(case, rows, suite),
            _ => Err("unknown native benchmark case".into()),
        }
    }
}
fn person_name(person: Row) -> AnyResult<Vec<S>> {
    match person.values.into_vec().into_iter().next() {
        Some(Value::Text(name)) => Ok(vec![S::Text(name)]),
        _ => Err("invalid native person row".into()),
    }
}

pub(super) fn execution_path(case: &str) -> &'static str {
    match case {
        "point" => "native public get",
        "equality" | "changing_parameters" | "changing_parameters_warm" => {
            "native public equality lookup"
        }
        "selective_join" => "native public equality lookups + client join",
        "aggregate_equality" => "native public equality lookup + client fold",
        "aggregate" | "aggregate_grouped" => "native public scan + client fold",
        _ => "native public scan + client filter and fold",
    }
}
