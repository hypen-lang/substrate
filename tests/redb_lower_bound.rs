//! Compatibility tests for the narrow, vendored read-only lower-bound API.
use redb::backends::InMemoryBackend;
use redb::{Database, ReadableDatabase, ReadableTableMetadata, StorageBackend, TableDefinition};
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

const NUMBERS: TableDefinition<i64, &[u8]> = TableDefinition::new("numbers");
const BYTES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bytes");

#[test]
fn lower_bound_matches_range_and_model_across_leaf_and_subtree_boundaries() {
    let db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let mut expected = BTreeMap::new();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(NUMBERS).unwrap();
        for n in (-2500i64..=2500).filter(|n| n % 7 != 0) {
            let value = n.to_be_bytes().repeat(16);
            table.insert(n * 3, value.as_slice()).unwrap();
            expected.insert(n * 3, value);
        }
    }
    tx.commit().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(NUMBERS).unwrap();
    assert!(table.stats().unwrap().tree_height() > 1);
    // Every gap is checked, including gaps at leaf and parent boundaries.
    // Signed integer ordering deliberately differs from encoded byte order.
    for query in std::iter::once(i64::MIN)
        .chain(-7502..=7502)
        .chain([i64::MAX])
    {
        let found = table
            .first_at_or_after(query)
            .unwrap()
            .map(|(k, v)| (k.value(), v.value().to_vec()));
        let range = table
            .range(query..)
            .unwrap()
            .next()
            .transpose()
            .unwrap()
            .map(|(k, v)| (k.value(), v.value().to_vec()));
        let model = expected.range(query..).next().map(|(k, v)| (*k, v.clone()));
        assert_eq!(found, model, "query {query}");
        assert_eq!(found, range, "range query {query}");
    }
}

#[test]
fn lower_bound_handles_empty_trees_variable_keys_and_prefixes() {
    let db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let tx = db.begin_write().unwrap();
    tx.open_table(BYTES).unwrap();
    tx.commit().unwrap();
    let empty = db.begin_read().unwrap().open_table(BYTES).unwrap();
    assert!(empty.first_at_or_after(b"".as_slice()).unwrap().is_none());
    let mut keys = vec![
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 255],
        vec![1],
        vec![255],
        vec![255; 4096],
    ];
    keys.extend((0..1000u64).map(|n| format!("key-{n:08}").into_bytes()));
    keys.sort();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(BYTES).unwrap();
        for key in &keys {
            table.insert(key.as_slice(), key.as_slice()).unwrap();
        }
    }
    tx.commit().unwrap();
    let table = db.begin_read().unwrap().open_table(BYTES).unwrap();
    for key in keys.iter().cloned().chain([
        vec![0, 1],
        vec![254],
        vec![255; 4097],
        b"key-00000100x".to_vec(),
    ]) {
        let expected = keys
            .iter()
            .find(|candidate| candidate.as_slice() >= key.as_slice());
        let actual = table
            .first_at_or_after(key.as_slice())
            .unwrap()
            .map(|(key, value)| {
                assert_eq!(key.value(), value.value());
                key.value().to_vec()
            });
        assert_eq!(actual.as_ref(), expected);
    }
    assert!(empty.first_at_or_after(b"".as_slice()).unwrap().is_none());
}

#[test]
fn lower_bound_guards_and_snapshot_survive_overwrites_deletes_and_page_reuse() {
    let db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let tx = db.begin_write().unwrap();
    tx.open_table(NUMBERS)
        .unwrap()
        .insert(10, b"original".as_slice())
        .unwrap();
    tx.commit().unwrap();
    let old = db.begin_read().unwrap().open_table(NUMBERS).unwrap();
    let (key_guard, value_guard) = old.first_at_or_after(9).unwrap().unwrap();
    for revision in 0..3 {
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(NUMBERS).unwrap();
            table.remove(10).unwrap();
            for n in 11..1011 {
                table.insert(n, vec![revision; 256].as_slice()).unwrap();
            }
        }
        tx.commit().unwrap();
        let (key, value) = old.first_at_or_after(9).unwrap().unwrap();
        assert_eq!(key.value(), 10);
        assert_eq!(value.value(), b"original");
        let fresh = db.begin_read().unwrap().open_table(NUMBERS).unwrap();
        assert_eq!(fresh.first_at_or_after(9).unwrap().unwrap().0.value(), 11);
    }
    drop(old);
    assert_eq!(key_guard.value(), 10);
    assert_eq!(value_guard.value(), b"original");
}

#[derive(Debug)]
struct FailingReads {
    inner: InMemoryBackend,
    fail: Arc<AtomicBool>,
}
impl StorageBackend for FailingReads {
    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, offset: u64, bytes: &mut [u8]) -> std::io::Result<()> {
        if self.fail.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("lower bound read sentinel"));
        }
        self.inner.read(offset, bytes)
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.inner.sync_data()
    }
    fn write(&self, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.write(offset, bytes)
    }
}

#[test]
fn lower_bound_propagates_backend_read_errors_instead_of_missing_rows() {
    let fail = Arc::new(AtomicBool::new(false));
    let db = Database::builder()
        .set_cache_size(0)
        .create_with_backend(FailingReads {
            inner: InMemoryBackend::new(),
            fail: fail.clone(),
        })
        .unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(NUMBERS).unwrap();
        for n in 0..500 {
            table.insert(n, vec![1; 1024].as_slice()).unwrap();
        }
    }
    tx.commit().unwrap();
    let table = db.begin_read().unwrap().open_table(NUMBERS).unwrap();
    assert!(table.stats().unwrap().tree_height() > 1);
    fail.store(true, Ordering::Relaxed);
    let error = match table.first_at_or_after(250) {
        Err(error) => error,
        Ok(_) => panic!("backend failure must not become a missing or returned row"),
    };
    assert!(error.to_string().contains("lower bound read sentinel"));
    fail.store(false, Ordering::Relaxed);
    // redb poisons the backend after an I/O failure; clearing the injected
    // fault cannot make this handle usable again. Reopening is required.
    assert!(matches!(
        table.first_at_or_after(250),
        Err(redb::StorageError::PreviousIo)
    ));
}

#[test]
fn range_guards_preserve_mixed_direction_bounds_and_pinned_values() {
    use std::collections::VecDeque;
    use std::ops::Bound::{Excluded, Included, Unbounded};

    let db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let model = (-200i64..=200)
        .filter(|n| n % 7 != 0)
        .map(|n| (n, n.to_be_bytes().repeat(16)))
        .collect::<BTreeMap<_, _>>();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(NUMBERS).unwrap();
        for (key, value) in &model {
            table.insert(*key, value.as_slice()).unwrap();
        }
    }
    tx.commit().unwrap();
    let pinned = db.begin_read().unwrap().open_table(NUMBERS).unwrap();
    assert!(pinned.stats().unwrap().tree_height() > 1);
    for bounds in [
        (Unbounded, Unbounded),
        (Included(-57), Excluded(64)),
        (Excluded(-56), Included(63)),
        (Included(3), Included(3)),
        (Excluded(3), Included(3)),
        (Included(201), Unbounded),
    ] {
        for mode in 0..3 {
            let mut expected = model.range(bounds).collect::<VecDeque<_>>();
            let mut range = pinned.range(bounds).unwrap();
            let mut held = Vec::new();
            let mut step = 0;
            while !expected.is_empty() {
                let reverse = mode == 1 || (mode == 2 && step % 2 == 1);
                let expected = if reverse {
                    expected.pop_back()
                } else {
                    expected.pop_front()
                }
                .unwrap();
                let actual = if reverse {
                    range.next_back()
                } else {
                    range.next()
                }
                .unwrap()
                .unwrap();
                assert_eq!(actual.0.value(), *expected.0);
                assert_eq!(actual.1.value(), expected.1.as_slice());
                held.push(actual);
                step += 1;
            }
            assert!(range.next().is_none());
            assert!(range.next_back().is_none());
            drop(range);
            // Returned guards stay pinned while a different write reuses the
            // logical keys. No iterator ownership shortcut may invalidate them.
            let tx = db.begin_write().unwrap();
            tx.open_table(NUMBERS)
                .unwrap()
                .insert(3, b"new".as_slice())
                .unwrap();
            tx.commit().unwrap();
            for (key, value) in held {
                assert_eq!(value.value(), model[&key.value()].as_slice());
            }
        }
    }
}

#[test]
fn borrowed_range_matches_model_with_bounds_breaks_and_mixed_iteration() {
    use std::collections::VecDeque;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    use std::ops::ControlFlow;

    let db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let model = (-1200i64..=1200)
        .filter(|n| n % 7 != 0)
        .map(|n| (n, n.to_be_bytes().repeat(16)))
        .collect::<BTreeMap<_, _>>();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(NUMBERS).unwrap();
        for (key, value) in &model {
            table.insert(*key, value.as_slice()).unwrap();
        }
    }
    tx.commit().unwrap();
    let table = db.begin_read().unwrap().open_table(NUMBERS).unwrap();
    assert!(table.stats().unwrap().tree_height() > 1);
    for bounds in [
        (Unbounded, Unbounded),
        (Included(-57), Excluded(64)),
        (Excluded(-56), Included(63)),
        (Included(3), Included(3)),
        (Excluded(3), Included(3)),
        (Included(1201), Unbounded),
        (Unbounded, Excluded(-1200)),
    ] {
        for batch in [1, 7, 127, usize::MAX] {
            let mut remaining = model.range(bounds).collect::<VecDeque<_>>();
            let mut range = table.range(bounds).unwrap();
            // Consume both ends before invoking the new API, as engine callers
            // may inspect a prefix before choosing the streaming fast path.
            for reverse in [false, true] {
                let expected = if reverse {
                    remaining.pop_back()
                } else {
                    remaining.pop_front()
                };
                let actual = if reverse {
                    range.next_back()
                } else {
                    range.next()
                };
                assert_eq!(
                    actual
                        .transpose()
                        .unwrap()
                        .map(|(k, v)| (k.value(), v.value().to_vec())),
                    expected.map(|(k, v)| (*k, v.clone()))
                );
            }
            while !remaining.is_empty() {
                let mut count = 0;
                let outcome = range
                    .visit_borrowed(|key, value| {
                        let (expected_key, expected_value) = remaining.pop_front().unwrap();
                        assert_eq!(key, *expected_key);
                        assert_eq!(value, expected_value.as_slice());
                        count += 1;
                        if count == batch {
                            ControlFlow::Break(key)
                        } else {
                            ControlFlow::Continue(())
                        }
                    })
                    .unwrap();
                if count < batch {
                    assert_eq!(outcome, ControlFlow::Continue(()));
                    assert!(remaining.is_empty());
                } else {
                    assert!(matches!(outcome, ControlFlow::Break(_)));
                }
                if let Some((key, value)) = remaining.pop_back() {
                    let (actual_key, actual_value) = range.next_back().unwrap().unwrap();
                    assert_eq!(actual_key.value(), *key);
                    assert_eq!(actual_value.value(), value.as_slice());
                }
            }
            assert_eq!(
                range
                    .visit_borrowed(|_, _| -> ControlFlow<()> {
                        panic!("exhausted range invoked callback")
                    })
                    .unwrap(),
                ControlFlow::Continue(())
            );
            assert!(range.next().is_none());
            assert!(range.next_back().is_none());
        }
    }
}

#[test]
fn borrowed_range_handles_empty_and_variable_key_tables() {
    use std::ops::ControlFlow;

    let db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let tx = db.begin_write().unwrap();
    tx.open_table(BYTES).unwrap();
    tx.commit().unwrap();
    let empty = db.begin_read().unwrap().open_table(BYTES).unwrap();
    assert_eq!(
        empty
            .range::<&[u8]>(..)
            .unwrap()
            .visit_borrowed(|_, _| -> ControlFlow<()> { panic!("empty range invoked callback") })
            .unwrap(),
        ControlFlow::Continue(())
    );
    let mut model = BTreeMap::new();
    for n in 0..800 {
        model.insert(format!("key-{n:08}").into_bytes(), vec![n as u8; n % 67]);
    }
    model.insert(vec![], vec![]);
    model.insert(vec![0], vec![0]);
    model.insert(vec![255; 4096], vec![255; 4096]);
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(BYTES).unwrap();
        for (key, value) in &model {
            table.insert(key.as_slice(), value.as_slice()).unwrap();
        }
    }
    tx.commit().unwrap();
    let table = db.begin_read().unwrap().open_table(BYTES).unwrap();
    let mut expected = model.iter();
    let outcome = table
        .range::<&[u8]>(..)
        .unwrap()
        .visit_borrowed(|key, value| {
            let (expected_key, expected_value) = expected.next().unwrap();
            assert_eq!(key, expected_key.as_slice());
            assert_eq!(value, expected_value.as_slice());
            ControlFlow::<()>::Continue(())
        })
        .unwrap();
    assert_eq!(outcome, ControlFlow::Continue(()));
    assert!(expected.next().is_none());
}

#[test]
fn borrowed_range_retains_snapshot_and_owning_guards_across_commits() {
    use std::ops::ControlFlow;

    let db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(NUMBERS).unwrap();
        for n in 0..2000i64 {
            table
                .insert(n, n.to_be_bytes().repeat(32).as_slice())
                .unwrap();
        }
    }
    tx.commit().unwrap();
    let old = db.begin_read().unwrap().open_table(NUMBERS).unwrap();
    let mut range = old.range::<i64>(..).unwrap();
    let held = range.next().unwrap().unwrap();
    let mut cloned = range.clone();
    let mut expected = 1;
    let outcome = range
        .visit_borrowed(|key, value| {
            assert_eq!(key, expected);
            assert_eq!(value, key.to_be_bytes().repeat(32));
            expected += 1;
            if key == 50 {
                // A callback may commit to another transaction. Its current page
                // borrow and the entire remaining range still belong to the old read.
                let tx = db.begin_write().unwrap();
                {
                    let mut table = tx.open_table(NUMBERS).unwrap();
                    for n in 0..2000i64 {
                        table.remove(n).unwrap();
                        table.insert(n + 4000, b"replacement".as_slice()).unwrap();
                    }
                }
                tx.commit().unwrap();
                assert_eq!(value, key.to_be_bytes().repeat(32));
                ControlFlow::Break("application stop")
            } else {
                ControlFlow::Continue(())
            }
        })
        .unwrap();
    assert_eq!(outcome, ControlFlow::Break("application stop"));
    drop(old);
    assert_eq!(range.next().unwrap().unwrap().0.value(), 51);
    assert_eq!(range.next_back().unwrap().unwrap().0.value(), 1999);
    let mut expected = 52;
    let completion = range
        .visit_borrowed(|key, value| {
            assert_eq!(key, expected);
            assert_eq!(value, key.to_be_bytes().repeat(32));
            expected += 1;
            ControlFlow::<()>::Continue(())
        })
        .unwrap();
    assert_eq!(completion, ControlFlow::Continue(()));
    assert_eq!(expected, 1999);
    assert_eq!(cloned.next().unwrap().unwrap().0.value(), 1);
    assert_eq!(cloned.next_back().unwrap().unwrap().0.value(), 1999);
    assert_eq!(held.0.value(), 0);
    assert_eq!(held.1.value(), 0i64.to_be_bytes().repeat(32));
    let fresh = db.begin_read().unwrap().open_table(NUMBERS).unwrap();
    assert_eq!(
        fresh
            .range::<i64>(..)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0
            .value(),
        4000
    );
}

#[test]
fn borrowed_range_consumes_entry_before_callback_panic() {
    use std::ops::ControlFlow;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(NUMBERS).unwrap();
        for n in 0..3 {
            table.insert(n, b"value".as_slice()).unwrap();
        }
    }
    tx.commit().unwrap();
    let table = db.begin_read().unwrap().open_table(NUMBERS).unwrap();
    let mut range = table.range::<i64>(..).unwrap();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _ = range.visit_borrowed(|key, _| -> ControlFlow<()> {
                assert_eq!(key, 0);
                panic!("callback sentinel");
            });
        }))
        .is_err()
    );
    assert_eq!(range.next().unwrap().unwrap().0.value(), 1);
    assert_eq!(range.next_back().unwrap().unwrap().0.value(), 2);
    assert!(range.next().is_none());
}

#[test]
fn borrowed_range_propagates_read_errors_after_exact_valid_prefix() {
    use std::ops::ControlFlow;

    let fail = Arc::new(AtomicBool::new(false));
    let db = Database::builder()
        .set_cache_size(0)
        .create_with_backend(FailingReads {
            inner: InMemoryBackend::new(),
            fail: fail.clone(),
        })
        .unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(NUMBERS).unwrap();
        for n in 0..500i64 {
            table
                .insert(n, n.to_be_bytes().repeat(128).as_slice())
                .unwrap();
        }
    }
    tx.commit().unwrap();
    let table = db.begin_read().unwrap().open_table(NUMBERS).unwrap();
    assert!(table.stats().unwrap().tree_height() > 1);
    let mut borrowed = table.range::<i64>(..).unwrap();
    let mut ordinary = borrowed.clone();
    fail.store(true, Ordering::Relaxed);
    let mut seen = Vec::new();
    let error = borrowed
        .visit_borrowed(|key, value| {
            assert_eq!(value, key.to_be_bytes().repeat(128));
            seen.push(key);
            ControlFlow::<()>::Continue(())
        })
        .unwrap_err();
    assert!(error.to_string().contains("lower bound read sentinel"));
    assert!(!seen.is_empty()); // The first leaf was pinned when constructing range.
    assert!(seen.len() < 500);
    assert_eq!(seen, (0..seen.len() as i64).collect::<Vec<_>>());
    for expected in seen {
        assert_eq!(ordinary.next().unwrap().unwrap().0.value(), expected);
    }
    assert!(matches!(
        ordinary.next().unwrap(),
        Err(redb::StorageError::PreviousIo)
    ));
    // Advancing failed after taking the left cursor, exactly as next() does.
    // The callback API must not report an extra row after that failure.
    assert_eq!(
        borrowed
            .visit_borrowed(|_, _| -> ControlFlow<()> {
                panic!("callback after failed forward cursor")
            })
            .unwrap(),
        ControlFlow::Continue(())
    );
    fail.store(false, Ordering::Relaxed);
    assert!(matches!(
        table.range::<i64>(..),
        Err(redb::StorageError::PreviousIo)
    ));
}
