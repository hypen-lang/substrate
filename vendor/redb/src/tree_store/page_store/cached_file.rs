use crate::tree_store::page_store::base::PageHint;
use crate::tree_store::page_store::lru_cache::LRUCache;
use crate::{CacheStats, DatabaseError, Result, StorageBackend, StorageError};
use std::ops::{Index, IndexMut};
use std::slice::SliceIndex;
#[cfg(feature = "cache_metrics")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

pub(super) struct WritablePage {
    buffer: Arc<Mutex<LRUWriteCache>>,
    offset: u64,
    data: Arc<Box<[u8]>>,
}

impl WritablePage {
    pub(super) fn mem(&self) -> &[u8] {
        &self.data
    }

    pub(super) fn mem_mut(&mut self) -> &mut [u8] {
        Arc::get_mut(&mut self.data).unwrap()
    }
}

impl Drop for WritablePage {
    fn drop(&mut self) {
        self.buffer
            .lock()
            .unwrap()
            .return_value(self.offset, self.data.clone());
    }
}

impl<I: SliceIndex<[u8]>> Index<I> for WritablePage {
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.mem().index(index)
    }
}

impl<I: SliceIndex<[u8]>> IndexMut<I> for WritablePage {
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.mem_mut().index_mut(index)
    }
}

#[derive(Default)]
struct LRUWriteCache {
    cache: LRUCache<Option<Arc<Box<[u8]>>>>,
}

impl LRUWriteCache {
    fn new() -> Self {
        Self {
            cache: Default::default(),
        }
    }

    fn insert(&mut self, key: u64, value: Arc<Box<[u8]>>) {
        assert!(self.cache.insert(key, Some(value)).is_none());
    }

    fn get(&self, key: u64) -> Option<&Arc<Box<[u8]>>> {
        self.cache.get(key).map(|x| x.as_ref().unwrap())
    }

    fn remove(&mut self, key: u64) -> Option<Arc<Box<[u8]>>> {
        if let Some(value) = self.cache.remove(key) {
            assert!(value.is_some());
            return value;
        }
        None
    }

    fn return_value(&mut self, key: u64, value: Arc<Box<[u8]>>) {
        assert!(self.cache.get_mut(key).unwrap().replace(value).is_none());
    }

    fn take_value(&mut self, key: u64) -> Option<Arc<Box<[u8]>>> {
        if let Some(value) = self.cache.get_mut(key) {
            let result = value.take().unwrap();
            return Some(result);
        }
        None
    }

    fn pop_lowest_priority(&mut self) -> Option<(u64, Arc<Box<[u8]>>)> {
        for _ in 0..self.cache.len() {
            if let Some((k, v)) = self.cache.pop_lowest_priority() {
                if let Some(v_inner) = v {
                    return Some((k, v_inner));
                }

                // Value is borrowed by take_value(). We can't evict it, so put it back.
                self.cache.insert(k, v);
            } else {
                break;
            }
        }
        None
    }

    fn clear(&mut self) {
        self.cache.clear();
    }
}

#[derive(Debug)]
struct CheckedBackend {
    file: Box<dyn StorageBackend>,
    io_failed: AtomicBool,
    closed: AtomicBool,
}

impl CheckedBackend {
    fn new(file: Box<dyn StorageBackend>) -> Self {
        Self {
            file,
            io_failed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }

    fn check_failure(&self) -> Result<()> {
        if self.io_failed.load(Ordering::Acquire) {
            if self.closed.load(Ordering::Acquire) {
                Err(StorageError::DatabaseClosed)
            } else {
                Err(StorageError::PreviousIo)
            }
        } else {
            Ok(())
        }
    }

    fn close(&self) -> Result {
        self.closed.store(true, Ordering::Release);
        self.io_failed.store(true, Ordering::Release);
        self.file.close()?;

        Ok(())
    }

    fn len(&self) -> Result<u64> {
        self.check_failure()?;
        let result = self.file.len();
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<()> {
        self.check_failure()?;
        let result = self.file.read(offset, out);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.check_failure()?;
        let result = self.file.set_len(len);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn sync_data(&self) -> Result<()> {
        self.check_failure()?;
        let result = self.file.sync_data();
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.check_failure()?;
        let result = self.file.write(offset, data);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }
}

pub(super) struct PagedCachedFile {
    file: CheckedBackend,
    page_size: u64,
    max_read_cache_bytes: usize,
    read_cache_bytes: AtomicUsize,
    max_write_buffer_bytes: usize,
    write_buffer_bytes: AtomicUsize,
    #[cfg(feature = "cache_metrics")]
    reads_total: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    reads_hits: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    writes_total: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    writes_hits: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    evictions: AtomicU64,
    read_cache: Vec<RwLock<LRUCache<Arc<Box<[u8]>>>>>,
    // TODO: maybe move this cache to WriteTransaction?
    write_buffer: Arc<Mutex<LRUWriteCache>>,
}

impl PagedCachedFile {
    pub(super) fn new(
        file: Box<dyn StorageBackend>,
        page_size: u64,
        max_read_cache_bytes: usize,
        max_write_buffer_bytes: usize,
    ) -> Result<Self, DatabaseError> {
        let read_cache = (0..Self::lock_stripes())
            .map(|_| RwLock::new(LRUCache::new()))
            .collect();

        Ok(Self {
            file: CheckedBackend::new(file),
            page_size,
            max_read_cache_bytes,
            read_cache_bytes: AtomicUsize::new(0),
            max_write_buffer_bytes,
            write_buffer_bytes: AtomicUsize::new(0),
            #[cfg(feature = "cache_metrics")]
            reads_total: Default::default(),
            #[cfg(feature = "cache_metrics")]
            reads_hits: Default::default(),
            #[cfg(feature = "cache_metrics")]
            writes_total: Default::default(),
            #[cfg(feature = "cache_metrics")]
            writes_hits: Default::default(),
            #[cfg(feature = "cache_metrics")]
            evictions: Default::default(),
            read_cache,
            write_buffer: Arc::new(Mutex::new(LRUWriteCache::new())),
        })
    }

    #[allow(clippy::unused_self)]
    pub(crate) fn cache_stats(&self) -> CacheStats {
        #[cfg(not(feature = "cache_metrics"))]
        {
            CacheStats {
                evictions: 0,
                read_hits: 0,
                read_misses: 0,
                write_hits: 0,
                write_misses: 0,
                used_bytes: 0,
            }
        }

        #[cfg(feature = "cache_metrics")]
        {
            let read_hits = self.reads_hits.load(Ordering::Acquire);
            let read_total = self.reads_total.load(Ordering::Acquire);
            let write_hits = self.writes_hits.load(Ordering::Acquire);
            let write_total = self.writes_total.load(Ordering::Acquire);
            let read_bytes = self.read_cache_bytes.load(Ordering::Acquire);
            let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
            CacheStats {
                evictions: self.evictions.load(Ordering::Acquire),
                read_hits,
                read_misses: read_total - read_hits,
                write_hits,
                write_misses: write_total - write_hits,
                used_bytes: read_bytes + write_bytes,
            }
        }
    }

    pub(crate) fn close(&self) -> Result {
        self.file.close()
    }

    pub(crate) fn check_io_errors(&self) -> Result {
        self.file.check_failure()
    }

    pub(crate) fn raw_file_len(&self) -> Result<u64> {
        self.file.len()
    }

    const fn lock_stripes() -> u64 {
        131
    }

    fn flush_write_buffer(&self) -> Result {
        let mut write_buffer = self.write_buffer.lock().unwrap();

        for (offset, buffer) in write_buffer.cache.iter() {
            self.file.write(*offset, buffer.as_ref().unwrap())?;
        }
        for (offset, buffer) in write_buffer.cache.iter_mut() {
            let buffer = buffer.take().unwrap();
            let cache_size = self
                .read_cache_bytes
                .fetch_add(buffer.len(), Ordering::AcqRel);

            if cache_size + buffer.len() <= self.max_read_cache_bytes {
                let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
                let mut lock = self.read_cache[cache_slot].write().unwrap();
                if let Some(replaced) = lock.insert(*offset, buffer) {
                    // A race could cause us to replace an existing buffer
                    self.read_cache_bytes
                        .fetch_sub(replaced.len(), Ordering::AcqRel);
                }
            } else {
                self.read_cache_bytes
                    .fetch_sub(buffer.len(), Ordering::AcqRel);
                break;
            }
        }
        self.write_buffer_bytes.store(0, Ordering::Release);
        write_buffer.clear();

        Ok(())
    }

    // Caller should invalidate all cached pages that are no longer valid
    pub(super) fn resize(&self, len: u64) -> Result {
        // TODO: be more fine-grained about this invalidation
        self.invalidate_cache_all();

        self.file.set_len(len)
    }

    pub(super) fn flush(&self) -> Result {
        self.flush_write_buffer()?;

        self.file.sync_data()
    }

    // Make writes visible to readers, but does not guarantee any durability
    pub(super) fn write_barrier(&self) -> Result {
        // TODO: non-durable commits would be much faster, if this did not issues writes to disk,
        // and instead just made the data visible to readers
        self.flush_write_buffer()
    }

    // Read directly from the file, ignoring any cached data
    pub(super) fn read_direct(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0; len];
        self.file.read(offset, &mut buffer)?;
        Ok(buffer)
    }

    fn read_direct_arc(&self, offset: u64, len: usize) -> Result<Arc<Box<[u8]>>> {
        // Keep the payload allocation exactly page-sized. Inline Arc<[u8]>
        // places its reference counts next to the bytes, which can push a 4 KiB
        // page into a larger allocator size class. The Vec starts initialized,
        // becomes a Box without copying, and is shared only after the read
        // succeeds. Retained guards own the same immutable payload.
        let buffer = self.read_direct(offset, len)?.into_boxed_slice();
        Ok(Arc::new(buffer))
    }

    fn read_reusing_evicted(
        &self,
        offset: u64,
        len: usize,
        cache_slot: usize,
    ) -> Result<Arc<Box<[u8]>>> {
        // A full cache would evict after allocating the replacement. Retire
        // one candidate first so an unpinned, equally sized allocation can be
        // reused instead. No spare pool is retained, and I/O holds no cache lock.
        let retired = if self
            .read_cache_bytes
            .load(Ordering::Acquire)
            .saturating_add(len)
            > self.max_read_cache_bytes
        {
            let mut cache = self.read_cache[cache_slot].write().unwrap();
            cache.pop_lowest_priority().map(|(_, page)| {
                self.read_cache_bytes
                    .fetch_sub(page.len(), Ordering::AcqRel);
                #[cfg(feature = "cache_metrics")]
                self.evictions.fetch_add(1, Ordering::Relaxed);
                page
            })
        } else {
            None
        };
        if let Some(mut page) = retired
            && page.len() == len
            && let Some(payload) = Arc::get_mut(&mut page)
        {
            // get_mut excludes both retained guards and weak owners. Keep the
            // same initialized, zero-filled backend contract as a fresh read;
            // failed partial reads never return or publish the recycled page.
            payload.fill(0);
            self.file.read(offset, payload)?;
            return Ok(page);
        }
        self.read_direct_arc(offset, len)
    }

    // Read with caching. Caller must not read overlapping ranges without first calling invalidate_cache().
    // Doing so will not cause UB, but is a logic error.
    pub(super) fn read(&self, offset: u64, len: usize, hint: PageHint) -> Result<Arc<Box<[u8]>>> {
        debug_assert_eq!(0, offset % self.page_size);
        #[cfg(feature = "cache_metrics")]
        self.reads_total.fetch_add(1, Ordering::AcqRel);

        if !matches!(hint, PageHint::Clean) {
            let lock = self.write_buffer.lock().unwrap();
            if let Some(cached) = lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.clone());
            }
        }

        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        {
            let read_lock = self.read_cache[cache_slot].read().unwrap();
            if let Some(cached) = read_lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.clone());
            }
        }

        let buffer = self.read_reusing_evicted(offset, len, cache_slot)?;
        let cache_size = self.read_cache_bytes.fetch_add(len, Ordering::AcqRel);
        let mut write_lock = self.read_cache[cache_slot].write().unwrap();
        let cache_size = if let Some(replaced) = write_lock.insert(offset, buffer.clone()) {
            // A race could cause us to replace an existing buffer
            self.read_cache_bytes
                .fetch_sub(replaced.len(), Ordering::AcqRel)
        } else {
            cache_size
        };
        let mut removed = 0;
        if cache_size + len > self.max_read_cache_bytes {
            while removed < len {
                if let Some((_, v)) = write_lock.pop_lowest_priority() {
                    #[cfg(feature = "cache_metrics")]
                    {
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                    removed += v.len();
                } else {
                    break;
                }
            }
        }
        if removed > 0 {
            self.read_cache_bytes.fetch_sub(removed, Ordering::AcqRel);
        }

        Ok(buffer)
    }

    // Discard pending writes to the given range
    pub(super) fn cancel_pending_write(&self, offset: u64, _len: usize) {
        assert_eq!(0, offset % self.page_size);
        if let Some(removed) = self.write_buffer.lock().unwrap().remove(offset) {
            self.write_buffer_bytes
                .fetch_sub(removed.len(), Ordering::Release);
        }
    }

    // Invalidate any caching of the given range. After this call overlapping reads of the range are allowed
    //
    // NOTE: Invalidating a cached region in subsections is permitted, as long as all subsections are invalidated
    pub(super) fn invalidate_cache(&self, offset: u64, len: usize) {
        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        let mut lock = self.read_cache[cache_slot].write().unwrap();
        if let Some(removed) = lock.remove(offset) {
            assert_eq!(len, removed.len());
            self.read_cache_bytes
                .fetch_sub(removed.len(), Ordering::AcqRel);
        }
    }

    pub(super) fn invalidate_cache_all(&self) {
        for cache_slot in 0..self.read_cache.len() {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            while let Some((_, removed)) = lock.pop_lowest_priority() {
                self.read_cache_bytes
                    .fetch_sub(removed.len(), Ordering::AcqRel);
            }
        }
    }

    // If overwrite is true, the page is initialized to zero
    // cache_policy takes the existing data as an argument and returns the priority. The priority should be stable and not change after WritablePage is dropped
    pub(super) fn write(&self, offset: u64, len: usize, overwrite: bool) -> Result<WritablePage> {
        assert_eq!(0, offset % self.page_size);
        let mut lock = self.write_buffer.lock().unwrap();

        // TODO: allow hint that page is known to be dirty and will not be in the read cache
        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        let existing = {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            if let Some(removed) = lock.remove(offset) {
                assert_eq!(
                    len,
                    removed.len(),
                    "cache inconsistency {len} != {} for offset {offset}",
                    removed.len()
                );
                self.read_cache_bytes
                    .fetch_sub(removed.len(), Ordering::AcqRel);
                Some(removed)
            } else {
                None
            }
        };

        let data = if let Some(removed) = lock.take_value(offset) {
            #[cfg(feature = "cache_metrics")]
            self.writes_hits.fetch_add(1, Ordering::AcqRel);
            removed
        } else {
            let previous = self.write_buffer_bytes.fetch_add(len, Ordering::AcqRel);
            if previous + len > self.max_write_buffer_bytes {
                let mut removed_bytes = 0;
                while removed_bytes < len {
                    if let Some((offset, buffer)) = lock.pop_lowest_priority() {
                        let removed_len = buffer.len();
                        let result = self.file.write(offset, &buffer);
                        if result.is_err() {
                            lock.insert(offset, buffer);
                        }
                        result?;
                        self.write_buffer_bytes
                            .fetch_sub(removed_len, Ordering::Release);
                        #[cfg(feature = "cache_metrics")]
                        {
                            self.evictions.fetch_add(1, Ordering::Relaxed);
                        }
                        removed_bytes += removed_len;
                    } else {
                        break;
                    }
                }
            }
            let result = if let Some(data) = existing {
                #[cfg(feature = "cache_metrics")]
                self.writes_hits.fetch_add(1, Ordering::AcqRel);
                data
            } else if overwrite {
                #[cfg(feature = "cache_metrics")]
                self.writes_hits.fetch_add(1, Ordering::AcqRel);
                Arc::new(vec![0; len].into_boxed_slice())
            } else {
                self.read_direct_arc(offset, len)?
            };
            lock.insert(offset, result);
            lock.take_value(offset).unwrap()
        };
        #[cfg(feature = "cache_metrics")]
        self.writes_total.fetch_add(1, Ordering::AcqRel);
        Ok(WritablePage {
            buffer: self.write_buffer.clone(),
            offset,
            data,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::StorageBackend;
    use crate::backends::InMemoryBackend;
    use crate::tree_store::PageHint;
    use crate::tree_store::page_store::cached_file::PagedCachedFile;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct ReadProbe {
        pointer: AtomicUsize,
        calls: AtomicUsize,
    }

    #[derive(Debug)]
    struct ProbeBackend {
        probe: Arc<ReadProbe>,
        fail: bool,
    }
    impl StorageBackend for ProbeBackend {
        fn len(&self) -> std::io::Result<u64> {
            Ok(4096)
        }
        fn read(&self, _: u64, out: &mut [u8]) -> std::io::Result<()> {
            self.probe.calls.fetch_add(1, Ordering::Relaxed);
            self.probe
                .pointer
                .store(out.as_ptr() as usize, Ordering::Relaxed);
            // Reading these bytes also checks that no uninitialized memory is
            // handed to a backend, including a backend that fails partway.
            assert!(out.iter().all(|byte| *byte == 0));
            for (index, byte) in out.iter_mut().enumerate() {
                *byte = index as u8;
                if self.fail && index == 7 {
                    return Err(std::io::Error::other("injected partial read"));
                }
            }
            Ok(())
        }
        fn set_len(&self, _: u64) -> std::io::Result<()> {
            Ok(())
        }
        fn sync_data(&self) -> std::io::Result<()> {
            Ok(())
        }
        fn write(&self, _: u64, _: &[u8]) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn cache_miss_fills_final_arc_allocation_and_hits_reuse_it() {
        let probe = Arc::new(ReadProbe::default());
        let backend = ProbeBackend {
            probe: probe.clone(),
            fail: false,
        };
        let file = PagedCachedFile::new(Box::new(backend), 128, 1024, 128).unwrap();
        let page = file.read(0, 128, PageHint::Clean).unwrap();
        assert_eq!(
            page.as_ptr() as usize,
            probe.pointer.load(Ordering::Relaxed)
        );
        assert_eq!(page.as_ref().as_ref(), (0..128u8).collect::<Vec<_>>());
        let cached = file.read(0, 128, PageHint::Clean).unwrap();
        assert!(Arc::ptr_eq(&page, &cached));
        assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
        assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 128);
        file.invalidate_cache(0, 128);
        assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 0);
        assert_eq!(page[127], 127); // Retained immutable guards remain usable.
        assert!(file.read_direct_arc(128, 0).unwrap().is_empty());
    }

    #[test]
    fn boxed_page_payload_is_not_copied_across_allocator_boundaries() {
        let probe = Arc::new(ReadProbe::default());
        let backend = ProbeBackend {
            probe: probe.clone(),
            fail: false,
        };
        let file = PagedCachedFile::new(Box::new(backend), 128, 131_072, 128).unwrap();
        for len in [
            0, 1, 15, 16, 127, 128, 511, 512, 4095, 4096, 4097, 8192, 16384, 65536,
        ] {
            let page = file.read_direct_arc(0, len).unwrap();
            assert_eq!(page.len(), len);
            assert_eq!(
                page.as_ptr() as usize,
                probe.pointer.load(Ordering::Relaxed)
            );
            assert!(
                page.iter()
                    .enumerate()
                    .all(|(index, byte)| *byte == index as u8)
            );
        }
        for len in [128, 512, 4096, 8192, 65536] {
            let page = file.read(0, len, PageHint::Clean).unwrap();
            assert_eq!(
                page.as_ptr() as usize,
                probe.pointer.load(Ordering::Relaxed)
            );
            assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), len);
            let hit = file.read(0, len, PageHint::Clean).unwrap();
            assert!(Arc::ptr_eq(&page, &hit));
            file.invalidate_cache(0, len);
            assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 0);
            assert_eq!(page[len - 1], (len - 1) as u8);
        }
    }

    #[test]
    fn partial_read_failure_does_not_publish_arc_or_charge_cache() {
        let probe = Arc::new(ReadProbe::default());
        let backend = ProbeBackend {
            probe: probe.clone(),
            fail: true,
        };
        let file = PagedCachedFile::new(Box::new(backend), 128, 1024, 128).unwrap();
        assert!(file.read(0, 128, PageHint::Clean).is_err());
        assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
        assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 0);
        assert!(
            file.read_cache
                .iter()
                .all(|cache| cache.read().unwrap().len() == 0)
        );
        assert!(matches!(
            file.read(0, 128, PageHint::Clean),
            Err(crate::StorageError::PreviousIo)
        ));
        assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
    }

    #[derive(Debug)]
    struct RecyclingBackend {
        probe: Arc<ReadProbe>,
        fail: Arc<AtomicBool>,
    }

    impl StorageBackend for RecyclingBackend {
        fn len(&self) -> std::io::Result<u64> {
            Ok(1 << 20)
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
            self.probe.calls.fetch_add(1, Ordering::Relaxed);
            self.probe
                .pointer
                .store(out.as_ptr() as usize, Ordering::Relaxed);
            assert!(out.iter().all(|byte| *byte == 0));
            for (index, byte) in out.iter_mut().enumerate() {
                *byte = (offset / 128) as u8;
                if self.fail.load(Ordering::Relaxed) && index == 7 {
                    return Err(std::io::Error::other("recycled partial read"));
                }
            }
            Ok(())
        }
        fn set_len(&self, _: u64) -> std::io::Result<()> {
            Ok(())
        }
        fn sync_data(&self) -> std::io::Result<()> {
            Ok(())
        }
        fn write(&self, _: u64, _: &[u8]) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn recycling_file(budget: usize) -> (PagedCachedFile, Arc<ReadProbe>, Arc<AtomicBool>) {
        let probe = Arc::new(ReadProbe::default());
        let fail = Arc::new(AtomicBool::new(false));
        let file = PagedCachedFile::new(
            Box::new(RecyclingBackend {
                probe: probe.clone(),
                fail: fail.clone(),
            }),
            128,
            budget,
            128,
        )
        .unwrap();
        (file, probe, fail)
    }

    #[test]
    fn full_cache_reuses_unpinned_payload_and_arc_without_extra_retention() {
        let (file, probe, _) = recycling_file(128);
        let first = file.read(0, 128, PageHint::Clean).unwrap();
        let payload = first.as_ptr() as usize;
        let owner = Arc::as_ptr(&first) as usize;
        drop(first);
        let offset = 128 * PagedCachedFile::lock_stripes();
        let second = file.read(offset, 128, PageHint::Clean).unwrap();
        assert_eq!(second.as_ptr() as usize, payload);
        assert_eq!(Arc::as_ptr(&second) as usize, owner);
        assert_eq!(probe.pointer.load(Ordering::Relaxed), payload);
        assert!(second.iter().all(|value| *value == 131));
        assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 128);
        let cached = file.read(offset, 128, PageHint::Clean).unwrap();
        assert!(Arc::ptr_eq(&second, &cached));
        assert_eq!(probe.calls.load(Ordering::Relaxed), 2);
        file.invalidate_cache(offset, 128);
        assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 0);
        assert!(second.iter().all(|value| *value == 131));
    }

    #[test]
    fn full_cache_never_reuses_payload_held_by_snapshot_guard() {
        let (file, _, _) = recycling_file(128);
        let first = file.read(0, 128, PageHint::Clean).unwrap();
        let second = file
            .read(128 * PagedCachedFile::lock_stripes(), 128, PageHint::Clean)
            .unwrap();
        assert_ne!(first.as_ptr(), second.as_ptr());
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(first.iter().all(|value| *value == 0));
        assert!(second.iter().all(|value| *value == 131));
        assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 128);
    }

    #[test]
    fn cache_recycling_does_not_repurpose_a_weakly_owned_page() {
        let (file, _, _) = recycling_file(128);
        let first = file.read(0, 128, PageHint::Clean).unwrap();
        let weak = Arc::downgrade(&first);
        drop(first);
        // The cache is now the sole strong owner, but the weak owner still
        // prevents mutating and repurposing that Arc allocation in place.
        {
            let held = weak.upgrade().unwrap();
            assert!(held.iter().all(|value| *value == 0));
        }
        let second = file
            .read(128 * PagedCachedFile::lock_stripes(), 128, PageHint::Clean)
            .unwrap();
        assert_ne!(Arc::as_ptr(&second), weak.as_ptr());
        assert!(weak.upgrade().is_none());
        assert!(second.iter().all(|value| *value == 131));
        assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 128);
    }

    #[test]
    fn cache_recycling_preserves_variable_lengths_and_disabled_cache_bounds() {
        for budget in [0, 128] {
            let (file, _, _) = recycling_file(budget);
            let first = file.read(0, 128, PageHint::Clean).unwrap();
            drop(first);
            let second = file
                .read(128 * PagedCachedFile::lock_stripes(), 256, PageHint::Clean)
                .unwrap();
            assert_eq!(second.len(), 256);
            assert!(second.iter().all(|value| *value == 131));
            assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 0);
            assert!(
                file.read_cache
                    .iter()
                    .all(|cache| cache.read().unwrap().len() == 0)
            );
        }
    }

    #[test]
    fn recycled_partial_read_failure_never_publishes_or_charges_cache() {
        let (file, probe, fail) = recycling_file(128);
        let first = file.read(0, 128, PageHint::Clean).unwrap();
        let pointer = first.as_ptr() as usize;
        drop(first);
        fail.store(true, Ordering::Relaxed);
        let offset = 128 * PagedCachedFile::lock_stripes();
        assert!(file.read(offset, 128, PageHint::Clean).is_err());
        assert_eq!(probe.pointer.load(Ordering::Relaxed), pointer);
        assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 0);
        assert!(
            file.read_cache
                .iter()
                .all(|cache| cache.read().unwrap().len() == 0)
        );
        fail.store(false, Ordering::Relaxed);
        assert!(matches!(
            file.read(offset, 128, PageHint::Clean),
            Err(crate::StorageError::PreviousIo)
        ));
        assert_eq!(probe.calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn concurrent_recycling_keeps_pinned_bytes_and_cache_accounting_consistent() {
        let (file, _, _) = recycling_file(256);
        let file = Arc::new(file);
        std::thread::scope(|scope| {
            for thread in 0..2 {
                let file = file.clone();
                scope.spawn(move || {
                    for iteration in 0..100 {
                        let slot = (iteration + thread) % 4;
                        let offset = slot * 128 * PagedCachedFile::lock_stripes();
                        let held = file.read(offset, 128, PageHint::Clean).unwrap();
                        std::thread::yield_now();
                        assert!(held.iter().all(|value| *value == (slot * 131) as u8));
                    }
                });
            }
        });
        // Check the live, quiescent accounting before invalidation can hide a
        // mismatched insertion/replacement/retirement charge.
        let retained_bytes: usize = file
            .read_cache
            .iter()
            .map(|cache| {
                cache
                    .read()
                    .unwrap()
                    .iter()
                    .map(|(_, page)| page.len())
                    .sum::<usize>()
            })
            .sum();
        assert_eq!(
            file.read_cache_bytes.load(Ordering::Acquire),
            retained_bytes
        );
        assert!(retained_bytes <= 256);
        for slot in 0..4 {
            file.invalidate_cache(slot * 128 * PagedCachedFile::lock_stripes(), 128);
        }
        assert_eq!(file.read_cache_bytes.load(Ordering::Acquire), 0);
        assert!(
            file.read_cache
                .iter()
                .all(|cache| cache.read().unwrap().len() == 0)
        );
    }

    #[test]
    fn cache_leak() {
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 1024, 128).unwrap();
        let cached_file = Arc::new(cached_file);

        let t1 = {
            let cached_file = cached_file.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    cached_file.read(0, 128, PageHint::None).unwrap();
                    cached_file.invalidate_cache(0, 128);
                }
            })
        };
        let t2 = {
            let cached_file = cached_file.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    cached_file.read(0, 128, PageHint::None).unwrap();
                    cached_file.invalidate_cache(0, 128);
                }
            })
        };

        t1.join().unwrap();
        t2.join().unwrap();
        cached_file.invalidate_cache(0, 128);
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 0);
    }
}
