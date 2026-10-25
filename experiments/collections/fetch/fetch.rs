// Dependency preparation only. Timed engine runs use a frozen full-source copy.
pub fn dependency_ready() -> imbl::OrdMap<u64, u64> {
    imbl::OrdMap::new()
}
