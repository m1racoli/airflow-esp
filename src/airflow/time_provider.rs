use airflow_common::datetime::{TimeProvider, UtcDateTime};
use embassy_sync::{blocking_mutex::raw::RawMutex, watch::Watch};
use embassy_time::{Duration, Instant};

// #[derive(Clone)]
pub struct OffsetTimeProvider<'a, M, const N: usize>
where
    M: RawMutex,
{
    watch: &'a Watch<M, i64, N>,
}

impl<'a, M, const N: usize> OffsetTimeProvider<'a, M, N>
where
    M: RawMutex,
{
    pub fn new(watch: &'a Watch<M, i64, N>) -> Self {
        Self { watch }
    }
}

impl<'a, M, const N: usize> TimeProvider for OffsetTimeProvider<'a, M, N>
where
    M: RawMutex,
{
    fn now(&self) -> UtcDateTime {
        let offset: i64 = self.watch.try_get().unwrap_or_default();
        let now = Instant::now();
        let now_corrected = now + Duration::from_micros(offset as u64);
        UtcDateTime::from_timestamp_micros(now_corrected.as_micros() as i64).unwrap()
    }
}

impl<'a, M, const N: usize> Clone for OffsetTimeProvider<'a, M, N>
where
    M: RawMutex,
{
    fn clone(&self) -> Self {
        Self { watch: self.watch }
    }
}
