use airflow_common::models::TaskInstanceKey;
use alloc::rc::Rc;
use core::{cell::RefCell, sync::atomic::Ordering};
use embassy_sync::{blocking_mutex::CriticalSectionMutex, lazy_lock::LazyLock};
use heapless::{FnvIndexMap, Vec};
use portable_atomic::AtomicUsize;
use tracing::{
    Collect, Metadata,
    span::{Attributes, Id, Record},
};
use tracing_core::span::Current;
use tracing_subscriber::registry::{LookupSpan, SpanData};

use crate::{RESOURCES, tracing::task_log::TaskInstanceKeyVisitor};

const REGISTRY_STORE_SIZE: usize = RESOURCES.tracing.registry_store_size as usize;
const SPAN_STACK_SIZE: usize = RESOURCES.tracing.span_stack_size as usize;

static REGISTRY_STORE: SpanStore = SpanStore::new();

struct SpanStore(LazyLock<RefCell<FnvIndexMap<usize, Rc<DataInner>, REGISTRY_STORE_SIZE>>>);

impl SpanStore {
    const fn new() -> Self {
        Self(LazyLock::new(|| RefCell::new(FnvIndexMap::new())))
    }

    fn get(&self, key: usize) -> Option<Rc<DataInner>> {
        self.0.get().borrow().get(&key).cloned()
    }

    fn insert(&self, key: usize, value: Rc<DataInner>) {
        self.0
            .get()
            .borrow_mut()
            .insert(key, value)
            .expect("duplicate span key");
    }

    fn remove(&self, key: usize) {
        self.0.get().borrow_mut().remove(&key);
    }

    fn get_ti_key(&self, id: &Id) -> Option<TaskInstanceKey> {
        let key = id.into_u64() as usize;
        self.0
            .get()
            .borrow()
            .get(&key)
            .and_then(|inner| inner.ti_key.clone())
    }
}

pub fn get_ti_key(id: &Id) -> Option<TaskInstanceKey> {
    REGISTRY_STORE.get_ti_key(id)
}

pub struct Registry {
    spans: &'static SpanStore,
    current_spans: CriticalSectionMutex<RefCell<SpanStack>>,
    next_id: AtomicUsize,
}

impl Registry {
    fn get(&self, id: &Id) -> Option<Rc<DataInner>> {
        let key = id.into_u64() as usize;
        self.spans.get(key)
    }

    fn new_key(&self) -> usize {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            spans: &REGISTRY_STORE,
            current_spans: CriticalSectionMutex::new(RefCell::new(SpanStack::new())),
            next_id: AtomicUsize::new(1),
        }
    }
}

impl Collect for Registry {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let parent = if attrs.is_root() {
            None
        } else if attrs.is_contextual() {
            self.current_span().id().map(|id| self.clone_span(id))
        } else {
            attrs.parent().map(|id| self.clone_span(id))
        };

        // we first check if the parent span has a TI key
        // because we care about earliest task context span,
        // in case the user creates their own task context span
        let ti_key = match &parent {
            Some(p) => self.get(p).and_then(|inner| inner.ti_key.clone()),
            None => {
                // parent span doesn't have a TI key, so let's see if this span has one
                let mut visitor = TaskInstanceKeyVisitor::default();
                attrs.record(&mut visitor);
                visitor.into()
            }
        };

        let key = self.new_key();
        let data = Rc::new(DataInner {
            metadata: attrs.metadata(),
            parent,
            ti_key,
        });
        self.spans.insert(key, data);
        Id::from_u64(key as u64)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, _event: &tracing::Event<'_>) {}

    fn enter(&self, id: &Id) {
        critical_section::with(|cs| {
            self.current_spans.borrow(cs).borrow_mut().push(id.clone());
        });
    }

    fn exit(&self, id: &Id) {
        critical_section::with(|cs| {
            self.current_spans.borrow(cs).borrow_mut().pop(id);
        });
    }

    fn current_span(&self) -> Current {
        critical_section::with(
            |cs| match self.current_spans.borrow(cs).borrow().current() {
                Some(id) => match self.get(id) {
                    Some(data) => Current::new(id.clone(), data.metadata),
                    None => Current::none(),
                },
                None => Current::none(),
            },
        )
    }

    fn try_close(&self, id: Id) -> bool {
        let key = id.into_u64() as usize;
        self.spans.remove(key);
        true
    }
}

impl<'a> LookupSpan<'a> for Registry {
    type Data = Data;

    fn span_data(&'a self, id: &Id) -> Option<Self::Data> {
        let key = id.into_u64() as usize;
        self.spans.get(key).map(|inner| Data { key, inner })
    }
}

struct SpanStack {
    stack: Vec<(Id, bool), SPAN_STACK_SIZE>,
}

impl SpanStack {
    fn new() -> Self {
        Self { stack: Vec::new() }
    }

    fn push(&mut self, id: Id) -> bool {
        let duplicate = self.stack.iter().any(|(i, _)| *i == id);
        self.stack.push((id, duplicate)).ok();
        duplicate
    }

    fn pop(&mut self, expected_id: &Id) -> bool {
        if let Some((idx, _)) = self
            .stack
            .iter()
            .enumerate()
            .rev()
            .find(|(_, (id, _))| id == expected_id)
        {
            let (_, duplicate) = self.stack.remove(idx);
            return !duplicate;
        }
        false
    }

    fn iter(&self) -> impl Iterator<Item = &Id> {
        self.stack
            .iter()
            .rev()
            .filter_map(|(id, duplicate)| if !*duplicate { Some(id) } else { None })
    }

    fn current(&self) -> Option<&Id> {
        self.iter().next()
    }
}

#[derive(Debug)]
struct DataInner {
    metadata: &'static Metadata<'static>,
    parent: Option<Id>,
    ti_key: Option<TaskInstanceKey>,
}

#[derive(Debug, Clone)]
pub struct Data {
    key: usize,
    inner: Rc<DataInner>,
}

impl<'a> SpanData<'a> for Data {
    fn id(&self) -> Id {
        Id::from_u64(self.key as u64)
    }

    fn metadata(&self) -> &'static Metadata<'static> {
        self.inner.metadata
    }

    fn parent(&self) -> Option<&Id> {
        self.inner.parent.as_ref()
    }
}
