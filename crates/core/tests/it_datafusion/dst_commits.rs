//! Seeded simulation of concurrent commits.
//!
//! Writers load the same table version and each runs one operation. [`GatedStore`] holds every
//! commit put until [`Scheduler`] releases it, and the scheduler picks the order from a seed, so
//! a failing seed replays exactly. Each run is checked against [`State::apply`]: the table must
//! match some serial order of the commits that landed.
//!
//! `dst_concurrent_commits_smoke` runs a few seeds with the other tests. For a longer run:
//!
//! ```text
//! DST_SEEDS=1000 cargo test -p deltalake-core --features datafusion --test it_datafusion dst_ -- --ignored --nocapture
//! ```
//!
//! `DST_SEED=<n>` replays one seed, `DST_FAULTS=<percent>` fails commit puts, and
//! `DST_CREATE_PCT=<percent>` sets how often writers race to create the table.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionContext;
use deltalake_core::kernel::transaction::CommitProperties;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableBuilder, DeltaTableError};
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use rstest::rstest;
use tokio::sync::{Notify, oneshot};
use url::Url;

/// Key in `commitInfo` that records which simulated writer made a commit.
const WRITER_KEY: &str = "dst_writer";
const TABLE_URL: &str = "memory:///dst";

/// SplitMix64, so a seed means the same run regardless of the `rand` version.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

type Rows = Vec<(i64, i64)>;

/// One operation a simulated writer runs against the table.
#[derive(Clone, Debug)]
enum Op {
    /// Write to a table that does not exist yet, with `SaveMode::ErrorIfExists`.
    Create {
        rows: Rows,
        name: String,
    },
    Append {
        rows: Rows,
    },
    Overwrite {
        rows: Rows,
    },
    /// `DELETE WHERE value < below`
    Delete {
        below: i64,
    },
    /// `UPDATE SET value = value + add WHERE value < below`
    Update {
        below: i64,
        add: i64,
    },
    /// Upsert by `id`.
    Merge {
        rows: Rows,
    },
    Optimize,
    /// `CHECK (value < below)`
    AddConstraint {
        name: String,
        below: i64,
    },
}

/// The parts of a table the check compares.
#[derive(Clone, Debug, PartialEq, Eq)]
struct State {
    exists: bool,
    name: Option<String>,
    /// Sorted `(id, value)` pairs, kept as a multiset so duplicated rows show up.
    rows: Rows,
    /// Constraint name to its `value < bound` bound.
    constraints: BTreeMap<String, i64>,
}

impl State {
    fn empty() -> Self {
        Self {
            exists: false,
            name: None,
            rows: vec![],
            constraints: BTreeMap::new(),
        }
    }

    fn allows(&self, value: i64) -> bool {
        for bound in self.constraints.values() {
            if value >= *bound {
                return false;
            }
        }
        true
    }

    /// Run `op` against this state on its own. Returns `None` if `op` could not succeed here.
    fn apply(&self, op: &Op) -> Option<State> {
        let mut next = self.clone();
        let creates = matches!(op, Op::Create { .. });
        if next.exists == creates {
            return None;
        }
        match op {
            Op::Create { rows, name } => {
                next.exists = true;
                next.name = Some(name.clone());
                next.rows = rows.clone();
            }
            Op::Append { rows } => {
                for (id, value) in rows {
                    if !self.allows(*value) {
                        return None;
                    }
                    next.rows.push((*id, *value));
                }
            }
            Op::Overwrite { rows } => {
                for (_, value) in rows {
                    if !self.allows(*value) {
                        return None;
                    }
                }
                next.rows = rows.clone();
            }
            Op::Delete { below } => next.rows.retain(|(_, value)| value >= below),
            Op::Update { below, add } => {
                for (_, value) in next.rows.iter_mut() {
                    if *value < *below {
                        *value += add;
                        if !self.allows(*value) {
                            return None;
                        }
                    }
                }
            }
            Op::Merge { rows } => {
                for (id, value) in rows {
                    if !self.allows(*value) {
                        return None;
                    }
                    let mut matched = false;
                    for (existing_id, existing_value) in next.rows.iter_mut() {
                        if existing_id == id {
                            *existing_value = *value;
                            matched = true;
                        }
                    }
                    if !matched {
                        next.rows.push((*id, *value));
                    }
                }
            }
            Op::Optimize => {}
            Op::AddConstraint { name, below } => {
                if next.constraints.contains_key(name) {
                    return None;
                }
                for (_, value) in &next.rows {
                    if value >= below {
                        return None;
                    }
                }
                next.constraints.insert(name.clone(), *below);
            }
        }
        next.rows.sort_unstable();
        Some(next)
    }
}

/// Whether some order of `ops`, applied one at a time, turns `state` into `target`.
fn has_serial_order(state: &State, ops: &mut Vec<&Op>, target: &State) -> bool {
    if ops.is_empty() {
        return state == target;
    }
    for i in 0..ops.len() {
        let op = ops.remove(i);
        if let Some(next) = state.apply(op)
            && has_serial_order(&next, ops, target)
        {
            return true;
        }
        ops.insert(i, op);
    }
    false
}

/// What the scheduler tells a held commit put to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Release {
    Land,
    /// Fail without writing.
    FailBefore,
    /// Write, then fail, like a response lost after the put landed.
    FailAfter,
}

#[derive(Debug)]
enum Slot {
    Running,
    Waiting {
        version: u64,
        release: oneshot::Sender<Release>,
    },
    Done,
}

/// Releases held commit puts one at a time, in an order chosen by its seed.
#[derive(Debug)]
struct Scheduler {
    slots: Mutex<Vec<Slot>>,
    changed: Notify,
    /// Releases in the order they happened, for printing a failing seed.
    trace: Mutex<Vec<String>>,
    /// Writers that had at least one commit put fail.
    faulted: Mutex<BTreeSet<usize>>,
}

impl Scheduler {
    fn new(writers: usize) -> Arc<Self> {
        let mut slots = vec![];
        for _ in 0..writers {
            slots.push(Slot::Running);
        }
        Arc::new(Self {
            slots: Mutex::new(slots),
            changed: Notify::new(),
            trace: Mutex::new(vec![]),
            faulted: Mutex::new(BTreeSet::new()),
        })
    }

    /// Hold `writer`'s commit of `version` until the scheduler releases it.
    fn hold(&self, writer: usize, version: u64) -> oneshot::Receiver<Release> {
        let (tx, rx) = oneshot::channel();
        self.slots.lock().unwrap()[writer] = Slot::Waiting {
            version,
            release: tx,
        };
        self.changed.notify_one();
        rx
    }

    fn done(&self, writer: usize) {
        self.slots.lock().unwrap()[writer] = Slot::Done;
        self.changed.notify_one();
    }

    /// Release held commits until every writer is done. A commit is released only while no
    /// writer is running, so the release order is the only thing that varies between runs.
    async fn run(&self, rng: &mut Rng, fault_percent: u64) {
        loop {
            {
                let mut slots = self.slots.lock().unwrap();
                let mut waiting = vec![];
                let mut running = false;
                for (writer, slot) in slots.iter().enumerate() {
                    match slot {
                        Slot::Running => running = true,
                        Slot::Waiting { .. } => waiting.push(writer),
                        Slot::Done => {}
                    }
                }
                if !running && waiting.is_empty() {
                    return;
                }
                if !running {
                    let writer = waiting[rng.below(waiting.len() as u64) as usize];
                    let mut release = Release::Land;
                    if fault_percent > 0 && rng.chance(fault_percent) {
                        release = if rng.chance(50) {
                            Release::FailBefore
                        } else {
                            Release::FailAfter
                        };
                        self.faulted.lock().unwrap().insert(writer);
                    }
                    let Slot::Waiting {
                        version,
                        release: tx,
                    } = std::mem::replace(&mut slots[writer], Slot::Running)
                    else {
                        unreachable!("writer {writer} is waiting");
                    };
                    self.trace
                        .lock()
                        .unwrap()
                        .push(format!("w{writer}->v{version}:{release:?}"));
                    // A dropped writer shows up as an error in its result.
                    let _ = tx.send(release);
                    continue;
                }
            }
            self.changed.notified().await;
        }
    }
}

/// An in-memory store that holds commit puts from simulated writers for the [`Scheduler`].
#[derive(Debug)]
struct GatedStore {
    inner: InMemory,
    scheduler: Mutex<Option<Arc<Scheduler>>>,
}

impl GatedStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: InMemory::new(),
            scheduler: Mutex::new(None),
        })
    }

    /// Start or stop holding commit puts.
    fn set_scheduler(&self, scheduler: Option<Arc<Scheduler>>) {
        *self.scheduler.lock().unwrap() = scheduler;
    }
}

impl Display for GatedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedStore({})", self.inner)
    }
}

/// The version of a `_delta_log/<20 digits>.json` commit file, or `None` for any other path.
fn commit_version(location: &Path) -> Option<u64> {
    let parts: Vec<_> = location.parts().collect();
    if parts.len() < 2 || parts[parts.len() - 2].as_ref() != "_delta_log" {
        return None;
    }
    let stem = parts[parts.len() - 1].as_ref().strip_suffix(".json")?;
    if stem.len() != 20 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

/// The simulated writer recorded in a commit's `commitInfo`, if any.
fn commit_writer(bytes: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(bytes).ok()?;
    for line in text.lines() {
        let Ok(action) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if let Some(info) = action.get("commitInfo") {
            return info.get(WRITER_KEY)?.as_u64().map(|w| w as usize);
        }
    }
    None
}

fn injected_failure(when: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "dst",
        source: format!("injected failure {when} the commit landed").into(),
    }
}

#[async_trait::async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let scheduler = self.scheduler.lock().unwrap().clone();
        if let (Some(scheduler), PutMode::Create, Some(version)) =
            (scheduler, &opts.mode, commit_version(location))
        {
            let mut bytes = vec![];
            for chunk in payload.iter() {
                bytes.extend_from_slice(chunk);
            }
            if let Some(writer) = commit_writer(&bytes) {
                let release = scheduler
                    .hold(writer, version)
                    .await
                    .unwrap_or(Release::Land);
                match release {
                    Release::Land => {}
                    Release::FailBefore => return Err(injected_failure("before")),
                    Release::FailAfter => {
                        self.inner.put_opts(location, payload, opts).await?;
                        return Err(injected_failure("after"));
                    }
                }
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn batch(rows: &Rows) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]));
    let mut ids = vec![];
    let mut values = vec![];
    for (id, value) in rows {
        ids.push(*id);
        values.push(*value);
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

/// Load the table at its latest version, or an empty handle if it does not exist yet.
async fn open(store: &Arc<GatedStore>) -> DeltaResult<DeltaTable> {
    let url = Url::parse(TABLE_URL).unwrap();
    let mut table = DeltaTableBuilder::from_url(url.clone())?
        .with_storage_backend(store.clone(), url)
        .build()?;
    match table.load().await {
        Ok(()) | Err(DeltaTableError::NotATable(_)) => Ok(table),
        Err(err) => Err(err),
    }
}

/// Run `op` and return the version of the table it returns. `writer` tags the commit so the
/// store can hold it; setup commits pass `None` and are never held.
async fn run_op(table: DeltaTable, op: Op, writer: Option<usize>) -> DeltaResult<Option<u64>> {
    let mut props = CommitProperties::default();
    if let Some(writer) = writer {
        props = props.with_metadata([(WRITER_KEY.to_string(), serde_json::json!(writer))]);
    }
    let table = match op {
        Op::Create { rows, name } => {
            table
                .write(vec![batch(&rows)])
                .with_save_mode(SaveMode::ErrorIfExists)
                .with_table_name(name)
                .with_commit_properties(props)
                .await?
        }
        Op::Append { rows } => {
            table
                .write(vec![batch(&rows)])
                .with_save_mode(SaveMode::Append)
                .with_commit_properties(props)
                .await?
        }
        Op::Overwrite { rows } => {
            table
                .write(vec![batch(&rows)])
                .with_save_mode(SaveMode::Overwrite)
                .with_commit_properties(props)
                .await?
        }
        Op::Delete { below } => {
            let (table, _) = table
                .delete()
                .with_predicate(col("value").lt(lit(below)))
                .with_commit_properties(props)
                .await?;
            table
        }
        Op::Update { below, add } => {
            let (table, _) = table
                .update()
                .with_predicate(col("value").lt(lit(below)))
                .with_update("value", col("value") + lit(add))
                .with_commit_properties(props)
                .await?;
            table
        }
        Op::Merge { rows } => {
            let source = SessionContext::new().read_batch(batch(&rows))?;
            let (table, _) = table
                .merge(source, col("target.id").eq(col("source.id")))
                .with_source_alias("source")
                .with_target_alias("target")
                .when_matched_update(|u| u.update("value", col("source.value")))?
                .when_not_matched_insert(|i| {
                    i.set("id", col("source.id"))
                        .set("value", col("source.value"))
                })?
                .with_commit_properties(props)
                .await?;
            table
        }
        Op::Optimize => {
            let (table, _) = table.optimize().with_commit_properties(props).await?;
            table
        }
        Op::AddConstraint { name, below } => {
            table
                .add_constraint()
                .with_constraint(name, format!("value < {below}"))
                .with_commit_properties(props)
                .await?
        }
    };
    Ok(table.version())
}

/// Read the table back. `bounds` maps constraint names to bounds, which the metadata only holds
/// as expressions.
async fn read_state(store: &Arc<GatedStore>, bounds: &BTreeMap<String, i64>) -> State {
    let table = open(store).await.unwrap();
    if table.version().is_none() {
        return State::empty();
    }
    let metadata = table.snapshot().unwrap().metadata();
    let mut constraints = BTreeMap::new();
    for key in metadata.configuration().keys() {
        if let Some(name) = key.strip_prefix("delta.constraints.") {
            let bound = bounds.get(name).copied().unwrap_or(i64::MIN);
            constraints.insert(name.to_string(), bound);
        }
    }

    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state()).unwrap();
    ctx.register_table("t", table.table_provider().await.unwrap())
        .unwrap();
    let batches = ctx
        .sql("SELECT id, value FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = vec![];
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), values.value(i)));
        }
    }
    rows.sort_unstable();
    State {
        exists: true,
        name: metadata.name().map(str::to_string),
        rows,
        constraints,
    }
}

/// The simulated writer behind each commit after `base`, by version.
async fn commit_writers(
    store: &Arc<GatedStore>,
    base: Option<u64>,
    latest: Option<u64>,
) -> BTreeMap<u64, Option<usize>> {
    let mut writers = BTreeMap::new();
    let Some(latest) = latest else {
        return writers;
    };
    let first = match base {
        Some(base) => base + 1,
        None => 0,
    };
    for version in first..=latest {
        let path = Path::from(format!("dst/_delta_log/{version:020}.json"));
        let bytes = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
        writers.insert(version, commit_writer(&bytes));
    }
    writers
}

/// The starting table, built from `setup` (one commit per entry, none if empty), and the
/// operations the writers race to commit on top of it.
#[derive(Clone, Debug)]
struct Scenario {
    setup: Vec<Rows>,
    ops: Vec<Op>,
}

fn scenario(seed: u64, create_percent: u64) -> Scenario {
    let mut rng = Rng(seed);
    let mut next_id = 100;
    let mut new_rows = |rng: &mut Rng, n: usize| {
        let mut rows = vec![];
        for _ in 0..n {
            next_id += 1;
            rows.push((next_id, rng.below(100) as i64));
        }
        rows
    };

    if rng.chance(create_percent) {
        let mut ops = vec![];
        for writer in 0..2 + rng.below(2) {
            ops.push(Op::Create {
                rows: new_rows(&mut rng, 2),
                name: format!("w{writer}"),
            });
        }
        return Scenario { setup: vec![], ops };
    }

    let mut setup = vec![];
    for commit in 0..3 {
        setup.push(vec![
            (commit * 2, rng.below(100) as i64),
            (commit * 2 + 1, rng.below(100) as i64),
        ]);
    }
    let mut ops = vec![];
    for writer in 0..2 + rng.below(3) {
        let op = match rng.below(100) {
            0..=21 => Op::Append {
                rows: new_rows(&mut rng, 2),
            },
            22..=29 => Op::Overwrite {
                rows: new_rows(&mut rng, 2),
            },
            30..=44 => Op::Delete {
                below: rng.below(100) as i64,
            },
            45..=59 => Op::Update {
                below: rng.below(100) as i64,
                add: 100,
            },
            60..=74 => {
                let existing = (rng.below(6) as i64, rng.below(100) as i64);
                let mut rows = new_rows(&mut rng, 1);
                rows.push(existing);
                Op::Merge { rows }
            }
            75..=84 => Op::Optimize,
            _ => Op::AddConstraint {
                name: format!("c{writer}"),
                below: 50 + rng.below(100) as i64,
            },
        };
        ops.push(op);
    }
    Scenario { setup, ops }
}

/// Everything observable about one run. Two runs of a seed must produce equal values.
#[derive(Debug, PartialEq, Eq)]
struct Run {
    trace: Vec<String>,
    results: Vec<String>,
    writers: BTreeMap<u64, Option<usize>>,
    state: State,
}

/// Run one seed and return what happened plus every problem found, as readable lines.
async fn run_seed(seed: u64, fault_percent: u64, create_percent: u64) -> (Run, Vec<String>) {
    let scenario = scenario(seed, create_percent);
    let store = GatedStore::new();

    let mut base = State::empty();
    for rows in &scenario.setup {
        let op = if base.exists {
            Op::Append { rows: rows.clone() }
        } else {
            Op::Create {
                rows: rows.clone(),
                name: "base".into(),
            }
        };
        run_op(open(&store).await.unwrap(), op.clone(), None)
            .await
            .unwrap();
        base = base.apply(&op).unwrap();
    }
    let base_version = open(&store).await.unwrap().version();

    // Every writer loads the starting version before any of them can commit.
    let mut tables = vec![];
    for _ in &scenario.ops {
        tables.push(open(&store).await.unwrap());
    }
    let scheduler = Scheduler::new(scenario.ops.len());
    store.set_scheduler(Some(scheduler.clone()));
    let mut handles = vec![];
    for (writer, (table, op)) in tables.into_iter().zip(scenario.ops.clone()).enumerate() {
        let scheduler = scheduler.clone();
        handles.push(tokio::spawn(async move {
            let result = run_op(table, op, Some(writer)).await;
            scheduler.done(writer);
            result
        }));
    }
    scheduler
        .run(&mut Rng(seed ^ 0xD57_5EED), fault_percent)
        .await;
    let mut results = vec![];
    for handle in handles {
        results.push(match handle.await {
            Ok(Ok(version)) => Ok(version),
            Ok(Err(err)) => Err(format!("error: {err}")),
            Err(join) => Err(format!("panic: {join}")),
        });
    }
    store.set_scheduler(None);

    let latest = open(&store).await.unwrap().version();
    let writers = commit_writers(&store, base_version, latest).await;
    let mut bounds = BTreeMap::new();
    for op in &scenario.ops {
        if let Op::AddConstraint { name, below } = op {
            bounds.insert(name.clone(), *below);
        }
    }
    let state = read_state(&store, &bounds).await;

    let mut problems = vec![];
    let mut committed = vec![];
    for (version, writer) in &writers {
        match writer {
            None => problems.push(format!("v{version} was not made by a simulated writer")),
            Some(w) if committed.contains(w) => {
                problems.push(format!("w{w} committed twice, again at v{version}"))
            }
            Some(w) => committed.push(*w),
        }
    }
    let faulted = scheduler.faulted.lock().unwrap().clone();
    for (w, result) in results.iter().enumerate() {
        let in_log = committed.contains(&w);
        match result {
            Err(err) if err.starts_with("panic") => problems.push(format!("w{w}: {err}")),
            // A put that landed and then failed cannot be told apart from one that did not land.
            Err(err) if in_log && !faulted.contains(&w) => {
                problems.push(format!("w{w} failed but its commit is in the log: {err}"))
            }
            Ok(Some(v)) if Some(*v) > base_version && writers.get(v) != Some(&Some(w)) => {
                problems.push(format!("w{w} reported v{v}, which another writer made"))
            }
            _ => {}
        }
    }
    let mut ops = vec![];
    for w in &committed {
        ops.push(&scenario.ops[*w]);
    }
    if !has_serial_order(&base, &mut ops, &state) {
        problems.push(format!(
            "no serial order of the commits {committed:?} gives the table {state:?}"
        ));
    }
    if !problems.is_empty() {
        problems.insert(0, format!("ops {:?}", scenario.ops));
    }

    let mut result_lines = vec![];
    for result in &results {
        result_lines.push(match result {
            Ok(version) => format!("ok {version:?}"),
            Err(err) => err.clone(),
        });
    }
    let run = Run {
        trace: scheduler.trace.lock().unwrap().clone(),
        results: result_lines,
        writers,
        state,
    };
    (run, problems)
}

/// Run `seeds`, replaying every 20th to check the run is deterministic, and fail on any problem.
async fn check_seeds(seeds: Vec<u64>, fault_percent: u64, create_percent: u64) {
    let mut failed = vec![];
    for seed in &seeds {
        let (run, mut problems) = run_seed(*seed, fault_percent, create_percent).await;
        if seed % 20 == 0 {
            let (again, _) = run_seed(*seed, fault_percent, create_percent).await;
            if again != run {
                problems.push(format!("replay differs: {run:?} vs {again:?}"));
            }
        }
        if seeds.len() == 1 || !problems.is_empty() {
            println!("seed {seed}: {:?} {:?}", run.trace, run.results);
            for problem in &problems {
                println!("  {problem}");
            }
        }
        if !problems.is_empty() {
            failed.push(*seed);
        }
    }
    assert!(failed.is_empty(), "failing seeds: {failed:?}");
}

fn env(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

// A single-threaded runtime keeps the setup and reads simple; the release order comes from the
// scheduler either way.
#[tokio::test(flavor = "current_thread")]
async fn dst_concurrent_commits_smoke() {
    // Create races are left out until #4982 is fixed.
    check_seeds((0..20).collect(), 0, 0).await;
}

/// Seeds that caught a conflict-checker bug with its fix reverted.
#[rstest]
#[case::concurrent_optimizes_4943(24)]
#[case::constraint_racing_appends_5019(74)]
#[tokio::test(flavor = "current_thread")]
async fn dst_concurrent_commits_regressions(#[case] seed: u64) {
    check_seeds(vec![seed], 0, 0).await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "long-running; see the module docs"]
async fn dst_concurrent_commits() {
    let seeds = match env("DST_SEED") {
        Some(seed) => vec![seed],
        None => (0..env("DST_SEEDS").unwrap_or(500)).collect(),
    };
    let fault_percent = env("DST_FAULTS").unwrap_or(0);
    let create_percent = env("DST_CREATE_PCT").unwrap_or(20);
    check_seeds(seeds, fault_percent, create_percent).await;
}
