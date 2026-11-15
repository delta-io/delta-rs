//! Deterministic simulation of concurrent commits.
//!
//! Several writers load the same table version and each runs one operation. A gated object
//! store parks every create-only put of a `_delta_log/NNN.json` commit. A seeded scheduler waits
//! until every live writer is parked or done, then picks which parked commit lands next. Commit
//! puts are the only place writers share state, so a seed fixes the whole interleaving and a
//! failing seed replays exactly.
//!
//! The oracle replays the committed operations against a small in-memory model. The final table
//! must equal the result of running the committed operations in some serial order (write
//! serializability). Every writer that reports success must own the version it reports, and no
//! writer that reports failure may appear in the log.
//!
//! The tests are ignored by default. Run them with:
//!
//! ```text
//! DST_SEEDS=300 cargo test -p deltalake-core --features datafusion --test it_datafusion dst_ -- --ignored --nocapture
//! DST_SEED=42 cargo test ...        # replay one seed and print its trace
//! DST_FAULTS=20 cargo test ...      # fail 20% of commit puts, before or after they land
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionContext;
use deltalake_core::kernel::transaction::CommitProperties;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaTable, DeltaTableBuilder, DeltaTableError};
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tokio::sync::{Notify, oneshot};
use url::Url;

const WRITER_KEY: &str = "dst_writer";
const TABLE_URL: &str = "memory:///dst";

// ---------------------------------------------------------------------------------------------
// Seeded randomness. SplitMix64 keeps seeds stable across `rand` upgrades.
// ---------------------------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

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

// ---------------------------------------------------------------------------------------------
// Operations and the model they run against.
// ---------------------------------------------------------------------------------------------

type Rows = Vec<(i64, i64)>;

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
    /// Upsert by `id`: matched rows take the source value, unmatched rows are inserted.
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

/// What a table holds, as far as the oracle cares.
#[derive(Clone, Debug, PartialEq, Eq)]
struct State {
    exists: bool,
    name: Option<String>,
    /// Sorted `(id, value)` pairs. A multiset, so duplicate ids show up as a difference.
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
        self.constraints.values().all(|bound| value < *bound)
    }

    /// Run `op` serially against this state. `None` means the operation could not have
    /// succeeded at this point in a serial order.
    fn apply(&self, op: &Op) -> Option<State> {
        let mut next = self.clone();
        if !matches!(op, Op::Create { .. }) && !next.exists {
            return None;
        }
        match op {
            Op::Create { rows, name } => {
                if next.exists {
                    return None;
                }
                next.exists = true;
                next.name = Some(name.clone());
                next.rows = rows.clone();
            }
            Op::Append { rows } => {
                if !rows.iter().all(|(_, v)| next.allows(*v)) {
                    return None;
                }
                next.rows.extend(rows.iter().copied());
            }
            Op::Overwrite { rows } => {
                if !rows.iter().all(|(_, v)| next.allows(*v)) {
                    return None;
                }
                next.rows = rows.clone();
            }
            Op::Delete { below } => next.rows.retain(|(_, v)| *v >= *below),
            Op::Update { below, add } => {
                for (_, v) in next.rows.iter_mut() {
                    if *v < *below {
                        *v += add;
                    }
                }
                if !next.rows.iter().all(|(_, v)| self.allows(*v)) {
                    return None;
                }
            }
            Op::Merge { rows } => {
                for (id, value) in rows {
                    if !next.allows(*value) {
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
                if next.constraints.contains_key(name)
                    || !next.rows.iter().all(|(_, v)| *v < *below)
                {
                    return None;
                }
                next.constraints.insert(name.clone(), *below);
            }
        }
        next.rows.sort_unstable();
        Some(next)
    }
}

/// Search for a serial order of `ops` that turns `base` into `target`.
fn serial_order(base: &State, ops: &[(usize, &Op)], target: &State) -> Option<Vec<usize>> {
    fn search(
        state: &State,
        remaining: &mut Vec<(usize, &Op)>,
        order: &mut Vec<usize>,
        target: &State,
    ) -> bool {
        if remaining.is_empty() {
            return state == target;
        }
        for i in 0..remaining.len() {
            let (writer, op) = remaining.remove(i);
            if let Some(next) = state.apply(op) {
                order.push(writer);
                if search(&next, remaining, order, target) {
                    return true;
                }
                order.pop();
            }
            remaining.insert(i, (writer, op));
        }
        false
    }
    let mut remaining = ops.to_vec();
    let mut order = vec![];
    search(base, &mut remaining, &mut order, target).then_some(order)
}

// ---------------------------------------------------------------------------------------------
// The gated store and the scheduler that releases parked commits.
// ---------------------------------------------------------------------------------------------

/// What the scheduler does with a parked commit put.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Release {
    /// Perform the put.
    Land,
    /// Return an error without writing anything.
    FailBefore,
    /// Perform the put, then return an error, as when a response is lost after a write lands.
    FailAfter,
}

#[derive(Debug)]
enum Slot {
    Running,
    Parked {
        version: u64,
        release: oneshot::Sender<Release>,
    },
    Done,
}

#[derive(Debug)]
struct Scheduler {
    slots: Mutex<Vec<Slot>>,
    changed: Notify,
    trace: Mutex<Vec<String>>,
}

impl Scheduler {
    fn new(writers: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: Mutex::new((0..writers).map(|_| Slot::Running).collect()),
            changed: Notify::new(),
            trace: Mutex::new(vec![]),
        })
    }

    fn park(&self, writer: usize, version: u64) -> oneshot::Receiver<Release> {
        let (tx, rx) = oneshot::channel();
        self.slots.lock().unwrap()[writer] = Slot::Parked {
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

    /// Release parked commits one at a time until every writer is done. A commit is released
    /// only when no writer is running, so the order of releases is the only freedom.
    async fn drive(&self, rng: &mut Rng, fault_percent: u64) {
        loop {
            {
                let mut slots = self.slots.lock().unwrap();
                if slots.iter().all(|s| matches!(s, Slot::Done)) {
                    return;
                }
                if !slots.iter().any(|s| matches!(s, Slot::Running)) {
                    let parked: Vec<usize> = (0..slots.len())
                        .filter(|i| matches!(slots[*i], Slot::Parked { .. }))
                        .collect();
                    let writer = parked[rng.below(parked.len() as u64) as usize];
                    let release = if fault_percent > 0 && rng.chance(fault_percent) {
                        if rng.chance(50) {
                            Release::FailBefore
                        } else {
                            Release::FailAfter
                        }
                    } else {
                        Release::Land
                    };
                    let Slot::Parked {
                        version,
                        release: tx,
                    } = std::mem::replace(&mut slots[writer], Slot::Running)
                    else {
                        unreachable!("picked a parked writer");
                    };
                    self.trace
                        .lock()
                        .unwrap()
                        .push(format!("w{writer}->v{version}:{release:?}"));
                    // The writer may have been dropped; it then shows up as an error result.
                    let _ = tx.send(release);
                    continue;
                }
            }
            self.changed.notified().await;
        }
    }
}

/// An in-memory store whose commit puts from simulated writers wait for the scheduler.
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

    fn gate(&self, scheduler: Option<Arc<Scheduler>>) {
        *self.scheduler.lock().unwrap() = scheduler;
    }
}

impl Display for GatedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedStore({})", self.inner)
    }
}

/// The version of a `_delta_log/NNNNNNNNNNNNNNNNNNNN.json` commit file.
fn commit_version(location: &Path) -> Option<u64> {
    let mut parts = location.parts().collect::<Vec<_>>();
    let file = parts.pop()?;
    let dir = parts.pop()?;
    if dir.as_ref() != "_delta_log" {
        return None;
    }
    let stem = file.as_ref().strip_suffix(".json")?;
    (stem.len() == 20 && stem.bytes().all(|b| b.is_ascii_digit()))
        .then(|| stem.parse().ok())
        .flatten()
}

/// The simulated writer that produced a commit, read from its commitInfo metadata.
fn commit_writer(bytes: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(bytes).ok()?;
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if let Some(info) = value.get("commitInfo") {
            return info.get(WRITER_KEY)?.as_u64().map(|w| w as usize);
        }
    }
    None
}

fn injected(what: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "dst",
        source: format!("injected failure {what} the commit landed").into(),
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
            let bytes: Vec<u8> = payload.iter().flat_map(|b| b.iter().copied()).collect();
            if let Some(writer) = commit_writer(&bytes) {
                match scheduler
                    .park(writer, version)
                    .await
                    .unwrap_or(Release::Land)
                {
                    Release::Land => {}
                    Release::FailBefore => return Err(injected("before")),
                    Release::FailAfter => {
                        self.inner.put_opts(location, payload, opts).await?;
                        return Err(injected("after"));
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

// ---------------------------------------------------------------------------------------------
// Running operations against delta-rs.
// ---------------------------------------------------------------------------------------------

fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]))
}

fn batch(rows: &Rows) -> RecordBatch {
    let ids = Int64Array::from(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>());
    let values = Int64Array::from(rows.iter().map(|(_, v)| *v).collect::<Vec<_>>());
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(values)]).unwrap()
}

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

type DeltaResult<T> = Result<T, DeltaTableError>;

fn props(writer: Option<usize>) -> CommitProperties {
    let props = CommitProperties::default();
    match writer {
        Some(w) => props.with_metadata([(WRITER_KEY.to_string(), serde_json::json!(w))]),
        None => props,
    }
}

/// Run `op` and return the version the returned table reports.
async fn run(table: DeltaTable, op: Op, writer: Option<usize>) -> DeltaResult<Option<u64>> {
    let props = props(writer);
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
            table
                .delete()
                .with_predicate(col("value").lt(lit(below)))
                .with_commit_properties(props)
                .await?
                .0
        }
        Op::Update { below, add } => {
            table
                .update()
                .with_predicate(col("value").lt(lit(below)))
                .with_update("value", col("value") + lit(add))
                .with_commit_properties(props)
                .await?
                .0
        }
        Op::Merge { rows } => {
            let source = SessionContext::new().read_batch(batch(&rows))?;
            table
                .merge(source, col("target.id").eq(col("source.id")))
                .with_source_alias("source")
                .with_target_alias("target")
                .when_matched_update(|u| u.update("value", col("source.value")))?
                .when_not_matched_insert(|i| {
                    i.set("id", col("source.id"))
                        .set("value", col("source.value"))
                })?
                .with_commit_properties(props)
                .await?
                .0
        }
        Op::Optimize => table.optimize().with_commit_properties(props).await?.0,
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

/// Read what the table actually holds.
async fn observe(store: &Arc<GatedStore>, constraint_bounds: &BTreeMap<String, i64>) -> State {
    let table = open(store).await.expect("open the final table");
    let Some(version) = table.version() else {
        return State::empty();
    };
    let _ = version;
    let snapshot = table.snapshot().expect("snapshot");
    let metadata = snapshot.metadata();
    let name = metadata.name().map(str::to_string);
    let constraints = metadata
        .configuration()
        .keys()
        .filter_map(|k| k.strip_prefix("delta.constraints."))
        .map(|n| {
            (
                n.to_string(),
                constraint_bounds.get(n).copied().unwrap_or(i64::MIN),
            )
        })
        .collect();

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
    for b in batches {
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let values = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            rows.push((ids.value(i), values.value(i)));
        }
        let _ = ids.len();
    }
    rows.sort_unstable();
    State {
        exists: true,
        name,
        rows,
        constraints,
    }
}

/// Which simulated writer authored each version in `(from, to]`.
async fn authors(
    store: &Arc<GatedStore>,
    from: Option<u64>,
    to: Option<u64>,
) -> BTreeMap<u64, Option<usize>> {
    let mut out = BTreeMap::new();
    let Some(to) = to else { return out };
    let start = from.map(|v| v + 1).unwrap_or(0);
    for version in start..=to {
        let path = Path::from(format!("dst/_delta_log/{version:020}.json"));
        let bytes = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
        out.insert(version, commit_writer(&bytes));
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Scenarios.
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Scenario {
    /// Commits that build the starting table, each one batch of rows. Empty means no table.
    setup: Vec<Rows>,
    ops: Vec<Op>,
}

fn scenario(seed: u64) -> Scenario {
    let mut rng = Rng::new(seed);
    let mut next_id = 100;
    let mut fresh = |rng: &mut Rng, n: usize| -> Rows {
        (0..n)
            .map(|_| {
                next_id += 1;
                (next_id, rng.below(100) as i64)
            })
            .collect()
    };

    if rng.chance(env("DST_CREATE_PCT").unwrap_or(20)) {
        let writers = 2 + rng.below(2) as usize;
        let ops = (0..writers)
            .map(|w| Op::Create {
                rows: fresh(&mut rng, 2),
                name: format!("w{w}"),
            })
            .collect();
        return Scenario { setup: vec![], ops };
    }

    let setup: Vec<Rows> = (0..3)
        .map(|c| (0..2).map(|i| (c * 2 + i, rng.below(100) as i64)).collect())
        .collect();
    let writers = 2 + rng.below(3) as usize;
    let mut ops = vec![];
    for w in 0..writers {
        let roll = rng.below(100);
        let op = match roll {
            0..=21 => Op::Append {
                rows: fresh(&mut rng, 2),
            },
            22..=29 => Op::Overwrite {
                rows: fresh(&mut rng, 2),
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
                let mut rows = fresh(&mut rng, 1);
                rows.push(existing);
                Op::Merge { rows }
            }
            75..=84 => Op::Optimize,
            _ => Op::AddConstraint {
                name: format!("c{w}"),
                below: 50 + rng.below(100) as i64,
            },
        };
        ops.push(op);
    }
    Scenario { setup, ops }
}

// ---------------------------------------------------------------------------------------------
// One run, checked.
// ---------------------------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    trace: Vec<String>,
    results: Vec<String>,
    authors: BTreeMap<u64, Option<usize>>,
    observed: State,
}

#[derive(Debug)]
struct Finding {
    seed: u64,
    kind: &'static str,
    detail: String,
}

async fn run_seed(seed: u64, fault_percent: u64) -> (Outcome, Vec<Finding>, Scenario) {
    let scenario = scenario(seed);
    let store = GatedStore::new();

    // Build the starting table without the gate.
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
        let table = open(&store).await.unwrap();
        run(table, op.clone(), None).await.unwrap();
        base = base.apply(&op).unwrap();
    }
    let base_version = open(&store).await.unwrap().version();

    // Every writer loads the starting version before any of them runs.
    let mut tables = vec![];
    for _ in &scenario.ops {
        tables.push(open(&store).await.unwrap());
    }

    let scheduler = Scheduler::new(scenario.ops.len());
    store.gate(Some(scheduler.clone()));
    let mut handles = vec![];
    for (writer, (table, op)) in tables.into_iter().zip(scenario.ops.clone()).enumerate() {
        let scheduler = scheduler.clone();
        handles.push(tokio::spawn(async move {
            let result = run(table, op, Some(writer)).await;
            scheduler.done(writer);
            result
        }));
    }
    let mut rng = Rng::new(seed ^ 0xD57_5EED);
    scheduler.drive(&mut rng, fault_percent).await;

    let mut results: Vec<Result<Option<u64>, String>> = vec![];
    for handle in handles {
        results.push(match handle.await {
            Ok(Ok(version)) => Ok(version),
            Ok(Err(err)) => Err(format!("error: {err}")),
            Err(join) => Err(format!("PANIC: {join}")),
        });
    }
    store.gate(None);

    let latest = open(&store).await.unwrap().version();
    let authors = authors(&store, base_version, latest).await;
    let bounds: BTreeMap<String, i64> = scenario
        .ops
        .iter()
        .filter_map(|op| match op {
            Op::AddConstraint { name, below } => Some((name.clone(), *below)),
            _ => None,
        })
        .collect();
    let observed = observe(&store, &bounds).await;

    let mut findings = vec![];
    let mut found = |kind, detail: String| findings.push(Finding { seed, kind, detail });

    // Each committed version must come from a simulated writer, once.
    let mut seen = BTreeSet::new();
    for (version, author) in &authors {
        match author {
            None => found("foreign-commit", format!("v{version} has no writer id")),
            Some(w) if !seen.insert(*w) => found(
                "double-commit",
                format!("w{w} committed twice (again at v{version})"),
            ),
            Some(_) => {}
        }
    }
    let fault_hit = |w: usize| {
        scheduler
            .trace
            .lock()
            .unwrap()
            .iter()
            .any(|t| t.starts_with(&format!("w{w}->")) && !t.ends_with("Land"))
    };
    for (w, result) in results.iter().enumerate() {
        let in_log = authors.values().any(|a| *a == Some(w));
        match result {
            Err(e) if e.starts_with("PANIC") => found("panic", format!("w{w}: {e}")),
            Err(e) if in_log && !fault_hit(w) => found(
                "phantom-commit",
                format!("w{w} failed but its commit is in the log: {e}"),
            ),
            Err(e) if in_log => found(
                "ambiguous-commit",
                format!("w{w} was told its commit failed after it landed: {e}"),
            ),
            Ok(Some(v)) if Some(*v) > base_version && authors.get(v) != Some(&Some(w)) => found(
                "stolen-version",
                format!("w{w} reported v{v}, which belongs to {:?}", authors.get(v)),
            ),
            _ => {}
        }
    }

    // The final table must equal some serial order of the committed operations.
    let committed: Vec<(usize, &Op)> = authors
        .values()
        .filter_map(|a| *a)
        .map(|w| (w, &scenario.ops[w]))
        .collect();
    if serial_order(&base, &committed, &observed).is_none() {
        let log_order = committed
            .iter()
            .try_fold(base.clone(), |s, (_, op)| s.apply(op));
        found(
            "not-serializable",
            format!(
                "committed in log order {:?}\n      observed {:?}\n      log-order replay {:?}",
                committed.iter().map(|(w, _)| *w).collect::<Vec<_>>(),
                observed,
                log_order
            ),
        );
    }

    let outcome = Outcome {
        trace: scheduler.trace.lock().unwrap().clone(),
        results: results
            .iter()
            .map(|r| match r {
                Ok(v) => format!("ok {v:?}"),
                Err(e) => e.clone(),
            })
            .collect(),
        authors,
        observed,
    };
    (outcome, findings, scenario)
}

fn env(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "deterministic simulation; run with --ignored"]
async fn dst_concurrent_commits() {
    let faults = env("DST_FAULTS").unwrap_or(0);
    let seeds: Vec<u64> = match env("DST_SEED") {
        Some(seed) => vec![seed],
        None => (0..env("DST_SEEDS").unwrap_or(100)).collect(),
    };
    let verbose = seeds.len() == 1;

    let mut findings: Vec<Finding> = vec![];
    let mut kinds: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
    // Per operation: [succeeded, failed with a commit conflict, failed otherwise].
    let mut tally: BTreeMap<String, [usize; 3]> = BTreeMap::new();
    // Conflict errors that blind appends got, keyed by the first line of the message.
    let mut append_conflicts: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for seed in &seeds {
        let (outcome, mut seed_findings, scenario) = run_seed(*seed, faults).await;
        for (op, result) in scenario.ops.iter().zip(&outcome.results) {
            let name = format!("{op:?}");
            let name = name.split([' ', '{']).next().unwrap().to_string();
            let conflict = result.contains("Commit failed") || result.contains("conflict");
            let slot = if result.starts_with("ok") {
                0
            } else if conflict {
                1
            } else {
                2
            };
            tally.entry(name.clone()).or_default()[slot] += 1;
            if name == "Append" && conflict {
                let line = result.lines().next().unwrap_or_default().to_string();
                append_conflicts.entry(line).or_default().push(*seed);
            }
        }
        if verbose || !seed_findings.is_empty() {
            println!("seed {seed}: ops {:?}", scenario.ops);
            println!("  trace   {:?}", outcome.trace);
            println!("  results {:?}", outcome.results);
            println!("  authors {:?}", outcome.authors);
            for f in &seed_findings {
                println!("  FINDING {}: {}", f.kind, f.detail);
            }
        }
        // Replaying a seed must reproduce the same run.
        if seed % 20 == 0 {
            let (again, _, _) = run_seed(*seed, faults).await;
            if again != outcome {
                seed_findings.push(Finding {
                    seed: *seed,
                    kind: "nondeterministic",
                    detail: format!("first {outcome:?}\nsecond {again:?}"),
                });
            }
        }
        for f in &seed_findings {
            kinds.entry(f.kind).or_default().push(f.seed);
        }
        findings.extend(seed_findings);
    }

    println!("\noperation outcomes [ok, conflict, other error]:");
    for (op, counts) in &tally {
        println!("  {op:<14} {counts:?}");
    }
    for (message, seeds) in &append_conflicts {
        println!(
            "  blind appends rejected ({} times, e.g. seeds {:?}): {message}",
            seeds.len(),
            &seeds[..seeds.len().min(5)]
        );
    }
    println!("\nDST: {} seeds, faults {faults}%", seeds.len());
    for (kind, seeds) in &kinds {
        println!(
            "  {kind}: {} seeds, e.g. {:?}",
            seeds.len(),
            &seeds[..seeds.len().min(8)]
        );
    }
    // A put that lands but returns an error is indistinguishable from one that failed, so the
    // writer reporting failure is expected. It is counted above, not treated as a bug.
    let bugs = findings
        .iter()
        .filter(|f| f.kind != "ambiguous-commit")
        .count();
    assert!(bugs == 0, "{bugs} findings");
}
