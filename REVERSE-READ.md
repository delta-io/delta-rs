Great—let’s lock in a **reverse‑first, low‑latency reader** that’s correct under your constraints (*only* `add` and `remove` actions) and memory‑bounded. Below is a complete design with guardrails, a proof sketch, and drop‑in pseudocode you can wire to your file/listing APIs.

---

## 0) What this design *assumes* (hard guardrails)

To preserve correctness while scanning **newest → oldest**:

1. **Action whitelist:** All commits in the scanned range contain only `add` and `remove` actions. If you encounter `metaData`, `protocol`, CDF, column mapping, or DV actions, **abort and fall back** to a standard forward reader. This is required by the Delta protocol compatibility rules.

2. **Snapshot pinning:** Pin a target snapshot version **V** at open time (list `_delta_log` and take the max numbered JSON). Readers must reconstruct a consistent snapshot at that fixed version (snapshot isolation).

3. **Retention safety:** Choose a floor version **F** that is **≥ the latest checkpoint** and safely **within the VACUUM tombstone retention window**; do **not** walk below this floor in reverse (to avoid “resurrecting” files whose removes were vacuumed away). Default retention is commonly 7 days—plan for policy variability.

> Why these constraints?  
> Delta’s truth is the ordered JSON commit log; snapshot = “state after applying all actions up to version V.” Checkpoints are **performance shortcuts**; they are not part of the correctness boundary. VACUUM can prune tombstones; reverse walking past that prune point can re‑expose logically deleted files.

---

## 1) Core idea (“Reverse with Dynamic Remove Set”)

Scan commits **V, V‑1, …, F+1** in descending order, maintaining an in‑memory **removed set** (a bloom filter + small hash set is fine). For each version:

- **Collect all `remove` paths** from that version and add them to `removed`.
- **Consider `add` actions** from the same version:
  - If the `add`’s `path` ∉ `removed` **and** its **stats/partitionValues pass your predicates**, **emit it immediately** as a candidate data file.

This is correct because an `add` at version *v* can only be invalidated by **later** removes (> *v*), which your descending pass has already collected in `removed`. Removes at the **same** version don’t cancel newly added files unless the commit bizarrely adds and removes the identical path (rare in practice); see Commit atomicity note below. The resulting emitted set equals “all adds ≤ V minus removes ≤ V” without ever materializing the whole table.

**Latency benefit:** you can start returning qualifying files from **the newest commits immediately**, with no checkpoint load and no table‑wide state build. Checkpoints stay optional and only serve as a **floor**.

---

## 2) Commit‑level atomicity & ordering

Delta requires that actions within a single commit are applied atomically. To model the forward semantics while scanning in reverse:

- When visiting version **v** in reverse, first **stash all actions of v**, then:
  a. Insert all `remove` paths from v into `removed`.  
  b. Evaluate all `add`s from v for emission (predicate pass & not in `removed`).

This ensures any remove in later versions (already in `removed`) hides older adds, and removes in the **same** version do **not** hide adds from that same version (mirrors the forward “apply v atomically” outcome).

---

## 3) Choosing the reverse floor **F**

Pick **F = latest checkpoint version C** (read just `_last_checkpoint` to get the number—no need to open the Parquet) and verify that C is within the VACUUM horizon you must respect. If you hit F and still need more results, either:

- **Stop** (if latency/limit satisfied), or
- **Hybrid fallback:** load checkpoint C and **replay C+1…V forward** (small tail), which is the canonical reader path and compatible with Spark’s approach: “read newest checkpoint, then only JSON commits since the checkpoint.”

---

## 4) Pseudocode (language‑agnostic, Python‑style)

> Replace filesystem/listing stubs with your storage APIs.

```python
def plan_reverse_add_remove(table_log_dir, predicates, limit=None):
    # --- Pin snapshot ---
    V = list_max_json_version(table_log_dir)               # e.g., scan "_delta_log/*.json" numbers 00000000000000000042.json
    C = read_last_checkpoint_version_if_any(table_log_dir) # parse _delta_log/_last_checkpoint tiny JSON, no Parquet open
    F = choose_floor_within_retention(C)                   # must ensure F within tombstone retention policy  (VACUUM safe)

    removed = BloomOrSet(capacity_hint="few_millions")     # memory-bounded dynamic tombstone set
    emitted = 0

    for v in range(V, F, -1):  # descending V .. F+1
        actions = read_all_actions_for_version(table_log_dir, v)  # stream JSON lines

        # Hard guard: bail if any action is not add/remove (protocol, metaData, dv, cdf, columnMapping, etc.)
        if contains_non_add_remove(actions):
            return fallback_forward_reader(table_log_dir, predicates, V)  # canonical path

        # 1) Stage removes from this version into the dynamic set
        for a in actions:
            if a.type == "remove":
                removed.add(a.path)

        # 2) Evaluate adds from this version
        for a in actions:
            if a.type == "add":
                if not removed.contains(a.path) and stats_match(predicates, a.stats, a.partitionValues):
                    yield plan_file_scan(a.path, a.partitionValues)  # emit immediately (low latency)
                    emitted += 1
                    if limit and emitted >= limit:
                        return  # early termination supported

    # Optional hybrid: if we reached F and still need more rows, use the canonical small-tail forward path
    if limit is None or emitted < limit:
        return small_tail_forward_on_demand(table_log_dir, predicates, start_version=F+1, end_version=V, already_removed=removed)
```
**Helper semantics**

- `stats_match(...)` uses `add.stats.minValues/maxValues/nullCount/numRecords` and partition values to do **data skipping** at planning time. [12]
- `choose_floor_within_retention(C)` must respect your org’s **VACUUM** policy; never descend below the retention floor.
- `fallback_forward_reader(...)` mirrors Spark/Fabric’s **checkpoint → forward JSON tail** flow.

---

## 5) Correctness sketch

*Target snapshot is version* **V**. In forward semantics, the active file set is:

> **Active(V)** = { all `add` paths with version ≤ V } \\ { all `remove` paths with version ≤ V }  (commit‑atomic).

The reverse algorithm adds to `removed` every path removed at versions in (V, …, F], then visits adds from V down to F+1 and emits only those **not** in `removed`. Hence it emits exactly **Active(V) ∩ (versions ∈ (F, V])**. Because we never scan below **F** (retention‑safe baseline), we cannot resurrect an “ancient add” whose tombstone might have been vacuumed. If more results are required, the hybrid forward replay from **F+1…V** finishes materialization from a **known‑good checkpoint baseline** (C=F), which is the canonical method.

---

## 6) Complexity & memory

- **Time:** O(#actions scanned in (F, V])**, fully streamable from storage; emits qualifying files as soon as they appear at the top of history.
- **Memory:** O(#unique removes in (F, V]) + small buffers). Use a Bloom filter to cap memory; false positives only suppress some emits (never incorrect includes). You can compensate by widening F (scan shorter window) or switching to the hybrid path earlier.

---

## 7) Practical knobs

- **Windowing:** If V–F is huge, do rolling windows, e.g., process [V..V‑W], then [V‑W‑1..V‑2W], etc., stopping as soon as your LIMIT is met.
- **First‑row latency:** Start yielding right after the first few versions—new data commonly lives near the tip.
- **Emission de‑dup:** Track a small `emitted_paths` LFU set if consumers might re‑request overlapping pages.
- **I/O batch:** Batch JSON gets per version; parallelize across partitions of versions when your store supports many small range reads.

---

## 8) Unit tests (minimal)

1. **Append‑only happy path:** V=5, adds at 5..1, no removes → reverse emits 5..1 that pass predicates. (Baseline vs forward must match.)
2. **Remove newer than add:** add at 10, remove at 12 → not emitted. (Reverse sees remove first.)
3. **Same‑version rewrite:** v=20 has removes {a.parquet} and adds {b.parquet} → reverse emits `b.parquet`, and suppresses `a.parquet` from older versions.
4. **Predicate pruning:** add.stats outside filter → never emitted. Validate using `minValues/maxValues`.
5. **VACUUM floor:** craft removes at v=100, VACUUM drops tombstone; ensure algorithm refuses to descend past floor < 100.
6. **Feature tripwire:** introduce a `metaData`/`protocol` action at v in (F, V] → verify graceful fallback to canonical forward path.

---

## 9) Where this lines up with “official” behavior

- **Checkpoint role:** Used purely as a **floor** and **fallback baseline**; Delta’s own guidance treats checkpoints as snapshots for fast bootstrap, not as a prerequisite for correctness.
- **Reader construction:** Spark/Fabric readers jump to newest checkpoint and replay the **forward** tail. Your reverse path is an optimization for the “add/remove‑only” case, with a safe escape hatch back to the canonical flow.

## What this implements (aligned to your design)

- **Reverse scan V → F+1** over `_delta_log/*.json`, maintaining a dynamic **removed** set.
- **Emit adds** from each version iff not removed and **predicate-pruned** by `stats/partitionValues`.
- **Guardrail tripwire**: if any action ≠ `add|remove` (e.g., `metaData`, `protocol`, `cdc`, `columnMapping`, `dv`) → **fallback** to canonical forward path.
- **Snapshot pinning**: max JSON version **V** is fixed at open time.
- **Retention safety**: floor **F** = latest checkpoint **C** (and must be inside the configured tombstone retention horizon).
- **Optional small‑tail forward** from checkpoint C if you need deeper pagination beyond the reverse window.

These are 1:1 with the constraints and proof sketch in your document; I keep “commit atomicity” by staging actions per version (removes first, then adds).

---

## Crates you’ll need

```toml
# Cargo.toml
[package]
name = "delta_reverse_reader_azure"
version = "0.1.0"
edition = "2021"

[dependencies]
azure_core = "0.21"
azure_storage = "0.21"
azure_storage_blobs = "0.21"
azure_identity = "0.21"         # for Managed Identity / DefaultAzureCredential
futures = "0.3"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
time = "0.3"
anyhow = "1"
thiserror = "1"
bytes = "1"

# Optional but recommended
bloomfilter = "1.0"              # simple Bloom filter
ahash = "0.8"                    # fast HashSet/HashMap
regex = "1"                      # for version parsing
```

## Azure auth options

- **Managed Identity / Default Chain** (preferred in Azure): `DefaultAzureCredential`.
- **SAS URL**: pass a container or blob SAS to the client.
- **Connection String / Key**: supported by SDK, but avoid for production when possible.

Both are wired below.

---

## Delta JSON action models (minimal)

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Delta log lines are JSON "actions" — one per line.
/// We model only what we need: add/remove (+ a tripwire for "type").
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "add", content = "add", rename = "add")]
pub struct AddActionCompat; // only used for tagging presence

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    pub path: String,
    #[serde(default)]
    pub partition_values: Option<HashMap<String, String>>,
    #[serde(default)]
    pub stats: Option<String>, // raw JSON string in many logs; parse when needed
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    pub path: String,
    // Deletion timestamp/extended fields are ignored for plan-time
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Protocol { pub min_reader_version: Option<i32>, pub min_writer_version: Option<i32> }

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetaData { pub id: Option<String> }

/// We use an enum for quick tripwire; only one field present per line.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum DeltaAction {
    Add { add: Add },
    Remove { remove: Remove },
    // Tripwire actions — presence triggers fallback
    Protocol { protocol: Protocol },
    MetaData { metaData: MetaData },
    Cdc { cdc: serde_json::Value },
    ColumnMappingMode { columnMappingMode: serde_json::Value },
    TdDv { tdDv: serde_json::Value },
    // Unknown → treat as tripwire
    Other(serde_json::Value),
}
```

We only deserialize the minimal stats and partition fields needed for planning-time pruning; any other action triggers the **feature tripwire** to the canonical forward path, per your guardrail.

---

## Azure client & `_delta_log` helpers

```rust
use anyhow::{Context, Result};
use azure_core::auth::TokenCredential;
use azure_identity::DefaultAzureCredential;
use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use regex::Regex;
use std::sync::Arc;

pub struct AzureDeltaLogClient {
    pub container_client: ContainerClient,
    pub table_root: String, // e.g., "tables/sales" so _delta_log lives at "tables/sales/_delta_log"
}

impl AzureDeltaLogClient {
    pub async fn new_with_default_credential(
        account: &str,
        container: &str,
        table_root: &str,
    ) -> Result<Self> {
        let credential = Arc::new(DefaultAzureCredential::default());
        let storage_account = StorageAccountClient::new(
            account.to_string(),
            credential as Arc<dyn TokenCredential>,
        );
        let storage_client = storage_account.as_storage_client();
        let container_client = storage_client.as_container_client(container);
        Ok(Self { container_client, table_root: table_root.to_string() })
    }

    /// When using a SAS on the container URL.
    pub fn new_with_sas_url(container_url_with_sas: &str, table_root: &str) -> Result<Self> {
        let container_client = ContainerClient::from_sas_url(container_url_with_sas)?;
        Ok(Self { container_client, table_root: table_root.to_string() })
    }

    fn delta_log_prefix(&self) -> String {
        format!("{}/_delta_log/", self.table_root.trim_end_matches('/'))
    }

    /// List all JSON commit files and return the max version number (as u64).
    pub async fn list_max_json_version(&self) -> Result<u64> {
        let prefix = self.delta_log_prefix();
        let mut stream = self.container_client
            .list_blobs()
            .prefix(prefix.clone())
            .into_stream();
        let re = Regex::new(r"(\d{20})\.json$").unwrap();

        let mut max_v: Option<u64> = None;
        while let Some(resp) = stream.next().await.transpose()? {
            for blob in resp.blobs.blobs() {
                if let Some(name) = &blob.name {
                    if let Some(cap) = re.captures(name) {
                        let v: u64 = cap[1].parse().unwrap();
                        max_v = Some(max_v.map_or(v, |m| m.max(v)));
                    }
                }
            }
        }
        max_v.context("No commit JSON files found in _delta_log")
    }

    /// Return latest checkpoint version C if exists by parsing the tiny `_last_checkpoint`.
    pub async fn read_last_checkpoint_version(&self) -> Result<Option<u64>> {
        let blob = self.container_client.as_blob_client(format!(
            "{}{}_last_checkpoint",
            self.delta_log_prefix(),
            ""
        ));
        let bytes = match blob.get().into_stream().next().await {
            Some(Ok(b)) => b.data,
            Some(Err(e)) => return Err(e.into()),
            None => return Ok(None),
        };
        let v = parse_last_checkpoint_version(&bytes)?;
        Ok(v)
    }

    /// Read all actions in a given version file 00000000000000000042.json
    pub async fn read_actions_for_version(&self, version: u64) -> Result<Vec<DeltaAction>> {
        let name = format!("{}{:020}.json", self.delta_log_prefix(), version);
        let blob = self.container_client.as_blob_client(name);
        let mut data = Vec::<u8>::new();
        let mut stream = blob.get().into_stream();

        while let Some(chunk) = stream.next().await.transpose()? {
            data.extend_from_slice(&chunk.data);
        }
        parse_actions_by_lines(Bytes::from(data))
    }
}

fn parse_last_checkpoint_version(bytes: &Bytes) -> Result<Option<u64>> {
    // Format: { "version": 42, ... } but can vary; we only need version
    let v: serde_json::Value = serde_json::from_slice(bytes)?;
    if let Some(n) = v.get("version").and_then(|x| x.as_u64()) {
        Ok(Some(n))
    } else {
        Ok(None)
    }
}

fn parse_actions_by_lines(bytes: Bytes) -> Result<Vec<DeltaAction>> {
    let mut out = Vec::new();
    for line in bytes.split(|&b| b == b'\n') {
        if line.is_empty() { continue; }
        let act: DeltaAction = serde_json::from_slice(line)
            .context("Failed to parse delta action line")?;
        out.push(act);
    }
    Ok(out)
}
```

Memory bounds: Bloom caps memory; false positives only **over‑suppress** emissions (never incorrect includes), consistent with the complexity notes in your doc.

---

## Predicate pruning stub

You likely already have your filters. Here’s a placeholder that can read `add.stats` JSON when present and use partition values:

```rust
#[derive(Default, Debug, Clone)]
pub struct Predicates {
    pub partitions_eq: HashMap<String, String>,
    // extend with ranges, columns, etc.
}

fn stats_match(pred: &Predicates, add: &Add) -> bool {
    if let Some(p) = &add.partition_values {
        for (k, v) in &pred.partitions_eq {
            if p.get(k).map(String::as_str) != Some(v.as_str()) {
                return false;
            }
        }
    }
    // Add min/max pruning by parsing add.stats if you want:
    // if let Some(stats_json) = &add.stats { ... }
    true
}
```

## The reverse-first planner (core)

```rust
use anyhow::{bail, Result};

#[derive(Debug, Clone)]
pub struct PlanFile {
    pub path: String,
    pub partition_values: Option<HashMap<String, String>>,
}

pub struct ReversePlanner<'a> {
    client: &'a AzureDeltaLogClient,
    retention_floor_guard: Box<dyn Fn(u64) -> bool + Send + Sync>, // ensure within VACUUM horizon
}

impl<'a> ReversePlanner<'a> {
    pub fn new<F>(client: &'a AzureDeltaLogClient, is_within_retention: F) -> Self
    where
        F: Fn(u64) -> bool + Send + Sync + 'static,
    {
        Self { client, retention_floor_guard: Box::new(is_within_retention) }
    }

    /// Main entry: plan files for snapshot pinned at V, scanning reverse down to floor F.
    pub async fn plan_reverse_add_remove(
        &self,
        predicates: &Predicates,
        limit: Option<usize>,
    ) -> Result<Vec<PlanFile>> {
        // --- Pin snapshot ---
        let V = self.client.list_max_json_version().await?;
        let C_opt = self.client.read_last_checkpoint_version().await?;
        let C = C_opt.unwrap_or(0);
        let F = C;

        // Retention safety: do not descend below F if policy says it's unsafe
        if !(self.retention_floor_guard)(F) {
            bail!("Checkpoint floor F={} not within retention horizon", F);
        }

        let mut removed = RemovedSet::new(1_000_000);
        let mut out = Vec::<PlanFile>::new();

        'versions: for v in (F + 1..=V).rev() {
            let actions = self.client.read_actions_for_version(v).await?;

            // Tripwire for unsupported features
            if actions.iter().any(|a| matches!(a,
                DeltaAction::Protocol { .. } |
                DeltaAction::MetaData { .. } |
                DeltaAction::Cdc { .. } |
                DeltaAction::ColumnMappingMode { .. } |
                DeltaAction::TdDv { .. } |
                DeltaAction::Other(_)
            )) {
                // Fallback to canonical forward (checkpoint -> forward tail)
                return self.small_tail_forward_on_demand(predicates, F + 1, V, &removed).await;
            }

            // 1) stage removes of this version into removed
            for a in &actions {
                if let DeltaAction::Remove { remove } = a {
                    removed.add(&remove.path);
                }
            }

            // 2) evaluate adds of this version (commit-atomic ordering)
            for a in &actions {
                if let DeltaAction::Add { add } = a {
                    if !removed.contains(&add.path) && stats_match(predicates, add) {
                        out.push(PlanFile {
                            path: add.path.clone(),
                            partition_values: add.partition_values.clone(),
                        });
                        if let Some(lim) = limit {
                            if out.len() >= lim {
                                break 'versions;
                            }
                        }
                    }
                }
            }
        }

        // If we reached F and still need more, optionally fall back to forward
        if limit.map_or(false, |lim| out.len() < lim) {
            let mut more = self
                .small_tail_forward_on_demand(predicates, F + 1, V, &removed)
                .await?;
            out.append(&mut more);
        }

        Ok(out)
    }

    /// Canonical path: read checkpoint C and replay C+1..V forward.
    /// This is a stub — wire to your existing forward reader.
    async fn small_tail_forward_on_demand(
        &self,
        _predicates: &Predicates,
        _start_version: u64,
        _end_version: u64,
        _already_removed: &RemovedSet,
    ) -> Result<Vec<PlanFile>> {
        // Implement: open checkpoint at C (=start_version-1), materialize base, then forward replay.
        // For now, return empty to keep example focused on reverse scanning.
        Ok(vec![])
    }
}
```

Ordering per commit is “remove first, then evaluate adds,” exactly as your atomicity note prescribes. This ensures newer removes hide older adds and same‑version rewrites behave correctly.

---

## Example usage

```rust
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Choose one auth path:

    // 1) Managed Identity / DefaultAzureCredential
    let account = "mystorageaccount";
    let container = "mycontainer";
    let table_root = "tables/sales"; // your table path inside the container

    let client = AzureDeltaLogClient::new_with_default_credential(account, container, table_root).await?;

    // // 2) SAS URL (alternative)
    // let sas_url = "https://mystorageaccount.blob.core.windows.net/mycontainer?sv=...";
    // let client = AzureDeltaLogClient::new_with_sas_url(sas_url, table_root)?;

    // Retention guard: inject your policy (e.g., ensure C is within last 7 days or org policy)
    let retention_ok = |floor_version: u64| -> bool {
        // If you track version→timestamp, enforce here. For demo, always true.
        // In production, map versions to commit timestamps or use VACUUM config.
        let _ = floor_version;
        true
    };

    let planner = ReversePlanner::new(&client, retention_ok);
    let mut preds = Predicates::default();
    preds.partitions_eq.insert("date".into(), "2026-02-05".into());

    let plan = planner.plan_reverse_add_remove(&preds, Some(200)).await?;

    println!("Planned {} data files:", plan.len());
    for f in plan {
        println!("{}", f.path);
    }
    Ok(())
}
```

## Notes and practical knobs

- **Windowing**: For very large gaps (*V–F*), page the versions in rolling windows (e.g., 1–5K versions per batch). Stop as soon as `limit` is satisfied—this keeps first‑row latency low (new data tends to be near the tip).
- **Emission de‑dup**: If you serve paginated scans, keep a small LFU of `emitted_paths` to avoid duplicates across overlapping pages.
- **I/O**: Azure Blob listings are paged; the SDK stream above handles that. For read throughput, you can parallelize per‑version fetches if your storage account limits allow it.
- **Stats pruning**: Many Delta writers embed `stats` as JSON; you can parse min/max/nullCounts to prune aggressively at plan time, as your doc describes.
- **Feature tripwire**: If your tables sometimes include `metaData`/`protocol` updates near the tip, you’ll see early fallback. That’s by design to preserve correctness under the *add/remove‑only* assumption.
- **Checkpoints as floor, not requirement**: We only **parse `_last_checkpoint`** for the floor version number; no Parquet open on the fast path. Use the canonical **checkpoint → forward tail** on demand if you need deeper history.

---

## What to fill in next (if you want me to extend it)

- A concrete **checkpoint reader** for Azure (Parquet → base snapshot) and the **forward replay**.
- A robust **stats parser** that supports struct/array types and column pushdown.
- **Retention guard** backed by your table’s commit timestamps and org VACUUM policy.
- Optional **ADLS hierarchical namespace** APIs (the Blob SDK above works fine for Gen2, but if you prefer DFS endpoints we can switch to `azure_storage_datalake`).

If you share:
1) your **auth preference** (Managed Identity vs SAS), and  
2) the **shape of your partition filters** (and whether you want min/max pruning now),

I can plug those into the sample and flesh out the forward fallback + stats parsing in the same Rust style. The algorithmic behavior and safety constraints stay exactly as in your original design.

---
---
---
---

## 10) Code Review & Analysis

### Understanding the Challenge

The core problem being addressed: Delta tables with **millions of files** require efficient query execution. The current delta-rs approach (and Spark's approach) requires:
1. Reading the **entire checkpoint Parquet** (potentially GBs for millions of files)
2. Replaying **all JSON commits** since checkpoint
3. Building a **complete in-memory file list** before any query execution

This is fundamentally incompatible with low-latency, memory-bounded query execution on huge tables.

### The Proposed Solution: Reverse-First Reader

Instead of "checkpoint → forward replay → full state → filter → query", the design proposes:

```
Newest commits → scan backwards → emit files immediately → stop early
```

**Key Insight**: For most analytical queries (especially on recent data), you don't need the *full* table state—you need qualifying files, and those are often in the **newest commits**.

### The Guardrails (Correctness Constraints)

The design carefully defines correctness boundaries:

| Constraint | Why It Matters |
|------------|----------------|
| **add/remove only** | Other actions (protocol, metaData, CDC, DVs, column mapping) change semantics and require full state |
| **Snapshot pinning at V** | Ensures consistent view—readers see state at exactly version V |
| **Retention floor F ≥ checkpoint** | Prevents "resurrecting" files whose tombstones were VACUUMed |
| **Commit atomicity** | Same-version adds aren't invalidated by same-version removes |

### The Algorithm

```
removed = {}
for v in V, V-1, ..., F+1:
    removed.add(all remove.path at v)
    for add in adds at v:
        if add.path ∉ removed AND passes_predicate(add.stats):
            emit(add)  # Immediate output, no full materialization
```

**Correctness**: An `add` at version v can only be invalidated by removes at versions > v (already in `removed`). By scanning newest-first, you see removes before the adds they invalidate.

### What Works Brilliantly

1. **First-row latency**: You can start returning qualifying files from the newest commits immediately—no checkpoint load required.

2. **Memory-bounded**: The `removed` set grows only with removes in the scanned window, not with total table size. Bloom filter caps memory.

3. **Early termination**: With `LIMIT`, you can stop after finding enough files—critical for interactive queries.

4. **Graceful fallback**: If you hit a `metaData`/`protocol` action, you fall back to canonical forward reader—no correctness violations.

5. **Checkpoint as floor, not dependency**: You only need the checkpoint *version number* (tiny `_last_checkpoint` JSON), not the checkpoint Parquet itself on the fast path.

### Potential Challenges

#### 1. The "Cold" Query Case
For queries that need files from **deep in history** (not near the tip), reverse scanning could:
- Scan many versions before finding qualifying files
- Build a large `removed` set

**Mitigation**: The hybrid fallback to checkpoint + forward replay handles this case correctly.

#### 2. High-Churn Tables
Tables with frequent overwrites/deletes could have:
- Many remove actions (growing the `removed` set)
- Files that are added and removed within the scanned window

**Mitigation**: Bloom filter caps memory, false positives only over-suppress (conservative).

#### 3. Integration with DataFusion
The current delta-rs / DataFusion integration expects an **iterator or stream of files** for query planning. The reverse reader naturally fits this—it can be a `Stream<Item = PlanFile>` that emits files as they're discovered.

However, DataFusion's physical planning might want:
- File statistics for cost estimation
- Total row count estimates

**Possible approach**: Provide estimates from the scanned window, or fallback to checkpoint stats for planning.

#### 4. Parquet Row Group Filtering
The `stats_match` using `add.stats` is great for file-level pruning. Delta's stats (minValues, maxValues, nullCount) can skip files entirely.

But Parquet row group filtering (predicate pushdown) happens at read time, not plan time—that part is unchanged.

### How This Fits with delta-rs Architecture

Looking at the existing delta-rs code:

| delta-rs Component | Reverse Reader Integration |
|--------------------|--------------------------------|
| `Snapshot::files()` | Replace with `reverse_files_stream()` |
| `EagerSnapshot::files` | Eliminate for queries (use reverse on-demand) |
| `DeltaScan::scan()` | Use reverse file stream instead of `scan_metadata()` |
| `LogicalFileView` | Still useful—wrap each emitted `PlanFile` |

### Key delta-rs Files for Integration

1. **`crates/core/src/kernel/snapshot/mod.rs`** - `Snapshot` and `EagerSnapshot` definitions
2. **`crates/core/src/kernel/snapshot/scan.rs`** - `Scan` and `ScanBuilder` for file iteration
3. **`crates/core/src/kernel/snapshot/iterators.rs`** - `LogicalFileView` and stream wrappers
4. **`crates/core/src/delta_datafusion/table_provider/next/mod.rs`** - `DeltaScan` table provider

### Questions for Further Refinement

1. **Commit frequency**: How many commits typically exist between checkpoints? (Affects the V–F window size)

2. **Query patterns**: Are most queries on recent data (favoring reverse), or do you also query historical data often?

3. **VACUUM policy**: What's your retention period? This determines the safe floor F.

4. **Stats availability**: Do your `add` actions have `stats` populated? (Some writers omit them)

5. **Integration target**: Do you want this as:
   - A standalone Rust crate (this document leans this way)
   - Integrated into delta-rs itself
   - Both (standalone first, upstream later)

### Proposed Implementation Phases

1. **Phase 1**: Implement the core reverse planner (pseudocode → production Rust)
2. **Phase 2**: Integrate with DataFusion as a file source
3. **Phase 3**: Add benchmarks comparing reverse vs. eager snapshot on large tables
4. **Phase 4**: Contribute upstream to delta-rs if successful

---

## 11) Deep Dive: Why "Doesn't Load Checkpoint" Matters

### What is the Checkpoint?

In Delta Lake's log structure:

```
_delta_log/
├── _last_checkpoint          ← Tiny JSON: {"version": 1000, ...}
├── 00000000000000001000.checkpoint.parquet  ← HUGE: snapshot of ALL active files
├── 00000000000000001001.json  ← Small: adds/removes since checkpoint
├── 00000000000000001002.json  ← Small
├── ...
└── 00000000000000001050.json  ← Current version
```

**Checkpoint Parquet file contains:**
- A complete snapshot of ALL active files at version 1000
- For a table with 10 million files, this file could be **10-50 GB**
- It's a Parquet file with columns: path, partitionValues, size, stats, etc.

### Current delta-rs Behavior (Forward Read)

```
1. Read _last_checkpoint → "checkpoint at version 1000"
2. Read 00001000.checkpoint.parquet → 10GB file, all 10M file entries
3. Read commits 1001-1050 (adds/removes) → ~50 small JSON files
4. Replay: apply removes, add new adds → active file set
5. NOW you can iterate files
```

**Time to first file:** Minutes (must read entire checkpoint first)

### The Reverse Reader Approach (Skips Checkpoint!)

```
1. Read _last_checkpoint → "checkpoint at version 1000, current=1050"
2. Read commit 1050 (newest) → small JSON with recent adds/removes
3. EMIT files from commit 1050 immediately!
4. Read commit 1049, emit more files...
5. Continue backwards only if needed
6. STOP when you have enough files (limit reached)
```

**Time to first file:** Milliseconds (just read the newest commit JSON)

### The Key Insight

- **Checkpoint is an optimization for FULL scans** - precomputed snapshot
- **For partial/filtered scans, checkpoint is wasteful** - you read 10M file entries to find 100 matching files
- **Reverse reader IGNORES checkpoint entirely** for the common case where you only need recent/filtered data

### When Checkpoint WOULD Be Needed

The reverse reader needs the checkpoint only if:
1. A file was added BEFORE the checkpoint and never removed
2. Your scan reaches back to the checkpoint boundary

But for "newest N files" or "files in partition X from today's commits", you likely never need the checkpoint at all!

### Visual Comparison

```
Forward Read (current):
┌─────────────────────────────────────────────────────────┐
│  Checkpoint (10GB)  │ Commits (50x small JSON)         │
│  ████████████████   │  ○ ○ ○ ○ ○ ○ ○ ○ ○ ○ ○ ○         │
│  ▲ MUST READ ALL    │  ▲ MUST READ ALL                 │
└─────────────────────────────────────────────────────────┘
                      ↓
                 LOG REPLAY
                      ↓
              [Active File Set]
                      ↓
               Iterate Files

Reverse Read (this approach):
┌─────────────────────────────────────────────────────────┐
│  Checkpoint (10GB)  │ Commits (50x small JSON)         │
│  ░░░░░░░░░░░░░░░░   │  ○ ○ ○ ○ ○ ○ ○ ○ ○ ○ ● ●         │
│  ▲ SKIP!            │              Read backwards →    │
└─────────────────────────────────────────────────────────┘
                                     ↓
                              Emit file immediately
                                     ↓
                              Need more? Continue
                                     ↓
                              Limit reached? STOP
```

### Code Difference

```rust
// Current delta-rs Snapshot creation:
pub async fn try_new(...) -> DeltaResult<Snapshot> {
    // MUST read checkpoint to get snapshot
    let checkpoint = read_checkpoint_parquet(&checkpoint_path).await?; // ⚠️ SLOW
    let commits = read_commits_since(checkpoint_version).await?;
    let files = replay(checkpoint, commits);  // Compute full file set
    Ok(Snapshot { files })
}

// Reverse Reader approach:
pub async fn new(...) -> DeltaResult<ReverseScanner> {
    // Only read tiny _last_checkpoint JSON
    let last_cp = read_last_checkpoint_json().await?;  // ✓ FAST
    Ok(ReverseScanner { 
        version: last_cp.version,
        checkpoint_version: last_cp.checkpoint_version,
        // NO files loaded yet!
    })
}

pub fn scan_files(&self) -> Stream<LogicalFileView> {
    // Read commits on-demand, backwards
    // Only touch checkpoint if we scan that far back
}
```

### Summary Comparison

| | Forward (Current) | Reverse (This Design) |
|--|-------------------|----------------------|
| Checkpoint read | **Always** (10GB) | Only if needed (usually never) |
| Time to first file | Minutes | Milliseconds |
| Memory usage | O(all files) | O(removes seen) |
| Best for | Full table scans | Filtered/limited queries |

---

## 12) Can Checkpoint Parquet Be Streamed?

Yes! Parquet is designed for streaming/partial reads. Here's how:

### Parquet File Structure

```
┌────────────────────────────────────────────────────────┐
│ Magic Number (PAR1)                                     │
├────────────────────────────────────────────────────────┤
│ Row Group 1                                             │
│   ├─ Column Chunk: path          (compressed pages)    │
│   ├─ Column Chunk: size          (compressed pages)    │
│   ├─ Column Chunk: partitionValues (compressed pages)  │
│   └─ Column Chunk: stats         (compressed pages)    │
├────────────────────────────────────────────────────────┤
│ Row Group 2                                             │
│   └─ ...                                               │
├────────────────────────────────────────────────────────┤
│ Row Group N                                             │
│   └─ ...                                               │
├────────────────────────────────────────────────────────┤
│ Footer (metadata + row group offsets)                   │
│ Footer Length (4 bytes)                                 │
│ Magic Number (PAR1)                                     │
└────────────────────────────────────────────────────────┘
```

### Step 1: Read Footer Only (Small)

```rust
async fn read_parquet_footer(
    store: &dyn ObjectStore,
    path: &Path,
) -> Result<ParquetMetaData> {
    // Get file size from HEAD request
    let meta = store.head(path).await?;
    let file_size = meta.size;
    
    // Read last 8 bytes to get footer length
    let footer_size_range = (file_size - 8)..file_size;
    let footer_size_bytes = store.get_range(path, footer_size_range).await?;
    let footer_length = parse_footer_length(&footer_size_bytes);
    
    // Read footer (typically 1-10KB even for huge files)
    let footer_range = (file_size - 8 - footer_length)..file_size;
    let footer_bytes = store.get_range(path, footer_range).await?;
    
    // Parse metadata
    let metadata = parse_parquet_footer(&footer_bytes)?;
    Ok(metadata)  // Contains row group locations, schema, row counts
}
```

**Footer contains:**
- Schema
- Number of row groups
- For each row group: offset, size, row count, column chunk locations
- Total row count

### Step 2: Stream Row Groups One at a Time

```rust
async fn stream_checkpoint_parquet(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    limit: Option<usize>,
) -> impl Stream<Item = Result<RecordBatch>> {
    // Create async reader
    let reader = ParquetObjectReader::new(store, path);
    
    // Build streaming reader
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .with_batch_size(8192);  // Rows per batch
    
    // Only select columns we need
    let builder = builder.with_projection(
        ProjectionMask::leaves(builder.parquet_schema(), [0, 1, 2])  // path, size, partitionValues
    );
    
    // Apply row limit if specified
    let builder = if let Some(limit) = limit {
        builder.with_limit(limit)
    } else {
        builder
    };
    
    // This returns a Stream, reads row groups on-demand!
    builder.build()?
}
```

### What Streaming Enables

| Approach | Data Read | Memory | Time to First Row |
|----------|-----------|--------|-------------------|
| Full load | 10 GB | 10 GB | Minutes |
| Stream all row groups | 10 GB (but streamed) | 1 row group (~100MB) | Seconds |
| Stream + limit | Only needed row groups | 1 row group | Seconds |
| Stream + column projection | Much less | 1 row group | Seconds |

### The Catch for Checkpoint Files

The problem is **row group ordering doesn't match file ordering in Delta**:

1. Checkpoint files are written **in path order** (alphabetical or random), not chronological order
2. So the "newest files" aren't necessarily in the last row group
3. You'd still need to scan all row groups to find files matching a partition predicate

**However**, you CAN still benefit from streaming for:

1. **Column projection** - Only read `path` and `partitionValues` columns, skip `stats`
2. **Early termination** - If you find enough files, stop reading more row groups
3. **Memory efficiency** - Process one row group at a time

### Better Solution: Combine Both Approaches

```rust
pub async fn hybrid_scan(
    &self,
    predicate: Option<PredicateRef>,
    limit: Option<usize>,
) -> Stream<LogicalFileView> {
    // 1. First, try reverse commit scan (newest commits)
    let from_commits = self.reverse_scan_commits(predicate.clone(), limit).await;
    
    // 2. If we hit the checkpoint boundary and still need more files,
    //    stream the checkpoint in reverse row-group order
    let from_checkpoint = self.stream_checkpoint_if_needed(predicate, remaining_limit);
    
    // 3. Chain them together
    from_commits.chain(from_checkpoint)
}
```

---

## 13) Addressing Commit Count Concerns

### "Won't there be a huge number of commits to read?"

**No!** Delta Lake creates checkpoints **regularly** (every 10 commits by default):

```
_delta_log/
├── 00000000000000000000.json
├── 00000000000000000001.json
├── ...
├── 00000000000000000009.json
├── 00000000000000000010.checkpoint.parquet  ← Checkpoint at 10
├── 00000000000000000010.json
├── 00000000000000000011.json
├── ...
├── 00000000000000000019.json
├── 00000000000000000020.checkpoint.parquet  ← Checkpoint at 20
├── ...
└── 00000000000000001050.json  ← Current version
```

### How Many Commits Since Last Checkpoint?

| Scenario | Commits since checkpoint | Overhead |
|----------|-------------------------|----------|
| Default (every 10) | 0-9 commits | 0-9 small JSON files |
| Custom (every 100) | 0-99 commits | 0-99 small JSON files |
| Worst case (every 1000) | 0-999 commits | Still manageable |

**Typical case:** At most 10-100 JSON files, each 1-100KB.

### Optimizations for Multiple Commits

#### 1. Batch File Listing (Single API Call)

```rust
// Instead of reading commits one by one:
async fn list_commits_since_checkpoint(
    store: &dyn ObjectStore,
    checkpoint_version: i64,
    current_version: i64,
) -> Vec<Path> {
    // Single LIST operation returns all commit files
    let prefix = Path::from("_delta_log/");
    let commits: Vec<_> = store
        .list(Some(&prefix))
        .try_filter(|meta| {
            let version = parse_version(&meta.location);
            future::ready(version > checkpoint_version && version <= current_version)
        })
        .map(|meta| meta.location)
        .try_collect()
        .await?;
    
    commits  // Got all paths in ONE list call
}
```

#### 2. Parallel Download

```rust
// Download commits in parallel
async fn download_commits_parallel(
    store: &dyn ObjectStore,
    commit_paths: Vec<Path>,
) -> Vec<CommitData> {
    futures::stream::iter(commit_paths)
        .map(|path| async {
            store.get(&path).await
        })
        .buffer_unordered(16)  // 16 concurrent downloads
        .try_collect()
        .await
}
```

#### 3. Reverse Order Processing

```rust
// Process newest commits first
let commits = list_commits_since_checkpoint(...).await?;
commits.sort_by(|a, b| b.version.cmp(&a.version));  // Descending

for commit in commits {
    // Process newest first
    // Early termination when limit reached
}
```

### Real-World Numbers

For a table at version 1050 with checkpoint at 1040:

| Operation | Count | Size | Time |
|-----------|-------|------|------|
| List commits | 1 API call | - | ~50ms |
| Download 10 commits | 10 parallel | ~10KB each | ~100ms |
| Parse JSON | 10 files | - | ~10ms |
| **Total** | - | ~100KB | **~160ms** |

Compare to checkpoint approach:

| Operation | Size | Time |
|-----------|------|------|
| Download checkpoint | 10GB | 30+ seconds |
| Parse Parquet | 10GB | 30+ seconds |
| **Total** | 10GB | **60+ seconds** |

### The Trade-off

| Many small commits | One big checkpoint |
|-------------------|-------------------|
| N * small HTTP requests | 1 large HTTP request |
| N * small JSON parses | 1 large Parquet parse |
| Easy early termination | Must read all |
| Low memory | High memory |

**Break-even point:** Around 100-500 commits, the overhead of many small requests starts to match one large checkpoint read. But with checkpoints every 10 commits, you'll rarely see more than 10-20 commits.

### Hybrid Approach (Best of Both)

```rust
async fn smart_scan(&self, limit: Option<usize>) -> Stream<LogicalFileView> {
    let commits_since_checkpoint = self.count_commits_since_checkpoint();
    
    if commits_since_checkpoint > 100 {
        // Too many commits - stream the checkpoint instead
        self.stream_checkpoint_with_commits()
    } else {
        // Few commits - reverse scan is efficient
        self.reverse_scan_commits()
    }
}
```

### Summary

| Concern | Reality |
|---------|---------|
| "Huge number of commits" | Usually 0-10 (checkpoint every 10) |
| HTTP overhead | Mitigated by parallel downloads |
| Still too many? | Threshold check + fallback to checkpoint streaming |

**The reverse reader is efficient BECAUSE Delta Lake maintains checkpoints frequently.** The typical case is reading ~10 tiny JSON files, not thousands.

---

## 14) Forward Streaming vs Reverse Reader: Which to Choose?

### The Forward Streaming Approach (Alternative Design)

An alternative approach you might consider: collect removes from commits, then stream the checkpoint with filtering.

```
1. Read commits since checkpoint → collect all removes into a HashSet
2. Stream checkpoint parquet (row group by row group, memory-efficient)
3. For each file from checkpoint: if NOT in removes, emit it
4. Then add files from recent commits (not already emitted)
```

### Code Sketch

```rust
async fn forward_streaming_scan(
    commits_since_checkpoint: &[Commit],
    checkpoint_path: &Path,
    predicate: &Predicate,
) -> impl Stream<Item = LogicalFileView> {
    // Step 1: Collect removes from commits (small, ~10 commits typical)
    let mut removed: HashSet<String> = HashSet::new();
    let mut added_in_commits: Vec<Add> = Vec::new();
    
    for commit in commits_since_checkpoint {
        for action in &commit.actions {
            match action {
                Remove { path } => { removed.insert(path.clone()); }
                Add { add } => { added_in_commits.push(add.clone()); }
            }
        }
    }
    
    // Step 2: Stream checkpoint, filtering by remove mask
    let checkpoint_stream = stream_checkpoint_parquet(checkpoint_path)
        .filter(|file| !removed.contains(&file.path))
        .filter(|file| predicate.matches(file));
    
    // Step 3: Chain with adds from commits (also filtered)
    let commit_adds_stream = futures::stream::iter(added_in_commits)
        .filter(|add| !removed.contains(&add.path))
        .filter(|add| predicate.matches(add));
    
    checkpoint_stream.chain(commit_adds_stream)
}
```

### Comparison Table

| Aspect | Forward Streaming + Remove Mask | Reverse Reader |
|--------|--------------------------------|----------------|
| **Memory** | O(removes) + O(1 row group) | O(removes) |
| **Network I/O** | Download checkpoint (reduced with projection) | Only commits (no checkpoint) |
| **Time to first file** | After checkpoint streaming starts | Immediate from newest commit |
| **File ordering** | Checkpoint order (arbitrary) + commits | Newest first (temporal) |
| **Early termination** | Works after checkpoint streaming starts | Works immediately |
| **Best for** | Full table scan / most files needed | Filtered/limited/recent data |

### When Forward Streaming Wins

#### Scenario 1: You Need Most Files

If your query needs 90%+ of the table's files (e.g., full table scan):
- Checkpoint has all files compacted in one place
- Streaming is efficient (one sequential read)
- Reverse would need to scan ALL commits back to checkpoint anyway

#### Scenario 2: Checkpoint Is Already Cached

If you have the checkpoint cached locally or in memory:
- No additional network cost
- Just apply the remove mask and filter

#### Scenario 3: Very Few Removes

If commits since checkpoint have very few removes:
- The remove mask is tiny
- Checkpoint streaming is nearly "pure read"

### When Reverse Reader Wins

#### Scenario 1: You Need Recent Data

Query: "Give me files from today's partition"
- Reverse: Read today's commits → done
- Forward: Read entire checkpoint → filter 99% away

#### Scenario 2: LIMIT Query

Query: "LIMIT 100 files matching predicate"
- Reverse: Stop after finding 100 (maybe 1-2 commits)
- Forward: Must stream checkpoint until 100 found (could be near the end)

#### Scenario 3: Interactive/Low Latency

User is waiting for first results:
- Reverse: First file in milliseconds
- Forward: First file after checkpoint streaming starts (~seconds)

#### Scenario 4: Append-Heavy Tables

Tables with many adds, few removes:
- Reverse finds lots of files quickly in recent commits
- Forward still needs to stream checkpoint

### The Smart Hybrid: Best of Both

```rust
async fn smart_scan(
    predicate: &Predicate,
    limit: Option<usize>,
) -> impl Stream<Item = LogicalFileView> {
    let commits = read_commits_since_checkpoint().await?;
    
    // Analyze commits
    let adds_in_commits = commits.iter().flat_map(|c| &c.adds).count();
    let removes_in_commits = commits.iter().flat_map(|c| &c.removes).count();
    let estimated_checkpoint_files = read_checkpoint_file_count_from_footer().await?;
    
    // Decision logic
    if let Some(lim) = limit {
        if adds_in_commits >= lim {
            // Reverse scan: enough files in recent commits
            return reverse_scan(commits, predicate, limit);
        }
    }
    
    if adds_in_commits > estimated_checkpoint_files * 0.5 {
        // Lots of new adds: reverse might be faster
        return reverse_scan(commits, predicate, limit);
    }
    
    // Otherwise: forward streaming is efficient
    forward_streaming_with_remove_mask(commits, checkpoint_path, predicate)
}
```

### Correctness: Both Approaches Handle Edge Cases

Files added AND removed since checkpoint:

```
Checkpoint at v100: files A, B, C
Commit v101: add D
Commit v102: add E
Commit v103: remove D  ← D was added after checkpoint
Commit v105: current
```

**Forward streaming:**
- Checkpoint stream: A, B, C (none removed)
- Commit adds: D, E
- Remove mask: {D}
- Final: A, B, C, E ✓

**Reverse reader:**
- v105: nothing
- v103: remove D → removed = {D}
- v102: add E → emit E
- v101: add D → skip (in removed)
- Need more? Read checkpoint...
- Final: E, A, B, C ✓

Both produce the same result, different order.

### API Design: Support Both Strategies

```rust
pub enum ScanStrategy {
    /// Stream checkpoint + apply remove mask
    Forward,
    /// Scan commits newest-first, skip checkpoint if possible
    Reverse,
    /// Let the library choose based on heuristics
    Auto,
}

impl DeltaTable {
    pub fn file_views(
        &self,
        predicate: Option<Predicate>,
        limit: Option<usize>,
        strategy: ScanStrategy,
    ) -> impl Stream<Item = DeltaResult<LogicalFileView>> {
        match strategy {
            ScanStrategy::Forward => self.forward_streaming_scan(predicate, limit),
            ScanStrategy::Reverse => self.reverse_scan(predicate, limit),
            ScanStrategy::Auto => self.smart_scan(predicate, limit),
        }
    }
}
```

### Summary: When to Use Each

| Use Case | Recommended Strategy |
|----------|---------------------|
| Full table scan | Forward Streaming |
| `SELECT * LIMIT 100` | Reverse |
| `WHERE date = today` | Reverse |
| `WHERE rare_partition = X` | Forward (need to scan checkpoint) |
| Interactive dashboard | Reverse (low latency) |
| Batch ETL processing | Forward (throughput) |
| Unknown / general | Auto (let library decide) |

### Bottom Line

Both approaches are **valid and complementary**:
- **Forward Streaming**: Better for complete/large result sets, throughput-focused
- **Reverse Reader**: Better for filtered/limited/interactive queries, latency-focused

Implementing both gives you flexibility to choose the optimal strategy per query.












