# Lazy Fast Iterator Design for Delta Lake

## A Memory-Efficient Streaming Architecture for Querying Huge Delta Tables

**Version:** 1.0  
**Date:** February 2026  
**Status:** Design Document     
**Target Client:** Azure Data Explorer (ADX / Kusto)  
**Storage Backend:** Azure Blob Storage (ADLS Gen2)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement](#2-problem-statement)
3. [Solution Overview](#3-solution-overview)
4. [Architecture](#4-architecture)
5. [Core Algorithm: Reverse-First Scanning](#5-core-algorithm-reverse-first-scanning)
6. [Feature Tripwire: Fallback to Standard Flow](#6-feature-tripwire-fallback-to-standard-flow)
7. [Forward Streaming Alternative](#7-forward-streaming-alternative)
8. [Smart Strategy Selection](#8-smart-strategy-selection)
9. [Memory Guarantees](#9-memory-guarantees)
10. [API Design](#10-api-design)
11. [Client Streaming (C# Integration)](#11-client-streaming-c-integration)
12. [Correctness Proofs](#12-correctness-proofs)
13. [Performance Analysis](#13-performance-analysis)
14. [Implementation Phases](#14-implementation-phases)

---

## 1. Executive Summary

This document describes a **lazy, streaming iterator** for reading Delta Lake tables that addresses two critical production issues:

### Primary Objectives

1. **Eliminate Out-of-Memory Failures**
   - Current architecture loads ALL file metadata into memory before query execution
   - For tables with millions of files, this causes OOM failures in production
   - Proposed: O(removes) memory instead of O(all files)

2. **Enable Parallel Execution**
   - Current: Query execution is BLOCKED until full Delta state is built
   - Proposed: Query processing starts IMMEDIATELY as files are discovered
   - Delta log scanning and data processing run in parallel

### Secondary Benefits

- Early termination for LIMIT queries
- Faster time-to-first-result for interactive workloads
- Safe fallback when advanced Delta features are detected

The design enables querying Delta tables with **millions of files** without exhausting memory and without the sequential blocking behavior that causes query execution to sit idle.

---

## 2. Problem Statement

### Current Behavior (delta-rs / Spark)

```
┌─────────────────────────────────────────────────────────────────────┐
│              SEQUENTIAL EXECUTION (Current)                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Time ──────────────────────────────────────────────────────────►   │
│                                                                      │
│  Phase 1: Build Delta State         Phase 2: Query Execution        │
│  ┌────────────────────────────┐     ┌────────────────────────────┐  │
│  │ 1. Read checkpoint (10GB)  │     │ 5. Execute query           │  │
│  │ 2. Read all JSON commits   │ ──► │ 6. Return results          │  │
│  │ 3. Replay log              │     │                            │  │
│  │ 4. Build full file list    │     │                            │  │
│  └────────────────────────────┘     └────────────────────────────┘  │
│  ◄────── BLOCKING ──────────►       ◄───── CAN START ─────────►    │
│        (Minutes for 10M files)                                      │
│                                                                      │
│  Query execution CANNOT START until FULL delta state is built       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**The Core Problem: Sequential Dependency**

In the current architecture:
1. **Delta state retrieval and query execution are SEQUENTIAL**
2. **Query processing CANNOT start until the ENTIRE file list is materialized**
3. **Even if you only need 100 files, you must wait for ALL 10 million to load**

This creates a **blocking dependency** where expensive I/O (reading checkpoint) must complete before any useful work can begin.

```
1. Read checkpoint Parquet (10-50 GB for 10M files)
2. Read all JSON commits since checkpoint
3. Replay log to compute full active file set
4. Load all file metadata into memory
5. ONLY THEN can query execution start ← BOTTLENECK
```

**Impact on Huge Tables:**

| Metric | 10M File Table |
|--------|----------------|
| Checkpoint size | 10-50 GB |
| Memory required | 10+ GB |
| Time to first file | Minutes |
| Query parallelism | ❌ None (blocked waiting for delta) |
| Suitable for interactive queries | ❌ No |

### The Fundamental Issue

The query execution layer (DataFusion, Spark) is highly parallelized and efficient. But it sits **idle** while waiting for the Delta log replay to complete:

```
                    Time →
Current:  [=====Delta Log Replay=====][===Query Execution===]
                   ▲                        ▲
                   │                        │
              CPU idle              Finally starts working
              Network busy          on parallelized query
```

**For a LIMIT 100 query on a 10M file table:**
- You pay the full cost of reading 10M file entries
- Query only needs ~1-2 files worth of data
- 99.99% of the work was wasted

### The Need

For many query patterns, we don't need **all** files:
- `SELECT * FROM table LIMIT 100`
- `SELECT * FROM table WHERE date = today`
- Interactive dashboard queries
- Partition-filtered analytics

These queries often need only files from **recent commits** or **specific partitions**.

---

## 3. Solution Overview

### Core Insight

Instead of materializing the full file list upfront, we:

1. **Scan commits in reverse** (newest first)
2. **Emit qualifying files immediately** as they're discovered
3. **Maintain a small remove set** to track deletions
4. **Stop early** when limit is reached or predicate is satisfied
5. **Skip the checkpoint entirely** for most queries

### The Key Innovation: Parallel Execution

The lazy iterator enables **parallel work** between delta log scanning and query execution:

```
┌─────────────────────────────────────────────────────────────────────┐
│              PARALLEL EXECUTION (Proposed)                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Time ──────────────────────────────────────────────────────────►   │
│                                                                      │
│  Delta Scanning (streams files)    Query Execution (parallel)       │
│  ┌────────────────────────────┐   ┌────────────────────────────┐   │
│  │ Read commit V (newest)     │──►│ Start reading Parquet 1    │   │
│  │ Emit file 1, file 2        │   │ Process data...            │   │
│  │                            │   │                            │   │
│  │ Read commit V-1            │──►│ Read Parquet 2 (parallel)  │   │
│  │ Emit file 3, file 4        │   │ Process data...            │   │
│  │                            │   │                            │   │
│  │ Read commit V-2            │   │ Already have enough → STOP │   │
│  │ STOP (limit reached)       │   │ Return results             │   │
│  └────────────────────────────┘   └────────────────────────────┘   │
│                                                                      │
│  ◄─────────── PARALLEL / PIPELINED ────────────────────────────►   │
│                                                                      │
│  Query execution STARTS IMMEDIATELY as first files are discovered  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Benefits of Parallel Execution:**

| Aspect | Sequential (Current) | Parallel (Proposed) |
|--------|---------------------|---------------------|
| CPU utilization during delta scan | Idle | Processing data |
| Network utilization | Delta only, then Parquet | Both in parallel |
| Time to first result | After ALL files loaded | After FIRST file found |
| Early termination | Wastes all prior work | Stops both streams |

### Result

| Metric | Current Approach | Lazy Iterator |
|--------|-----------------|---------------|
| Time to first file | Minutes | Milliseconds |
| Memory for 10M files | 10+ GB | 10-50 MB |
| Checkpoint required | Always | Only if needed |
| Early termination | Limited | Full support |
| Query parallelism | ❌ Blocked | ✅ Pipelined |

---

## 4. Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Delta Table                                  │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                      _delta_log/                              │   │
│  │  ├── _last_checkpoint (tiny JSON)                            │   │
│  │  ├── 00000001000.checkpoint.parquet (HUGE)                   │   │
│  │  ├── 00000001001.json (small)                                │   │
│  │  ├── 00000001002.json (small)                                │   │
│  │  └── 00000001050.json (current version)                      │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    LazyDeltaScanner                                  │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  1. Read _last_checkpoint (get version V, checkpoint C)      │   │
│  │  2. Choose strategy: Reverse / Forward / Auto                │   │
│  │  3. Scan commits V → C+1 in chosen order                     │   │
│  │  4. Maintain remove set (memory-bounded)                     │   │
│  │  5. Emit files as Stream<LogicalFileView>                    │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                              │                                       │
│                              │ Feature Tripwire                      │
│                              ▼                                       │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  If non-add/remove action found → Fallback to Standard Flow  │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Streaming Output                                  │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  Stream<LogicalFileView> → Arrow Flight / FFI / Direct       │   │
│  │                                                               │   │
│  │  • Backpressure supported                                    │   │
│  │  • Early termination on LIMIT                                │   │
│  │  • Memory bounded                                            │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Client (C#, Python, etc.)                         │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  • Receives files as they're discovered                      │   │
│  │  • Can stop consuming at any time                            │   │
│  │  • Reads Parquet files directly from storage                 │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | Responsibility |
|-----------|---------------|
| **LazyDeltaScanner** | Main coordinator, strategy selection |
| **ReverseCommitReader** | Reads commits newest-first |
| **RemoveSet** | Memory-bounded tracking of removed files |
| **PredicateEvaluator** | Applies partition/stats filtering |
| **StreamingOutput** | Yields files to client with backpressure |
| **FeatureTripwire** | Detects unsupported actions, triggers fallback |

---

## 5. Core Algorithm: Reverse-First Scanning with Hybrid Checkpoint

### Algorithm Overview

```
Pin snapshot version V (max JSON version in _delta_log)
Get checkpoint version C from _last_checkpoint
Set floor F = C (within VACUUM retention)

Initialize removed = {} (memory-bounded set)
Initialize emitted = 0

// ═══════════════════════════════════════════════════════════════════
// PHASE 1: Scan commits in reverse (V → C+1)
// ═══════════════════════════════════════════════════════════════════

FOR v = V down to F+1:
    actions = read_commit(v)
    
    // TRIPWIRE: Check for unsupported actions
    IF any action is NOT add/remove:
        RETURN fallback_to_standard_flow()
    
    // Stage removes first (commit atomicity)
    FOR each remove in actions:
        removed.add(remove.path)
    
    // Evaluate adds
    FOR each add in actions:
        IF add.path NOT IN removed AND predicate.matches(add):
            EMIT add as LogicalFileView
            emitted++
            IF limit AND emitted >= limit:
                RETURN  // Early termination - DONE

// ═══════════════════════════════════════════════════════════════════
// PHASE 2: Stream checkpoint in bounded chunks (if more files needed)
// ═══════════════════════════════════════════════════════════════════

IF no limit OR emitted < limit:
    // Need more files - stream checkpoint with memory bounds
    
    checkpoint = open_checkpoint_parquet(C)
    row_groups_per_batch = config.checkpoint_row_groups_per_batch  // e.g., 10
    
    FOR batch in checkpoint.read_in_batches(row_groups_per_batch):
        FOR each file_entry in batch:
            // Skip if removed (using remove set from Phase 1)
            IF file_entry.path IN removed:
                CONTINUE
            
            // Skip if predicate doesn't match
            IF NOT predicate.matches(file_entry):
                CONTINUE
            
            EMIT file_entry as LogicalFileView
            emitted++
            IF limit AND emitted >= limit:
                RETURN  // Early termination - DONE
        
        // Memory released: batch goes out of scope
```

### Why This Works

1. **Correctness**: An `add` at version v can only be invalidated by removes at versions > v
2. **Order**: By scanning newest-first, we see removes before the adds they invalidate
3. **Early termination**: We can stop as soon as we have enough files
4. **Memory**: We only store remove paths, not all file metadata

### Commit Atomicity

Within a single commit, actions are applied atomically. The algorithm:
1. First collects all removes from the commit
2. Then evaluates adds from the same commit

This ensures same-version removes don't invalidate same-version adds (matching forward semantics).

---

## 5.1. Hybrid Flow: When Commits Don't Have Enough Files

### The Scenario

When scanning commits in reverse:
1. All commits contain only `add` and `remove` actions ✓ (safe to continue)
2. We've collected the remove set from all commits
3. We've emitted all qualifying files from commits
4. **But we still need more files** (no limit, or limit not yet reached)

In this case, we **continue to the checkpoint** but stream it in a memory-bounded way.

### Hybrid Two-Phase Algorithm

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PHASE 1: Reverse Commit Scan                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Scan commits V → C+1 (newest to checkpoint)                        │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ • Emit files from commits (filtered by predicate)              │ │
│  │ • Build remove set as we scan                                  │ │
│  │ • If limit reached → STOP (early termination)                  │ │
│  │ • If tripwire triggered → FALLBACK to standard flow            │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  Result: Emitted N files from commits, still need more              │
│  Remove set: Contains all removes from commits (memory-bounded)     │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│          PHASE 2: Stream Checkpoint (Memory-Bounded)                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Stream checkpoint Parquet in CHUNKS of X row groups                │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ FOR each batch of X row groups (e.g., X=10 → ~100MB):          │ │
│  │   1. Read X row groups from checkpoint Parquet                 │ │
│  │   2. For each file entry in batch:                             │ │
│  │      - Skip if path IN remove set (from Phase 1)               │ │
│  │      - Skip if predicate doesn't match                         │ │
│  │      - EMIT file (streaming to client)                         │ │
│  │   3. Release batch memory                                      │ │
│  │   4. If limit reached → STOP                                   │ │
│  │   5. Continue to next batch                                    │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  Memory: O(X row groups) + O(remove set) at any time                │
│  NOT O(all files in checkpoint)                                     │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Memory-Bounded Checkpoint Streaming

```rust
/// Stream checkpoint in row group batches
async fn stream_checkpoint_bounded(
    &self,
    remove_set: &RemoveSet,
    predicate: &Predicate,
    row_groups_per_batch: usize,  // e.g., 10
) -> impl Stream<Item = DeltaResult<LogicalFileView>> {
    let checkpoint_path = self.get_checkpoint_path().await?;
    let parquet_reader = self.open_parquet_streaming(&checkpoint_path).await?;
    
    let total_row_groups = parquet_reader.num_row_groups();
    
    // Process in batches of X row groups
    for batch_start in (0..total_row_groups).step_by(row_groups_per_batch) {
        let batch_end = (batch_start + row_groups_per_batch).min(total_row_groups);
        
        // Read only this batch of row groups
        let batch = parquet_reader
            .read_row_groups(batch_start..batch_end)
            .await?;
        
        // Filter and emit
        for file_entry in batch.iter() {
            // Skip if removed (using remove set from Phase 1)
            if remove_set.contains(&file_entry.path) {
                continue;
            }
            
            // Skip if predicate doesn't match
            if !predicate.matches(&file_entry) {
                continue;
            }
            
            // Emit to stream
            yield Ok(file_entry.into());
        }
        
        // Batch memory is released here when batch goes out of scope
    }
}
```

### Configuration

```rust
pub struct LazyScanConfig {
    // ... existing fields ...
    
    /// Number of row groups to read per batch from checkpoint
    /// Higher = more memory, fewer I/O round trips
    /// Lower = less memory, more I/O round trips
    /// Default: 10 (~100MB per batch)
    pub checkpoint_row_groups_per_batch: usize,
}
```

### Memory Comparison

| Phase | Memory Usage | Notes |
|-------|--------------|-------|
| Phase 1: Commits | O(removes) | Bloom filter capped |
| Phase 2: Checkpoint | O(X row groups) | ~100MB for X=10 |
| **Total** | O(removes) + O(X row groups) | ~50-150MB |
| Standard reader | O(all files) | 10+ GB for 10M files |

### Why This Works

1. **Remove set is already built** from Phase 1 - no extra work
2. **Checkpoint is streamed** - never fully loaded into memory
3. **Files are emitted immediately** as they pass filters
4. **Parallelism continues** - client processes files while we stream more
5. **Early termination** - stop streaming checkpoint if limit reached

### Example: Full Table Scan

For a table with:
- 10M files in checkpoint
- 1000 removes in recent commits
- No limit (need all files)

```
Phase 1: Scan 10 commits
├── Build remove set: 1000 paths (~50KB with Bloom filter)
├── Emit 500 files from commits
└── Need more files → continue to Phase 2

Phase 2: Stream checkpoint (10M files)
├── Batch 1: Row groups 0-9 → ~100MB → filter → emit 50K files → release
├── Batch 2: Row groups 10-19 → ~100MB → filter → emit 50K files → release
├── ...
└── Batch N: Last row groups → emit remaining files

Memory at any point: ~150MB (remove set + current batch)
NOT 10GB for full checkpoint
```

---

## 6. Feature Tripwire: Fallback to Standard Flow

### Critical Safety Guarantee

**When scanning commits, if we encounter ANY action that is NOT `add` or `remove`, we IMMEDIATELY:**

1. **ABORT** the lazy/reverse scan
2. **ROLLBACK** to the standard forward reader flow
3. **Use the canonical** checkpoint + forward replay approach

### Trigger Actions

| Action Type | Why It Triggers Fallback |
|-------------|-------------------------|
| `metaData` | Schema changes affect data interpretation |
| `protocol` | Version upgrades change action semantics |
| `cdc` | Change data capture has special handling |
| `columnMapping` | Column mapping mode changes file reading |
| `deletionVector` | DVs modify how files are read, not just which |
| `txn` | Transaction markers affect commit semantics |
| Unknown actions | Future compatibility safety |

### Implementation

```rust
fn process_commit(&mut self, version: u64) -> Result<ProcessResult> {
    let actions = self.client.read_actions_for_version(version).await?;
    
    for action in &actions {
        match action {
            DeltaAction::Add { .. } => { /* Safe to process */ }
            DeltaAction::Remove { .. } => { /* Safe to process */ }
            
            // TRIPWIRE: Any other action type
            DeltaAction::Protocol { .. } |
            DeltaAction::MetaData { .. } |
            DeltaAction::Cdc { .. } |
            DeltaAction::ColumnMapping { .. } |
            DeltaAction::DeletionVector { .. } |
            DeltaAction::Txn { .. } |
            DeltaAction::Other(_) => {
                // ABORT lazy scan, use standard flow
                return Ok(ProcessResult::FallbackRequired);
            }
        }
    }
    
    // Safe to continue with lazy processing
    self.process_add_remove_actions(&actions)
}
```

### Fallback Behavior

When fallback is triggered:

```rust
fn fallback_to_standard_flow(&self) -> impl Stream<Item = LogicalFileView> {
    // Use existing delta-rs Snapshot implementation
    // This loads checkpoint + replays all commits
    let snapshot = Snapshot::try_new(self.log_store, None).await?;
    snapshot.file_views(self.log_store, self.predicate)
}
```

### Why This Matters

1. **Protocol Safety**: Delta protocol actions can change interpretation of other actions
2. **Schema Evolution**: metaData changes require full schema context for correctness
3. **Deletion Vectors**: DVs change *how* files are read, not just *which* files
4. **Forward Compatibility**: Unknown actions might have semantics we can't safely ignore

---

## 7. Forward Streaming Alternative

For certain workloads, forward streaming with a remove mask is more efficient.

### Algorithm

```
1. Read commits since checkpoint → collect removes into HashSet
2. Stream checkpoint Parquet (row group by row group)
3. For each file: if NOT in removes AND matches predicate → emit
4. Chain with adds from recent commits
```

### When to Use

| Scenario | Recommended |
|----------|-------------|
| Full table scan | Forward Streaming |
| Need 90%+ of files | Forward Streaming |
| Checkpoint is cached | Forward Streaming |
| Few removes | Forward Streaming |

### Memory Profile

- **O(removes)**: HashSet of removed paths
- **O(1 row group)**: Streaming checkpoint ~100MB at a time
- **Not O(all files)**: Never loads full file list

---

## 8. Smart Strategy Selection

### Auto-Selection Logic

```rust
pub async fn smart_scan(
    &self,
    predicate: Option<Predicate>,
    limit: Option<usize>,
) -> impl Stream<Item = LogicalFileView> {
    let commits = self.list_commits_since_checkpoint().await?;
    let adds_in_commits = commits.iter().flat_map(|c| &c.adds).count();
    let checkpoint_file_count = self.read_checkpoint_footer_count().await?;
    
    // Decision logic
    match (limit, adds_in_commits, checkpoint_file_count) {
        // LIMIT query with enough files in recent commits
        (Some(lim), adds, _) if adds >= lim => {
            ScanStrategy::Reverse
        }
        
        // Interactive query (any limit)
        (Some(_), _, _) => {
            ScanStrategy::Reverse
        }
        
        // Lots of new adds relative to checkpoint
        (None, adds, cp) if adds > cp / 2 => {
            ScanStrategy::Reverse
        }
        
        // Default: forward streaming
        _ => {
            ScanStrategy::Forward
        }
    }
}
```

### Decision Matrix

| Query Pattern | Strategy | Reason |
|--------------|----------|--------|
| `LIMIT 100` | Reverse | Stop after 100 files |
| `WHERE date = today` | Reverse | Recent data in recent commits |
| `WHERE rare_partition = X` | Forward | Need to scan checkpoint |
| Full table scan | Forward | Need all files anyway |
| Interactive dashboard | Reverse | Low latency priority |
| Batch ETL | Forward | Throughput priority |

---

## 9. Memory Guarantees

### Memory Model

| Component | Size | Bound |
|-----------|------|-------|
| Remove set | O(removes in window) | Capped by Bloom filter |
| Current commit | O(1 commit) | ~10-100KB |
| Row group buffer | O(1 row group) | ~100MB max |
| Output buffer | O(batch size) | Configurable |

### Total Memory

**For a 10M file table with 1000 removes since checkpoint:**

| Approach | Memory |
|----------|--------|
| Current delta-rs | 10+ GB |
| Lazy iterator | ~10-50 MB |

### Bloom Filter for Remove Set

```rust
pub struct RemoveSet {
    bloom: BloomFilter,       // Fixed memory cap
    exact: HashSet<String>,   // Overflow for precision
    capacity: usize,
}

impl RemoveSet {
    pub fn new(capacity: usize) -> Self {
        Self {
            bloom: BloomFilter::with_capacity(capacity, 0.01),
            exact: HashSet::new(),
            capacity,
        }
    }
    
    pub fn add(&mut self, path: &str) {
        self.bloom.insert(path);
        if self.exact.len() < self.capacity {
            self.exact.insert(path.to_string());
        }
    }
    
    pub fn contains(&self, path: &str) -> bool {
        // Bloom filter may have false positives
        // False positives = over-suppress (safe, conservative)
        self.bloom.contains(path)
    }
}
```

**Note**: False positives only *over-suppress* file emission—never incorrectly include files.

---

## 10. API Design

### Rust API

```rust
/// Strategy for scanning Delta table files
pub enum ScanStrategy {
    /// Scan commits newest-first, skip checkpoint if possible
    Reverse,
    /// Stream checkpoint + apply remove mask
    Forward,
    /// Auto-select based on query characteristics
    Auto,
}

/// Configuration for lazy scanning
pub struct LazyScanConfig {
    pub predicate: Option<PredicateRef>,
    pub limit: Option<usize>,
    pub strategy: ScanStrategy,
    pub batch_size: usize,
}

impl Default for LazyScanConfig {
    fn default() -> Self {
        Self {
            predicate: None,
            limit: None,
            strategy: ScanStrategy::Auto,
            batch_size: 8192,
        }
    }
}

/// Main entry point for lazy Delta table scanning
pub struct LazyDeltaScanner {
    log_store: Arc<dyn LogStore>,
    config: LazyScanConfig,
}

impl LazyDeltaScanner {
    /// Create a new scanner for the given table
    pub async fn new(
        table_url: &str,
        config: LazyScanConfig,
    ) -> DeltaResult<Self> {
        let log_store = LogStoreFactory::create(table_url).await?;
        Ok(Self { log_store, config })
    }
    
    /// Stream files matching the configuration
    /// 
    /// Files are emitted as they're discovered, enabling:
    /// - Early termination on LIMIT
    /// - Backpressure from slow consumers
    /// - Memory-bounded operation
    pub fn scan_files(&self) -> impl Stream<Item = DeltaResult<LogicalFileView>> + '_ {
        // Implementation based on selected strategy
        match self.config.strategy {
            ScanStrategy::Reverse => self.reverse_scan(),
            ScanStrategy::Forward => self.forward_scan(),
            ScanStrategy::Auto => self.auto_scan(),
        }
    }
}

// Usage
let config = LazyScanConfig {
    predicate: Some(Arc::new(partition_eq("date", "2026-02-08"))),
    limit: Some(100),
    strategy: ScanStrategy::Auto,
    ..Default::default()
};

let scanner = LazyDeltaScanner::new("abfss://container@account/table", config).await?;

// Stream processing - memory bounded
let mut count = 0;
pin_mut!(scanner.scan_files());
while let Some(file) = stream.next().await {
    let file = file?;
    println!("{}", file.path());
    count += 1;
}
```

### C# Client API

```csharp
public interface ILazyDeltaScanner : IDisposable
{
    /// Stream files from a Delta table with minimal memory
    IAsyncEnumerable<DeltaFile> ScanFilesAsync(
        string tablePath,
        ScanOptions? options = null,
        CancellationToken cancellationToken = default);
}

public class ScanOptions
{
    public string? PredicateJson { get; set; }
    public int? Limit { get; set; }
    public ScanStrategy Strategy { get; set; } = ScanStrategy.Auto;
}

public enum ScanStrategy
{
    Auto,
    Reverse,
    Forward
}

public record DeltaFile(
    string Path,
    long Size,
    Dictionary<string, string>? PartitionValues,
    DateTime? ModificationTime);

// Usage
await using var scanner = new DeltaFlightScanner("http://delta-server:50051");

var options = new ScanOptions
{
    PredicateJson = "{\"date\": \"2026-02-08\"}",
    Limit = 100,
    Strategy = ScanStrategy.Auto
};

await foreach (var file in scanner.ScanFilesAsync(tablePath, options))
{
    Console.WriteLine($"{file.Path}: {file.Size} bytes");
    
    // Can break early - stops streaming from server
    if (someCondition) break;
}
```

---

## 11. Client Streaming (ADX/Kusto & C# Integration)

### Primary Target: Azure Data Explorer (Kusto)

The primary client for this lazy iterator is **Azure Data Explorer (ADX/Kusto)**, which needs to query Delta tables stored in Azure Blob Storage. ADX is highly optimized for parallel data ingestion and query execution, but currently faces the same sequential bottleneck when accessing Delta tables.

### ADX Integration Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Azure Data Explorer (Kusto)                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                   Kusto Query Engine                           │  │
│  │  • Highly parallel query execution                            │  │
│  │  • Can process data as soon as file paths arrive              │  │
│  │  • Currently BLOCKED waiting for full Delta file list         │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │              Delta Connector (Current: Sequential)            │  │
│  │  1. Load entire checkpoint (10GB) ──── BLOCKING               │  │
│  │  2. Replay all commits ──────────────── BLOCKING              │  │
│  │  3. Build full file list ────────────── BLOCKING              │  │
│  │  4. THEN pass to query engine ───────── Finally starts        │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    Azure Blob Storage (Delta Table)
```

### Proposed: Lazy Iterator for ADX

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Azure Data Explorer (Kusto)                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                   Kusto Query Engine                           │  │
│  │  • Starts processing IMMEDIATELY as files stream in           │  │
│  │  • Parallel Parquet reads while more files are discovered     │  │
│  │  • Early termination for LIMIT / take queries                 │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              ▲                                       │
│                              │ Streaming file paths                  │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │              Delta Connector (Proposed: Streaming)            │  │
│  │  • LazyDeltaScanner (Rust)                                   │  │
│  │  • Emits files as discovered                                 │  │
│  │  • Arrow Flight or FFI callback                              │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    Azure Blob Storage (Delta Table)
```

### Benefits for ADX

| Aspect | Current (Sequential) | Proposed (Streaming) |
|--------|---------------------|----------------------|
| Query start | After ALL files loaded | After FIRST file found |
| ADX parallelism | Idle during Delta scan | Fully utilized |
| Memory in connector | O(all files) | O(removes only) |
| `| take 100` queries | Full checkpoint load | ~2-3 commits read |
| Interactive queries | Minutes latency | Sub-second latency |

### Integration Options for ADX

#### Option 1: Arrow Flight (Recommended)

Arrow Flight provides efficient streaming with:
- gRPC/HTTP2 transport
- Arrow IPC format (minimal serialization overhead)
- Built-in backpressure
- Early termination support

```
┌─────────────────────────────────────────────────────────┐
│                  ADX / C# Application                    │
│  ┌───────────────────────────────────────────────────┐  │
│  │           Arrow Flight Client                      │  │
│  │  • GetFlightInfo(table, predicate)                │  │
│  │  • DoGet(ticket) → Stream<RecordBatch>            │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                          │ gRPC
                          ▼
┌─────────────────────────────────────────────────────────┐
│             Rust Arrow Flight Server                     │
│  ┌───────────────────────────────────────────────────┐  │
│  │         LazyDeltaScanner Integration              │  │
│  │  • Receives scan request                          │  │
│  │  • Uses lazy iterator internally                  │  │
│  │  • Streams files/data as RecordBatches            │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
                   Azure Blob Storage
```

#### Option 2: In-Process FFI (Lower Latency)

For tighter integration, load the Rust library directly into the ADX process:

```csharp
// ADX Delta Connector using FFI
public class DeltaLazyConnector
{
    [DllImport("delta_lazy")]
    private static extern int delta_scan_lazy(
        string tablePath,
        string predicateJson,
        UIntPtr limit,
        FileCallback callback);

    public IEnumerable<string> GetFiles(string tablePath, string predicate, int? limit)
    {
        var files = new List<string>();
        delta_scan_lazy(
            tablePath,
            predicate,
            (UIntPtr)(limit ?? 0),
            (path, size) => {
                files.Add(path);
                return true; // continue
            });
        return files;
    }
}
```

### Other Delta Clients (For Reference)

While ADX is the primary target, this design also benefits:

| Client | Platform | Integration Method |
|--------|----------|-------------------|
| **ADX (Kusto)** | Azure | Arrow Flight / FFI (primary) |
| Databricks | JVM/Python | Could use similar approach |
| Spark | JVM | Already has internal optimizations |
| DataFusion | Rust | Direct integration |
| Trino/Presto | JVM | Similar architecture |

### Alternative: FFI with Streaming Callback

For same-machine deployment:

```rust
#[no_mangle]
pub extern "C" fn delta_scan_lazy(
    table_path: *const c_char,
    predicate_json: *const c_char,
    limit: usize,
    callback: extern "C" fn(*const c_char, i64) -> bool,
) -> i32 {
    // callback returns false to stop streaming
}
```

```csharp
public delegate bool FileCallback(string path, long size);

[DllImport("delta_lazy")]
private static extern int delta_scan_lazy(
    string tablePath,
    string predicateJson,
    UIntPtr limit,
    FileCallback callback);
```

---

## 12. Correctness Proofs

### Theorem 1: Snapshot Isolation

**Claim**: The lazy iterator returns exactly the same files as the standard forward reader for snapshot version V.

**Proof**: 
- Active(V) = {adds ≤ V} \ {removes ≤ V}
- Reverse scan visits all versions (V, F] where F = checkpoint
- For each version, removes are added to `removed` before evaluating adds
- An add at version v is emitted iff no remove at version > v exists for that path
- This is equivalent to: add ∈ Active(V)

### Theorem 2: Early Termination Safety

**Claim**: Stopping after N files still produces correct results.

**Proof**:
- Each emitted file satisfies: path ∉ removed AND matches predicate
- The first N such files are a valid subset of Active(V) ∩ Predicate
- Order may differ from forward reader, but set semantics are correct

### Theorem 3: VACUUM Safety

**Claim**: The algorithm never resurrects files that were vacuumed.

**Proof**:
- Floor F ≥ latest checkpoint C
- VACUUM only removes files whose tombstones are older than retention
- By not scanning below F, we never see removes that were vacuumed
- Files added before F but not removed before F are in the checkpoint
- Checkpoint is the source of truth for files at version C

---

## 13. Performance Analysis

### Time to First File

| Scenario | Standard Reader | Lazy Iterator |
|----------|-----------------|---------------|
| 10M files, need 100 | 60+ seconds | < 100 ms |
| 10M files, need 1000 | 60+ seconds | < 500 ms |
| 10M files, full scan | 60+ seconds | 60+ seconds |

### Memory Usage

| Scenario | Standard Reader | Lazy Iterator |
|----------|-----------------|---------------|
| 10M files | 10+ GB | 50 MB |
| 100M files | 100+ GB | 50 MB |
| 1B files | Not feasible | 50 MB |

### Network I/O

| Scenario | Standard Reader | Lazy Iterator |
|----------|-----------------|---------------|
| LIMIT 100 | 10 GB (checkpoint) | ~100 KB (commits) |
| Today's partition | 10 GB (checkpoint) | ~1 MB (commits) |
| Full scan | 10 GB | 10 GB |

---

## 14. Implementation Phases

### Timeline Analysis

#### Context & Assumptions

| Factor | Value | Impact |
|--------|-------|--------|
| Team size | 1 experienced SW engineer | Single-threaded development |
| Rust experience | None (learning required) | +50-100% time overhead initially |
| AI assistance | Cline + Claude (Opus 4.5/4.6) | ~50% reduction in Rust learning curve |
| Test coverage | Full coverage required | Significant testing overhead |
| ADX integration | Existing Rust wrapper | Modification, not new development |
| Delta-rs coordination | Required, scope unknown | Potential blocker |

#### Codebase Complexity (from analysis)

| Component | Complexity | Notes |
|-----------|------------|-------|
| `LogicalFileView` | Medium (~300 LOC) | Well-structured, can reuse |
| Snapshot/iterators | High | Need to understand internals |
| Azure storage | Low | Exists in `crates/azure`, reusable |
| Log replay logic | High | Core of the implementation |
| Parquet streaming | Medium | Standard Arrow/Parquet patterns |

### Phase 1: Core Lazy Scanner (Weeks 1-6)

**Duration: 4-6 weeks** (includes Rust learning curve)

- [ ] Study delta-rs codebase and Rust patterns
- [ ] Implement `LazyDeltaScanner` struct
- [ ] Implement reverse commit reading from Azure Blob Storage
- [ ] Implement `RemoveSet` with Bloom filter
- [ ] Implement feature tripwire and fallback mechanism
- [ ] Implement hybrid checkpoint streaming (Phase 2 of algorithm)
- [ ] Unit tests with full coverage

**Deliverable**: Working lazy scanner that can stream files from commits and checkpoint.

### Phase 2: Strategy Selection & Optimization (Weeks 7-8)

**Duration: 2 weeks**

- [ ] Implement forward streaming with remove mask (alternative strategy)
- [ ] Implement auto-selection heuristics
- [ ] Add configuration options
- [ ] Integration tests with real Delta tables on Azure
- [ ] Performance profiling and optimization

**Deliverable**: Complete scanning logic with automatic strategy selection.

### Phase 3: ADX Integration (Weeks 9-11)

**Duration: 2-3 weeks**

- [ ] Modify existing ADX Rust wrapper for lazy iterator
- [ ] Implement streaming callback mechanism for ADX
- [ ] End-to-end testing with ADX queries
- [ ] Handle ADX-specific error cases
- [ ] Performance testing with production-scale tables

**Deliverable**: ADX can query Delta tables using the lazy iterator.

### Phase 4: Production Readiness (Weeks 12-15)

**Duration: 3-4 weeks**

- [ ] Full test coverage verification
- [ ] Benchmarks vs standard reader (memory, latency, throughput)
- [ ] Documentation (code, API, operational)
- [ ] Error handling hardening
- [ ] Logging and metrics integration
- [ ] Coordinate with delta-rs maintainers (if upstream contribution)
- [ ] Security review for Azure credential handling

**Deliverable**: Production-ready implementation with full test coverage.

### Timeline Summary

| Phase | Duration | Cumulative |
|-------|----------|------------|
| Phase 1: Core Scanner | 4-6 weeks | 4-6 weeks |
| Phase 2: Strategy | 2 weeks | 6-8 weeks |
| Phase 3: ADX Integration | 2-3 weeks | 8-11 weeks |
| Phase 4: Production | 3-4 weeks | 11-15 weeks |
| **Total** | **11-15 weeks** | |

### Risk Factors

| Risk | Impact | Mitigation |
|------|--------|------------|
| Delta-rs upstream coordination | +2-4 weeks if significant changes needed | Early engagement with maintainers |
| Complex edge cases (DVs, column mapping) | +1-2 weeks per edge case | Focus on add/remove-only path first |
| ADX wrapper compatibility | +1 week if major changes needed | Early prototype integration |
| Rust learning curve | Already factored in | AI assistance, pair programming |
| Test coverage for streaming code | Included in estimates | Use property-based testing |

---

## Appendix A: Configuration Reference

```rust
pub struct LazyScanConfig {
    /// Predicate for partition/stats filtering
    pub predicate: Option<PredicateRef>,
    
    /// Maximum files to return
    pub limit: Option<usize>,
    
    /// Scan strategy
    pub strategy: ScanStrategy,
    
    /// Rows per batch in output stream
    pub batch_size: usize,
    
    /// Maximum memory for remove set (bytes)
    pub remove_set_memory_limit: usize,
    
    /// Fallback to standard reader if unsupported action found
    pub enable_fallback: bool,
    
    /// Parallel commit reads
    pub commit_read_parallelism: usize,
}
```

## Appendix B: Error Handling

| Error | Handling |
|-------|----------|
| Commit read failure | Retry with backoff, then fail |
| Checkpoint missing | Use version 0 as floor |
| Unsupported action | Trigger fallback (if enabled) |
| Memory limit exceeded | Switch to checkpoint streaming |
| Network timeout | Configurable retry policy |

## Appendix C: Monitoring Metrics

| Metric | Description |
|--------|-------------|
| `delta.lazy.commits_scanned` | Commits read before completing |
| `delta.lazy.files_emitted` | Files returned to client |
| `delta.lazy.fallback_triggered` | Count of fallbacks to standard |
| `delta.lazy.remove_set_size` | Current remove set cardinality |
| `delta.lazy.time_to_first_file_ms` | Latency to first result |

---

## Summary

The Lazy Fast Iterator addresses **two critical production issues** when querying huge Delta tables:

### Primary Objectives Achieved

1. **Eliminates Out-of-Memory Failures**
   - Current: O(all files) memory → OOM on tables with millions of files
   - Proposed: O(removes) memory → Constant ~50MB regardless of table size

2. **Enables Parallel Execution**
   - Current: Query engine sits IDLE while Delta state is built (sequential)
   - Proposed: Query processing starts IMMEDIATELY as files are discovered (pipelined)

### How It Works

1. **Stream files as they're discovered** instead of loading all upfront
2. **Maintain a bounded remove set** (Bloom filter) instead of full file list
3. **Fallback safely** when unsupported Delta features (metaData, protocol, etc.) are detected

### Additional Benefits

- Early termination for LIMIT queries
- Faster time-to-first-result for interactive workloads
- No checkpoint loading required for most queries

This enables ADX/Kusto to query Delta tables with **millions of files** stored in Azure Blob Storage without exhausting memory and without blocking on sequential Delta log processing.
