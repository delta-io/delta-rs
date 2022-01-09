use stateright::*;
use std::collections::HashSet;
use std::ops::Range;

const LOCK_TIMEOUT: u8 = 3;

type WriterId = usize;

#[derive(Clone)]
struct AtomicRenameSys {
    pub writers: Range<WriterId>,
    pub writer_cnt: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum WriterState {
    Init,
    LockAcquired,
    LockFailed,
    NewVersionChecked,
    NewVersionObjectCopied,
    RenameReturned,
    LockReleased,
    Shutdown,
}

#[derive(Clone, Debug)]
enum Action {
    TryAcquireLock(WriterId),
    NewVersionObjectCheckExists(WriterId),
    NewVersionObjectCopy(WriterId),
    OldVersionObjectDelete(WriterId),
    ReleaseLock(WriterId),
    CheckRenameStatus(WriterId),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum RenameResult {
    Success,
    ErrAlreadyExists,
}

#[derive(Clone, Debug, Hash)]
struct LockData {
    dst: String,
    src: String,
}

type BlobObjectPut = LockData;

#[derive(Clone, Debug, Hash)]
struct WriterContext {
    state: WriterState,
    lock_data: LockData,
    rename_result: Option<RenameResult>,
    target_version: usize,
}

#[derive(Clone, Debug, Hash)]
struct GlobalLock {
    data: LockData,
    owner: WriterId,
    lock_failures: u8,
}

#[derive(Clone, Debug, Hash)]
struct AtomicRenameState {
    writer_ctx: Vec<WriterContext>,
    lock: Option<GlobalLock>, // lock managed by dynamodb
    // modeled as a sequence of COPY operations
    blob_store_copy: Vec<BlobObjectPut>,
    // modeled as a sequence of DELETE operations
    blob_store_delete: Vec<String>,
}

impl AtomicRenameState {
    #[inline]
    fn derive_actual_deletes(&self) -> Vec<&String> {
        let mut actual_deletes = self.blob_store_delete.iter().collect::<Vec<_>>();
        actual_deletes.sort();
        actual_deletes
    }
}

#[inline]
fn source_key_from_wid(wid: WriterId) -> String {
    format!("writer_{}", wid)
}

fn writer_acquire_lock(state: &mut AtomicRenameState, wid: WriterId) {
    let mut writer = &mut state.writer_ctx[wid];
    let src = source_key_from_wid(wid);
    let dst = format!("{}", writer.target_version);
    // lock is not held by any other worker
    state.lock = Some(GlobalLock {
        data: LockData {
            src: src.clone(),
            dst: dst.clone(),
        },
        lock_failures: 0,
        owner: wid,
    });
    writer.lock_data = LockData { src, dst };
    // issue S3 API call to check for version conflict while holding the lock
    writer.state = WriterState::LockAcquired;
}

impl AtomicRenameSys {
    fn new(writer_cnt: usize) -> Self {
        Self {
            writers: 0..writer_cnt,
            writer_cnt,
        }
    }

    #[inline]
    fn derive_expected_deletes(&self) -> Vec<String> {
        let mut expected_deletes = self
            .writers
            .clone()
            .map(|wid| source_key_from_wid(wid))
            .collect::<Vec<_>>();
        expected_deletes.sort();
        expected_deletes
    }
}

impl Model for AtomicRenameSys {
    type State = AtomicRenameState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![AtomicRenameState {
            writer_ctx: self
                .writers
                .clone()
                .map(|_| WriterContext {
                    state: WriterState::Init,
                    lock_data: LockData {
                        src: "".to_string(),
                        dst: "".to_string(),
                    },
                    rename_result: None,
                    target_version: 0,
                })
                .collect(),
            lock: None,
            blob_store_copy: Vec::new(),
            blob_store_delete: Vec::new(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        for wid in self.writers.clone() {
            match &state.writer_ctx[wid].state {
                WriterState::Init | WriterState::LockFailed => {
                    actions.push(Action::TryAcquireLock(wid));
                }
                // begin of unsafe rename
                WriterState::LockAcquired => {
                    actions.push(Action::NewVersionObjectCheckExists(wid));
                }
                WriterState::NewVersionChecked => {
                    actions.push(Action::NewVersionObjectCopy(wid));
                }
                WriterState::NewVersionObjectCopied => {
                    actions.push(Action::OldVersionObjectDelete(wid));
                }
                // end of unsafe rename
                WriterState::RenameReturned => {
                    actions.push(Action::ReleaseLock(wid));
                }
                WriterState::LockReleased => {
                    actions.push(Action::CheckRenameStatus(wid));
                }
                // nothing to for shutdown
                WriterState::Shutdown => {}
            }
        }
    }

    fn next_state(&self, last_state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last_state.clone();
        match action {
            Action::TryAcquireLock(wid) => {
                match &mut state.lock {
                    Some(lock) => {
                        // use lock_failures to simulate timeout
                        if lock.lock_failures >= LOCK_TIMEOUT {
                            // lock expired, let writer preempt it
                            writer_acquire_lock(&mut state, wid);
                        } else {
                            // retry lock
                            let mut writer = &mut state.writer_ctx[wid];
                            writer.state = WriterState::LockFailed;
                            lock.lock_failures += 1;
                        }
                    }
                    None => {
                        writer_acquire_lock(&mut state, wid);
                    }
                }
            }
            Action::NewVersionObjectCheckExists(wid) => {
                let mut writer = &mut state.writer_ctx[wid];
                if state
                    .blob_store_copy
                    .iter()
                    .any(|data| data.dst == writer.lock_data.dst)
                {
                    // retry with newer version
                    // TODO: support specific failure reason?
                    writer.rename_result = Some(RenameResult::ErrAlreadyExists);
                    writer.state = WriterState::RenameReturned;
                } else {
                    writer.state = WriterState::NewVersionChecked;
                }
            }
            Action::NewVersionObjectCopy(wid) => {
                let mut writer = &mut state.writer_ctx[wid];
                state.blob_store_copy.push(writer.lock_data.clone());
                writer.state = WriterState::NewVersionObjectCopied;
            }
            Action::OldVersionObjectDelete(wid) => {
                let mut writer = &mut state.writer_ctx[wid];
                // TODO: model delete error, i.e. already deleted by another worker
                state.blob_store_delete.push(writer.lock_data.src.clone());
                writer.rename_result = Some(RenameResult::Success);
                writer.state = WriterState::RenameReturned;
            }
            Action::ReleaseLock(wid) => {
                // atomicity of this operation is guaranteed by dymanodb transaction
                let mut writer = &mut state.writer_ctx[wid];
                match &state.lock {
                    Some(lock) => {
                        if lock.owner == wid {
                            // still owner of the lock, good to release
                            state.lock = None;
                        } else {
                            // TODO: lock already acquired by another worker
                        }
                    }
                    None => {
                        // TODO: lock already released by another worker
                    }
                }
                writer.state = WriterState::LockReleased;
            }
            Action::CheckRenameStatus(wid) => {
                let mut writer = &mut state.writer_ctx[wid];
                if let Some(re) = &writer.rename_result {
                    match re {
                        RenameResult::Success => {}
                        _err => {
                            // TODO: do something about the error?
                        }
                    }
                }
                // TODO: error if status not set
                writer.state = WriterState::Shutdown;
            }
        }
        Some(state)
    }

    fn format_step(&self, last_state: &Self::State, action: Self::Action) -> Option<String>
    where
        Self::State: std::fmt::Debug,
    {
        self.next_state(last_state, action).map(|next_state| {
            let mut lines = vec![format!("{:#?}", next_state)];
            lines.push(format!(
                "expected_deletes: {:?}",
                self.derive_expected_deletes()
            ));
            lines.push(format!(
                "actual_deletes: {:?}",
                next_state.derive_actual_deletes()
            ));
            lines.join("\n")
        })
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::sometimes("lock contention", |_, state| {
                if let Some(lock) = &state.lock {
                    return lock.lock_failures > 0;
                }
                return false;
            }),
            Property::<Self>::always("no overwrite", |_, state| {
                // make sure each object key is only written once
                let mut written = HashSet::new();
                state.blob_store_copy.iter().all(|data| {
                    if written.contains(&data.dst) {
                        false
                    } else {
                        written.insert(data.dst.clone());
                        true
                    }
                })
            }),
            Property::<Self>::eventually("all writer clean shutdown", |_, state| {
                state
                    .writer_ctx
                    .iter()
                    .all(|ctx| ctx.state == WriterState::Shutdown)
            }),
            Property::<Self>::eventually("all source objects are purged", |sys, state| {
                let expected_deletes = sys.derive_expected_deletes();
                let actual_deletes = state.derive_actual_deletes();
                actual_deletes
                    .iter()
                    .zip(expected_deletes.iter())
                    .all(|(x, y)| x == &y)
            }),
            Property::<Self>::eventually("all renames are performed", |sys, state| {
                let written_versions = state
                    .writer_ctx
                    .iter()
                    .map(|ctx| format!("{}", ctx.target_version))
                    .collect::<HashSet<_>>();

                // TODO: check for object content

                // object count equals writer count
                state.blob_store_copy.len() == sys.writer_cnt
                    // each writer writes a different version in
                    && written_versions.len() == sys.writer_cnt
                    // all versions have been written into blobl store
                    && written_versions
                        == state.blob_store_copy.iter().map(|data| data.dst.clone()).collect::<HashSet<_>>()
                    // all rename calls returned with success
                    && state.writer_ctx.iter().all(|ctx| {
                        // hack to make sure we error out if rename result is not set
                        ctx.rename_result.as_ref().unwrap_or(&RenameResult::ErrAlreadyExists) == &RenameResult::Success
                    })
            }),
        ]
    }
}

fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info")); // `RUST_LOG=${LEVEL}` env variable to override

    let default_worker_count = "3";
    let default_address = "localhost:3000";
    let app = clap::App::new("delta-stateright")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand(
            clap::App::new("check").args(&[clap::Arg::new("worker_count")
                .default_value(default_worker_count)
                .long("worker-count")
                .takes_value(true)
                .required(false)
                .number_of_values(1)]),
        )
        .subcommand(
            clap::App::new("explore").args(&[
                clap::Arg::new("worker_count")
                    .default_value(default_worker_count)
                    .long("worker-count")
                    .takes_value(true)
                    .required(false)
                    .number_of_values(1),
                clap::Arg::new("address")
                    .long("address")
                    .default_value(default_address)
                    .takes_value(true)
                    .required(false)
                    .number_of_values(1),
            ]),
        );

    let matches = app.get_matches();
    match matches.subcommand() {
        Some(("check", args)) => {
            let worker_count = args
                .value_of("worker_count")
                .unwrap()
                .parse::<usize>()
                .unwrap();
            AtomicRenameSys::new(worker_count)
                .checker()
                .threads(num_cpus::get())
                // .symmetry()
                .spawn_dfs()
                .report(&mut std::io::stdout());
        }
        Some(("explore", args)) => {
            let worker_count = args
                .value_of("worker_count")
                .unwrap()
                .parse::<usize>()
                .unwrap();
            let address = args.value_of("address").unwrap();
            AtomicRenameSys::new(worker_count)
                .checker()
                .threads(num_cpus::get())
                .serve(address);
        }
        _ => unreachable!(),
    }
}
