use stateright::*;
use std::collections::{HashMap, HashSet};
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
    RepairConflictChecked,
    RepairObjectCopied,
    RepairRenameReturned,
    ExpiredLockUpdated,
    NewVersionChecked,
    NewVersionObjectCopied,
    RenameReturned,
    LockReleased,
    Shutdown,
}

#[derive(Clone, Debug)]
enum Action {
    TryAcquireLock(WriterId),
    RepairObjectCheckExists(WriterId),
    RepairObjectCopy(WriterId),
    RepairObjectDelete(WriterId),
    UpdateLockData(WriterId),
    NewVersionObjectCheckExists(WriterId),
    NewVersionObjectCopy(WriterId),
    OldVersionObjectDelete(WriterId),
    ReleaseLock(WriterId),
    CheckRenameStatus(WriterId),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum RenameErr {
    AlreadyExists,
    NotFound,
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
    acquired_expired_lock: bool,
    rename_err: Option<RenameErr>,
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

    #[inline]
    fn blob_store_obj_keys(&self) -> HashSet<String> {
        // TODO: change to return HashSet<&str>
        self.blob_store_copy
            .iter()
            .map(|data| data.dst.clone())
            .collect::<HashSet<_>>()
    }

    #[inline]
    fn writer_versions(&self) -> HashSet<String> {
        self.writer_ctx
            .iter()
            .map(|ctx| format!("{}", ctx.target_version))
            .collect::<HashSet<_>>()
    }
}

#[inline]
fn source_key_from_wid(wid: WriterId) -> String {
    format!("writer_{}", wid)
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
                    rename_err: None,
                    acquired_expired_lock: false,
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
            let writer = &state.writer_ctx[wid];
            match writer.state {
                WriterState::Init | WriterState::LockFailed => {
                    actions.push(Action::TryAcquireLock(wid));
                }
                // begin of unsafe rename
                WriterState::LockAcquired => {
                    if writer.acquired_expired_lock {
                        actions.push(Action::RepairObjectCheckExists(wid));
                    } else {
                        actions.push(Action::NewVersionObjectCheckExists(wid));
                    }
                }
                WriterState::RepairConflictChecked => {
                    actions.push(Action::RepairObjectCopy(wid));
                }
                WriterState::RepairObjectCopied => {
                    actions.push(Action::RepairObjectDelete(wid));
                }
                WriterState::RepairRenameReturned => {
                    match writer.rename_err {
                        Some(RenameErr::AlreadyExists) => {
                            // already reapired by other writer
                            // TODO: still need to perform the delete cleanup?
                            actions.push(Action::UpdateLockData(wid));
                        }
                        // not found happens when clean was already performend by another worker
                        None | Some(RenameErr::NotFound) => {
                            actions.push(Action::UpdateLockData(wid));
                        } // TODO: model other unrecoverable network error?
                    }
                }
                WriterState::ExpiredLockUpdated => {
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
                let mut writer = &mut state.writer_ctx[wid];
                match &mut state.lock {
                    Some(lock) => {
                        // use lock_failures to simulate timeout
                        if lock.lock_failures >= LOCK_TIMEOUT {
                            // lock expired, let writer preempt it
                            lock.owner = wid;
                            lock.lock_failures = 0;
                            writer.acquired_expired_lock = true;
                            writer.lock_data = lock.data.clone();
                            writer.state = WriterState::LockAcquired;
                        } else {
                            // retry lock
                            lock.lock_failures += 1;
                            writer.state = WriterState::LockFailed;
                        }
                    }
                    None => {
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
                        writer.acquired_expired_lock = false;
                        writer.lock_data = LockData { src, dst };
                        writer.state = WriterState::LockAcquired;
                    }
                }
            }
            Action::RepairObjectCheckExists(wid) => {
                let mut writer = &mut state.writer_ctx[wid];
                if state
                    .blob_store_copy
                    .iter()
                    .any(|data| data.dst == writer.lock_data.dst)
                {
                    writer.rename_err = Some(RenameErr::AlreadyExists);
                    writer.state = WriterState::RepairRenameReturned;
                } else {
                    writer.rename_err = None;
                    writer.state = WriterState::RepairConflictChecked;
                }
            }
            Action::RepairObjectCopy(wid) => {
                let mut writer = &mut state.writer_ctx[wid];
                state.blob_store_copy.push(writer.lock_data.clone());
                writer.state = WriterState::RepairObjectCopied;
            }
            Action::RepairObjectDelete(wid) => {
                let mut writer = &mut state.writer_ctx[wid];
                // TODO: model delete error, i.e. already deleted by another worker
                if state.blob_store_delete.contains(&writer.lock_data.src) {
                    writer.rename_err = Some(RenameErr::NotFound);
                } else {
                    state.blob_store_delete.push(writer.lock_data.src.clone());
                    writer.rename_err = None;
                }
                writer.state = WriterState::RepairRenameReturned;
            }
            Action::UpdateLockData(wid) => {
                let mut writer = &mut state.writer_ctx[wid];

                // TODO: add lock rvn
                if let Some(lock) = state.lock.as_mut() {
                    if lock.owner != wid {
                        // lock already expired and acquired by another worker
                        // try rename from scratch
                        writer.state = WriterState::Init;
                    } else {
                        let src = source_key_from_wid(wid);
                        let dst = format!("{}", writer.target_version);

                        lock.data = LockData {
                            src: src.clone(),
                            dst: dst.clone(),
                        };
                        lock.lock_failures = 0;
                        lock.owner = wid;
                        writer.acquired_expired_lock = false;
                        writer.lock_data = LockData { src, dst };
                        writer.state = WriterState::ExpiredLockUpdated;
                    }
                } else {
                    // lock already expired and released by another worker
                    // try rename from scratch
                    writer.state = WriterState::Init;
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
                    writer.rename_err = Some(RenameErr::AlreadyExists);
                    writer.state = WriterState::RenameReturned;
                } else {
                    writer.rename_err = None;
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
                writer.rename_err = None;
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
                if let Some(re) = &writer.rename_err {
                    match re {
                        RenameErr::AlreadyExists => {
                            // previous repair caused the version conflict, retry with a newer
                            // version
                            writer.target_version += 1;
                            writer.state = WriterState::Init;
                        }
                        RenameErr::NotFound => {
                            // TODO: not found should be fine? old object already purged by another
                            // repair
                            //
                            // setting to shutdown for now because we expect application to hard
                            // crash and start from scratch, see:
                            // https://github.com/delta-io/delta-rs/pull/391
                            writer.state = WriterState::Shutdown;
                        }
                    }
                } else {
                    writer.state = WriterState::Shutdown;
                }
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

            lines.push(format!(
                "writer_versions: {:?}",
                next_state.writer_versions()
            ));

            lines.push(format!(
                "blob_store_obj_keys: {:?}",
                next_state.blob_store_obj_keys()
            ));

            lines.join("\n")
        })
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::<Self>::always("no overwrite", |_, state| {
                // make sure each object key is only written once
                let mut written = HashMap::new();
                state.blob_store_copy.iter().all(|data| {
                    if let Some(src) = written.insert(&data.dst, &data.src) {
                        // copy from same source to the same dest is considered idempotent and safe
                        src == &data.src
                    } else {
                        true
                    }
                })
            }),
            Property::<Self>::always("no unexpected rename", |sys, state| {
                let writer_versions = state.writer_versions();
                let blob_store_obj_keys = state.blob_store_obj_keys();

                blob_store_obj_keys.len() <= sys.writer_cnt
                    && writer_versions.len() <= sys.writer_cnt
                    && writer_versions.is_superset(&blob_store_obj_keys)
            }),
            Property::<Self>::always("not retry on successful rename", |sys, state| {
                let blob_store_source_keys = state
                    .blob_store_copy
                    .iter()
                    .map(|data| data.src.as_str())
                    .collect::<HashSet<&str>>();
                sys.writers.clone().all(|wid| {
                    let writer = &state.writer_ctx[wid];
                    !(writer.state == WriterState::Init
                        && blob_store_source_keys.contains(source_key_from_wid(wid).as_str()))
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
                let writer_versions = state.writer_versions();
                let blob_store_obj_keys = state.blob_store_obj_keys();

                // TODO: check for object content

                // object count greater writer count
                blob_store_obj_keys.len() >= sys.writer_cnt
                    // each writer writes a different version in
                    && writer_versions.len() >= sys.writer_cnt
                    // all versions have been written into blobl store
                    && blob_store_obj_keys.is_superset(&writer_versions)
                    // all rename calls returned with success
                    && state.writer_ctx.iter().all(|ctx| {
                        ctx.rename_err.is_none()
                    })
            }),
        ];

        if self.writer_cnt > 1 {
            properties.push(Property::<Self>::sometimes(
                "lock contention",
                |_, state| {
                    if let Some(lock) = &state.lock {
                        return lock.lock_failures > 0;
                    }
                    return false;
                },
            ));
        }

        properties
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
