//! Wire codec for [`DeltaScanExec`] — makes kernel-based Delta scans usable in
//! distributed DataFusion engines (Ballista, datafusion-distributed, custom
//! schedulers) that serialize physical plans between processes.
//!
//! `DeltaScanExec` holds live kernel state (scan plan, per-file transforms,
//! deletion-vector masks) that has no stable wire form. What IS wire-safe is the
//! request that produced it: the serializable [`DeltaScan`] provider (whose
//! snapshot embeds the materialized file list) plus the `scan()` arguments.
//! [`DeltaScanExecCodec`] therefore serializes that request ([`ScanReplay`],
//! captured at plan time) and **replays the scan** on the receiving side:
//!
//! - metadata replay is served from the snapshot's materialized files
//!   (`scan_metadata_seeded`) — no log-store round trip;
//! - deletion vectors and data files are read through the object stores registered
//!   in the receiving side's `RuntimeEnv` (taken from the decode `TaskContext`).
//!
//! Addresses <https://github.com/delta-io/delta-rs/issues/4171>.

use std::sync::Arc;

use datafusion::common::error::{DataFusionError, Result};
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::{
    DefaultLogicalExtensionCodec, from_proto::parse_exprs, to_proto::serialize_exprs,
};
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};
use datafusion_proto::protobuf::LogicalExprList;
use prost::Message;
use serde::{Deserialize, Serialize};

use super::scan::DeltaScanExec;
use super::{DeltaScan, ScanReplay};

/// Serialized form of a [`ScanReplay`]. Filter expressions travel as a
/// prost-encoded [`LogicalExprList`] (datafusion-proto), everything else as
/// serde via the already-serializable [`DeltaScan`].
#[derive(Serialize, Deserialize)]
struct DeltaScanExecWire {
    provider: DeltaScan,
    projection: Option<Vec<usize>>,
    filters: Vec<u8>,
    limit: Option<usize>,
}

/// Physical extension codec for [`DeltaScanExec`].
///
/// Compose it with an engine's own codec (try Delta first, fall back), mirroring
/// how [`DeltaLogicalCodec`](crate::delta_datafusion::DeltaLogicalCodec) is used
/// on the logical side.
#[derive(Debug, Default)]
pub struct DeltaScanExecCodec {}

impl DeltaScanExecCodec {
    fn internal(what: impl std::fmt::Display) -> DataFusionError {
        DataFusionError::Internal(format!("DeltaScanExecCodec: {what}"))
    }

    /// Run the replayed scan to completion from a synchronous decode context.
    ///
    /// `PhysicalExtensionCodec::try_decode` is sync while `scan()` is async (it may
    /// read deletion vectors). Inside a multi-thread tokio runtime the blocking is
    /// delegated via `block_in_place`; outside any runtime a local executor drives
    /// the future.
    fn block_on_scan(
        replay_fut: impl std::future::Future<Output = Result<Arc<dyn ExecutionPlan>>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(replay_fut))
            }
            Ok(_) => Err(Self::internal(
                "decoding requires a multi-thread tokio runtime (current-thread runtime would deadlock)",
            )),
            Err(_) => futures::executor::block_on(replay_fut),
        }
    }
}

impl PhysicalExtensionCodec for DeltaScanExecCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let wire: DeltaScanExecWire = serde_json::from_slice(buf)
            .map_err(|e| Self::internal(format!("unable to decode wire form: {e}")))?;

        // The receiving side's session: same config the plan was created under
        // (shipped in the TaskContext) + the receiving side's RuntimeEnv, so data
        // and deletion-vector IO resolve through ITS object store registry.
        let state = SessionStateBuilder::new()
            .with_config(ctx.session_config().clone())
            .with_runtime_env(ctx.runtime_env().clone())
            .with_default_features()
            .build();

        let expr_list = LogicalExprList::decode(wire.filters.as_slice())
            .map_err(|e| Self::internal(format!("unable to decode filter exprs: {e}")))?;
        let filters = parse_exprs(expr_list.expr.iter(), ctx, &DefaultLogicalExtensionCodec {})?;

        // Replaying `scan()` rebuilds the full plan — including the parquet read
        // child — deterministically from the embedded snapshot, so the framework's
        // decoded child (`_inputs`) is intentionally unused.
        Self::block_on_scan(async {
            use datafusion::catalog::TableProvider as _;
            wire.provider
                .scan(&state, wire.projection.as_ref(), &filters, wire.limit)
                .await
        })
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        // Both scan shapes carry the same replay: the data-reading DeltaScanExec and
        // the metadata-only DeltaScanMetaExec (COUNT(*)-style plans). Replaying the
        // request on the receiving side re-derives whichever shape applies.
        let any = node.as_ref() as &dyn std::any::Any;
        let replay = if let Some(exec) = any.downcast_ref::<DeltaScanExec>() {
            exec.replay()
        } else if let Some(exec) = any.downcast_ref::<super::scan::DeltaScanMetaExec>() {
            exec.replay()
        } else {
            return Err(Self::internal("not a Delta scan node"));
        };
        let replay: &ScanReplay = replay.ok_or_else(|| {
            Self::internal(
                "Delta scan carries no scan replay — plans must be produced via DeltaScan::scan \
                 (TableProvider) to be wire-serializable",
            )
        })?;

        let filters = LogicalExprList {
            expr: serialize_exprs(replay.filters.iter(), &DefaultLogicalExtensionCodec {})?,
        }
        .encode_to_vec();

        let wire = DeltaScanExecWire {
            provider: replay.provider.clone(),
            projection: replay.projection.clone(),
            filters,
            limit: replay.limit,
        };
        serde_json::to_writer(buf, &wire)
            .map_err(|e| Self::internal(format!("unable to encode wire form: {e}")))
    }
}
