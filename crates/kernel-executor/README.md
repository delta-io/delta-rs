# deltalake-kernel-executor

A [DataFusion](https://datafusion.apache.org) implementation of the delta-kernel
[`PlanExecutor`](https://docs.rs/buoyant_kernel/latest/buoyant_kernel/plans/trait.PlanExecutor.html)
trait.

The kernel describes its data work (log replay, checkpoint reads, file listings)
as a declarative plan. This crate compiles that plan into a DataFusion
`LogicalPlan`, runs it on a `SessionContext`, and hands the result back to the
kernel as `EngineData`.

## Layout

| Module        | Concern                                                               |
| ------------- | --------------------------------------------------------------------- |
| `lib.rs`      | `DataFusionPlanExecutor` and the `PlanExecutor` dispatch              |
| `engine`      | `DataFusionEngine`: a kernel `Engine` that reads through the executor |
| `io`          | `IoOperation`s: storage, JSON, and parquet handlers                   |
| `query`       | `Plan` → DataFusion `LogicalPlan`                                     |
| `query::scan` | `ScanParquet` / `ScanJson` as DataFusion file scans                   |
| `expressions` | kernel expressions, predicates, and scalars ↔ DataFusion              |
| `stream`      | DataFusion record batches → kernel `EngineData`                       |
| `runtime`     | Runs async DataFusion work from the kernel's sync calls               |
| `error`       | `ExecutorError` and its mapping to `delta_kernel::Error`              |
