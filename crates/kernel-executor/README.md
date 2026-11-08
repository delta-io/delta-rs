# deltalake-kernel-executor

A [DataFusion](https://datafusion.apache.org) implementation of the delta-kernel
[`PlanExecutor`](https://docs.rs/buoyant_kernel/latest/buoyant_kernel/plans/trait.PlanExecutor.html)
trait.

The kernel describes its data work (log replay, checkpoint reads, file listings)
as a declarative plan. This crate compiles that plan into a DataFusion
`LogicalPlan`, runs it on a `SessionContext`, and hands the result back to the
kernel as `EngineData`.

`deltalake-core` depends on this crate behind its `datafusion` feature. This
crate therefore depends only on the kernel, DataFusion, and `object_store`,
never on `deltalake-core`.

## Layout

| Module             | Concern                                                                 |
| ------------------ | ----------------------------------------------------------------------- |
| `lib.rs`           | `DataFusionPlanExecutor` and the `PlanExecutor` dispatch                |
| `io`               | `IoOperation` variants, delegating to the two handlers below            |
| `io::storage`      | `StorageHandler` over the session's object store registry               |
| `io::file_formats` | `JsonHandler` + `ParquetHandler` over the session's object store registry |
| `expressions`      | kernel `Expression`/`Predicate`/`Scalar` → DataFusion `Expr`            |
| `query`            | `Plan` → DataFusion `LogicalPlan` (operators)                           |
| `stream`           | DataFusion record batches → `PlanResult::Data`                          |
| `error`            | Crate errors and their mapping to `delta_kernel::Error`                 |
