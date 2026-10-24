//! Crate errors and their mapping to [`delta_kernel::Error`].
//!
//! Kernel entry points return [`delta_kernel::DeltaResult`], so every public function in this
//! crate converts its [`Error`] into a kernel error at the boundary.

/// Errors raised while compiling or running a kernel plan on DataFusion.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The plan uses an operator, expression, or I/O operation this executor does not handle yet.
    #[error("unsupported by the DataFusion plan executor: {0}")]
    Unsupported(String),

    /// DataFusion failed to plan or run the translated query.
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    /// An object store request failed.
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    /// An error raised by the kernel while the executor adapted its data.
    #[error(transparent)]
    Kernel(#[from] delta_kernel::Error),
}

/// Shorthand for results inside this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Builds an [`Error::Unsupported`] for `what`.
    pub fn unsupported(what: impl std::fmt::Display) -> Self {
        Self::Unsupported(what.to_string())
    }
}

impl From<Error> for delta_kernel::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Unsupported(msg) => delta_kernel::Error::unsupported(msg),
            Error::Kernel(err) => err,
            other => delta_kernel::Error::generic_err(other),
        }
    }
}
