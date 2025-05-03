//! Async Sharable Buffer for async writer
//!

use std::sync::Arc;

use futures::TryFuture;

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::sync::RwLock as TokioRwLock;

/// An in-memory buffer that allows for shared ownership and interior mutability.
/// The underlying buffer is wrapped in an `Arc` and `RwLock`, so cloning the instance
/// allows multiple owners to have access to the same underlying buffer.
#[derive(Debug, Default, Clone)]
pub struct AsyncShareableBuffer {
    buffer: Arc<TokioRwLock<Vec<u8>>>,
}

impl AsyncShareableBuffer {
    /// Consumes this instance and returns the underlying buffer.
    /// Returns `None` if there are other references to the instance.
    pub async fn into_inner(self) -> Option<Vec<u8>> {
        Arc::try_unwrap(self.buffer)
            .ok()
            .map(|lock| lock.into_inner())
    }

    /// Returns a clone of the underlying buffer as a `Vec`.
    #[allow(dead_code)]
    pub async fn to_vec(&self) -> Vec<u8> {
        let inner = self.buffer.read().await;
        inner.clone()
    }

    /// Returns the number of bytes in the underlying buffer.
    pub async fn len(&self) -> usize {
        let inner = self.buffer.read().await;
        inner.len()
    }

    /// Returns `true` if the underlying buffer is empty.
    #[allow(dead_code)]
    pub async fn is_empty(&self) -> bool {
        let inner = self.buffer.read().await;
        inner.is_empty()
    }

    /// Creates a new instance with the buffer initialized from the provided bytes.
    #[allow(dead_code)]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            buffer: Arc::new(TokioRwLock::new(bytes.to_vec())),
        }
    }
}

impl AsyncWrite for AsyncShareableBuffer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.clone();
        let buf = buf.to_vec();

        let fut = async move {
            let mut buffer = this.buffer.write().await;
            buffer.extend_from_slice(&buf);
            Ok(buf.len())
        };

        tokio::pin!(fut);
        fut.try_poll(cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
