//! Backend abstraction layer for networking implementations
//!
//! This module provides the traits and implementations for different
//! networking backends (Iroh, RDMA) used by prime-iroh.

pub mod iroh;

// Feature-gated RDMA backend
#[cfg(feature = "rdma")]
pub mod rdma;

use anyhow::Result;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// ConnectionBackend trait defines the common interface for all backend implementations
pub trait ConnectionBackend: Send + Sync + 'static {
    /// Get a unique identifier for this node
    fn node_id(&self) -> String;

    /// Connect to a peer node with the given ID
    fn connect(&mut self, peer_id: String, num_retries: usize) -> Result<()>;

    /// Check if this node can receive data
    fn can_recv(&self) -> bool;

    /// Check if this node can send data
    fn can_send(&self) -> bool;

    /// Check if the node is fully ready for bidirectional communication
    fn is_ready(&self) -> bool {
        self.can_recv() && self.can_send()
    }

    /// Asynchronously send a message with the given tag
    fn isend(&mut self, msg: Vec<u8>, tag: usize, latency: Option<usize>)
    -> Result<SendWorkHandle>;

    /// Asynchronously receive a message with the given tag
    fn irecv(&mut self, tag: usize) -> Result<RecvWorkHandle>;

    /// Close this connection and cleanup resources
    fn close(&mut self) -> Result<()>;
}

/// Common work handle for asynchronous send operations
pub struct SendWorkHandle {
    /// The runtime the work is executing on
    pub runtime: Arc<Runtime>,
    /// The handle to the work task
    pub handle: tokio::task::JoinHandle<Result<()>>,
}

impl SendWorkHandle {
    pub fn new(runtime: Arc<Runtime>, handle: tokio::task::JoinHandle<Result<()>>) -> Self {
        Self { runtime, handle }
    }

    /// Wait for the send operation to complete
    pub fn wait(self) -> Result<()> {
        self.runtime.block_on(self.handle)?
    }
}

/// Common work handle for asynchronous receive operations
pub struct RecvWorkHandle {
    /// The runtime the work is executing on
    pub runtime: Arc<Runtime>,
    /// The handle to the work task
    pub handle: tokio::task::JoinHandle<Result<Vec<u8>>>,
}

impl RecvWorkHandle {
    pub fn new(runtime: Arc<Runtime>, handle: tokio::task::JoinHandle<Result<Vec<u8>>>) -> Self {
        Self { runtime, handle }
    }

    /// Wait for the receive operation to complete and return the received data
    pub fn wait(self) -> Result<Vec<u8>> {
        self.runtime.block_on(self.handle)?
    }
}

/// Creates a new backend instance based on available features and runtime detection
pub fn create_backend(num_streams: usize, seed: Option<u64>) -> Result<Box<dyn ConnectionBackend>> {
    // Try to create an RDMA backend if the feature is enabled and hardware is available
    #[cfg(feature = "rdma")]
    if rdma::is_available() {
        log::info!("Using RDMA backend");
        return Ok(Box::new(rdma::RdmaBackend::new(num_streams, seed)?));
    }

    // Fall back to Iroh backend
    log::info!("Using Iroh backend");
    Ok(Box::new(iroh::IrohBackend::new(num_streams, seed)?))
}
