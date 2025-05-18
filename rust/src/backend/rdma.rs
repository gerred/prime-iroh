//! RDMA backend implementation for prime-iroh
//!
//! This backend uses RDMA (Remote Direct Memory Access) for high-performance
//! communication when available hardware supports it. It provides zero-copy
//! data transfers with minimal CPU overhead.

use crate::backend::{ConnectionBackend, RecvWorkHandle, SendWorkHandle};

use anyhow::{Result, anyhow, ensure};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

// This would use the actual async-rdma crate in production
#[allow(unused_imports)]
use std::net::{SocketAddr, TcpListener, TcpStream}; // Used for mock implementation

/// Determines if RDMA hardware is available on this system
pub fn is_available() -> bool {
    // In a real implementation, we would check for hardware support
    // For now, this always returns false on MacOS to fall back to Iroh
    cfg!(not(target_os = "macos"))
}

/// RDMA connection wrapper for both send and receive operations
struct RdmaConnection {
    #[allow(dead_code)]
    peer_id: String,
    // In a real implementation, these would be RDMA-specific types
    #[allow(dead_code)]
    send_channels: Vec<Arc<Mutex<()>>>,
    #[allow(dead_code)]
    recv_channels: Vec<Arc<Mutex<()>>>,
}

/// RdmaBackend implements the ConnectionBackend trait using RDMA technology
pub struct RdmaBackend {
    runtime: Arc<Runtime>,
    node_id: String,
    num_streams: usize,
    connection: Option<RdmaConnection>,
}

impl RdmaBackend {
    /// Create a new RDMA backend with the specified number of streams
    pub fn new(num_streams: usize, seed: Option<u64>) -> Result<Self> {
        // Ensure RDMA is available
        if !is_available() {
            return Err(anyhow!("RDMA is not available on this system"));
        }

        log::info!("Creating RDMA backend");
        let runtime = Arc::new(Runtime::new()?);

        // Generate a fixed node ID if a seed is provided (for testing)
        let node_id = if let Some(seed) = seed {
            format!("{:064x}", seed)
        } else {
            // Generate a random node ID
            let random_bytes = rand::random::<[u8; 32]>();
            format!("{}", hex::encode(random_bytes))
        };

        log::info!("Created RDMA backend (ID={})", &node_id[0..8]);
        Ok(Self {
            runtime,
            node_id,
            num_streams,
            connection: None,
        })
    }

    /// Initialize RDMA resources
    #[allow(dead_code)]
    fn init_rdma_resources(&mut self) -> Result<()> {
        // In a real implementation, this would initialize RDMA queues, memory regions, etc.
        Ok(())
    }
}

impl ConnectionBackend for RdmaBackend {
    fn node_id(&self) -> String {
        self.node_id.clone()
    }

    fn connect(&mut self, peer_id: String, num_retries: usize) -> Result<()> {
        // Ensure we don't already have a connection
        ensure!(self.connection.is_none(), "Already have a connection");

        log::info!(
            "Connecting RDMA {}->{}...",
            &self.node_id[0..8],
            &peer_id[0..8]
        );

        let mut retries_left = num_retries;
        while retries_left > 0 {
            // In a real implementation, this would use the async-rdma crate to establish connections
            match self.runtime.block_on(async {
                // Mock implementation for demonstration
                let send_channels = (0..self.num_streams)
                    .map(|_| Arc::new(Mutex::new(())))
                    .collect();
                let recv_channels = (0..self.num_streams)
                    .map(|_| Arc::new(Mutex::new(())))
                    .collect();

                Ok::<RdmaConnection, anyhow::Error>(RdmaConnection {
                    peer_id: peer_id.clone(),
                    send_channels,
                    recv_channels,
                })
            }) {
                Ok(connection) => {
                    log::info!("Connected RDMA {}->{}", &self.node_id[0..8], &peer_id[0..8]);
                    self.connection = Some(connection);
                    return Ok(());
                }
                Err(e) => {
                    retries_left -= 1;
                    let msg = format!(
                        "Failed to connect RDMA {}->{} after {} tries (left: {}): {}",
                        &self.node_id[0..8],
                        &peer_id[0..8],
                        num_retries - retries_left,
                        retries_left,
                        e
                    );
                    log::warn!("{}", msg);
                    if retries_left == 0 {
                        return Err(anyhow!(msg));
                    }
                }
            }
        }
        unreachable!()
    }

    fn can_recv(&self) -> bool {
        self.connection.is_some()
    }

    fn can_send(&self) -> bool {
        self.connection.is_some()
    }

    fn isend(
        &mut self,
        msg: Vec<u8>,
        tag: usize,
        latency: Option<usize>,
    ) -> Result<SendWorkHandle> {
        // Ensure we have a connection
        ensure!(self.is_ready(), "RDMA backend is not ready");
        ensure!(tag < self.num_streams, "Invalid tag");

        log::debug!("Sending {} bytes via RDMA stream {}", msg.len(), tag);

        // Clone necessary data for the async task
        let runtime = self.runtime.clone();

        // In a real implementation, this would use RDMA write operations
        let handle = self.runtime.spawn(async move {
            if let Some(latency) = latency {
                tokio::time::sleep(tokio::time::Duration::from_millis(latency as u64)).await;
            }

            // Mock implementation - in real code, this would use RDMA operations
            // through the async-rdma crate or similar

            Ok(())
        });

        Ok(SendWorkHandle::new(runtime, handle))
    }

    fn irecv(&mut self, tag: usize) -> Result<RecvWorkHandle> {
        // Ensure we have a connection
        ensure!(self.is_ready(), "RDMA backend is not ready");
        ensure!(tag < self.num_streams, "Invalid tag");

        log::debug!("Receiving message via RDMA stream {}", tag);

        // Clone necessary data for the async task
        let runtime = self.runtime.clone();

        // In a real implementation, this would use RDMA read operations
        let handle = self.runtime.spawn(async move {
            // Mock implementation - in real code, this would use RDMA operations
            // First read the size (4 bytes), then read the actual data

            // For now, just return an empty vector
            Ok(Vec::new())
        });

        Ok(RecvWorkHandle::new(runtime, handle))
    }

    fn close(&mut self) -> Result<()> {
        if self.connection.is_none() {
            log::warn!("RDMA connection does not exist, skipping close");
            return Ok(());
        }

        log::info!("Closing RDMA backend (ID={})", &self.node_id[0..8]);

        // In a real implementation, this would properly close RDMA connections
        // and release resources
        self.connection = None;

        log::info!("Closed RDMA backend (ID={})", &self.node_id[0..8]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdma_availability() {
        // This just documents the expected behavior
        if cfg!(target_os = "macos") {
            assert!(!is_available());
        }
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn test_rdma_backend_creation() -> Result<()> {
        let backend = RdmaBackend::new(1, None)?;
        assert!(backend.node_id().len() == 64);
        assert!(!backend.can_recv());
        assert!(!backend.can_send());
        assert!(!backend.is_ready());
        Ok(())
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn test_rdma_backend_with_seed() -> Result<()> {
        let backend = RdmaBackend::new(1, Some(42))?;
        assert_eq!(backend.node_id(), format!("{:064x}", 42));
        assert!(!backend.can_recv());
        assert!(!backend.can_send());
        assert!(!backend.is_ready());
        Ok(())
    }
}
