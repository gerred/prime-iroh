//! Iroh backend implementation for prime-iroh
//!
//! This backend uses Iroh for P2P communication, providing
//! reliable, secure transport over the internet.

use crate::backend::{ConnectionBackend, RecvWorkHandle, SendWorkHandle};
use crate::receiver::Receiver;
use crate::sender::Sender;

use anyhow::Result;
use iroh::{Endpoint, SecretKey};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// IrohBackend implements the ConnectionBackend trait using Iroh's P2P networking
pub struct IrohBackend {
    num_streams: usize,
    endpoint: Endpoint,
    receiver: Receiver,
    sender: Sender,
}

impl IrohBackend {
    /// Create a new Iroh backend with the specified number of streams
    pub fn new(num_streams: usize, seed: Option<u64>) -> Result<Self> {
        log::info!("Creating Iroh backend");
        let runtime = Arc::new(Runtime::new()?);
        let endpoint = runtime.block_on(async {
            let mut builder = Endpoint::builder().discovery_n0();
            if let Some(seed) = seed {
                let mut rng = StdRng::seed_from_u64(seed);
                let secret_key = SecretKey::generate(&mut rng);
                builder = builder.secret_key(secret_key);
            }
            builder.bind().await
        })?;
        let receiver = Receiver::new(runtime.clone(), endpoint.clone(), num_streams);
        let sender = Sender::new(runtime.clone(), endpoint.clone());
        log::info!(
            "Created Iroh backend (ID={})",
            endpoint.node_id().fmt_short()
        );
        Ok(Self {
            num_streams,
            endpoint,
            receiver,
            sender,
        })
    }
}

impl ConnectionBackend for IrohBackend {
    fn node_id(&self) -> String {
        self.endpoint.node_id().to_string()
    }

    fn connect(&mut self, peer_id: String, num_retries: usize) -> Result<()> {
        self.sender.connect(peer_id, self.num_streams, num_retries)
    }

    fn can_recv(&self) -> bool {
        self.receiver.is_ready()
    }

    fn can_send(&self) -> bool {
        self.sender.is_ready()
    }

    fn isend(
        &mut self,
        msg: Vec<u8>,
        tag: usize,
        latency: Option<usize>,
    ) -> Result<SendWorkHandle> {
        let send_work = self.sender.isend(msg, tag, latency)?;
        Ok(SendWorkHandle::new(send_work.runtime, send_work.handle))
    }

    fn irecv(&mut self, tag: usize) -> Result<RecvWorkHandle> {
        let recv_work = self.receiver.irecv(tag)?;
        Ok(RecvWorkHandle::new(recv_work.runtime, recv_work.handle))
    }

    fn close(&mut self) -> Result<()> {
        log::info!(
            "Closing Iroh backend (ID={})",
            self.endpoint.node_id().fmt_short()
        );
        self.sender.close()?;
        self.receiver.close()?;
        log::info!(
            "Closed Iroh backend (ID={})",
            self.endpoint.node_id().fmt_short()
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iroh_backend_creation() -> Result<()> {
        let backend = IrohBackend::new(1, None)?;
        assert!(backend.node_id().len() == 64);
        assert!(!backend.can_recv());
        assert!(!backend.can_send());
        assert!(!backend.is_ready());
        Ok(())
    }

    #[test]
    fn test_iroh_backend_with_seed() -> Result<()> {
        let backend = IrohBackend::new(1, Some(42))?;
        // The node ID should be deterministic with a fixed seed
        assert!(backend.node_id().len() == 64);
        assert!(!backend.can_recv());
        assert!(!backend.can_send());
        assert!(!backend.is_ready());
        Ok(())
    }
}
