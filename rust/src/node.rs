use crate::backend::{self, ConnectionBackend};
use crate::work::{RecvWork, SendWork};

use anyhow::Result;

pub struct Node {
    backend: Box<dyn ConnectionBackend>,
}

impl Node {
    pub fn new(num_streams: usize) -> Result<Self> {
        Self::with_seed(num_streams, None)
    }

    pub fn with_seed(num_streams: usize, seed: Option<u64>) -> Result<Self> {
        log::info!("Creating node");
        let backend = backend::create_backend(num_streams, seed)?;
        log::info!("Created node (ID={})", backend.node_id());
        Ok(Self { backend })
    }

    pub fn node_id(&self) -> String {
        self.backend.node_id()
    }

    pub fn connect(&mut self, peer_id_str: String, num_retries: usize) -> Result<()> {
        self.backend.connect(peer_id_str, num_retries)
    }

    pub fn can_recv(&self) -> bool {
        self.backend.can_recv()
    }

    pub fn can_send(&self) -> bool {
        self.backend.can_send()
    }

    pub fn is_ready(&self) -> bool {
        self.backend.is_ready()
    }

    pub fn isend(&mut self, msg: Vec<u8>, tag: usize, latency: Option<usize>) -> Result<SendWork> {
        let handle = self.backend.isend(msg, tag, latency)?;
        Ok(SendWork::from_handle(handle))
    }

    pub fn irecv(&mut self, tag: usize) -> Result<RecvWork> {
        let handle = self.backend.irecv(tag)?;
        Ok(RecvWork::from_handle(handle))
    }

    pub fn close(&mut self) -> Result<()> {
        log::info!("Closing node (ID={})", self.backend.node_id());
        self.backend.close()?;
        log::info!("Closed node (ID={})", self.backend.node_id());
        Ok(())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Err(err) = self.close() {
            log::error!("Failed to close node on drop: {:?}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() -> Result<()> {
        let node = Node::new(1)?;
        assert!(node.node_id().len() == 64);
        assert!(!node.can_recv());
        assert!(!node.can_send());
        assert!(!node.is_ready());

        Ok(())
    }

    #[test]
    fn test_node_creation_with_seed() -> Result<()> {
        let node = Node::with_seed(1, Some(42))?;
        // The node ID should be deterministic with a fixed seed
        assert!(node.node_id().len() == 64);
        assert!(!node.can_recv());
        assert!(!node.can_send());
        assert!(!node.is_ready());

        Ok(())
    }
}
