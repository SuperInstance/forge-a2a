use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForgeMessage {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub msg_type: ForgeMsgType,
    pub payload: ForgePayload,
    pub sender: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ForgeMsgType {
    PipelineStarted,
    StageCompleted,
    PipelineCompleted,
    PipelineFailed,
    TileProduced,
    TransformRequest,
    TransformResult,
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ForgePayload {
    Status {
        stage: String,
        tiles_in: usize,
        tiles_out: usize,
        cr: f64,
    },
    Tile {
        index: u64,
        kind: String,
        size: usize,
    },
    Error {
        stage: String,
        message: String,
    },
    Transform {
        name: String,
        params: HashMap<String, String>,
    },
    Subscription {
        event_types: Vec<ForgeMsgType>,
    },
}

impl ForgePayload {
    fn as_status(&self) -> Option<(&str, usize, usize, f64)> {
        match self {
            ForgePayload::Status {
                stage,
                tiles_in,
                tiles_out,
                cr,
            } => Some((stage, *tiles_in, *tiles_out, *cr)),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Subscriber
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ForgeSubscriber {
    pub id: Uuid,
    pub name: String,
    pub filters: Vec<ForgeMsgType>,
}

// ---------------------------------------------------------------------------
// Bus
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ForgeBus {
    subscribers: Vec<ForgeSubscriber>,
    history: Vec<ForgeMessage>,
}

impl ForgeBus {
    pub fn new() -> Self {
        ForgeBus {
            subscribers: Vec::new(),
            history: Vec::new(),
        }
    }

    pub fn subscribe(&mut self, name: &str, filters: Vec<ForgeMsgType>) -> Uuid {
        let id = Uuid::new_v4();
        self.subscribers.push(ForgeSubscriber {
            id,
            name: name.to_string(),
            filters,
        });
        id
    }

    pub fn unsubscribe(&mut self, id: Uuid) -> bool {
        let before = self.subscribers.len();
        self.subscribers.retain(|s| s.id != id);
        self.subscribers.len() < before
    }

    /// Publish a message. Returns copies of the message for each subscriber
    /// whose filters match (empty filters = match everything).
    pub fn publish(&mut self, msg: ForgeMessage) -> Vec<ForgeMessage> {
        let matched: Vec<ForgeMessage> = self
            .subscribers
            .iter()
            .filter(|s| s.filters.is_empty() || s.filters.contains(&msg.msg_type))
            .map(|_| msg.clone())
            .collect();

        self.history.push(msg);
        matched
    }

    /// Query history, optionally filtered by pipeline_id, limited to `limit` most recent.
    pub fn history(&self, pipeline_id: Option<Uuid>, limit: usize) -> Vec<&ForgeMessage> {
        let mut iter: Vec<&ForgeMessage> = self
            .history
            .iter()
            .rev()
            .filter(|m| pipeline_id.is_none() || m.pipeline_id == pipeline_id.unwrap())
            .take(limit)
            .collect();
        iter.reverse();
        iter
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    pub fn message_count(&self) -> usize {
        self.history.len()
    }
}

impl Default for ForgeBus {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Message builder
// ---------------------------------------------------------------------------

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub struct ForgeMessageBuilder {
    pipeline_id: Uuid,
}

impl ForgeMessageBuilder {
    pub fn new(pipeline_id: Uuid) -> Self {
        ForgeMessageBuilder { pipeline_id }
    }

    fn make(&self, msg_type: ForgeMsgType, payload: ForgePayload, sender: &str) -> ForgeMessage {
        ForgeMessage {
            id: Uuid::new_v4(),
            pipeline_id: self.pipeline_id,
            msg_type,
            payload,
            sender: sender.to_string(),
            timestamp_ms: now_ms(),
        }
    }

    pub fn pipeline_started(&self, name: &str) -> ForgeMessage {
        self.make(
            ForgeMsgType::PipelineStarted,
            ForgePayload::Status {
                stage: name.to_string(),
                tiles_in: 0,
                tiles_out: 0,
                cr: 0.0,
            },
            name,
        )
    }

    pub fn stage_completed(&self, stage: &str, tiles_in: usize, tiles_out: usize, cr: f64) -> ForgeMessage {
        self.make(
            ForgeMsgType::StageCompleted,
            ForgePayload::Status {
                stage: stage.to_string(),
                tiles_in,
                tiles_out,
                cr,
            },
            "pipeline",
        )
    }

    pub fn pipeline_completed(&self, stages: usize, overall_cr: f64) -> ForgeMessage {
        self.make(
            ForgeMsgType::PipelineCompleted,
            ForgePayload::Status {
                stage: format!("completed ({} stages)", stages),
                tiles_in: 0,
                tiles_out: 0,
                cr: overall_cr,
            },
            "pipeline",
        )
    }

    pub fn pipeline_failed(&self, stage: &str, error: &str) -> ForgeMessage {
        self.make(
            ForgeMsgType::PipelineFailed,
            ForgePayload::Error {
                stage: stage.to_string(),
                message: error.to_string(),
            },
            "pipeline",
        )
    }

    pub fn tile_produced(&self, index: u64, kind: &str, size: usize) -> ForgeMessage {
        self.make(
            ForgeMsgType::TileProduced,
            ForgePayload::Tile {
                index,
                kind: kind.to_string(),
                size,
            },
            "pipeline",
        )
    }

    pub fn transform_request(&self, name: &str, params: HashMap<String, String>) -> ForgeMessage {
        self.make(
            ForgeMsgType::TransformRequest,
            ForgePayload::Transform {
                name: name.to_string(),
                params,
            },
            "agent",
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn pid() -> Uuid {
        Uuid::new_v4()
    }

    // 1. Basic message creation with builder
    #[test]
    fn message_builder_pipeline_started() {
        let p = pid();
        let msg = ForgeMessageBuilder::new(p).pipeline_started("ingest");
        assert_eq!(msg.pipeline_id, p);
        assert_eq!(msg.msg_type, ForgeMsgType::PipelineStarted);
        assert_eq!(msg.sender, "ingest");
    }

    // 2. Stage completed
    #[test]
    fn message_builder_stage_completed() {
        let msg = ForgeMessageBuilder::new(pid()).stage_completed("encode", 100, 95, 0.95);
        assert_eq!(msg.msg_type, ForgeMsgType::StageCompleted);
        let (stage, tin, tout, cr) = msg.payload.as_status().unwrap();
        assert_eq!(stage, "encode");
        assert_eq!(tin, 100);
        assert_eq!(tout, 95);
        assert!((cr - 0.95).abs() < f64::EPSILON);
    }

    // 3. Pipeline completed
    #[test]
    fn message_builder_pipeline_completed() {
        let msg = ForgeMessageBuilder::new(pid()).pipeline_completed(5, 0.88);
        assert_eq!(msg.msg_type, ForgeMsgType::PipelineCompleted);
    }

    // 4. Pipeline failed
    #[test]
    fn message_builder_pipeline_failed() {
        let msg = ForgeMessageBuilder::new(pid()).pipeline_failed("render", "OOM");
        assert_eq!(msg.msg_type, ForgeMsgType::PipelineFailed);
        if let ForgePayload::Error { stage, message } = &msg.payload {
            assert_eq!(stage, "render");
            assert_eq!(message, "OOM");
        } else {
            panic!("expected Error payload");
        }
    }

    // 5. Tile produced
    #[test]
    fn message_builder_tile_produced() {
        let msg = ForgeMessageBuilder::new(pid()).tile_produced(42, "heightmap", 2048);
        assert_eq!(msg.msg_type, ForgeMsgType::TileProduced);
        if let ForgePayload::Tile { index, kind, size } = &msg.payload {
            assert_eq!(*index, 42);
            assert_eq!(kind, "heightmap");
            assert_eq!(*size, 2048);
        } else {
            panic!("expected Tile payload");
        }
    }

    // 6. Transform request
    #[test]
    fn message_builder_transform_request() {
        let mut params = HashMap::new();
        params.insert("scale".to_string(), "2.0".to_string());
        let msg = ForgeMessageBuilder::new(pid()).transform_request("upscale", params.clone());
        assert_eq!(msg.msg_type, ForgeMsgType::TransformRequest);
        if let ForgePayload::Transform { name, params: p } = &msg.payload {
            assert_eq!(name, "upscale");
            assert_eq!(p.get("scale").unwrap(), "2.0");
        } else {
            panic!("expected Transform payload");
        }
    }

    // 7. Bus subscribe and unsubscribe
    #[test]
    fn bus_subscribe_unsubscribe() {
        let mut bus = ForgeBus::new();
        let id = bus.subscribe("agent-1", vec![ForgeMsgType::TileProduced]);
        assert_eq!(bus.subscriber_count(), 1);
        assert!(bus.unsubscribe(id));
        assert_eq!(bus.subscriber_count(), 0);
        assert!(!bus.unsubscribe(id)); // already gone
    }

    // 8. Publish with matching filter
    #[test]
    fn publish_with_filter() {
        let mut bus = ForgeBus::new();
        bus.subscribe(
            "watcher",
            vec![ForgeMsgType::TileProduced, ForgeMsgType::PipelineFailed],
        );
        let msg = ForgeMessageBuilder::new(pid()).tile_produced(0, "dem", 1024);
        let delivered = bus.publish(msg);
        assert_eq!(delivered.len(), 1);
    }

    // 9. Publish with non-matching filter
    #[test]
    fn publish_no_match() {
        let mut bus = ForgeBus::new();
        bus.subscribe("watcher", vec![ForgeMsgType::PipelineFailed]);
        let msg = ForgeMessageBuilder::new(pid()).tile_produced(0, "dem", 1024);
        let delivered = bus.publish(msg);
        assert!(delivered.is_empty());
    }

    // 10. Empty filters match everything
    #[test]
    fn empty_filter_matches_all() {
        let mut bus = ForgeBus::new();
        bus.subscribe("omni", vec![]);
        let msg = ForgeMessageBuilder::new(pid()).pipeline_started("test");
        let delivered = bus.publish(msg);
        assert_eq!(delivered.len(), 1);
    }

    // 11. History filtering by pipeline_id
    #[test]
    fn history_by_pipeline() {
        let mut bus = ForgeBus::new();
        let p1 = pid();
        let p2 = pid();
        bus.publish(ForgeMessageBuilder::new(p1).pipeline_started("a"));
        bus.publish(ForgeMessageBuilder::new(p2).pipeline_started("b"));
        bus.publish(ForgeMessageBuilder::new(p1).pipeline_started("c"));
        let h = bus.history(Some(p1), 10);
        assert_eq!(h.len(), 2);
        assert!(h.iter().all(|m| m.pipeline_id == p1));
    }

    // 12. History with limit
    #[test]
    fn history_limit() {
        let mut bus = ForgeBus::new();
        let p = pid();
        for i in 0..10 {
            bus.publish(ForgeMessageBuilder::new(p).stage_completed(
                &format!("stage-{}", i),
                100,
                90,
                0.9,
            ));
        }
        let h = bus.history(None, 3);
        assert_eq!(h.len(), 3);
    }

    // 13. Serialization round-trip
    #[test]
    fn serde_roundtrip() {
        let msg = ForgeMessageBuilder::new(pid()).tile_produced(7, "satellite", 4096);
        let json = serde_json::to_string(&msg).unwrap();
        let back: ForgeMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.id, back.id);
        assert_eq!(msg.pipeline_id, back.pipeline_id);
        assert_eq!(msg.msg_type, back.msg_type);
    }

    // 14. Multiple subscribers
    #[test]
    fn multiple_subscribers() {
        let mut bus = ForgeBus::new();
        bus.subscribe("a", vec![ForgeMsgType::TileProduced]);
        bus.subscribe("b", vec![ForgeMsgType::TileProduced]);
        bus.subscribe("c", vec![ForgeMsgType::PipelineFailed]);
        let msg = ForgeMessageBuilder::new(pid()).tile_produced(1, "tex", 512);
        let delivered = bus.publish(msg);
        assert_eq!(delivered.len(), 2);
    }

    // 15. Message count
    #[test]
    fn message_count() {
        let mut bus = ForgeBus::new();
        let p = pid();
        assert_eq!(bus.message_count(), 0);
        bus.publish(ForgeMessageBuilder::new(p).pipeline_started("x"));
        bus.publish(ForgeMessageBuilder::new(p).pipeline_completed(1, 1.0));
        assert_eq!(bus.message_count(), 2);
    }

    // 16. Unique message IDs
    #[test]
    fn unique_ids() {
        let b = ForgeMessageBuilder::new(pid());
        let m1 = b.pipeline_started("a");
        let m2 = b.pipeline_started("a");
        assert_ne!(m1.id, m2.id);
    }

    // 17. Timestamp is reasonable (within last 5 seconds)
    #[test]
    fn timestamp_reasonable() {
        let msg = ForgeMessageBuilder::new(pid()).pipeline_started("test");
        let now = now_ms();
        assert!(msg.timestamp_ms > 0);
        assert!(now - msg.timestamp_ms < 5000);
    }

    // 18. Error payload serializes correctly
    #[test]
    fn error_payload_serde() {
        let msg = ForgeMessageBuilder::new(pid()).pipeline_failed("crunch", "divide by zero");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("divide by zero"));
        let back: ForgeMessage = serde_json::from_str(&json).unwrap();
        if let ForgePayload::Error { stage, message } = &back.payload {
            assert_eq!(stage, "crunch");
            assert_eq!(message, "divide by zero");
        } else {
            panic!("expected Error");
        }
    }
}
