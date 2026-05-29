# forge-a2a

A2A (agent-to-agent) messaging for **ForgeFlux** tile pipelines.

Plato agents subscribe to pipeline events and can inject transforms mid-pipeline. This crate provides the in-process messaging layer.

## Core Types

- **`ForgeMessage`** — A single message on the bus, carrying a type, payload, sender, and timestamp.
- **`ForgeMsgType`** — Enum of event types (`PipelineStarted`, `StageCompleted`, `TileProduced`, `TransformRequest`, etc.).
- **`ForgePayload`** — Tagged union for status, tile, error, transform, and subscription data.
- **`ForgeBus`** — In-process publish/subscribe bus with filter-based routing and history.
- **`ForgeSubscriber`** — A named subscriber with an optional filter list.
- **`ForgeMessageBuilder`** — Fluent builder for constructing messages tied to a pipeline.

## Usage

```rust
use forge_a2a::{ForgeBus, ForgeMessageBuilder, ForgeMsgType};
use uuid::Uuid;

let mut bus = ForgeBus::new();

// Subscribe an agent to tile events
bus.subscribe("tile-watcher", vec![ForgeMsgType::TileProduced]);

// Build and publish messages
let pipeline_id = Uuid::new_v4();
let builder = ForgeMessageBuilder::new(pipeline_id);

let msg = builder.stage_completed("ingest", 100, 100, 1.0);
let delivered = bus.publish(msg);
// delivered.len() == 0 (tile-watcher only wants TileProduced)

let tile = builder.tile_produced(0, "heightmap", 2048);
let delivered = bus.publish(tile);
// delivered.len() == 1
```

## Dependencies

- `serde` + `serde_json` — serialization
- `uuid` — unique identifiers

## License

MIT
