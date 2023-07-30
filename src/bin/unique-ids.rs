use anyhow::Context;

use maelstrom::*;
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

struct UniqueNode {
    id: usize,
    node: String,
    index: AtomicU64,
}

impl Node<(), Payload> for UniqueNode {
    fn from_init(_state: (), init: Init) -> anyhow::Result<Self> {
        Ok(UniqueNode {
            node: init.node_id,
            id: 1,
            index: AtomicU64::new(0),
        })
    }
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Generate => {
                let mut guid = format!("{}-{}", self.node, self.id);
                guid.push_str(input.body.id.unwrap().to_string().as_str());
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::GenerateOk { guid },
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to generate")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueNode, _>(())
}
