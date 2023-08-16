use anyhow::Context;

use maelstrom::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::StdoutLock;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

struct BroadcastNode {
    id: usize,
    messages: Vec<usize>,
}

impl Node<(), Payload> for BroadcastNode {
    fn from_init(_state: (), _: Init) -> anyhow::Result<Self> {
        Ok(Self {
            id: 1,
            messages: Vec::new(),
        })
    }

    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Broadcast { message } => {
                self.messages.push(message);
                reply.body.payload = Payload::BroadcastOk;
                reply.send(output).context("reply to broadcast")?
            }
            Payload::Read => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.messages.clone(),
                };
                reply.send(output).context("reply to broadcast")?
            }
            Payload::Topology { topology: _ } => {
                reply.body.payload = Payload::TopologyOk;
                reply.send(output).context("reply to broadcast")?
            }
            Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _>(())
}
