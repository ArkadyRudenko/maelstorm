use anyhow::Context;

use maelstrom::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::StdoutLock;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
    Gossip { vec: HashMap<usize, usize> },
}

enum InjectedPayload {
    Gossip,
}

struct CounterNode {
    id: usize,
    node_id: usize,
    node_count: usize,
    counters: HashMap<usize, usize>,
    str_id: String,
    cluster: Vec<String>,
    tx: tokio::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
}

fn parse_id(s: &str) -> usize {
    s[1..]
        .parse::<usize>()
        .expect(format!("node id err with {}", s).as_str())
}

impl Node<(), Payload, InjectedPayload> for CounterNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: tokio::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
        let map: HashMap<usize, usize> = init
            .node_ids
            .iter()
            .map(|str_id| (parse_id(str_id), 0))
            .collect();
        assert_eq!(init.node_ids.len(), map.len());
        eprintln!("cluster = {cluster:?}", cluster = init.node_ids.clone());
        Ok(Self {
            id: 1,
            node_id: parse_id(init.node_id.as_str()),
            node_count: init.node_ids.len(),
            counters: HashMap::new(),
            str_id: init.node_id,
            cluster: init.node_ids,
            tx: tx,
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        eprintln!("-------------------");
        eprintln!("Node with id {id} get Message", id = self.str_id.as_str());
        match input {
            Event::EOF => {}
            Event::Injected(payload) => match payload {
                InjectedPayload::Gossip => {
                    eprintln!("Injected");
                }
            },
            Event::Message(input) => {
                let src = input.src.clone();
                match input.body.payload {
                    Payload::Add { delta } => {
                        eprintln!("Add:");
                        *self.counters.entry(parse_id(&src)).or_insert(0) += delta;
                        let mut messages = Vec::new();
                        for dst in &self.cluster {
                            if dst == &self.str_id {
                                continue;
                            }
                            let gossip_msg = Message {
                                src: self.str_id.to_string(),
                                dst: dst.to_string(),
                                body: Body {
                                    payload: Payload::Gossip {
                                        vec: self.counters.clone(),
                                    },
                                    id: None,
                                    in_reply_to: None,
                                },
                            };
                            messages.push(gossip_msg);
                            // gossip_msg.send(output).context("ReadOk inject context")?;
                            // eprintln!(
                            //     "msg = {msg:?} with id = {id}",
                            //     msg = gossip_msg,
                            //     id = self.id
                            // );
                        }
                        let mut reply = input.into_reply(Some(&mut self.id));
                        reply.body.payload = Payload::AddOk;
                        reply.send(output).context("addOk context")?;
                        eprintln!("Add:Send with id = {id}", id = self.id);
                        for m in messages {
                            m.send(output).context("ReadOk inject context")?;
                        }
                    }
                    Payload::Read => {
                        let mut reply = input.into_reply(Some(&mut self.id));
                        reply.body.payload = Payload::ReadOk {
                            value: self.counters.values().sum(),
                        };
                        reply.send(output).context("ReadOk context")?;
                        let val: usize = self.counters.values().sum();
                        eprintln!("Read = {val}", val = val);
                    }
                    Payload::Gossip { vec } => {
                        eprintln!("Gossip");
                        for (id, val) in vec {
                            self.counters
                                .entry(id)
                                .and_modify(|v| *v = std::cmp::max(*v, val))
                                .or_insert(val);
                        }
                    }
                    Payload::ReadOk { .. } | Payload::AddOk => {}
                }
            }
        }
        eprintln!("-------------------");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<_, CounterNode, _, _>(()).await
}
