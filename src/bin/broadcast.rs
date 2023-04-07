use rustengan::*;

use anyhow::Context;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{StdoutLock, Write},
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        seen: HashSet<usize>,
    },
}

enum InjectedPayload {
    Gossip,
}

struct BroadcastNode {
    node: String,
    id: usize,
    messages: HashSet<usize>,
    known: HashMap<String, HashSet<usize>>,
    neighborhood: Vec<String>,
}

impl Node<(), Payload, InjectedPayload> for BroadcastNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
        std::thread::spawn(move || {
            // generate gossip events
            // TODO: handle EOF signal
            loop {
                std::thread::sleep(Duration::from_millis(300));
                if let Err(_) = tx.send(Event::Injected(InjectedPayload::Gossip)) {
                    break;
                }
            }
        });

        Ok(Self {
            node: init.node_id,
            id: 1,
            messages: HashSet::new(),
            known: init
                .node_ids
                .into_iter()
                .map(|nid| (nid, HashSet::new()))
                .collect(),
            neighborhood: Vec::new(),
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::Injected(payload) => match payload {
                InjectedPayload::Gossip => {
                    for n in &self.neighborhood {
                        let known_to_n = &self.known[n];
                        let (already_known, mut notify_of): (HashSet<_>, HashSet<_>) = self
                            .messages
                            .iter()
                            .copied()
                            .partition(|m| known_to_n.contains(m));
                        // if we know that n knows m, we don't tell n that _we_ know m, so n will
                        // send us m for all eternity. so, we include a couple of extra `m`s so
                        // they gradually know all the things that we know without sending lots of
                        // extra stuff each time.
                        // we cap the number of extraneous `m`s we include to be at most 10% of the
                        // number of `m`s` we _have_ to include to avoid excessive overhead.
                        let mut rng = rand::thread_rng();
                        let additional_cap = (10 * notify_of.len() / 100) as u32;
                        notify_of.extend(already_known.iter().filter(|_| {
                            rng.gen_ratio(
                                additional_cap.min(already_known.len() as u32),
                                already_known.len() as u32,
                            )
                        }));
                        Message {
                            src: self.node.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip { seen: notify_of },
                            },
                        }
                        .send(&mut *output)
                        .with_context(|| format!("gossip to {}", n))?;
                    }
                }
            },
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Gossip { seen } => {
                        self.known
                            .get_mut(&reply.dst)
                            .expect("got gossip from unknown node")
                            .extend(seen.iter().copied());
                        self.messages.extend(seen);
                    }
                    Payload::Broadcast { message } => {
                        self.messages.insert(message);
                        reply.body.payload = Payload::BroadcastOk;
                        reply.send(&mut *output).context("reply to broadcast")?;
                    }
                    Payload::Read => {
                        reply.body.payload = Payload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        reply.send(&mut *output).context("reply to read")?;
                    }
                    Payload::Topology { mut topology } => {
                        self.neighborhood = topology
                            .remove(&self.node)
                            .unwrap_or_else(|| panic!("no topology given for node {}", self.node));
                        reply.body.payload = Payload::TopologyOk;
                        reply.send(&mut *output).context("reply to topology")?;
                    }
                    Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => {}
                }
            }
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _, _>(())
}
