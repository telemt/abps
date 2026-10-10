//! Versioned concrete histories; legacy four-byte corpus decoding is unchanged.
#![forbid(unsafe_code)]
#![allow(dead_code)]

use super::rights::{Action, CAPACITIES, Configuration, InvariantOracle};
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub const MAX_ACTIONS: usize = 1024;

/// Identity of a violated postcondition, independent of its diagnostic wording
/// and action index. Numeric operands may shrink; logical owners may not change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureKey {
    pub backend: String,
    pub check: String,
    pub operation: String,
    pub owners: Vec<usize>,
}
#[derive(Debug)]
pub struct ReplayFailure {
    pub key: FailureKey,
    pub message: String,
    pub context: Value,
}
impl FailureKey {
    pub fn from_assertion(backend: &str, message: &str, action: &Action) -> Option<Self> {
        // Only assertions explicitly bound to the specification can participate
        // in shrinking. An unrelated panic retains its original concrete trace.
        let check = [
            "PROG-01 available",
            "MEM-01 fixed capacity",
            "MEM-01 fixed reservation",
            "SI-05 pinned grants",
            "SI-05 unassigned",
            "LIFE-02 free records",
            "LIFE-02 explicit maintenance",
            "LIFE-02 missing domain incarnation",
            "LIFE-02 phase",
            "LIFE-02 repeated incarnation",
            "LIFE-02 bounded examination",
            "MEM-02 charge",
            "OWN-01/02 allocation rights",
            "OWN-02 lost allocation",
            "OWN-01 prefix",
            "OWN-01 payload",
            "OWN-01 stale bytes",
            "SI-10 partial error output",
            "API outcome",
        ]
        .into_iter()
        .find(|label| message.contains(label))?;
        use Action::*;
        let (operation, owners) = match action {
            Open { id, .. } => ("open", vec![*id]),
            Acquire { id, domain, .. } => ("acquire", vec![*domain, *id]),
            Write { id, .. } => ("write", vec![*id]),
            Initialize { id, .. } => ("initialize", vec![*id]),
            SetFilled { id, .. } => ("set-filled", vec![*id]),
            Freeze { id } => ("freeze", vec![*id]),
            Clone { source, id } => ("clone", vec![*source, *id]),
            Slice { source, id, .. } => ("slice", vec![*source, *id]),
            Batch { source, ids } => (
                "batch",
                std::iter::once(*source)
                    .chain(ids.iter().copied())
                    .collect(),
            ),
            Drop { id } => ("drop", vec![*id]),
            Close { domain } => ("close", vec![*domain]),
            DropControls { domain } => ("drop-controls", vec![*domain]),
            Poll { .. } => ("poll", vec![]),
            Snapshot => ("snapshot", vec![]),
        };
        Some(Self {
            backend: backend.into(),
            check: check.into(),
            operation: operation.into(),
            owners,
        })
    }
    pub fn json(&self) -> Value {
        json!({"backend":self.backend,"check":self.check,"operation":self.operation,"owners":self.owners})
    }
}

pub fn minimize_failure(trace: Trace, key: &FailureKey, mut replay: impl FnMut(&Trace)) -> Trace {
    minimize(trace, |candidate| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| replay(candidate)))
            .err()
            .and_then(|p| p.downcast::<ReplayFailure>().ok())
            .is_some_and(|failure| failure.key == *key)
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trace {
    pub seed: u64,
    pub config: Configuration,
    pub actions: Vec<Action>,
}
impl Trace {
    pub fn valid(&self) -> bool {
        let mut model = InvariantOracle::new(self.config.clone(), true);
        self.actions.iter().all(|a| model.apply(a).is_ok())
    }
    pub fn json(&self) -> Value {
        use Action::*;
        let actions: Vec<_> = self
            .actions
            .iter()
            .map(|a| match a {
                Open { id, limit, grants } => json!(["open", id, limit, grants]),
                Acquire { id, domain, size } => json!(["acquire", id, domain, size]),
                Write { id, bytes } => json!(["write", id, bytes]),
                Initialize { id, len } => json!(["initialize", id, len]),
                SetFilled { id, len } => json!(["set_filled", id, len]),
                Freeze { id } => json!(["freeze", id]),
                Clone { source, id } => json!(["clone", source, id]),
                Slice {
                    source,
                    id,
                    start,
                    end,
                } => json!(["slice", source, id, start, end]),
                Batch { source, ids } => json!(["batch", source, ids]),
                Drop { id } => json!(["drop", id]),
                Close { domain } => json!(["close", domain]),
                DropControls { domain } => json!(["drop_controls", domain]),
                Poll { budget } => json!(["poll", budget]),
                Snapshot => json!(["snapshot"]),
            })
            .collect();
        json!({"version":1,"seed":self.seed,"config":{
            "counts":self.config.counts,"records":self.config.records,
            "reserved":self.config.reserved,"reference_limit":self.config.reference_limit},
            "actions":actions})
    }
    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > 4 * 1024 * 1024 {
            return Err("trace too large".into());
        }
        let v: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        let number = |v: &Value| {
            v.as_u64()
                .and_then(|x| usize::try_from(x).ok())
                .ok_or_else(|| "expected usize".to_owned())
        };
        let array = |v: &Value| {
            v.as_array()
                .cloned()
                .ok_or_else(|| "expected array".to_owned())
        };
        if v["version"] != 1 {
            return Err("unsupported trace version".into());
        }
        let pair = |v: &Value| -> Result<[usize; 2], String> {
            let a = array(v)?;
            if a.len() != 2 {
                return Err("expected pair".into());
            }
            Ok([number(&a[0])?, number(&a[1])?])
        };
        let config = Configuration {
            counts: pair(&v["config"]["counts"])?,
            records: number(&v["config"]["records"])?,
            reserved: v["config"]["reserved"].as_bool().ok_or("expected policy")?,
            reference_limit: number(&v["config"]["reference_limit"])?,
        };
        if config
            .counts
            .iter()
            .any(|n| *n == 0 || *n > 2 * usize::BITS as usize + 1)
            || !(1..=3).contains(&config.records)
            || !(1..=8).contains(&config.reference_limit)
        {
            return Err("configuration exceeds replay bounds".into());
        }
        let rows = array(&v["actions"])?;
        if rows.len() > MAX_ACTIONS {
            return Err("too many actions".into());
        }
        let mut actions = Vec::new();
        for row in rows {
            let a = array(&row)?;
            let n = |i: usize| {
                a.get(i)
                    .ok_or_else(|| "missing operand".into())
                    .and_then(number)
            };
            let tag = a.first().and_then(Value::as_str).ok_or("missing action")?;
            let arity = match tag {
                "snapshot" => 1,
                "freeze" | "drop" | "close" | "drop_controls" | "poll" => 2,
                "write" | "initialize" | "set_filled" | "clone" | "batch" => 3,
                "open" | "acquire" => 4,
                "slice" => 5,
                _ => return Err("unknown action".into()),
            };
            if a.len() != arity {
                return Err("wrong action arity".into());
            }
            use Action::*;
            actions.push(match tag {
                "open" => Open {
                    id: n(1)?,
                    limit: n(2)?,
                    grants: pair(&a[3])?,
                },
                "acquire" => Acquire {
                    id: n(1)?,
                    domain: n(2)?,
                    size: n(3)?,
                },
                "write" => {
                    let bytes = array(&a[2])?
                        .iter()
                        .map(|v| {
                            number(v)
                                .and_then(|n| u8::try_from(n).map_err(|_| "invalid byte".into()))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if bytes.len() > 64 {
                        return Err("write exceeds replay bounds".into());
                    }
                    Write { id: n(1)?, bytes }
                }
                "initialize" => Initialize {
                    id: n(1)?,
                    len: n(2)?,
                },
                "set_filled" => SetFilled {
                    id: n(1)?,
                    len: n(2)?,
                },
                "freeze" => Freeze { id: n(1)? },
                "clone" => Clone {
                    source: n(1)?,
                    id: n(2)?,
                },
                "slice" => Slice {
                    source: n(1)?,
                    id: n(2)?,
                    start: n(3)?,
                    end: n(4)?,
                },
                "batch" => {
                    let ids = array(&a[2])?
                        .iter()
                        .map(number)
                        .collect::<Result<Vec<_>, _>>()?;
                    if ids.len() > 8 {
                        return Err("batch exceeds replay bounds".into());
                    }
                    Batch { source: n(1)?, ids }
                }
                "drop" => Drop { id: n(1)? },
                "close" => Close { domain: n(1)? },
                "drop_controls" => DropControls { domain: n(1)? },
                "poll" => Poll { budget: n(1)? },
                "snapshot" => Snapshot,
                _ => unreachable!(),
            });
        }
        let trace = Self {
            seed: v["seed"].as_u64().ok_or("expected seed")?,
            config,
            actions,
        };
        if !trace.valid() {
            return Err("invalid trace dependencies".into());
        }
        Ok(trace)
    }
    pub fn normalized(&self) -> Self {
        let mut t = self.clone();
        t.actions = self
            .actions
            .iter()
            .flat_map(|a| {
                [
                    a.clone(),
                    Action::Poll {
                        budget: self.config.records,
                    },
                ]
            })
            .collect();
        t
    }
}

/// Concrete delta debugging: never resolve an ID by position in live handles.
/// The predicate must recognize the same failure class, not just any panic.
pub fn minimize(mut trace: Trace, mut fails: impl FnMut(&Trace) -> bool) -> Trace {
    let mut chunk = trace.actions.len().div_ceil(2);
    while chunk > 0 {
        let mut start = 0;
        while start + chunk <= trace.actions.len() {
            let mut candidate = trace.clone();
            candidate.actions.drain(start..start + chunk);
            if candidate.valid() && fails(&candidate) {
                trace = candidate;
            } else {
                start += 1;
            }
        }
        chunk /= 2;
    }
    // Reduce numeric operands without changing producer/consumer identities.
    for i in 0..trace.actions.len() {
        for value in [0, 1] {
            let mut candidate = trace.clone();
            use Action::*;
            match &mut candidate.actions[i] {
                Acquire { size, .. } => *size = (*size).min(value),
                Write { bytes, .. } => bytes.truncate(value),
                Initialize { len, .. } | SetFilled { len, .. } => *len = (*len).min(value),
                Slice { start, end, .. } => {
                    *start = (*start).min(value);
                    *end = (*end).min(value);
                }
                Poll { budget } => *budget = (*budget).min(value),
                _ => continue,
            }
            if candidate.valid() && fails(&candidate) {
                trace = candidate;
            }
        }
    }
    trace
}

pub fn pressure(model: &InvariantOracle) -> Vec<&'static str> {
    let mut found = Vec::new();
    if (0..2).any(|c| model.available(c) <= 1) {
        found.push("last-slot");
    }
    if model
        .handles
        .values()
        .any(|h| model.references(h.allocation) + 1 >= model.config.reference_limit)
    {
        found.push("reference-limit");
    }
    if model.domains.values().any(|d| d.closed && !d.recycled) {
        found.push("closing");
    }
    if model.config.reserved && (0..2).any(|c| model.assigned(c) == model.config.counts[c]) {
        found.push("grant-pressure");
    }
    if !model.pending.is_empty() {
        found.push("pending-maintenance");
    }
    found
}

pub fn generated(seed: u64, input: &[u8], max: usize) -> (Trace, BTreeMap<&'static str, usize>) {
    let byte = |i| input.get(i).copied().unwrap_or(0) as usize;
    let config = Configuration {
        counts: [1 + byte(0) % 3, 1 + byte(1) % 3],
        records: 1 + byte(2) % 3,
        reserved: byte(3) & 1 != 0,
        reference_limit: 1 + byte(4) % 4,
    };
    let mut model = InvariantOracle::new(config.clone(), true);
    let mut trace = Trace {
        seed,
        config,
        actions: Vec::new(),
    };
    let mut hits = BTreeMap::new();
    // Entropy drives choices, but eligibility comes only from the specification.
    // Three out of four choices prefer an eligible pressure state; absent one,
    // ordinary transitions build the next state. No observed SUT value influences
    // generation, and invalid dependencies are never silently skipped.
    for (step, bytes) in input
        .get(5..)
        .unwrap_or_default()
        .chunks(4)
        .take(max.min(MAX_ACTIONS))
        .enumerate()
    {
        let x = bytes[0] as usize;
        let y = bytes.get(1).copied().unwrap_or(0) as usize;
        let z = bytes.get(2).copied().unwrap_or(0) as usize;
        let id = step * 8 + 1;
        let live: Vec<_> = model
            .domains
            .iter()
            .filter(|(_, d)| d.controls)
            .map(|(&id, _)| id)
            .collect();
        let handles: Vec<_> = model.handles.keys().copied().collect();
        for p in pressure(&model) {
            *hits.entry(p).or_default() += 1;
        }
        use Action::*;
        let action = if !x.is_multiple_of(4) && !model.pending.is_empty() {
            if !handles.is_empty() && y.is_multiple_of(2) {
                Drop {
                    id: handles[y % handles.len()],
                }
            } else {
                Poll { budget: y % 3 }
            }
        } else if !x.is_multiple_of(4)
            && !handles.is_empty()
            && model.handles.values().any(|h| {
                !h.mutable && model.references(h.allocation) + 1 >= model.config.reference_limit
            })
        {
            let (&source, _) = model
                .handles
                .iter()
                .find(|(_, h)| {
                    !h.mutable && model.references(h.allocation) + 1 >= model.config.reference_limit
                })
                .unwrap();
            if y.is_multiple_of(3) {
                Drop { id: source }
            } else if y % 3 == 1 {
                Clone { source, id }
            } else {
                Batch {
                    source,
                    ids: (id..id + 1 + z % 3).collect(),
                }
            }
        } else if !x.is_multiple_of(4)
            && let Some(&domain) = live.iter().find(|d| model.domains[d].closed)
        {
            if y.is_multiple_of(2) {
                DropControls { domain }
            } else {
                Acquire {
                    id,
                    domain,
                    size: 1,
                }
            }
        } else if !x.is_multiple_of(4)
            && model.config.reserved
            && (0..2).any(|c| model.assigned(c) == model.config.counts[c])
        {
            if y.is_multiple_of(2) && !live.is_empty() {
                DropControls {
                    domain: live[y % live.len()],
                }
            } else {
                Open {
                    id,
                    limit: 128,
                    grants: [1, 1],
                }
            }
        } else if !x.is_multiple_of(4)
            && let Some(class) = (0..2).find(|&c| model.available(c) <= 1)
            && let Some(&domain) = live.iter().find(|d| !model.domains[d].closed)
        {
            if !handles.is_empty() && y.is_multiple_of(3) {
                Drop {
                    id: handles[z % handles.len()],
                }
            } else if y % 3 == 1
                && let Some((&id, _)) = model.handles.iter().find(|(_, h)| h.mutable)
            {
                Freeze { id }
            } else {
                Acquire {
                    id,
                    domain,
                    size: CAPACITIES[class],
                }
            }
        } else if live.is_empty() || x.is_multiple_of(12) {
            Open {
                id,
                limit: [0, 8, 16, 32, 64, 128][y % 6],
                grants: [
                    y % (model.config.counts[0] + 2),
                    z % (model.config.counts[1] + 2),
                ],
            }
        } else if x % 12 <= 4 {
            let domain = live[y % live.len()];
            if model.domains[&domain].closed {
                DropControls { domain }
            } else {
                Acquire {
                    id,
                    domain,
                    size: [0, 1, 8, 9, 32, 33, usize::MAX][z % 7],
                }
            }
        } else if x % 12 == 5 {
            Close {
                domain: live[y % live.len()],
            }
        } else if x % 12 == 6 {
            DropControls {
                domain: live[y % live.len()],
            }
        } else if handles.is_empty() {
            Poll { budget: y % 3 }
        } else {
            let source = handles[y % handles.len()];
            let h = &model.handles[&source];
            if x % 12 == 7 {
                Drop { id: source }
            } else if h.mutable {
                match z % 4 {
                    0 => Write {
                        id: source,
                        bytes: vec![z as u8; y % 35],
                    },
                    1 => Freeze { id: source },
                    2 => Initialize {
                        id: source,
                        len: y % 35,
                    },
                    _ => SetFilled {
                        id: source,
                        len: y % 35,
                    },
                }
            } else {
                Slice {
                    source,
                    id,
                    start: 0,
                    end: z % (h.len + 2),
                }
            }
        };
        model.apply(&action).expect("generator dependency bug").ok();
        trace.actions.push(action);
    }
    (trace, hits)
}

pub fn critical_trace(reserved: bool) -> Trace {
    use Action::*;
    Trace {
        seed: 0xABF50021,
        config: Configuration {
            counts: [1, 1],
            records: 1,
            reserved,
            reference_limit: 3,
        },
        actions: vec![
            Open {
                id: 0,
                limit: 40,
                grants: [1, 1],
            },
            Acquire {
                id: 0,
                domain: 0,
                size: 8,
            },
            Write {
                id: 0,
                bytes: vec![0xa5; 8],
            },
            Freeze { id: 0 },
            Batch {
                source: 0,
                ids: vec![1, 2],
            },
            Clone { source: 0, id: 3 },
            Slice {
                source: 0,
                id: 3,
                start: 0,
                end: usize::MAX,
            },
            Drop { id: 1 },
            Slice {
                source: 0,
                id: 4,
                start: 1,
                end: 7,
            },
            Drop { id: 2 },
            Slice {
                source: 4,
                id: 5,
                start: 2,
                end: 4,
            },
            Drop { id: 4 },
            Close { domain: 0 },
            Acquire {
                id: 6,
                domain: 0,
                size: 1,
            },
            DropControls { domain: 0 },
            Drop { id: 0 },
            Poll { budget: 0 },
            Snapshot,
            Poll { budget: 1 },
            Drop { id: 5 },
            Open {
                id: 1,
                limit: 40,
                grants: [1, 1],
            },
            Poll { budget: 0 },
            Snapshot,
            Poll { budget: 1 },
            Open {
                id: 2,
                limit: 8,
                grants: [1, 0],
            },
            Acquire {
                id: 7,
                domain: 2,
                size: 8,
            },
            Acquire {
                id: 8,
                domain: 2,
                size: 8,
            },
            SetFilled { id: 7, len: 1 },
            Initialize { id: 7, len: 1 },
            SetFilled { id: 7, len: 1 },
            Freeze { id: 7 },
            Slice {
                source: 7,
                id: 9,
                start: 1,
                end: 1,
            },
            Drop { id: 7 },
            Drop { id: 9 },
            DropControls { domain: 2 },
            Poll { budget: 2 },
        ],
    }
}
