//! AP-13 research adapter. No changes to core admission, return or grants.
#![forbid(unsafe_code)]
#[cfg(feature = "mosaic-spike")]
mod bank;
pub mod controller;
#[cfg(feature = "mosaic-spike")]
mod cycle;
#[cfg(feature = "mosaic-spike")]
mod shared;
use super::*;
#[cfg(feature = "mosaic-spike")]
pub use bank::{Bank, Handle};
use controller::{GROUPS, Mode, Priority, STREAMS};
#[cfg(feature = "mosaic-spike")]
pub use cycle::run as cycle;
#[derive(Clone, Debug)]
pub struct Options {
    pub mode: Mode,
    pub priority: Priority,
    pub groups: Vec<usize>,
    pub ranks: [usize; GROUPS],
    pub correlated: bool,
    pub soft: Option<usize>,
    pub elasticity: String,
    pub chunk: usize,
    pub headroom: usize,
    pub trace: usize,
    pub horizon_ms: u64,
    pub pressure_ms: u64,
    pub eligible_demand: bool,
    pub cached_accounting: bool,
}
impl Options {
    pub fn parse(args: &Args, streams: usize) -> Result<Option<Self>> {
        let names = [
            "memory-priority",
            "memory-groups",
            "memory-correlation",
            "memory-soft-bytes",
            "elasticity",
            "root-chunk-percent",
            "root-headroom-percent",
            "memory-trace-capacity",
            "memory-pressure-ms",
            "memory-demand",
            "memory-accounting",
        ];
        let mode = match args.get("memory-policy", "off") {
            "off" => {
                if names.iter().any(|k| args.0.contains_key(*k)) {
                    return Err(
                        "memory options require memory-policy=fixed|adaptive|adaptive-shared"
                            .into(),
                    );
                }
                return Ok(None);
            }
            "fixed" => Mode::Fixed,
            "adaptive" => Mode::Adaptive,
            "adaptive-shared" => Mode::Shared,
            _ => return Err("invalid memory-policy".into()),
        };
        if !(1..=STREAMS).contains(&streams) {
            return Err("memory policy supports 1..64 streams".into());
        }
        let priority = match args.get("memory-priority", "weighted") {
            "weighted" => Priority::Weighted,
            "strict" => Priority::Strict,
            _ => return Err("invalid memory-priority".into()),
        };
        let mut ranks = [0; GROUPS];
        let mut assigned = [false; GROUPS];
        let groups = if let Some(value) = args.0.get("memory-groups") {
            let mut groups = Vec::new();
            for item in value.split(',') {
                let (group, rank) = item
                    .split_once(':')
                    .ok_or("groups use id:high|normal|low")?;
                let g: usize = group.parse()?;
                let r = match rank {
                    "high" => 0,
                    "normal" => 1,
                    "low" => 2,
                    _ => return Err("invalid priority rank".into()),
                };
                if g >= GROUPS || (assigned[g] && ranks[g] != r) {
                    return Err("inconsistent group rank".into());
                }
                ranks[g] = r;
                assigned[g] = true;
                groups.push(g);
            }
            if groups.len() != streams {
                return Err("one group per stream required".into());
            }
            groups
        } else {
            ranks[1] = 1;
            ranks[2] = 2;
            (0..streams).map(|i| i % 3).collect()
        };
        let number = |key: &str, default: usize| -> Result<usize> {
            Ok(args
                .0
                .get(key)
                .map(|s| s.parse::<usize>())
                .transpose()?
                .unwrap_or(default))
        };
        let elasticity = args.get("elasticity", "off");
        if !matches!(elasticity, "off" | "cold" | "warm") {
            return Err("invalid elasticity".into());
        }
        let shared = mode == Mode::Shared;
        if shared
            && (elasticity != "off" || args.get("capacity-policy", "unreserved") != "unreserved")
        {
            return Err("adaptive-shared requires elasticity=off and unreserved capacity".into());
        }
        if !shared
            && ["memory-pressure-ms", "memory-demand", "memory-accounting"]
                .iter()
                .any(|k| args.0.contains_key(*k))
        {
            return Err("shared policy options require memory-policy=adaptive-shared".into());
        }
        let pressure_ms = number("memory-pressure-ms", if shared { 10 } else { 0 })?;
        if ![0, 10, 50, 100].contains(&pressure_ms) {
            return Err("memory-pressure-ms requires 0|10|50|100".into());
        }
        let eligible_demand =
            match args.get("memory-demand", if shared { "eligible" } else { "offered" }) {
                "eligible" => true,
                "offered" => false,
                _ => return Err("memory-demand requires offered|eligible".into()),
            };
        let cached_accounting =
            match args.get("memory-accounting", if shared { "cached" } else { "full" }) {
                "cached" => true,
                "full" => false,
                _ => return Err("memory-accounting requires full|cached".into()),
            };
        let chunk = number("root-chunk-percent", 10)?;
        let headroom = number("root-headroom-percent", 25)?;
        if ![5, 10].contains(&chunk) || ![0, 10, 25].contains(&headroom) {
            return Err("chunk 5|10, headroom 0|10|25 required".into());
        }
        let trace = number("memory-trace-capacity", 0)?;
        if trace > 1_000_000 {
            return Err("trace capacity exceeds 1000000".into());
        }
        let correlated = match args.get("memory-correlation", "individual") {
            "individual" => false,
            "group" => true,
            _ => return Err("invalid memory-correlation".into()),
        };
        Ok(Some(Self {
            mode,
            priority,
            groups,
            ranks,
            correlated,
            soft: args
                .0
                .get("memory-soft-bytes")
                .map(|s| s.parse())
                .transpose()?,
            elasticity: elasticity.into(),
            chunk,
            headroom,
            trace,
            horizon_ms: number("freshness-target-ms", 20)? as u64,
            pressure_ms: pressure_ms as u64,
            eligible_demand,
            cached_accounting,
        }))
    }
}
#[cfg(feature = "mosaic-spike")]
#[derive(Clone)]
pub struct Plan {
    pub options: Options,
    pub config: abps_reference::mosaic::Config,
    pub bytes: usize,
    pub original_bytes: usize,
    pub ceiling: usize,
    pub initial: usize,
    pub standby: usize,
    pub hard: Vec<usize>,
    pub extra_external: usize,
}
#[cfg(feature = "mosaic-spike")]
impl Plan {
    pub fn new(
        s: &Settings,
        root: &plan::RootPlan,
        baseline_external: usize,
        hard: Vec<usize>,
    ) -> Result<Self> {
        use abps_reference::mosaic::{Config, PreparationPolicy, Reservation};
        let options = s.memory.clone().ok_or("memory options missing")?;
        if options.mode == Mode::Shared
            && (s.capacity_policy != CapacityPolicy::Unreserved || options.elasticity != "off")
        {
            return Err("adaptive-shared requires one unreserved root".into());
        }
        let mut config = Config::new(root.classes.clone(), s.streams, usize::MAX);
        config.capacity_policy = s.capacity_policy;
        if s.zero_payload {
            config.preparation = PreparationPolicy::ZeroPayload;
        }
        let original_bytes = Reservation::for_config(&config)?.total_bytes;
        let ceiling = original_bytes
            .checked_add(
                original_bytes
                    .checked_mul(options.headroom)
                    .ok_or("headroom overflow")?
                    / 100,
            )
            .ok_or("headroom overflow")?;
        let (bytes, initial, standby) = if options.elasticity == "off" {
            (original_bytes, 1, 0)
        } else {
            let max = original_bytes
                .checked_mul(options.chunk)
                .ok_or("chunk overflow")?
                / 100;
            // Layout is monotone in slot counts. Search integer thousandths of
            // the original payload topology; all metadata/padding is charged.
            let mut best = None;
            for scale in 1..=options.chunk * 10 {
                let mut candidate = config.clone();
                for (c, base) in candidate.classes.iter_mut().zip(&root.classes) {
                    c.slots = (base.slots * scale / 1000).max(1);
                }
                let r = Reservation::for_config(&candidate)?;
                if r.total_bytes <= max {
                    best = Some((candidate, r.total_bytes));
                }
            }
            let (c, bytes) = best.ok_or("root metadata alone exceeds chunk budget")?;
            config = c;
            let initial = original_bytes / bytes;
            let standby = if options.elasticity == "warm" {
                (ceiling - initial * bytes) / bytes
            } else {
                0
            };
            if initial + standby > 32 || initial == 0 {
                return Err("root catalog exceeds 32 records".into());
            }
            (bytes, initial, standby)
        };
        if s.capacity_policy == CapacityPolicy::Reserved
            && config.classes.iter().any(|c| c.slots < s.streams)
        {
            return Err("small reserved roots need at least one slot per stream per class".into());
        }
        config.root_limit = bytes;
        let extra_external = bank::external_bytes(
            s.streams,
            config.classes.len(),
            if options.elasticity == "off" { 1 } else { 32 },
            options.mode == Mode::Shared,
        )?;
        if extra_external > baseline_external / 20 {
            return Err(format!("memory controls require {extra_external} bytes, above 5% of external model {baseline_external}").into());
        }
        Ok(Self {
            options,
            config,
            bytes,
            original_bytes,
            ceiling,
            initial,
            standby,
            hard,
            extra_external,
        })
    }
    pub fn json(&self) -> Value {
        json!({"mode":format!("{:?}",self.options.mode),"priority":format!("{:?}",self.options.priority),"groups":self.options.groups,"ranks":self.options.ranks,
        "pressure_ms":self.options.pressure_ms,"demand":if self.options.eligible_demand{"eligible"}else{"offered"},"accounting":if self.options.cached_accounting{"cached"}else{"full"},
        "correlation":if self.options.correlated{"group"}else{"individual"},"soft_total":self.options.soft.unwrap_or_else(||self.config.classes.iter().map(|c|c.capacity*c.slots).sum::<usize>()*self.initial),
        "elasticity":self.options.elasticity,"original_reservation_bytes":self.original_bytes,"ceiling_bytes":self.ceiling,"root_bytes":self.bytes,"initial_roots":self.initial,"standby_roots":self.standby,
        "initial_reserved_bytes":(self.initial+self.standby)*self.bytes,"max_records":32,"max_attempts":64,"extra_external_bytes":self.extra_external,"trace_bytes":bank::trace_bytes(self.options.trace),"trace_capacity":self.options.trace,
        "layout_scope":"selected topology; outer pipeline roots/domain grants describe original baseline",
        "prepared_records":if self.options.elasticity=="off"{1}else{32},
        "root_classes":self.config.classes.iter().map(|c|json!({"capacity":c.capacity,"slots":c.slots,"initial_slots":c.slots*self.initial,
            "reserved_per_stream_per_root":if self.config.capacity_policy==CapacityPolicy::Reserved{Some(c.slots/self.hard.len())}else{None},
            "initial_unassigned_slots":if self.config.capacity_policy==CapacityPolicy::Reserved{(c.slots%self.hard.len())*self.initial}else{c.slots*self.initial}})).collect::<Vec<_>>()})
    }
}

#[derive(Debug)]
pub enum Failure {
    Core(Error),
    Policy(&'static str),
}
pub enum StreamOrigin {
    Ordinary(Origin),
    #[cfg(feature = "mosaic-spike")]
    Managed(Handle),
}
impl From<Origin> for StreamOrigin {
    fn from(value: Origin) -> Self {
        Self::Ordinary(value)
    }
}
impl StreamOrigin {
    pub fn ordinary(self) -> Result<Origin> {
        match self {
            Self::Ordinary(o) => Ok(o),
            #[cfg(feature = "mosaic-spike")]
            Self::Managed(_) => Err("managed memory does not support worker branches".into()),
        }
    }
    pub fn offer(&self, _size: usize) {
        #[cfg(feature = "mosaic-spike")]
        if let Self::Managed(h) = self {
            h.offer(_size);
        }
    }
    pub fn acquire_once(
        &self,
        size: usize,
    ) -> std::result::Result<super::super::backend::Lease, Failure> {
        match self {
            Self::Ordinary(o) => o.acquire(size).map_err(Failure::Core),
            #[cfg(feature = "mosaic-spike")]
            Self::Managed(h) => h.acquire(size).map(super::super::backend::Lease::Mosaic),
        }
    }
    pub async fn acquire(
        &self,
        size: usize,
        contended: &mut u64,
    ) -> std::result::Result<super::super::backend::Lease, Failure> {
        match self {
            Self::Ordinary(o) => super::acquire(o, size, contended)
                .await
                .map_err(Failure::Core),
            #[cfg(feature = "mosaic-spike")]
            Self::Managed(h) => {
                let result = h.acquire(size).map(super::super::backend::Lease::Mosaic);
                if matches!(result, Err(Failure::Core(Error::Contended))) {
                    *contended += 1;
                }
                result
            }
        }
    }
}

#[cfg(feature = "mosaic-spike")]
mod replay;
#[cfg(feature = "mosaic-spike")]
pub use replay::run as replay;

#[cfg(feature = "mosaic-spike")]
pub fn allocation_probe(
    allocator: &stats_alloc::StatsAlloc<std::alloc::System>,
    args: &Args,
) -> Result<Value> {
    let iterations = args.n("iterations", 10000);
    if iterations == 0 {
        return Err("memory allocation probe requires a nonempty workload".into());
    }
    let s = Settings::new(args)?;
    let p = plan::PipelinePlan::new(&s, false, "mosaic")?
        .memory
        .ok_or("memory policy required")?;
    if p.options.elasticity != "off" {
        return Err("allocation probe excludes explicit cold resizing".into());
    }
    let bank = Bank::prepare(p)?;
    let handles = (0..s.streams)
        .map(|_| Bank::open(&bank))
        .collect::<Result<Vec<_>>>()?;
    let region = stats_alloc::Region::new(allocator);
    for i in 0..iterations {
        let h = &handles[i % handles.len()];
        h.offer(64);
        let mut b = h
            .acquire(64)
            .map_err(|e| format!("memory allocation probe admission failed: {e:?}"))?;
        b.put_slice(&[7; 64])?;
        let b = b.freeze();
        let view = b.try_clone()?;
        drop((view, b));
        bank.borrow_mut().maintenance(i as u64);
    }
    let measured = region.change();
    assert_eq!(
        (
            measured.allocations,
            measured.deallocations,
            measured.reallocations
        ),
        (0, 0, 0)
    );
    drop(handles);
    assert!(bank.borrow_mut().shutdown());
    Ok(
        json!({"accepted":iterations,"allocations":measured.allocations,"deallocations":measured.deallocations,"reallocations":measured.reallocations,"scope":"prepared policy/check/acquire/fanout/drop/maintenance; excludes cold root changes","memory":bank.borrow_mut().json()}),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ap14_configuration_scopes_all_shared_controls_before_resources() {
        for (key, value) in [
            ("memory-pressure-ms", "11"),
            ("memory-pressure-ms", "-1"),
            ("memory-demand", "received"),
            ("memory-accounting", "approximate"),
            ("elasticity", "warm"),
            ("capacity-policy", "reserved"),
        ] {
            let args = Args(
                [
                    ("memory-policy".into(), "adaptive-shared".into()),
                    (key.into(), value.into()),
                ]
                .into_iter()
                .collect(),
            );
            assert!(Options::parse(&args, 2).is_err(), "{key}={value}");
        }
        for mode in ["off", "fixed", "adaptive"] {
            for (key, value) in [
                ("memory-pressure-ms", "10"),
                ("memory-demand", "eligible"),
                ("memory-accounting", "cached"),
            ] {
                let args = Args(
                    [
                        ("memory-policy".into(), mode.into()),
                        (key.into(), value.into()),
                    ]
                    .into_iter()
                    .collect(),
                );
                assert!(Options::parse(&args, 2).is_err());
            }
        }
        let args = Args(
            [("memory-policy".into(), "adaptive-shared".into())]
                .into_iter()
                .collect(),
        );
        let opts = Options::parse(&args, 2).unwrap().unwrap();
        assert_eq!(opts.mode, Mode::Shared);
        assert_eq!(opts.pressure_ms, 10);
        assert!(opts.eligible_demand && opts.cached_accounting);
    }
    #[test]
    fn ap13_configuration_rejects_invalid_resource_or_priority_choices() {
        for (key, value) in [
            ("root-headroom-percent", "26"),
            ("root-chunk-percent", "7"),
            ("memory-groups", "0:high,0:low"),
            ("memory-trace-capacity", "1000001"),
        ] {
            let args = Args(
                [
                    ("memory-policy".into(), "adaptive".into()),
                    (key.into(), value.into()),
                ]
                .into_iter()
                .collect(),
            );
            assert!(Options::parse(&args, 2).is_err());
        }
    }
    #[cfg(feature = "mosaic-spike")]
    #[test]
    fn ap13_external_ceiling_is_enforced_before_prepare() {
        let args = Args(
            [
                ("backend", "mosaic"),
                ("streams", "8"),
                ("fanout", "8"),
                ("queue", "64"),
                ("queue-backend", "local"),
                ("return", "local"),
                ("overflow", "skip"),
                ("memory-policy", "fixed"),
                ("class-slots", "128,128"),
            ]
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect(),
        );
        let mut s = Settings::new(&args).unwrap();
        let p = plan::PipelinePlan::new(&s, false, "mosaic").unwrap();
        let memory = p.memory.as_ref().unwrap();
        let j = p.json();
        let baseline = j["baseline_external_storage_model_bytes"].as_u64().unwrap() as usize;
        assert!(memory.extra_external <= baseline / 20);
        s.external_memory_limit = Some(baseline);
        assert!(plan::PipelinePlan::new(&s, false, "mosaic").is_err());
        s.external_memory_limit = Some(baseline + memory.extra_external);
        assert!(plan::PipelinePlan::new(&s, false, "mosaic").is_ok());
    }
}
