use super::*;

/// SI-08: cold, typed source of truth for preparation and reporting.
/// JSON is a projection, never configuration consumed by a backend.
#[derive(Clone)]
pub(super) struct DomainPlan {
    pub stream: usize,
    pub limit: usize,
    pub grants: Vec<ClassReservation>,
    grant_deficits: Vec<usize>,
    envelope: usize,
    required_bytes: usize,
}
pub(super) struct RootPlan {
    pub classes: Vec<ClassConfig>,
    domains: Vec<DomainPlan>,
    writers: usize,
    queued: usize,
    in_flight: usize,
    envelope: usize,
    backing_bytes: usize,
    pub reservation_bytes: usize,
    metadata_bytes: usize,
    metadata_is_modeled: bool,
    backing_is_modeled: bool,
    scratch_bytes: usize,
    queue_storage: usize,
    mailbox_storage: usize,
    semaphore_storage: usize,
    descriptors: usize,
    upper_bytes: usize,
    writer_storage: usize,
    external_bytes: usize,
    admission_bytes: usize,
}
pub(super) struct PipelinePlan {
    pub roots: Vec<RootPlan>,
    #[cfg(feature = "mosaic-spike")]
    pub memory: Option<memory::Plan>,
    settings: Settings,
    shared: bool,
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| "pipeline bound overflow".into())
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| "pipeline bound overflow".into())
}
/// Stages retain their positions through destructor/refund completion. An
/// adapter adding a transform must explicitly add its overlapping output here.
fn envelope(fanout: usize, queue: usize, batch: usize, output: usize) -> Result<usize> {
    add(add(1, mul(fanout, add(queue, batch)?)?)?, output)
}
impl PipelinePlan {
    pub fn new(s: &Settings, shared: bool, backend: &str) -> Result<Self> {
        if shared && backend != "abps" {
            return Err("shared root is reference-only".into());
        }
        let roots = if shared { 1 } else { s.workers };
        let capacities = s.capacities();
        let per_stream = envelope(s.fanout, s.queue, s.write_batch, 0)?;
        let required_bytes = mul(per_stream, s.max_class())?;
        // Preserve the historical default independently of class-slots.
        let limit = s.domain_limit.unwrap_or(mul(
            capacities.iter().try_fold(0, |sum, &n| add(sum, n))?,
            64 / s.workers,
        )?);
        let mut plans = Vec::with_capacity(roots);
        for root in 0..roots {
            let streams: Vec<_> = (0..s.streams)
                .filter(|id| shared || id % s.workers == root)
                .collect();
            let classes: Vec<_> = capacities
                .iter()
                .copied()
                .zip(s.class_slots.iter().copied())
                .map(|(capacity, total)| ClassConfig {
                    capacity,
                    slots: total / roots + usize::from(root < total % roots),
                })
                .collect();
            let mut config = Config::new(classes.clone(), streams.len(), isize::MAX as usize);
            config.capacity_policy = s.capacity_policy;
            // Layout-only validation before any storage, socket, or runtime. In
            // particular, SI classes cannot silently grow to multi-word bitmaps.
            let (backing_bytes, metadata_bytes, metadata_is_modeled, reservation_bytes) =
                match backend {
                    "berry" => {
                        let r = abps_reference::berry::Reservation::for_config(&config)?;
                        (r.backing_bytes, r.metadata_bytes, false, r.total_bytes)
                    }
                    #[cfg(feature = "mosaic-spike")]
                    "mosaic" => {
                        let mut c = abps_reference::mosaic::Config::new(
                            classes.clone(),
                            streams.len(),
                            isize::MAX as usize,
                        );
                        c.capacity_policy = s.capacity_policy;
                        #[cfg(feature = "local-branches-spike")]
                        if s.local_branches || s.worker_branches {
                            c.local_branches = Some(abps_reference::mosaic::LocalBranchConfig {
                                max_branches_per_slot: if s.worker_branches { 1 } else { s.fanout },
                                local_reference_limit: if s.worker_branches {
                                    s.fanout
                                } else {
                                    isize::MAX as usize
                                },
                            });
                        }
                        let r = abps_reference::mosaic::Reservation::for_config(&c)?;
                        (r.backing_bytes, r.metadata_bytes, false, r.total_bytes)
                    }
                    "abps" | "bytes" => {
                        let r = reference::Reservation::for_config(&config)?;
                        (
                            r.backing_bytes,
                            r.modeled_metadata_bytes,
                            true,
                            r.total_bytes,
                        )
                    }
                    _ => return Err("unknown backend".into()),
                };
            let writers = mul(streams.len(), s.fanout)?;
            let queued = mul(writers, s.queue)?;
            let in_flight = mul(writers, s.write_batch)?;
            let root_envelope = mul(streams.len(), per_stream)?;
            let mut domains = Vec::with_capacity(streams.len());
            for (rank, &stream) in streams.iter().enumerate() {
                let mut grants = Vec::new();
                let mut grant_deficits = Vec::new();
                if s.capacity_policy == CapacityPolicy::Reserved {
                    for class in &classes {
                        let slots = class.slots / streams.len()
                            + usize::from(rank < class.slots % streams.len());
                        grant_deficits.push(per_stream.saturating_sub(slots));
                        if slots != 0 {
                            grants.push(ClassReservation {
                                capacity: class.capacity,
                                slots,
                            });
                        }
                    }
                }
                if s.require_envelope
                    && (limit < required_bytes || grant_deficits.iter().any(|&n| n != 0))
                {
                    return Err(format!("stream {stream} envelope uncovered: byte deficit {}, class slot deficits {:?}", required_bytes.saturating_sub(limit), grant_deficits).into());
                }
                domains.push(DomainPlan {
                    stream,
                    limit,
                    grants,
                    grant_deficits,
                    envelope: per_stream,
                    required_bytes,
                });
            }
            if s.require_envelope
                && s.capacity_policy == CapacityPolicy::Unreserved
                && classes.iter().any(|c| c.slots < root_envelope)
            {
                return Err(format!(
                    "root {root} class capacity cannot cover {root_envelope} backing claims"
                )
                .into());
            }
            plans.push(RootPlan {
                classes,
                domains,
                writers,
                queued,
                in_flight,
                envelope: root_envelope,
                backing_bytes,
                reservation_bytes,
                metadata_bytes,
                metadata_is_modeled,
                backing_is_modeled: backend == "bytes",
                scratch_bytes: mul(streams.len(), s.scratch_bytes())?,
                queue_storage: if s.local_queue {
                    mul(
                        writers,
                        local_queue::storage_model_bytes::<Message>(s.queue)
                            .ok_or("local queue model overflow")?,
                    )?
                } else if !s.worker_branches {
                    mul(
                        writers,
                        tracked_queue::storage_model_bytes::<Message>(s.queue)
                            .ok_or("tracked queue model overflow")?,
                    )?
                } else {
                    mul(queued, {
                        #[cfg(feature = "local-branches-spike")]
                        if s.worker_branches {
                            std::mem::size_of::<worker_fanout::LocalDelivery>()
                        } else {
                            std::mem::size_of::<Message>()
                        }
                        #[cfg(not(feature = "local-branches-spike"))]
                        {
                            std::mem::size_of::<Message>()
                        }
                    })?
                },
                mailbox_storage: {
                    #[cfg(feature = "local-branches-spike")]
                    if s.worker_branches {
                        mul(queued, std::mem::size_of::<worker_fanout::Bundle>())?
                    } else {
                        0
                    }
                    #[cfg(not(feature = "local-branches-spike"))]
                    {
                        0
                    }
                },
                semaphore_storage: if s.worker_branches {
                    mul(writers, std::mem::size_of::<tokio::sync::Semaphore>())?
                } else {
                    0
                },
                descriptors: mul(writers, mul(16, std::mem::size_of::<IoSlice<'_>>())?)?,
                upper_bytes: mul(root_envelope, s.max_class())?,
                writer_storage: mul(
                    writers,
                    add(
                        mul(
                            8,
                            add(
                                std::mem::size_of::<Option<Message<WriterFrame>>>(),
                                add(
                                    HEADER_V2,
                                    std::mem::size_of::<Option<tokio::time::Instant>>(),
                                )?,
                            )?,
                        )?,
                        add(
                            std::mem::size_of::<tokio::time::Sleep>(),
                            if s.adaptive.is_some() {
                                std::mem::size_of::<tokio::time::Interval>()
                            } else {
                                0
                            },
                        )?,
                    )?,
                )?,
                external_bytes: 0,
                admission_bytes: if let Some(o) = &s.admission {
                    admission::storage_bytes(streams.len(), s.fanout, o)
                } else {
                    0
                },
            });
        }
        let mut external_total = 0;
        for r in &mut plans {
            r.external_bytes = [
                r.scratch_bytes,
                r.queue_storage,
                r.mailbox_storage,
                r.semaphore_storage,
                r.descriptors,
                r.writer_storage,
                r.admission_bytes,
            ]
            .into_iter()
            .try_fold(0, add)?;
            external_total = add(external_total, r.external_bytes)?;
        }
        #[cfg(feature = "mosaic-spike")]
        let memory = if s.memory.is_some() {
            Some(memory::Plan::new(
                s,
                &plans[0],
                external_total,
                plans[0].domains.iter().map(|d| d.limit).collect(),
            )?)
        } else {
            None
        };
        #[cfg(feature = "mosaic-spike")]
        if let Some(p) = &memory {
            if s.require_envelope {
                for class in &p.config.classes {
                    let covered = match s.capacity_policy {
                        CapacityPolicy::Unreserved => {
                            mul(class.slots, p.initial)? >= plans[0].envelope
                        }
                        CapacityPolicy::Reserved => {
                            let own = mul(class.slots / s.streams, p.initial)?;
                            plans[0].domains.iter().all(|d| own >= d.envelope)
                        }
                    };
                    if !covered {
                        return Err(format!(
                            "selected memory topology cannot cover class {} envelope",
                            class.capacity
                        )
                        .into());
                    }
                }
            }
            external_total = add(external_total, p.extra_external)?;
        }
        if s.external_memory_limit
            .is_some_and(|limit| external_total > limit)
        {
            return Err(format!(
                "external storage model {external_total} exceeds configured ceiling"
            )
            .into());
        }
        Ok(Self {
            roots: plans,
            #[cfg(feature = "mosaic-spike")]
            memory,
            settings: s.clone(),
            shared,
        })
    }
    pub fn domain(&self, stream: usize) -> &DomainPlan {
        let root = if self.shared {
            0
        } else {
            stream % self.settings.workers
        };
        let rank = if self.shared {
            stream
        } else {
            stream / self.settings.workers
        };
        &self.roots[root].domains[rank]
    }
    pub fn json(&self) -> Value {
        #[cfg(feature = "mosaic-spike")]
        let memory = self.memory.as_ref().map(memory::Plan::json);
        #[cfg(not(feature = "mosaic-spike"))]
        let memory: Option<Value> = None;

        let s = &self.settings;
        let roots: Vec<_> = self.roots.iter().enumerate().map(|(index, r)| {
            let classes: Vec<_> = r.classes.iter().map(|c| json!({
                "capacity":c.capacity,"slots":c.slots,"can_cover_all_frames_in_this_class":c.slots>=r.envelope,
                "envelope_slot_deficit":r.envelope.saturating_sub(c.slots)
            })).collect();
            let domains: Vec<_> = r.domains.iter().map(|d| {
                let grants: Vec<_> = d.grants.iter().map(|g| json!({"capacity":g.capacity,"slots":g.slots,"covers_own_envelope":g.slots>=d.envelope})).collect();
                json!({"stream":d.stream,"domain_limit":d.limit,"grants":grants,
                    "grant_slot_deficits":d.grant_deficits,"required_byte_limit":d.required_bytes,
                    "byte_deficit":d.required_bytes.saturating_sub(d.limit),
                    "queued_upper_backings":s.fanout*s.queue,"writer_upper_backings":s.fanout*s.write_batch,
                    "ingress_headroom_slots":1,"transform_output_headroom_slots":0,"simultaneous_upper_backings":d.envelope})
            }).collect();
            json!({"root":index,"streams":r.domains.len(),"classes":classes,"domains":domains,
                "domain_limit_each":r.domains[0].limit,"ingress_headroom_slots":r.domains.len(),
                "transform_output_headroom_slots":0,"queued_upper_backings":r.queued,
                "writer_upper_backings":r.in_flight,"simultaneous_upper_backings":r.envelope,
                "upper_backing_bytes_at_max_class":r.upper_bytes,"scratch_bytes":r.scratch_bytes,
                "queue_item_storage_model_bytes":r.queue_storage,"writer_storage_model_bytes":r.writer_storage,"external_storage_model_bytes":r.external_bytes,
                "mailbox_item_storage_model_bytes":r.mailbox_storage,"semaphore_storage_model_bytes":r.semaphore_storage,"writer_descriptors_model_bytes":r.descriptors,
                "reservation_bytes":r.reservation_bytes,"writers":r.writers,"configured_backing_bytes":r.backing_bytes,"backing_is_modeled":r.backing_is_modeled,"metadata_bytes":r.metadata_bytes,
                "metadata_is_modeled":r.metadata_is_modeled})
        }).collect();
        json!({"admission":self.settings.admission.as_ref().map(|a|a.json()),"admission_metadata_bytes":self.roots.iter().map(|r|r.admission_bytes).sum::<usize>(),"memory":memory,"baseline_external_storage_model_bytes":self.roots.iter().map(|r|r.external_bytes).sum::<usize>(),"external_storage_model_bytes":self.roots.iter().map(|r|r.external_bytes).sum::<usize>()+memory.as_ref().and_then(|m|m["extra_external_bytes"].as_u64()).unwrap_or(0) as usize,"external_memory_limit_bytes":s.external_memory_limit,"ingress_buffer_bytes":s.scratch_bytes(),"queue_accounting":if s.local_queue {"local"} else if s.worker_branches {"worker-credits"} else {"receipts-v1"},"queue_backend":if s.local_queue {"local"} else {"tokio"},"freshness_priority":if s.adaptive.is_some() {"adaptive"} else if s.freshness_min_age.is_some() {"oldest"} else {"off"},"freshness_min_age_ms":s.freshness_min_age.map(|v|v.as_millis()),"adaptive":s.adaptive.map(|p|json!({"target_ms":p.target.as_millis(),"grace_ms":p.grace.as_millis(),"stall_ms":p.stall.as_millis(),"keyframe_wait_ms":p.key_wait.as_millis(),"keyed":p.keyed})),"wire_version":if s.keyed(){2}else{1},"adaptive_timer_model_bytes":if s.adaptive.is_some(){std::mem::size_of::<tokio::time::Interval>()*s.streams*s.fanout}else{0},"roots":roots,"class_slots":s.class_slots,"class_capacities":s.class_capacities,"distribution":s.distribution,"write_batch":s.write_batch,
            "capacity_check":if s.require_envelope {"require-envelope"} else {"report"},
            "fanout_api":if s.worker_branches {"worker-branches"} else if s.local_branches {"local-branches"} else if s.batch_fanout {"batch"} else {"scalar"},
            "capacity_policy":format!("{:?}",s.capacity_policy),"max_frame_age_ms":s.max_frame_age.map(|a|a.as_millis()),
            "send_buffer_bytes_requested":s.send_buffer_bytes,"ingress":s.ingress_name(),
            "mailbox_is_subset_of_queue_credits":s.worker_branches,
            "mailbox_capacity_per_stream":if s.worker_branches {s.fanout*s.queue} else {0},
            "availability_guaranteed":false,"bounds_are_conservative":true,"occupancy_includes_pending_returns":true,
            "external_overhead":"Full future/task/channel overhead, allocator rounding and socket buffers are outside the selected external storage model"})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn settings(pairs: &[(&str, &str)]) -> Settings {
        Settings::new(&Args(
            pairs
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
        ))
        .unwrap()
    }
    #[cfg(feature = "mosaic-spike")]
    #[test]
    fn ap09_queue_profiles_match_exact_envelopes_and_reject_deficits() {
        for (backend, fanout, queue, slots, budget) in [
            ("berry", 1, 8, 34, 25500),
            ("mosaic", 4, 8, 130, 97500),
            ("mosaic", 4, 32, 322, 241500),
            ("mosaic", 4, 64, 578, 433500),
        ] {
            let mut s = settings(&[
                ("streams", "2"),
                ("capacity-policy", "reserved"),
                ("capacity-check", "require-envelope"),
            ]);
            s.fanout = fanout;
            s.queue = queue;
            s.write_batch = 8;
            s.class_slots = vec![slots, slots];
            s.domain_limit = Some(budget);
            let p = PipelinePlan::new(&s, false, backend).unwrap();
            assert_eq!(p.domain(0).envelope, slots / 2);
            assert_eq!(p.domain(0).required_bytes, budget);
            s.class_slots[0] -= 1;
            assert!(PipelinePlan::new(&s, false, backend).is_err());
            s.class_slots[0] += 1;
            s.domain_limit = Some(budget - 1);
            assert!(PipelinePlan::new(&s, false, backend).is_err());
        }
    }
    #[test]
    fn ap12_external_ceiling_and_scratch_are_checked_before_preparation() {
        let mut s = settings(&[("fanout", "1"), ("ingress", "staged-copy")]);
        let base = PipelinePlan::new(&s, false, "berry").unwrap();
        let bytes = base.json()["external_storage_model_bytes"]
            .as_u64()
            .unwrap() as usize;
        assert_eq!(base.roots[0].scratch_bytes, 1500);
        s.external_memory_limit = Some(bytes);
        assert!(PipelinePlan::new(&s, false, "berry").is_ok());
        s.external_memory_limit = Some(bytes - 1);
        assert!(PipelinePlan::new(&s, false, "berry").is_err());
        s.external_memory_limit = None;
        s.ingress_buffer_bytes = Some(2048);
        assert_eq!(
            PipelinePlan::new(&s, false, "berry").unwrap().json()["external_storage_model_bytes"],
            bytes + 548
        );
        for pairs in [
            vec![("ingress-buffer-bytes", "1")],
            vec![("ingress", "staged-copy"), ("ingress-buffer-bytes", "0")],
            vec![
                ("ingress", "staged-copy"),
                ("ingress-buffer-bytes", "16385"),
            ],
        ] {
            assert!(
                Settings::new(&Args(
                    pairs
                        .into_iter()
                        .map(|(k, v)| (k.into(), v.into()))
                        .collect()
                ))
                .is_err()
            );
        }
    }
    #[test]
    fn si08_strict_and_report_share_the_same_typed_plan() {
        for policy in ["reserved", "unreserved"] {
            let mut s = settings(&[
                ("fanout", "1"),
                ("queue", "1"),
                ("capacity-policy", policy),
                ("class-slots", "2,3"),
            ]);
            assert!(PipelinePlan::new(&s, false, "berry").is_ok());
            s.require_envelope = true;
            assert!(PipelinePlan::new(&s, false, "berry").is_err());
            s.class_slots = vec![3, 3];
            s.domain_limit = Some(4499);
            assert!(PipelinePlan::new(&s, false, "berry").is_err());
            s.domain_limit = Some(4500);
            let p = PipelinePlan::new(&s, false, "berry").unwrap();
            assert_eq!(p.domain(0).required_bytes, 4500);
            assert_eq!(p.json()["roots"][0]["domains"][0]["byte_deficit"], 0);
        }
    }
    #[test]
    fn si08_slot_growth_never_changes_default_byte_limits_or_representation() {
        let mut s = settings(&[
            ("threads", "2"),
            ("streams", "64"),
            ("capacity-policy", "reserved"),
        ]);
        let a = PipelinePlan::new(&s, false, "berry").unwrap();
        s.class_slots = vec![128, 128];
        let b = PipelinePlan::new(&s, false, "berry").unwrap();
        assert_eq!(a.domain(0).limit, b.domain(0).limit);
        assert_eq!(
            (a.domain(0).grants[0].slots, b.domain(0).grants[0].slots),
            (1, 2)
        );
        s.class_slots[0] = 2 * usize::BITS as usize + 1;
        assert!(PipelinePlan::new(&s, false, "berry").is_err());
        s.class_slots = vec![1, 64];
        assert!(PipelinePlan::new(&s, false, "berry").is_err());
    }
    #[cfg(feature = "local-branches-spike")]
    #[test]
    fn ap07_worker_plan_matches_storage_and_models_mailbox_without_double_payload() {
        let s = settings(&[
            ("backend", "mosaic"),
            ("fanout-api", "worker-branches"),
            ("fanout", "20"),
            ("queue", "8"),
            ("write-batch", "8"),
        ]);
        let p = PipelinePlan::new(&s, false, "mosaic").unwrap();
        let r = &p.roots[0];
        assert_eq!(r.envelope, 321);
        assert_eq!(
            r.mailbox_storage,
            20 * 8 * std::mem::size_of::<worker_fanout::Bundle>()
        );
        let f = Fixture::prepared_branch_layout(
            "mosaic",
            r.classes.clone(),
            r.domains.len(),
            None,
            s.capacity_policy,
            false,
            (1, 20),
        )
        .unwrap();
        assert_eq!(f.snapshot()["metadata"], r.metadata_bytes);
        f.finish().unwrap();
        let mut old = s.clone();
        old.worker_branches = false;
        old.local_branches = true;
        let old = PipelinePlan::new(&old, false, "mosaic").unwrap();
        assert_eq!(old.roots[0].envelope, r.envelope);
        assert_eq!(
            old.roots[0].metadata_bytes - r.metadata_bytes,
            r.classes.iter().map(|c| c.slots).sum::<usize>() * 19 * 64
        );
        assert_eq!(p.json()["mailbox_capacity_per_stream"], 160);
    }
    #[cfg(feature = "local-branches-spike")]
    #[test]
    fn br04_plan_and_actual_prepare_charge_identical_branch_metadata() {
        let s = settings(&[
            ("backend", "mosaic"),
            ("fanout-api", "local-branches"),
            ("fanout", "3"),
        ]);
        let p = PipelinePlan::new(&s, false, "mosaic").unwrap();
        let r = &p.roots[0];
        let fixture = Fixture::prepared_branches(
            "mosaic",
            r.classes.clone(),
            r.domains.len(),
            None,
            s.capacity_policy,
            s.zero_payload,
            s.fanout,
        )
        .unwrap();
        assert_eq!(fixture.snapshot()["metadata"], r.metadata_bytes);
        assert_eq!(p.json()["fanout_api"], "local-branches");
        fixture.finish().unwrap();
        let mut ordinary = s.clone();
        ordinary.local_branches = false;
        let before = PipelinePlan::new(&ordinary, false, "mosaic").unwrap();
        let records = r.classes.iter().map(|c| c.slots).sum::<usize>() * 3;
        assert_eq!(
            r.metadata_bytes - before.roots[0].metadata_bytes,
            records * 64
        );
        assert!(
            Settings::new(&Args(
                [
                    ("backend".into(), "berry".into()),
                    ("fanout-api".into(), "local-branches".into())
                ]
                .into()
            ))
            .is_err()
        );
    }
    #[test]
    fn si08_checked_overlap_accounts_for_output_and_pending_return_positions() {
        assert_eq!(envelope(2, 8, 8, 0).unwrap(), 33);
        assert_eq!(envelope(2, 8, 8, 1).unwrap(), 34);
        assert!(envelope(1, usize::MAX, 1, 0).is_err());
        assert!(envelope(usize::MAX, 2, 1, 0).is_err());
        assert!(mul(usize::MAX, 2).is_err());
        // One writer stalled in final Drop still occupies its writer position;
        // the producer can also hold one input and the queue can already refill.
        assert_eq!(envelope(1, 1, 1, 0).unwrap(), 3);
    }
    #[cfg(all(feature = "mosaic-spike", target_pointer_width = "64"))]
    #[test]
    fn si15_full_envelope_is_layout_only_and_preserves_explicit_cost() {
        let s = settings(&[
            ("backend", "mosaic"),
            ("threads", "2"),
            ("streams", "64"),
            ("fanout", "20"),
            ("queue", "8"),
            ("write-batch", "8"),
            ("distribution", "frames"),
            ("capacity-policy", "reserved"),
            ("capacity-check", "require-envelope"),
            ("class-slots", "20544,20544"),
            ("domain-limit-bytes", "673185792"),
        ]);
        let p = PipelinePlan::new(&s, false, "mosaic").unwrap();
        assert_eq!(
            p.roots.iter().map(|r| r.backing_bytes).sum::<usize>(),
            44_430_262_272
        );
        for id in 0..64 {
            let d = p.domain(id);
            assert_eq!(d.envelope, 321);
            assert!(d.grants.iter().all(|g| g.slots == 321));
        }
        assert!(PipelinePlan::new(&s, false, "berry").is_err());
    }
    #[cfg(feature = "mosaic-spike")]
    #[test]
    fn ap13_strict_envelope_checks_selected_active_topology() {
        for policy in ["unreserved", "reserved"] {
            let mut s = settings(&[
                ("backend", "mosaic"),
                ("threads", "1"),
                ("streams", "8"),
                ("fanout", "8"),
                ("queue", "64"),
                ("write-batch", "8"),
                ("queue-backend", "local"),
                ("return", "local"),
                ("overflow", "skip"),
                ("capacity-policy", policy),
                ("capacity-check", "require-envelope"),
                ("class-slots", "4616,4616"),
                ("domain-limit-bytes", "865500"),
                ("memory-policy", "adaptive"),
            ]);
            // Original slots exactly cover 8 * (8 * 64 + 8 * 8 + 1).
            assert!(PipelinePlan::new(&s, false, "mosaic").is_ok());
            for elasticity in ["cold", "warm"] {
                s.memory.as_mut().unwrap().elasticity = elasticity.into();
                let error = PipelinePlan::new(&s, false, "mosaic").err().unwrap();
                assert!(error.to_string().contains("selected memory topology"));
            }
            // Report mode exposes the deficit without promising coverage.
            s.require_envelope = false;
            let p = PipelinePlan::new(&s, false, "mosaic").unwrap();
            let selected = p.memory.as_ref().unwrap();
            assert!(selected.config.classes[0].slots * selected.initial < 4616);
            s.require_envelope = true;
            s.class_slots = vec![6000, 6000];
            assert!(PipelinePlan::new(&s, false, "mosaic").is_ok());
        }
    }
    #[test]
    fn si15_source_does_not_follow_class_rounding() {
        for profile in ["packets", "frames", "mixed-frames", "boundaries"] {
            let a = settings(&[("distribution", profile)]);
            let b = settings(&[
                ("distribution", profile),
                ("class-capacities", "131072,2097152"),
            ]);
            for stream in 0..4 {
                for seq in 0..200 {
                    assert_eq!(a.payload_size(stream, seq), b.payload_size(stream, seq));
                }
            }
            if profile == "mixed-frames" {
                assert_eq!(a.seed, 1);
                assert_eq!(
                    (0..200)
                        .filter(|&seq| a.payload_size(0, seq) >= 512 * 1024)
                        .count(),
                    10
                );
            }
        }
        let geometric = settings(&[
            ("distribution", "mixed-frames"),
            ("class-profile", "geometric"),
            ("class-slots", "1,1,1,1,1,1,1,1,1,1"),
        ]);
        for n in 4097..=2097152 {
            let class = geometric.capacities().iter().find(|&&c| c >= n).unwrap();
            assert!(*class < 2 * n);
        }
    }
}
