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
}
pub(super) struct PipelinePlan {
    pub roots: Vec<RootPlan>,
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
                scratch_bytes: mul(streams.len(), s.max_payload().min(16 * 1024))?,
                queue_storage: mul(queued, {
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
                })?,
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
            });
        }
        Ok(Self {
            roots: plans,
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
                "queue_item_storage_model_bytes":r.queue_storage,
                "mailbox_item_storage_model_bytes":r.mailbox_storage,"semaphore_storage_model_bytes":r.semaphore_storage,"writer_descriptors_model_bytes":r.descriptors,
                "reservation_bytes":r.reservation_bytes,"writers":r.writers,"configured_backing_bytes":r.backing_bytes,"backing_is_modeled":r.backing_is_modeled,"metadata_bytes":r.metadata_bytes,
                "metadata_is_modeled":r.metadata_is_modeled})
        }).collect();
        json!({"roots":roots,"class_slots":s.class_slots,"class_capacities":s.class_capacities,"distribution":s.distribution,"write_batch":s.write_batch,
            "capacity_check":if s.require_envelope {"require-envelope"} else {"report"},
            "fanout_api":if s.worker_branches {"worker-branches"} else if s.local_branches {"local-branches"} else if s.batch_fanout {"batch"} else {"scalar"},
            "capacity_policy":format!("{:?}",s.capacity_policy),"max_frame_age_ms":s.max_frame_age.map(|a|a.as_millis()),
            "send_buffer_bytes_requested":s.send_buffer_bytes,"ingress":if s.read_buf {"read-buf"} else {"initialized"},
            "mailbox_is_subset_of_queue_credits":s.worker_branches,
            "mailbox_capacity_per_stream":if s.worker_branches {s.fanout*s.queue} else {0},
            "availability_guaranteed":false,"bounds_are_conservative":true,"occupancy_includes_pending_returns":true,
            "external_overhead":"Tokio tasks/channels, allocator overhead and socket buffers are outside the item storage model"})
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
