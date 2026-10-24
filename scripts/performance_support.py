"""AP-09 measurement contracts. Checks remain active under optimized Python.

Schema 2 describes the runner envelope, not historical raw probe formats. These
oracles reconcile observed stages; residuals do not identify a cause of loss.
"""
import json
import math
import os
from pathlib import Path

SCHEMA = 2


class MeasurementError(ValueError):
    pass


def require(condition, path, message):
    if not condition:
        raise MeasurementError(f"{path}: {message}")


def field(obj, name, path):
    require(isinstance(obj, dict) and name in obj, f"{path}.{name}", "missing field")
    return obj[name]


def count(obj, name, path):
    value = field(obj, name, path)
    require(type(value) is int and value >= 0, f"{path}.{name}", "expected nonnegative integer")
    return value


def equal(left, right, path):
    require(left == right, path, f"expected {right}, observed {left}")


def validate_probe_envelope(report, arguments):
    expected={key:str(value) for key,value in arguments.items()}
    equal(field(report,"args","probe"),expected,"probe.args")
    result=field(report,"result","probe")
    require(isinstance(result,dict) and bool(result),"probe.result","expected nonempty object")
    require(result.get("failed",False) is False,"probe.result.failed","probe reported failure")
    return result


def drained(value, path):
    equal(count(value, "admitted", path), 0, path + ".admitted")
    equal(count(value, "available", path), count(value, "total_slots", path), path + ".available")


def validate_consumers(client, *, freshness=True):
    consumers = field(client, "consumers", "loadgen")
    require(isinstance(consumers, list) and consumers, "loadgen.consumers", "expected nonempty array")
    total = field(client, "total", "loadgen")
    names = ["delivered", "bytes"] + (["timely_delivered", "expired_delivered", "timely_bytes"] if freshness else [])
    for name in names:
        equal(count(total, name, "loadgen.total"), sum(count(c, name, f"consumer[{i}]") for i, c in enumerate(consumers)), "loadgen.total." + name)
    for path, c in [(f"consumer[{i}]", c) for i, c in enumerate(consumers)] + [("loadgen.total", total)]:
        hist = field(c, "scheduled_arrival_latency", path)
        equal(count(c, "delivered", path), count(hist, "samples", path + ".latency") + count(hist, "out_of_range", path + ".latency"), path + ".latency")
        if freshness:
            equal(count(c, "invalid", path), 0, path + ".invalid")
            equal(c["delivered"], c["timely_delivered"] + c["expired_delivered"], path + ".freshness")
            require(c["timely_bytes"] <= c["bytes"], path + ".timely_bytes", "exceeds delivered bytes")
    return total


def validate_issuer_flow(server, client):
    require(field(server, "failed", "server") is False, "server.failed", "server reported failure")
    require(field(client, "failed", "loadgen") is False, "loadgen.failed", "loadgen reported failure")
    equal(count(server, "unconfirmed_workers", "server"), 0, "server.unconfirmed_workers")
    supervisor = field(server, "supervisor", "server")
    for name in ("charged_bytes", "abandoned_bytes", "abandoned_roots", "unconfirmed_bytes"):
        equal(count(supervisor, name, "supervisor"), 0, "supervisor." + name)
    source = field(client, "source", "loadgen")
    n = {k: count(source, k, "source") for k in ("planned", "attempted", "missed_arrivals", "enqueued", "queue_full", "queue_closed", "sent", "failed_writes")}
    equal(n["planned"], n["attempted"] + n["missed_arrivals"], "source.planned")
    equal(n["attempted"], n["enqueued"] + n["queue_full"] + n["queue_closed"], "source.attempted")
    equal(n["enqueued"], n["sent"] + n["failed_writes"], "source.enqueued")
    fanout = count(server, "fanout", "server")
    require(fanout > 0, "server.fanout", "must be positive")
    streams = []
    workers = field(server, "workers", "server")
    require(isinstance(workers, list) and workers, "server.workers", "expected nonempty array")
    for i, w in enumerate(workers):
        path = f"server.workers[{i}]"
        require("error" not in w, path, "worker reported error")
        for flag in ("destroyed", "flow_complete"):
            require(field(w, flag, path) is True, path + "." + flag, "not confirmed")
        d = field(w, "drained", path)
        if d is not None:
            drained(d, path + ".drained")
        # Writer-only workers have no storage. Shared reference storage drains once.
        require(d is not None or not w["streams"] or server.get("shared_drained") is not None,
                path + ".drained", "issuer storage drain missing")
        entries=field(w,"streams",path)
        require(isinstance(entries,list),path+".streams","expected array")
        streams.extend(entries)
    if server.get("shared_drained") is not None:
        drained(server["shared_drained"], "server.shared_drained")
    sums = dict.fromkeys(("received", "accepted", "discarded", "enqueued", "cancelled_before_forward", "queue_credit_attempts"), 0)
    for i, s in enumerate(streams):
        path = f"streams[{i}]"
        for name in sums:
            sums[name] += count(s, name, path)
        equal(s["received"], s["accepted"] + s["discarded"], path + ".received")
        equal(count(s, "dispatched_recipient_rights", path), count(s, "locally_enqueued", path) + s["cancelled_before_forward"], path + ".dispatched_recipient_rights")
        equal(s["received"] * fanout, sum(count(s, k, path) for k in ("enqueued", "cancelled_before_forward", "skipped", "queue_rejected", "no_subscriber_deliveries", "ingress_rejected_deliveries")), path + ".recipient_rights")
        reasons = field(s, "contention_by_operation", path)
        equal(count(s, "contended", path), sum(count(reasons, k, path + ".contention_by_operation") for k in reasons), path + ".contended")
        reasons = field(s, "discard_reasons", path)
        equal(s["discarded"], sum(count(reasons, k, path + ".discard_reasons") for k in reasons), path + ".discarded")
    require(sums["accepted"] <= n["sent"], "streams.accepted", "exceeds source sent")
    total = validate_consumers(client)
    delivered = total["delivered"]
    written = sum(count(w, "writer_completed", "worker") for w in workers)
    calls = sum(count(w, "write_api_calls", "worker") for w in workers)
    require(delivered <= written <= sums["enqueued"], "flow", "delivered <= written <= enqueued violated")
    evicted = replacements = queue_cancelled = 0
    if server.get("pipeline_plan", {}).get("queue_backend") == "local":
        queues = [q for w in workers for q in field(w, "local_queues", "worker")]
        equal(len(queues), len(streams)*fanout, "local_queues.count")
        identities = [(count(q,"stream","queue"),count(q,"subscriber","queue")) for q in queues]
        require(len(set(identities))==len(identities), "local_queues", "duplicate recipient")
        enqueued = 0
        for i,q in enumerate(queues):
            path=f"local_queues[{i}]";m=field(q,"metrics",path)
            capacity=count(m,"capacity",path)
            require(capacity>0,path,"zero capacity")
            equal(sum(count(m,k,path) for k in ('free','reserved','queued','transferring','releasing')),capacity,path+'.credits')
            equal(count(m,'free',path),capacity,path+'.drained')
            equal(count(m,'enqueued',path),sum(count(m,k,path) for k in ('transferred','evicted','cancelled')),path+'.ownership')
            require(count(m,'failed_replacements',path)<=m['evicted'],path,"failed replacement without eviction")
            require(count(m,'occupancy_high_water',path)<=capacity,path,"occupancy exceeded capacity")
            require(count(m,'prepared_capacity',path)>=capacity,path,"unprepared queue storage")
            for k in ('storage_model_bytes','rejected_full','queue_age_samples','queue_age_max_ns'):count(m,k,path)
            if server['pipeline_plan'].get('freshness_priority')=='adaptive':
                a=field(m,'adaptive',path)
                require(1<=count(a,'window',path)<=capacity,path,'invalid adaptive window')
                require(count(a,'pressure_q16',path)<=65536,path,'invalid pressure')
                equal(sum(count(a,k,path) for k in ('age_evictions','window_evictions','dependency_evictions')),m['evicted'],path+'.eviction_reasons')
                for k in ('debt_ms','dependency_skips','policy_rejections','stale_commits','recovery_generation'):count(a,k,path)
                require(a['state'] in ('streaming','await_keyframe','closing'),path,'invalid controller state')
                require(a['disconnect_reason'] in (None,'sustained_pressure','write_stall','keyframe_timeout','frame_deadline','recovery_generation_exhausted'),path,'invalid disconnect reason')
            evicted+=m['evicted'];replacements+=m['failed_replacements'];queue_cancelled+=m['cancelled'];enqueued+=m['enqueued']
        equal(enqueued,sums['enqueued'],'local_queues.enqueued')
        require(evicted+queue_cancelled<=enqueued-written,'local_queues.losses','losses exceed uncompleted enqueued')
        if server['pipeline_plan'].get('freshness_priority')=='off':equal(evicted,0,'local_queues.fifo_evictions')
    receipt_mode = server.get("pipeline_plan", {}).get("queue_accounting") == "receipts-v1"
    transferred = 0
    if receipt_mode:
        queues = [q for w in workers for q in field(w, "tokio_queues", "worker")]
        equal(len(queues), len(streams)*fanout, "tokio_queues.count")
        identities = [(count(q,"stream","queue"), count(q,"subscriber","queue")) for q in queues]
        require(len(set(identities)) == len(identities), "tokio_queues", "duplicate recipient")
        enqueued = 0
        for q in queues:
            m = field(q, "metrics", "queue")
            equal(count(m,"enqueued","queue"), count(m,"transferred","queue") + count(m,"cancelled","queue"), "queue.receipts")
            enqueued += m["enqueued"]
            transferred += m["transferred"]
            queue_cancelled += m["cancelled"]
        equal(enqueued, sums["enqueued"], "tokio_queues.enqueued")
    # Legacy binaries lack batch outcomes. Retain their residual explicitly.
    aborted = 0
    reported = sum("writer_aborted" in w for w in workers)
    require(reported in (0, len(workers)), "writer_aborted", "mixed writer abort schemas")
    classified = reported == len(workers)
    if classified:
        for w in workers:
            reasons = field(w, "writer_aborted", "worker")
            aborted += sum(count(reasons, k, "writer_aborted") for k in
                           ("deadline", "policy", "io", "cancel", "unwind"))
    residual = sums["enqueued"] - written - evicted - queue_cancelled - aborted
    require(residual >= 0, "writer_aborted", "duplicate loss accounting")
    if receipt_mode:
        require(classified, "writer_aborted", "receipt schema requires writer outcomes")
        equal(transferred, written + aborted, "queue.writer_transfer")
        equal(residual, 0, "flow.classified_tokio_losses")
    # Legacy Tokio queues retain an explicit residual.
    if classified and server.get("pipeline_plan", {}).get("queue_backend") == "local":
        equal(residual, 0, "flow.classified_local_losses")
    seconds = field(client, "measurement_seconds", "loadgen")
    require(type(seconds) in (int, float) and math.isfinite(seconds) and seconds > 0, "loadgen.measurement_seconds", "positive finite duration required")
    planned = n["planned"] * fanout
    return dict(sums, writer_completed=written, delivered=delivered,
                freshness_evicted=evicted, failed_replacements=replacements, cancelled_queued=queue_cancelled,
                writer_aborted=aborted, writer_abort_classification=classified,
                unclassified_uncompleted_enqueued=residual,
                uncompleted_enqueued=sums["enqueued"] - written,
                unobserved_completed=written - delivered,
                undelivered_enqueued=sums["enqueued"] - delivered,
                write_api_calls=calls, frames_per_write=written / max(1, calls),
                planned_deliveries=planned, delivery_fraction=delivered / max(1, planned),
                timely_delivered=total["timely_delivered"], expired_delivered=total["expired_delivered"],
                timely_delivery_fraction=total["timely_delivered"] / max(1, planned),
                network_bytes_per_planned_s=total["bytes"] / seconds,
                useful_bytes_per_planned_s=total["timely_bytes"] / seconds)


def validate_reference_flow(result):
    result = result.get("pipeline", result)
    if "source" not in result:
        return
    source = result["source"]
    equal(count(source, "planned", "source"), sum(count(source, n, "source") for n in ("attempted", "missed_arrivals", "not_attempted_after_stop")), "source.planned")
    require(count(result, "accepted", "result") <= count(source, "sent", "source") <= source["attempted"], "source.sent", "invalid admission count")
    equal(result["accepted"] * count(result, "subscribers", "result"), sum(count(result, n, "result") for n in ("enqueued", "skipped", "queue_rejected", "no_subscriber_deliveries")), "result.enqueued")
    require(result.get("failed") is False, "result.failed", "failure or missing status")
    total = validate_consumers(result, freshness=False)
    for i, c in enumerate(result["consumers"]):
        equal(count(c, "invalid", f"consumers[{i}]"), 0, f"consumers[{i}].invalid")
    equal(count(total, "invalid", "total"), 0, "total.invalid")
    equal(result["enqueued"], total["delivered"] + count(result, "undelivered_enqueued", "result"), "result.delivered")
    drained(field(result, "drained", "result"), "result.drained")


def validate_branch_result(result, mode):
    require(field(result, "drained", "branch") is True, "branch.drained", "not confirmed")
    if "offered_views" in result:
        equal(count(result, "offered_views", "branch"), count(result, "accepted_views", "branch") + count(result, "contended_views", "branch"), "branch.offered_views")
        if mode in ("local", "borrowed"):
            equal(result["contended_views"], 0, "branch.contended_views")


def read_records(path):
    records = {}
    if not path.exists():
        return records
    for line in path.read_text().splitlines():
        record = json.loads(line)
        require(record.get("schema_version") == SCHEMA, str(path), "unknown or legacy schema; use a new output directory")
        require(record.get("status") == "passed", str(path), "prior failed/incomplete/unvalidated run; use a new output directory")
        require(record["id"] not in records, str(path), "duplicate run id")
        records[record["id"]] = record
    return records


def physical_cpus(allowed=None, root=Path("/sys/devices/system/cpu")):
    allowed = sorted(os.sched_getaffinity(0) if allowed is None else allowed)
    cores = {}
    for cpu in allowed:
        p = root / f"cpu{cpu}" / "topology"
        try:
            key = (int((p / "physical_package_id").read_text()), int((p / "core_id").read_text()))
        except (OSError, ValueError) as error:
            raise MeasurementError("CPU topology unavailable; supply --smt for a separately labelled logical-CPU experiment") from error
        require(min(key) >= 0, "topology", "unknown physical CPU identity")
        cores.setdefault(key, cpu)
    require(bool(cores), "affinity", "no CPUs available")
    return list(cores.values())


def cpu_selection(smt=False):
    return sorted(os.sched_getaffinity(0)) if smt else physical_cpus()


if __name__ == "__main__":
    print(",".join(map(str, physical_cpus())))
