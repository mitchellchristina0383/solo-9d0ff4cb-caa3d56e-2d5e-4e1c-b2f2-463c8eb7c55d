"""Incremental, component-based evaluation cache for one collection window.

Why per-component evaluation is exact
-------------------------------------
Traces and services form a bipartite graph: an edge means "this trace has a
span on this service". A service's quota consumption is only influenced by
traces touching that service, so traces in *different* connected components
can never affect each other's keep/drop decision, decision reason or
per-service occupancy. Evaluating each connected component on its own — with
the shared ``sampler.evaluate_window`` routine and the same priority order
(error/timeout → slow → normal, then start time, then trace id) — therefore
produces exactly the same result as a full-window recompute over the same
committed data, while only touching the spans of the affected component.

What is cached
--------------
* ``traces`` — in-memory trace objects, updated span-by-span on arrival, so
  unchanged chains are never re-scanned from SQLite.
* per-component ``decisions`` / ``occupancy`` — reused verbatim until the
  component is marked dirty.
* per-trace UI payloads — rebuilt only when the trace's spans or its
  presented decision actually changed.

Invalidation
------------
* new span  → dirties its trace's component; if the span carries a service
  that belongs to another component, the two previously independent groups
  are merged and re-evaluated together in the original priority order;
* quota change → dirties the component containing that service (if any);
* finalize   → the window is re-evaluated once in "closed" mode (exported
  traces keep their quota slots and get the idempotent-keep reason);
* reset / window switch / (simulated) restart → the cache is dropped by the
  server layer and the next snapshot is rebuilt from the current SQLite
  contents (a full recompute over the committed data).
"""

from __future__ import annotations

from typing import Any, Iterable

from . import sampler
from .sampler import Trace


class _Component:
    """One connected group of traces linked through shared services."""

    __slots__ = ("trace_ids", "services", "dirty")

    def __init__(self) -> None:
        self.trace_ids: set[str] = set()
        self.services: set[str] = set()
        self.dirty = True


class WindowCache:
    """Incremental evaluation + snapshot cache for a single window.

    The cache is rebuilt from SQLite (``from_storage``) whenever the server
    has no live state for the window: first touch, after a reset, after a
    window switch and after every (simulated or real) restart.
    """

    def __init__(self, w, quotas: dict[str, int], exported: set[str]) -> None:
        self.window_id = w["id"]
        self.slow_ms = w["slow_ms"]
        self.default_quota = w["default_quota"]
        self.mode = "closed" if w["status"] == "closed" else "open"
        self.quotas = dict(quotas)
        self.exported = set(exported)

        self.traces: dict[str, Trace] = {}
        self.span_version: dict[str, int] = {}
        self.span_count = 0
        self._span_ids: set[str] = set()

        self.comps: dict[int, _Component] = {}
        self.comp_of_trace: dict[str, int] = {}
        self.comp_of_service: dict[str, int] = {}
        self._next_comp_id = 1

        self.decisions: dict[str, dict[str, Any]] = {}
        self.occupancy: dict[str, dict[str, Any]] = {}
        self._payloads: dict[str, tuple[Any, dict]] = {}

        self.stats = {
            "full_builds": 0,       # rebuilds from the committed SQLite state
            "span_rows_scanned": 0,  # span rows read from SQLite (builds only)
            "spans_applied": 0,     # spans folded in incrementally
            "component_evals": 0,   # component re-evaluations
            "traces_evaluated": 0,  # traces inside re-evaluated components
            "merges": 0,            # incremental group merges (late spans)
            "payloads_built": 0,    # UI payload rebuilds
            "snapshots": 0,         # snapshots served from this cache
        }
        # Merges and closed-mode re-evaluations happen eagerly inside
        # mutation paths (before the next snapshot's watermark), so they are
        # accrued here and reported once per snapshot.
        self._pending_merges = 0
        self._pending_evals = 0
        self._pending_traces = 0
        # Set by from_storage: the next snapshot reports a full build even
        # when the build happened earlier in the same request (e.g. inside
        # advance() right after a restart).
        self._fresh_build = False

    # ---------------------------------------------------------- construction
    @classmethod
    def from_storage(cls, storage, w) -> "WindowCache":
        """Full build from the committed SQLite state.

        Used on first touch and after reset / window switch / restart: the
        preview is generated from the data currently persisted in SQLite.
        """
        span_rows = storage.get_spans(w["id"])
        cache = cls(w, storage.get_quotas(w["id"]),
                    storage.get_exported_trace_ids(w["id"]))
        cache.stats["full_builds"] += 1
        cache.stats["span_rows_scanned"] += len(span_rows)
        cache._fresh_build = True
        cache.traces = sampler.build_traces(span_rows)
        cache.span_count = len(span_rows)
        for row in span_rows:
            cache._span_ids.add(row["span_id"])
        for tid, t in cache.traces.items():
            cache.span_version[tid] = len(t.spans)
            cache._attach_trace(tid, t.services, incremental=False)
        cache.ensure_clean()
        return cache

    # ------------------------------------------------------- component graph
    def _new_comp(self) -> int:
        cid = self._next_comp_id
        self._next_comp_id += 1
        self.comps[cid] = _Component()
        return cid

    def _merge(self, a: int, b: int, incremental: bool = True) -> int:
        """Union two components; the merged group is re-evaluated as a whole.

        Re-evaluation uses the unchanged priority order (tier, start time,
        trace id), i.e. the two previously independent groups are merged
        "按原优先级" and recomputed together.
        """
        ca, cb = self.comps[a], self.comps[b]
        keep, drop = (a, b) if len(ca.trace_ids) >= len(cb.trace_ids) else (b, a)
        ck, cd = self.comps[keep], self.comps[drop]
        ck.trace_ids |= cd.trace_ids
        ck.services |= cd.services
        for tid in cd.trace_ids:
            self.comp_of_trace[tid] = keep
        for svc in cd.services:
            self.comp_of_service[svc] = keep
        del self.comps[drop]
        ck.dirty = True
        if incremental:
            self.stats["merges"] += 1
            self._pending_merges += 1
        return keep

    def _attach_trace(self, tid: str, services: Iterable[str],
                      incremental: bool) -> int:
        """Place a trace into the component graph, merging bridged groups."""
        cids: list[int] = []
        for svc in services:
            cid = self.comp_of_service.get(svc)
            if cid is not None and cid not in cids:
                cids.append(cid)
        if not cids:
            cid = self._new_comp()
        else:
            cid = cids[0]
            for other in cids[1:]:
                cid = self._merge(cid, other, incremental=incremental)
        comp = self.comps[cid]
        comp.trace_ids.add(tid)
        self.comp_of_trace[tid] = cid
        for svc in services:
            if svc not in self.comp_of_service:
                self.comp_of_service[svc] = cid
                comp.services.add(svc)
        comp.dirty = True
        return cid

    # ---------------------------------------------------------- incremental
    def add_span(self, span: dict) -> None:
        """Fold one freshly committed span into the cached state.

        Only the affected component (possibly two merged ones) is dirtied;
        every other chain keeps its previous result untouched.
        """
        sid = span["span_id"]
        if sid in self._span_ids:
            return  # defensive: duplicate spans never mutate the cache
        self._span_ids.add(sid)
        tid = span["trace_id"]
        t = self.traces.get(tid)
        if t is None:
            t = Trace(tid)
            self.traces[tid] = t
            self.span_version[tid] = 0
            self._attach_trace(tid, [span["service"]], incremental=True)
        else:
            cid = self.comp_of_trace[tid]
            svc = span["service"]
            if svc not in t.services:
                other = self.comp_of_service.get(svc)
                if other is None:
                    self.comp_of_service[svc] = cid
                    self.comps[cid].services.add(svc)
                    self.comps[cid].dirty = True
                elif other != cid:
                    # A (typically late) span bridges two previously
                    # independent groups: merge and recompute as one.
                    self._merge(cid, other, incremental=True)
                else:
                    self.comps[cid].dirty = True
            else:
                self.comps[cid].dirty = True
        t.spans.append(span)
        self.span_version[tid] += 1
        self.span_count += 1
        self.stats["spans_applied"] += 1

    def set_quota(self, service: str, quota: int) -> None:
        """Dirty only the component containing ``service`` (if any)."""
        self.quotas[service] = quota
        cid = self.comp_of_service.get(service)
        if cid is not None:
            self.comps[cid].dirty = True

    def set_closed(self, exported: set[str]) -> None:
        """Switch to closed mode: re-evaluate once with exported slots held."""
        exported = set(exported)
        if self.mode == "closed" and self.exported == exported:
            return
        self.mode = "closed"
        self.exported = exported
        before = (self.stats["component_evals"], self.stats["traces_evaluated"])
        for comp in self.comps.values():
            comp.dirty = True
        self.ensure_clean()
        self._pending_evals += self.stats["component_evals"] - before[0]
        self._pending_traces += self.stats["traces_evaluated"] - before[1]

    # ------------------------------------------------------------- evaluation
    def ensure_clean(self) -> None:
        for cid in list(self.comps):
            comp = self.comps.get(cid)
            if comp is not None and comp.dirty:
                self._eval_component(comp)

    def _eval_component(self, comp: _Component) -> None:
        sub = {tid: self.traces[tid] for tid in comp.trace_ids}
        already = (
            (self.exported & comp.trace_ids) if self.mode == "closed" else None
        )
        res = sampler.evaluate_window(
            sub, self.quotas, self.default_quota, self.slow_ms, already
        )
        for tid in comp.trace_ids:
            self.decisions.pop(tid, None)
        self.decisions.update(res["decisions"])
        for svc in comp.services:
            self.occupancy.pop(svc, None)
        self.occupancy.update(res["occupancy"])
        comp.dirty = False
        self.stats["component_evals"] += 1
        self.stats["traces_evaluated"] += len(comp.trace_ids)

    # ------------------------------------------------------------ presentation
    def _present(self, stored: dict) -> dict:
        """Open windows show tentative (waiting) decisions; closed ones final."""
        if self.mode == "closed":
            return stored
        if stored["decision"] == sampler.KEEP:
            return {
                "decision": sampler.WAITING,
                "tier": stored["tier"],
                "reason": "等待窗口结束：暂定保留 — " + stored["reason"],
                "blocked_service": None,
            }
        return {
            "decision": sampler.WAITING,
            "tier": stored["tier"],
            "reason": "等待窗口结束：暂定丢弃 — " + stored["reason"],
            "blocked_service": stored["blocked_service"],
        }

    def _payload_for(self, tid: str, presented: dict) -> dict:
        key = (self.span_version[tid], presented)
        ent = self._payloads.get(tid)
        if ent is not None and ent[0] == key:
            return ent[1]
        payload = sampler.trace_payload(self.traces[tid], self.slow_ms, presented)
        self._payloads[tid] = (key, payload)
        self.stats["payloads_built"] += 1
        return payload

    # --------------------------------------------------------------- snapshot
    def snapshot(self, w, phase: str, now_ms: int, export_rows) -> dict:
        """Assemble the full window JSON, reusing every clean cached piece."""
        before = {k: self.stats[k] for k in
                  ("component_evals", "traces_evaluated", "payloads_built")}
        self.ensure_clean()

        presented = {tid: self._present(d) for tid, d in self.decisions.items()}
        ordered = sorted(
            self.traces.values(),
            key=lambda t: (t.tier(self.slow_ms), t.start_ms, t.trace_id),
        )
        trace_list = [
            self._payload_for(t.trace_id, presented[t.trace_id]) for t in ordered
        ]

        services_seen: set[str] = set()
        for comp in self.comps.values():
            services_seen |= comp.services
        quota_view = []
        for svc in sorted(services_seen | set(self.quotas)):
            quota_view.append({
                "service": svc,
                "quota": self.quotas.get(svc, self.default_quota),
                "explicit": svc in self.quotas,
                "used": self.occupancy.get(svc, {"used": 0})["used"],
            })

        groups = [
            {"id": cid, "services": sorted(comp.services),
             "traces": sorted(comp.trace_ids)}
            for cid, comp in sorted(self.comps.items())
        ]

        d_evals = (self.stats["component_evals"] - before["component_evals"]
                   + self._pending_evals)
        d_traces = (self.stats["traces_evaluated"] - before["traces_evaluated"]
                    + self._pending_traces)
        d_merges = self._pending_merges
        self._pending_merges = 0
        self._pending_evals = 0
        self._pending_traces = 0
        d_payloads = self.stats["payloads_built"] - before["payloads_built"]
        self.stats["snapshots"] += 1
        built = self._fresh_build
        self._fresh_build = False
        if built:
            source = "full_build"
        elif d_evals or d_merges:
            source = "incremental"
        else:
            source = "reuse"

        collect_end = w["start_ms"] + w["collect_ms"]
        grace_end = collect_end + w["grace_ms"]
        return {
            "window": {
                "id": w["id"],
                "start_ms": w["start_ms"],
                "collect_ms": w["collect_ms"],
                "grace_ms": w["grace_ms"],
                "slow_ms": w["slow_ms"],
                "default_quota": w["default_quota"],
                "status": w["status"],
                "phase": phase,
                "collect_end_ms": collect_end,
                "grace_end_ms": grace_end,
                "finalized_at_ms": w["finalized_at_ms"],
                "server_now_ms": now_ms,
            },
            "quotas": quota_view,
            "traces": trace_list,
            "exports": [dict(r) for r in export_rows],
            "span_count": self.span_count,
            "kept_trace_count": sum(
                1 for t in trace_list if t["decision"] == sampler.KEEP),
            "dropped_trace_count": sum(
                1 for t in trace_list if t["decision"] == sampler.DROP_QUOTA),
            "groups": groups,
            "incremental": {
                "source": source,
                "components": len(self.comps),
                "component_evals": d_evals,
                "traces_evaluated": d_traces,
                "traces_reused": len(self.traces) - d_traces,
                "merges": d_merges,
                "payloads_built": d_payloads,
            },
        }

    # ------------------------------------------------------------------ debug
    def debug_info(self) -> dict:
        return {
            "window_id": self.window_id,
            "mode": self.mode,
            "trace_count": len(self.traces),
            "span_count": self.span_count,
            "stats": dict(self.stats),
            "components": [
                {"id": cid, "services": sorted(c.services),
                 "traces": sorted(c.trace_ids), "dirty": c.dirty}
                for cid, c in sorted(self.comps.items())
            ],
        }
