"""Self tests for the incremental (component-based) evaluation cache.

Verifies the four guarantees of the incremental preview:

1. Polling an unchanged window reuses the previous computation (no
   component re-evaluation, no payload rebuild, no span re-scan).
2. A new span or a quota change only re-evaluates the chains affected
   directly or indirectly through shared services; unrelated chains reuse
   their previous results.
3. A late span that bridges two previously independent groups merges them
   and the merged group is recomputed in the original priority order.
4. Every preview (decisions, reasons, quota usage) matches a full recompute
   over the same committed SQLite data — checked after every operation,
   including a randomized scenario and restarts.

Run:  python3 -I tests/test_incremental.py
"""

from __future__ import annotations

import json
import os
import random
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

# Isolated mode (-I) puts neither cwd nor PYTHONPATH on sys.path; the test
# file lives in tests/, so add the project root explicitly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import sampler
from app.server import AppState, create_server


class FakeClock:
    def __init__(self, start_ms: int):
        self.now = start_ms

    def __call__(self) -> int:
        return self.now

    def advance(self, ms: int) -> None:
        self.now += ms


class Harness:
    def __init__(self):
        self.tmpdir = tempfile.mkdtemp(prefix="tsinc-")
        self.db = os.path.join(self.tmpdir, "test.db")
        self.httpd: ThreadingHTTPServer = create_server("127.0.0.1", 0, self.db)
        self.clock = FakeClock(int(time.time() * 1000))
        self.httpd.state.now_ms = self.clock  # type: ignore[attr-defined]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.httpd.state.storage.close()

    @property
    def state(self) -> AppState:
        return self.httpd.state  # type: ignore[attr-defined]

    def request(self, method: str, path: str, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    # convenience wrappers ------------------------------------------------
    def create_window(self, collect=4000, grace=2000, slow=500,
                      default_quota=3, quotas=None):
        return self.request("POST", "/api/windows", {
            "collect_ms": collect, "grace_ms": grace, "slow_ms": slow,
            "default_quota": default_quota, "quotas": quotas or {},
        })

    def span(self, span_id, trace_id, service, start, duration=100,
             status="ok", operation="op"):
        return self.request("POST", "/api/spans", {
            "span_id": span_id, "trace_id": trace_id, "service": service,
            "operation": operation, "start_ms": start,
            "duration_ms": duration, "status": status,
        })

    def set_quota(self, wid, service, quota):
        return self.request("POST", f"/api/windows/{wid}/quotas",
                            {"service": service, "quota": quota})

    def current(self):
        return self.request("GET", "/api/window/current")[1]

    def finalize(self, wid):
        return self.request("POST", f"/api/windows/{wid}/finalize", {})

    def cache_stats(self):
        return self.request("GET", "/api/debug/cache")[1]


def reference_result(state: AppState, wid: int) -> dict:
    """Full recompute over the committed SQLite data (the ground truth)."""
    w = state.storage.get_window(wid)
    rows = state.storage.get_spans(wid)
    traces = sampler.build_traces(rows)
    quotas = state.storage.get_quotas(wid)
    exported = state.storage.get_exported_trace_ids(wid)
    if w["status"] == "closed":
        return sampler.evaluate_window(
            traces, quotas, w["default_quota"], w["slow_ms"], exported)
    return sampler.tentative_evaluation(
        traces, quotas, w["default_quota"], w["slow_ms"])


class IncrementalTests(unittest.TestCase):
    def setUp(self):
        self.h = Harness()

    def tearDown(self):
        self.h.stop()

    # ---------------------------------------------------------- helpers
    def assert_consistent(self, snap, wid):
        """The incremental preview must equal a full recompute of the same
        committed data: keep/drop decision, reason, blocked service, tier and
        per-service quota usage."""
        ref = reference_result(self.h.state, wid)
        got = {t["trace_id"]: t for t in snap["traces"]}
        self.assertEqual(set(got), set(ref["decisions"]),
                         "链路集合必须与全量重算一致")
        for tid, d in ref["decisions"].items():
            g = got[tid]
            self.assertEqual(g["decision"], d["decision"], f"{tid} 决定不一致")
            self.assertEqual(g["reason"], d["reason"], f"{tid} 原因不一致")
            self.assertEqual(g["blocked_service"], d["blocked_service"],
                             f"{tid} 阻塞服务不一致")
            self.assertEqual(g["tier"], d["tier"], f"{tid} 优先级不一致")
        for q in snap["quotas"]:
            occ = ref["occupancy"].get(q["service"], {"used": 0})
            self.assertEqual(q["used"], occ["used"],
                             f"服务 {q['service']} 配额用量不一致")

    def stats_for(self, wid):
        dbg = self.h.cache_stats()
        for w in dbg["windows"]:
            if w["window_id"] == wid:
                return w
        self.fail(f"窗口 {wid} 没有缓存")

    # --------------------------------------- 1 unchanged poll reuses everything
    def test_01_unchanged_poll_reuses_previous_result(self):
        _, snap = self.h.create_window(collect=6000, grace=2000)
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        self.h.span("a1", "T-a", "svcA", base + 10, 50)
        self.h.span("b1", "T-b", "svcB", base + 20, 50)
        before = self.stats_for(wid)["stats"].copy()
        snap = self.h.current()
        after = self.stats_for(wid)["stats"]
        inc = snap["incremental"]
        self.assertEqual(inc["source"], "reuse")
        self.assertEqual(inc["component_evals"], 0)
        self.assertEqual(inc["traces_evaluated"], 0)
        self.assertEqual(inc["traces_reused"], 2)
        self.assertEqual(inc["payloads_built"], 0)
        # No component was re-evaluated and no span row was re-scanned.
        self.assertEqual(after["component_evals"], before["component_evals"])
        self.assertEqual(after["traces_evaluated"], before["traces_evaluated"])
        self.assertEqual(after["payloads_built"], before["payloads_built"])
        self.assertEqual(after["span_rows_scanned"], before["span_rows_scanned"])
        self.assert_consistent(snap, wid)

    # --------------------------------------- 2 unrelated chains are reused
    def test_02_unrelated_chains_reused_on_span_and_quota_change(self):
        _, snap = self.h.create_window(collect=6000, grace=2000,
                                       default_quota=5)
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        # Two independent groups: {T-a -> svcA} and {T-b -> svcB}.
        self.h.span("a1", "T-a", "svcA", base + 10, 50)
        self.h.span("a2", "T-a", "svcA", base + 20, 50)
        _, snap = self.h.span("b1", "T-b", "svcB", base + 30, 50)
        self.assertEqual(snap["incremental"]["components"], 2)
        reason_b_before = next(t for t in snap["traces"]
                               if t["trace_id"] == "T-b")["reason"]

        # A new span on T-a must only re-evaluate T-a's group.
        _, snap = self.h.span("a3", "T-a", "svcA", base + 40, 50)
        inc = snap["incremental"]
        self.assertEqual(inc["source"], "incremental")
        self.assertEqual(inc["component_evals"], 1, "只重算受影响的组")
        self.assertEqual(inc["traces_evaluated"], 1, "只重算 T-a 一条链路")
        self.assertEqual(inc["traces_reused"], 1, "T-b 沿用原结果")
        self.assertEqual(inc["payloads_built"], 1, "只有 T-a 的展示重建")
        reason_b_after = next(t for t in snap["traces"]
                              if t["trace_id"] == "T-b")["reason"]
        self.assertEqual(reason_b_after, reason_b_before)
        self.assert_consistent(snap, wid)

        # A quota change on svcB must only re-evaluate T-b's group.
        _, snap = self.h.set_quota(wid, "svcB", 0)
        inc = snap["incremental"]
        self.assertEqual(inc["component_evals"], 1)
        self.assertEqual(inc["traces_evaluated"], 1)
        self.assertEqual(inc["traces_reused"], 1)
        got = {t["trace_id"]: t for t in snap["traces"]}
        self.assertIn("svcB 配额已耗尽", got["T-b"]["reason"])
        self.assert_consistent(snap, wid)

        # A quota for a service nobody uses re-evaluates nothing.
        _, snap = self.h.set_quota(wid, "svc-unused", 1)
        self.assertEqual(snap["incremental"]["component_evals"], 0)
        self.assert_consistent(snap, wid)

    # --------------------------------------- 3 late span merges two groups
    def test_03_late_span_merges_two_independent_groups(self):
        _, snap = self.h.create_window(collect=3000, grace=3000, slow=500,
                                       default_quota=5,
                                       quotas={"svcB": 1})
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        # Two independent groups, both tentatively kept.
        self.h.span("a1", "T-a", "svcA", base + 50, 100)
        _, snap = self.h.span("b1", "T-b", "svcB", base + 60, 100)
        self.assertEqual(len(snap["groups"]), 2)
        got = {t["trace_id"]: t for t in snap["traces"]}
        self.assertEqual(got["T-a"]["tier"], 2)
        self.assertEqual(got["T-b"]["decision"], "waiting")

        # Enter grace, then a late error span gives T-a a span on svcB:
        # the two groups merge and are recomputed in the original priority
        # order — T-a (now error, tier 0) wins svcB's single slot and T-b
        # flips to a tentative drop.
        self.h.clock.advance(3100)
        code, snap = self.h.span("a2-late", "T-a", "svcB", base + 2500, 80,
                                 status="error")
        self.assertEqual(code, 200)
        inc = snap["incremental"]
        self.assertEqual(inc["merges"], 1, "迟到 span 应触发一次分组合并")
        self.assertEqual(inc["components"], 1, "两组应合并为一组")
        self.assertEqual(inc["traces_evaluated"], 2, "合并组整体重算")
        self.assertEqual(len(snap["groups"]), 1)
        self.assertEqual(sorted(snap["groups"][0]["traces"]), ["T-a", "T-b"])
        got = {t["trace_id"]: t for t in snap["traces"]}
        self.assertEqual(got["T-a"]["tier"], 0)
        self.assertIn("暂定保留", got["T-a"]["reason"])
        self.assertIn("暂定丢弃", got["T-b"]["reason"])
        self.assertIn("svcB 配额已耗尽", got["T-b"]["reason"])
        self.assert_consistent(snap, wid)

        # Finalize: the merged outcome must hold in the exported result.
        self.h.clock.advance(3000)
        self.h.current()  # tick auto-finalize
        snap = self.h.current()
        self.assertEqual(snap["window"]["status"], "closed")
        got = {t["trace_id"]: t for t in snap["traces"]}
        self.assertEqual(got["T-a"]["decision"], "keep")
        self.assertEqual(got["T-b"]["decision"], "drop_quota")
        self.assertEqual({e["trace_id"] for e in snap["exports"]}, {"T-a"})
        self.assert_consistent(snap, wid)

    # --------------------------------------- 4 restart rebuilds from SQLite
    def test_04_restart_regenerates_preview_from_sqlite(self):
        _, snap = self.h.create_window(collect=4000, grace=2000,
                                       default_quota=2)
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        self.h.span("r1", "T-r1", "svcA", base + 10, 50)
        self.h.span("r2", "T-r2", "svcB", base + 20, 600, status="error")
        before = self.h.current()

        # Simulated restart via the API.
        st, r = self.h.request("POST", "/api/simulate-restart", {})
        self.assertEqual(st, 200)
        snap = self.h.current()
        self.assertEqual(snap["incremental"]["source"], "full_build",
                         "重启后首个预览必须全量重建")
        self.assertEqual(self.stats_for(wid)["stats"]["full_builds"], 1)
        # Same decisions/reasons/quota usage as before the restart.
        for field in ("trace_id", "decision", "reason", "tier"):
            self.assertEqual([t[field] for t in snap["traces"]],
                             [t[field] for t in before["traces"]], field)
        self.assertEqual([(q["service"], q["used"], q["quota"])
                          for q in snap["quotas"]],
                         [(q["service"], q["used"], q["quota"])
                          for q in before["quotas"]])
        self.assert_consistent(snap, wid)

        # A real process-style restart: brand new AppState on the same file.
        self.h.state.storage.close()
        self.h.httpd.state = AppState(self.h.db)
        self.h.httpd.state.now_ms = self.h.clock
        snap = self.h.current()
        self.assertEqual(snap["incremental"]["source"], "full_build")
        self.assertEqual(snap["window"]["id"], wid)
        self.assert_consistent(snap, wid)

        # The window keeps collecting after the restart; a late span still
        # merges incrementally on top of the rebuilt cache.
        self.h.clock.advance(4100)  # into grace
        code, snap = self.h.span("r3-late", "T-r1", "svcB", base + 3500, 40,
                                 status="error")
        self.assertEqual(code, 200)
        self.assertEqual(snap["incremental"]["merges"], 1)
        self.assert_consistent(snap, wid)

    # --------------------------------------- 5 reset and window switch
    def test_05_reset_and_new_window_rebuild_from_sqlite(self):
        _, snap = self.h.create_window()
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        self.h.span("x1", "T-x", "svcA", base + 10, 10)
        self.h.request("POST", "/api/reset", {})
        snap = self.h.current()
        self.assertIsNone(snap["window"])
        self.assertEqual(self.h.cache_stats()["cached_windows"], 0)
        # A new window (换窗) builds its own fresh cache from SQLite.
        _, snap = self.h.create_window()
        wid2 = snap["window"]["id"]
        self.assertNotEqual(wid2, wid)
        self.assertEqual(snap["incremental"]["source"], "full_build")
        self.assert_consistent(snap, wid2)

    # --------------------------------------- 6 closed window polls reuse too
    def test_06_closed_window_poll_reuses_result(self):
        _, snap = self.h.create_window(collect=1000, grace=500,
                                       default_quota=1)
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        self.h.span("c1", "T-c", "svcA", base + 10, 10)
        self.h.clock.advance(1600)
        self.h.finalize(wid)
        snap = self.h.current()
        self.assertEqual(snap["window"]["status"], "closed")
        self.assertEqual(snap["traces"][0]["decision"], "keep")
        self.assertIn("已导出", snap["traces"][0]["reason"])
        before = self.stats_for(wid)["stats"].copy()
        snap = self.h.current()
        after = self.stats_for(wid)["stats"]
        self.assertEqual(snap["incremental"]["source"], "reuse")
        self.assertEqual(after["component_evals"], before["component_evals"])
        self.assert_consistent(snap, wid)

    # ----------------------------- 6b poll that auto-finalizes reports recompute
    def test_06b_poll_triggering_finalize_reports_closed_mode_reeval(self):
        _, snap = self.h.create_window(collect=1000, grace=500,
                                       default_quota=1)
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        self.h.span("f1", "T-f", "svcA", base + 10, 10)
        self.h.span("f2", "T-g", "svcB", base + 20, 10)
        self.h.clock.advance(1600)  # past grace: next poll finalizes
        snap = self.h.current()
        self.assertEqual(snap["window"]["status"], "closed")
        inc = snap["incremental"]
        self.assertEqual(inc["source"], "incremental",
                         "关闭时的闭窗模式重算必须可见")
        self.assertEqual(inc["component_evals"], 2)
        self.assertEqual(inc["traces_evaluated"], 2)
        self.assert_consistent(snap, wid)
        # The following poll fully reuses the closed result.
        snap = self.h.current()
        self.assertEqual(snap["incremental"]["source"], "reuse")
        self.assertEqual(snap["incremental"]["component_evals"], 0)
        self.assert_consistent(snap, wid)

    # --------------------------------------- 7 randomized consistency check
    def test_07_randomized_ops_always_match_full_recompute(self):
        rng = random.Random(20261122)
        _, snap = self.h.create_window(collect=6000, grace=4000, slow=400,
                                       default_quota=2,
                                       quotas={"svcA": 1, "svcB": 2})
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        services = ["svcA", "svcB", "svcC", "svcD"]
        traces = [f"T-{i}" for i in range(8)]
        statuses = ["ok"] * 8 + ["error", "timeout"]
        closed = False
        for i in range(160):
            op = rng.random()
            if op < 0.62 and not closed:
                start = base + rng.randint(0, min(
                    self.h.clock.now - base, 6000 + 4000))
                code, snap = self.h.span(
                    f"sp-{i}", rng.choice(traces), rng.choice(services),
                    start, rng.randint(10, 900), rng.choice(statuses))
                self.assertEqual(code, 200)
            elif op < 0.82 and not closed:
                _, snap = self.h.set_quota(
                    wid, rng.choice(services), rng.randint(0, 3))
            else:
                self.h.clock.advance(rng.randint(200, 800))
                snap = self.h.current()
                if snap["window"] and snap["window"]["status"] == "closed":
                    closed = True
            if snap.get("window"):
                self.assert_consistent(snap, wid)
        # Drive the window to closure and verify the final exported state.
        self.h.clock.advance(12000)
        snap = self.h.current()
        self.assertEqual(snap["window"]["status"], "closed")
        self.assert_consistent(snap, wid)
        # Idempotent re-finalize keeps the same preview.
        _, snap = self.h.finalize(wid)
        self.assert_consistent(snap, wid)

    # --------------------------------------- 8 debug endpoint exposes layout
    def test_08_debug_endpoint_reports_components(self):
        _, snap = self.h.create_window()
        wid, base = snap["window"]["id"], snap["window"]["start_ms"]
        self.h.span("d1", "T-d1", "svcA", base + 10, 10)
        self.h.span("d2", "T-d2", "svcA", base + 20, 10)
        self.h.span("d3", "T-d3", "svcZ", base + 30, 10)
        st, dbg = self.h.request("GET", "/api/debug/cache")
        self.assertEqual(st, 200)
        entry = next(w for w in dbg["windows"] if w["window_id"] == wid)
        self.assertEqual(entry["mode"], "open")
        comps = sorted(len(c["traces"]) for c in entry["components"])
        self.assertEqual(comps, [1, 2], "svcA 上的两条链路同组，svcZ 独立成组")
        self.assertGreaterEqual(entry["stats"]["component_evals"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
