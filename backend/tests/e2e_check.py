"""Regression tests for the drill platform.

Covers the unified event-log allocation rules (join / start / submit /
deadline & manual settle / end): one atomic commit == one new version ==
last_version + 1, and events inside a commit are numbered seq 0..k in a
fixed causal order, so (session_id, version, seq) never collides. Also
covers deadline races, the stale-timer identity check across two
consecutive timed stages, concurrent idempotency, deterministic replay of
every version prefix, and an end-to-end HTTP + WebSocket pass through the
real FastAPI app.

Run from the repo root:   python backend/tests/e2e_check.py
Run from backend/:        python tests/e2e_check.py
Exit code is non-zero when any check fails.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path

_TMP = tempfile.mkdtemp(prefix="drill-test-")
os.environ["DATA_DIR"] = _TMP
os.environ["DRILLS_DIR"] = str(Path(_TMP) / "drills")
os.environ["STATIC_DIR"] = str(Path(__file__).resolve().parents[2] / "frontend")
os.environ["HOST_KEY"] = "test-key"

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

from app.db import Database  # noqa: E402
from app.definitions import DrillError, load_file, parse_definition  # noqa: E402
from app.engine import Engine, EngineError, now_ts, reduce_events  # noqa: E402
from app.joints import (  # noqa: E402
    MISSED,
    RELEASED,
    WAITING,
    JointError,
    JointOrchestrator,
    assert_acyclic,
    branch_satisfied,
)
from app.main import app  # noqa: E402
from app.records import build_replay, load_events  # noqa: E402
from app.snapshots import build_snapshot  # noqa: E402
from app.versions import content_digest  # noqa: E402

HOST = {"X-Host-Key": "test-key"}
PASS, FAIL = 0, 0


def check(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {extra}")


# --------------------------------------------------------------------------- #
# drill definitions used by the tests
# --------------------------------------------------------------------------- #
DUEL = {
    "id": "duel",
    "name": "双角色演练",
    "roles": [
        {"id": "a", "name": "甲", "invite": "AAA"},
        {"id": "b", "name": "乙", "invite": "BBB"},
        {"id": "c", "name": "丙", "invite": "CCC"},
    ],
    "stages": [
        {
            "id": "s1", "title": "阶段一", "public": "公开一", "duration": 30,
            "default_next": "s2",
            "views": {"a": "甲的隐秘s1", "b": "乙的隐秘s1"},
            "options": {
                "a": [{"id": "x", "text": "甲选X"}, {"id": "y", "text": "甲选Y"}],
                "b": [{"id": "p", "text": "乙选P"}, {"id": "q", "text": "乙选Q"}],
            },
            "transitions": [{"when": {"a": "x", "b": "p"}, "next": "s2"}],
        },
        {
            "id": "s2", "title": "阶段二", "public": "公开二", "duration": None,
            "default_next": None,
            "views": {"a": "甲的隐秘s2"},
            "options": {"a": [{"id": "k", "text": "甲终选"}]},
        },
    ],
}

FAST = {
    "id": "fast",
    "name": "限时演练",
    "roles": [{"id": "a", "name": "甲", "invite": "AAA"}],
    "stages": [
        {
            "id": "s1", "title": "一", "public": "p1", "duration": 1,
            "default_next": "s2",
            "options": {"a": [{"id": "x", "text": "X"}]},
        },
        {
            "id": "s2", "title": "二", "public": "p2", "duration": None,
            "default_next": None,
            "options": {"a": [{"id": "k", "text": "K"}]},
        },
    ],
}

# Two consecutive timed stages: the stale-timer regression needs both s1 and
# s2 to carry a duration, so a delayed s1 callback can mistake itself for s2's
# timeout. A clock mock drives the engine deterministically (no real sleeps).
TIMED2 = {
    "id": "timed2",
    "name": "连续限时演练",
    "roles": [{"id": "a", "name": "甲", "invite": "AAA"}],
    "stages": [
        {
            "id": "s1", "title": "一", "public": "p1", "duration": 10,
            "default_next": "s2",
            "options": {"a": [{"id": "x", "text": "X"}]},
        },
        {
            "id": "s2", "title": "二", "public": "p2", "duration": 20,
            "default_next": None,
            "options": {"a": [{"id": "k", "text": "K"}]},
        },
    ],
}

WS_DRILL = {
    "id": "wsdrill",
    "name": "WS演练",
    "roles": [
        {"id": "a", "name": "甲", "invite": "AAA"},
        {"id": "b", "name": "乙", "invite": "BBB"},
    ],
    "stages": [
        {
            "id": "s1", "title": "一", "public": "公开一", "duration": 60,
            "default_next": "s2",
            "views": {"a": "甲私有一", "b": "乙私有一"},
            "options": {
                "a": [{"id": "x", "text": "X"}],
                "b": [{"id": "p", "text": "P"}],
            },
        },
        {
            "id": "s2", "title": "二", "public": "公开二", "duration": None,
            "default_next": None,
            "views": {"b": "乙私有二"},
            "options": {"b": [{"id": "k", "text": "K"}]},
        },
    ],
}


# Two independent drills used by the joint-orchestration tests. They pin to
# different script identities on purpose: a joint must validate against each
# session's own pinned version and keep their event logs independent.
JUP = {
    "id": "jup",
    "name": "联演上游",
    "roles": [{"id": "a", "name": "甲", "invite": "AAA"}],
    "stages": [
        {
            "id": "s1", "title": "上游阶段", "public": "上游公开", "duration": None,
            "default_next": "s2",
            "options": {"a": [{"id": "go", "text": "推进"}, {"id": "stop", "text": "中止"}]},
            "transitions": [{"when": {"a": "stop"}, "next": None}],
        },
        {
            "id": "s2", "title": "上游后续", "public": "p2", "duration": None,
            "default_next": None,
            "options": {"a": [{"id": "k", "text": "K"}]},
        },
    ],
}

JDOWN = {
    "id": "jdown",
    "name": "联演下游",
    "roles": [
        {"id": "b", "name": "乙", "invite": "BBB"},
        {"id": "x", "name": "旁观者", "invite": "XXX"},
    ],
    "stages": [
        {
            "id": "t1", "title": "下游阶段", "public": "下游公开", "duration": None,
            "default_next": "t2",
            "views": {"b": "乙的台面信息"},
            "options": {"b": [{"id": "k", "text": "乙决定"}]},
        },
        {
            "id": "t2", "title": "下游后续", "public": "t2", "duration": None,
            "default_next": None,
            "options": {"b": [{"id": "z", "text": "Z"}]},
        },
    ],
}

JUP2 = {
    "id": "jup2",
    "name": "联演上游二",
    "roles": [{"id": "c", "name": "丙", "invite": "CCC"}],
    "stages": [
        {
            "id": "s1", "title": "第二上游阶段", "public": "上游二公开", "duration": None,
            "default_next": None,
            "options": {"c": [{"id": "go", "text": "推进"}]},
        },
    ],
}


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
async def fetch_events(db, sid):
    rows = await db.all(
        "SELECT version, seq, type, actor, at, payload FROM events "
        "WHERE session_id=? ORDER BY version, seq",
        (sid,),
    )
    return load_events(rows)


def log_error(events):
    """Validate the unified allocation invariants of an event log."""
    versions = sorted({e["version"] for e in events})
    if versions != list(range(1, len(versions) + 1)):
        return f"版本号不是从 1 连续递增: {versions}"
    keys = [(e["version"], e["seq"]) for e in events]
    if len(set(keys)) != len(keys):
        return "存在重复 (version, seq)"
    for v in versions:
        seqs = sorted(e["seq"] for e in events if e["version"] == v)
        if seqs != list(range(len(seqs))):
            return f"v{v} 内 seq 不是从 0 连续分配: {seqs}"
    return None


def check_log(name, events):
    err = log_error(events)
    check(name, err is None, err or "")


def ev_at(events, version, seq):
    for e in events:
        if e["version"] == version and e["seq"] == seq:
            return e
    return None


# --------------------------------------------------------------------------- #
# [1] YAML validation
# --------------------------------------------------------------------------- #
def test_definitions():
    print("[1] YAML 校验")
    d = load_file(Path(__file__).resolve().parents[2] / "drills" / "chem-plant.yaml")
    check("示例剧本可解析", d.id == "chem-plant")
    check("3 个角色", len(d.roles) == 3)
    check("8 个阶段", len(d.stages) == 8)

    base = yaml.safe_load(yaml.safe_dump(DUEL))
    parse_definition(base)  # valid baseline must not raise
    bad_invite = {**base, "roles": base["roles"] + [{"id": "z", "name": "丙", "invite": "AAA"}]}
    try:
        parse_definition(bad_invite)
        check("重复邀请码被拒", False)
    except DrillError:
        check("重复邀请码被拒", True)
    bad_next = yaml.safe_load(yaml.safe_dump(DUEL))
    bad_next["stages"][0]["default_next"] = "nope"
    try:
        parse_definition(bad_next)
        check("悬空 next 被拒", False)
    except DrillError:
        check("悬空 next 被拒", True)


# --------------------------------------------------------------------------- #
# [2] join -> start allocation (regression: start used to hardcode version=1)
# --------------------------------------------------------------------------- #
async def test_join_then_start(eng):
    print("[2] 先加入再启动的版本分配")
    sid = await eng.create_session("duel")
    ja = await eng.join(sid, "AAA", "张三")
    jb = await eng.join(sid, "BBB", "李四")
    check("两次加入各占一个版本", (ja["version"], jb["version"]) == (1, 2))

    r = await eng.start(sid)
    check("启动占用下一版本 v3", r["version"] == 3)
    ev = await fetch_events(eng.db, sid)
    check_log("事件日志分配规则一致", ev)
    check("v3.0 为 session_started", (ev_at(ev, 3, 0) or {}).get("type") == "session_started")
    check("v3.1 为 stage_opened", (ev_at(ev, 3, 1) or {}).get("type") == "stage_opened")

    _, _, st = await eng.load(sid)
    check("状态 running 且进入首阶段", st.status == "running" and st.current == "s1")
    check("名册含两名参与者", sorted(st.roster) == ["a", "b"])

    # start may still run with zero joins and must then be v1
    sid2 = await eng.create_session("duel")
    r2 = await eng.start(sid2)
    check("无人加入时启动为 v1", r2["version"] == 1)
    check_log("无人加入日志一致", await fetch_events(eng.db, sid2))

    # idempotency / rejection paths must not add events
    n0 = len(await fetch_events(eng.db, sid))
    try:
        await eng.start(sid)
        check("重复 start 被拒", False)
    except EngineError as exc:
        check("重复 start 被拒", exc.status == 409)
    try:
        await eng.join(sid, "AAA", "王五")
        check("重复邀请码加入被拒", False)
    except EngineError as exc:
        check("重复邀请码加入被拒", exc.status == 409)
    try:
        await eng.join(sid, "NOPE", "王五")
        check("无效邀请码被拒", False)
    except EngineError as exc:
        check("无效邀请码被拒", exc.status == 403)
    check("被拒请求不产生事件", len(await fetch_events(eng.db, sid)) == n0)

    jc = await eng.join(sid, "CCC", "王五")  # late join while running
    check("进行中加入占下一版本", jc["version"] == 4)
    check_log("加入后日志仍一致", await fetch_events(eng.db, sid))


# --------------------------------------------------------------------------- #
# [3] auto-settle on last submission (regression: choice and seal shared
#     the same (version, seq=0))
# --------------------------------------------------------------------------- #
async def test_auto_settle(eng):
    print("[3] 最后一名决策者提交触发自动结算")
    sid = await eng.create_session("duel")
    await eng.join(sid, "AAA", "张三")
    await eng.join(sid, "BBB", "李四")
    await eng.start(sid)  # v3

    r1 = await eng.submit(sid, "a", "s1", "x")
    check("首名提交被接受且未结算", r1["accepted"] and not r1["settled"])
    r2 = await eng.submit(sid, "b", "s1", "p")
    check("末名提交被接受并结算", r2["accepted"] and r2["settled"])
    check("选择与结算同属一个版本", r1["version"] + 1 == r2["version"] == r2["seal_version"])
    check("结算原因为 complete", r2["seal_reason"] == "complete")
    check("自动进入下一阶段 s2", r2["next_stage"] == "s2" and not r2["ended"])

    ev = await fetch_events(eng.db, sid)
    v = r2["version"]
    check("vN.0 为 stage_choice", (ev_at(ev, v, 0) or {}).get("type") == "stage_choice")
    check("vN.1 为 stage_sealed", (ev_at(ev, v, 1) or {}).get("type") == "stage_sealed")
    check("vN.2 为 stage_opened", (ev_at(ev, v, 2) or {}).get("type") == "stage_opened")
    check_log("日志分配规则一致", ev)

    subs = await eng.db.all(
        "SELECT role_id, choice_id FROM submissions WHERE session_id=? ORDER BY role_id",
        (sid,),
    )
    check("两名决策者的提交均已落库", [dict(s)["role_id"] for s in subs] == ["a", "b"])

    # final stage: single decider submits -> seal + session end in one version
    r3 = await eng.submit(sid, "a", "s2", "k")
    check("末阶段提交后演练结束", r3["settled"] and r3["ended"])
    ev = await fetch_events(eng.db, sid)
    v3 = r3["version"]
    check("终版 v.0 为 stage_choice", (ev_at(ev, v3, 0) or {}).get("type") == "stage_choice")
    check("终版 v.1 为 stage_sealed", (ev_at(ev, v3, 1) or {}).get("type") == "stage_sealed")
    check("终版 v.2 为 session_ended", (ev_at(ev, v3, 2) or {}).get("type") == "session_ended")
    check_log("终版日志一致", ev)

    # idempotent replays after settlement: no new events, original outcome
    n0 = len(ev)
    d1 = await eng.submit(sid, "a", "s1", "x")
    check("阶段结算后重发幂等", d1["duplicate"] and d1["same"] and d1["settled"])
    d2 = await eng.submit(sid, "b", "s1", "q")  # different choice, same slot
    check("重发不同选项返回首次结果", d2["duplicate"] and not d2["same"] and d2["choice_id"] == "p")
    check("幂等重放不产生事件", len(await fetch_events(eng.db, sid)) == n0)


# --------------------------------------------------------------------------- #
# [4] deadline races
# --------------------------------------------------------------------------- #
async def test_deadline_race(eng):
    print("[4] 截止竞争")
    sid = await eng.create_session("fast")
    start1 = await eng.start(sid)
    await asyncio.sleep(1.2)  # let the 1s deadline pass

    r = await eng.submit(sid, "a", "s1", "x")
    check("截止后提交被拒", not r["accepted"] and not r["duplicate"])
    check("拒绝原因 deadline", r.get("reason") == "deadline")
    check("拒绝与结算同版本完成", r["settled"] and r["next_stage"] == "s2")
    ev = await fetch_events(eng.db, sid)
    check("拒绝路径不含 choice 事件", all(e["type"] != "stage_choice" for e in ev))
    check("v2.0 为 stage_sealed", (ev_at(ev, 2, 0) or {}).get("type") == "stage_sealed")
    check("v2.1 为 stage_opened", (ev_at(ev, 2, 1) or {}).get("type") == "stage_opened")
    check_log("拒绝路径日志一致", ev)

    n0 = len(ev)
    # s2 has duration: null; the captured s1 deadline is stale here as well.
    noop = await eng.settle(sid, "deadline", expected_deadline=start1["deadline"])
    check("无时限阶段的 deadline 结算为空转", noop.get("noop") is True)
    try:
        await eng.submit(sid, "a", "s1", "x")
        check("已结算阶段的迟到提交被拒", False)
    except EngineError as exc:
        check("已结算阶段的迟到提交被拒", exc.status == 409)
    check("空转与迟到提交不产生事件", len(await fetch_events(eng.db, sid)) == n0)

    # concurrent storm right after the deadline: exactly one seal may survive
    sid2 = await eng.create_session("fast")
    start2 = await eng.start(sid2)
    await asyncio.sleep(1.2)
    results = await asyncio.gather(
        *[eng.submit(sid2, "a", "s1", "x") for _ in range(5)],
        *[eng.settle(sid2, "deadline", expected_deadline=start2["deadline"])
          for _ in range(3)],
        return_exceptions=True,
    )
    ev2 = await fetch_events(eng.db, sid2)
    seals = [e for e in ev2 if e["type"] == "stage_sealed"]
    check("并发下只有一次结算事件", len(seals) == 1)
    check("结算原因为 deadline", seals[0]["payload"]["reason"] == "deadline")
    submits_ok = all(
        isinstance(r, Exception)
        or (not r.get("accepted") and r.get("reason") == "deadline")
        for r in results[:5]
    )
    check("并发提交全部被拒/报错", submits_ok)
    settles = results[5:]
    check("并发结算至多一个生效", sum(1 for r in settles if not r.get("noop")) <= 1)
    _, _, st2 = await eng.load(sid2)
    check("竞争后进入 s2 且未结束", st2.current == "s2" and st2.status == "running")
    check_log("竞争后日志一致", ev2)


# --------------------------------------------------------------------------- #
# [5] stale timer across two consecutive timed stages (regression: a delayed
#     stage-1 deadline callback used to seal the already-open timed stage 2)
# --------------------------------------------------------------------------- #
async def test_stale_deadline_timer(eng):
    print("[5] 连续限时阶段的旧定时器身份核对")
    import app.engine as emod

    real_now = emod.now_ts

    async def open_timed2():
        """Start a timed2 session; returns (sid, s1_deadline) at now=t0."""
        sid = await eng.create_session("timed2")
        r = await eng.start(sid)
        _, _, st = await eng.load(sid)
        check("进入限时阶段 s1", st.current == "s1" and st.deadline is not None)
        return sid, r["deadline"]

    def sealed_stages(sid, events):
        return [e["payload"]["stage_id"] for e in events if e["type"] == "stage_sealed"]

    try:
        # --- A: the last participant's submission advances s1 -> timed s2 ---
        clock = {"t": real_now()}
        emod.now_ts = lambda: clock["t"]
        sid, d1 = await open_timed2()

        # A timer still armed for s1 that wakes up *early* must never settle.
        clock["t"] = d1 - 1
        early = await eng.settle(sid, "deadline", expected_deadline=d1)
        check("旧定时器提前唤醒为空转", early.get("noop") and early.get("early"))
        check("提前唤醒不产生事件", sealed_stages(sid, await fetch_events(eng.db, sid)) == [])

        # Last submission lands while s1 is still open: complete -> s2, armed.
        clock["t"] = d1 - 0.5
        sub = await eng.submit(sid, "a", "s1", "x")
        d2 = sub["next_deadline"]
        check("末名提交推进到同样限时的 s2",
              sub["accepted"] and sub["sealed_stage"] == "s1"
              and sub["next_stage"] == "s2" and d2 is not None and d2 != d1)
        _, _, st = await eng.load(sid)
        check("当前截止已换成 s2 的 d2", st.current == "s2" and st.deadline == d2)
        n_ev = len(await fetch_events(eng.db, sid))

        # The stale s1 callback is delayed by the session lock / event loop
        # until stage 2's deadline has also passed. Old code mistook this for
        # s2's timeout and wrote stage_sealed(s2, deadline).
        clock["t"] = d2 + 5
        stale = await eng.settle(sid, "deadline", expected_deadline=d1)
        check("身份不符的旧回调空转", stale.get("noop") is True and stale.get("stale") is True)
        ev = await fetch_events(eng.db, sid)
        check("旧回调不新增任何事件", len(ev) == n_ev)
        check("s2 未被旧回调误结算", sealed_stages(sid, ev) == ["s1"])
        _, _, st = await eng.load(sid)
        check("s2 仍然开放且未结束", st.current == "s2" and st.status == "running")
        check_log("旧回调后日志仍一致", ev)

        # A callback that lost its deadline (e.g. duration:null stage) is also
        # stale rather than a seal.
        lost = await eng.settle(sid, "deadline", expected_deadline=None)
        check("携带空截止的回调按身份不符空转", lost.get("noop") and lost.get("stale"))
        check("空截止回调不新增事件", len(await fetch_events(eng.db, sid)) == n_ev)

        # The genuine s2 timer settles s2 exactly once.
        legit = await eng.settle(sid, "deadline", expected_deadline=d2)
        check("正常回调结算 s2", legit.get("sealed_stage") == "s2"
              and legit.get("seal_reason") == "deadline" and legit.get("ended"))
        again = await eng.settle(sid, "deadline", expected_deadline=d2)
        check("正常回调只能结算一次（重复者空转）", again.get("noop") is True)
        ev = await fetch_events(eng.db, sid)
        check("最终每个阶段仅一条 seal", sealed_stages(sid, ev) == ["s1", "s2"])
        check("s2 seal 原因为 deadline",
              next(e for e in ev if e["type"] == "stage_sealed"
                   and e["payload"]["stage_id"] == "s2")["payload"]["reason"] == "deadline")
        check_log("正常回调后日志一致", ev)

        # --- B: a host settle advances s1 -> timed s2 before the timer runs --
        clock["t"] = real_now()
        sid2, d1b = await open_timed2()
        clock["t"] = d1b + 1  # host manually settles once s1 is overdue
        host = await eng.settle(sid2, "host")
        d2b = host["next_deadline"]
        check("主持人手动结算推进到限时 s2",
              host.get("sealed_stage") == "s1" and host.get("next_stage") == "s2"
              and d2b is not None and d2b != d1b)
        n_ev2 = len(await fetch_events(eng.db, sid2))

        clock["t"] = d2b + 5
        stale2 = await eng.settle(sid2, "deadline", expected_deadline=d1b)
        check("手动结算后的旧 s1 回调空转",
              stale2.get("noop") is True and stale2.get("stale") is True)
        check("手动竞争旧回调不新增事件",
              len(await fetch_events(eng.db, sid2)) == n_ev2)
        check("s2 未被旧回调误结算",
              sealed_stages(sid2, await fetch_events(eng.db, sid2)) == ["s1"])

        legit2 = await eng.settle(sid2, "deadline", expected_deadline=d2b)
        check("s2 的正常回调仍可结算",
              legit2.get("sealed_stage") == "s2" and legit2.get("seal_reason") == "deadline")
        ev2 = await fetch_events(eng.db, sid2)
        check("手动竞争下每阶段仅一条 seal", sealed_stages(sid2, ev2) == ["s1", "s2"])
        check_log("手动竞争后日志一致", ev2)

        # --- C: several identical s1 callbacks firing concurrently ----------
        # (event-loop wakes the timer more than once): lock serializes them,
        # exact-deadline identity guarantees at most one seal.
        clock["t"] = real_now()
        sid3, d1c = await open_timed2()
        clock["t"] = d1c + 1
        results = await asyncio.gather(*[
            eng.settle(sid3, "deadline", expected_deadline=d1c) for _ in range(4)
        ])
        effective = [r for r in results if not r.get("noop")]
        ev3 = await fetch_events(eng.db, sid3)
        check("重复正常回调仅一次结算", len(effective) == 1)
        check("重复回调只 seal s1 一次", sealed_stages(sid3, ev3) == ["s1"])
        _, _, st3 = await eng.load(sid3)
        check("竞争后停留在 s2", st3.current == "s2" and st3.status == "running")
        check_log("重复回调并发后日志一致", ev3)
    finally:
        emod.now_ts = real_now


# --------------------------------------------------------------------------- #
# [6] TimerService bookkeeping: a firing stale task must not unregister the
#     successor timer (defence in depth; settle's identity check is authoritative)
# --------------------------------------------------------------------------- #
async def test_timer_bookkeeping():
    print("[6] 定时器登记薄不被旧任务覆盖")
    from app.main import TimerService

    timers = TimerService()
    fired = []
    import app.main as mainmod
    orig = mainmod.on_deadline

    async def record(session_id, expected_deadline):
        fired.append((session_id, expected_deadline))

    mainmod.on_deadline = record
    try:
        d1 = time.time() + 0.2
        timers.schedule("s", d1)
        await asyncio.sleep(0.25)  # s1 timer fires and unregisters itself
        check("触发后任务自行注销", "s" not in timers._tasks)
        check("s1 回调携带捕获的截止值", fired == [("s", d1)])

        # Simulate the regression window directly: a stage-2 successor has
        # already been armed when the stale stage-1 task finally reaches the
        # unregister step of _fire. The stale task must not pop the successor.
        async def stale_fire(session_id, deadline):
            # same unregister-then-callback sequence as TimerService._fire
            if timers._tasks.get(session_id) is asyncio.current_task():
                timers._tasks.pop(session_id, None)
            await mainmod.on_deadline(session_id, deadline)

        d2 = time.time() + 1
        timers.schedule("s", d2)
        successor = timers._tasks["s"]
        stale_task = asyncio.create_task(stale_fire("s", d1))
        await asyncio.sleep(0)  # let the stale task run to completion
        check("旧任务的注销步骤不会移除后继定时器",
              timers._tasks.get("s") is successor and not stale_task.cancelled())
        check("旧任务仍以旧截止值触发（由事务身份核对空转）", fired[-1] == ("s", d1))
        timers.cancel("s")
    finally:
        mainmod.on_deadline = orig
        timers.cancel("s")


# --------------------------------------------------------------------------- #
# [7] concurrent idempotency
# --------------------------------------------------------------------------- #
async def test_concurrent_idempotency(eng):
    print("[7] 并发幂等")
    sid = await eng.create_session("duel")
    await eng.start(sid)

    res = await asyncio.gather(*[eng.submit(sid, "a", "s1", "x") for _ in range(5)])
    accepted = [r for r in res if r["accepted"]]
    dups = [r for r in res if r.get("duplicate")]
    check("5 并发同提交仅 1 个被接受", len(accepted) == 1 and len(dups) == 4)
    ev = await fetch_events(eng.db, sid)
    check("仅产生一条 choice 事件", sum(1 for e in ev if e["type"] == "stage_choice") == 1)
    rows = await eng.db.all("SELECT * FROM submissions WHERE session_id=?", (sid,))
    check("submissions 仅一行", len(rows) == 1)

    res2 = await asyncio.gather(*[eng.submit(sid, "b", "s1", "p") for _ in range(3)])
    ok2 = [r for r in res2 if r["accepted"]]
    check("末名并发提交仅 1 个生效", len(ok2) == 1)
    check("生效者完成自动结算", ok2[0]["settled"] is True)
    ev = await fetch_events(eng.db, sid)
    check("仅产生一条 seal 事件", sum(1 for e in ev if e["type"] == "stage_sealed") == 1)
    check_log("并发提交后日志一致", ev)

    # concurrent manual settles: serialized by the session lock, each
    # effective settle seals exactly one stage, no stage is sealed twice
    sid2 = await eng.create_session("duel")
    await eng.start(sid2)
    settles = await asyncio.gather(*[eng.settle(sid2, "host") for _ in range(3)])
    effective = [r for r in settles if not r.get("noop")]
    ev2 = await fetch_events(eng.db, sid2)
    seals2 = [e for e in ev2 if e["type"] == "stage_sealed"]
    check("并发手动结算逐阶段推进", len(effective) == 2 and len(seals2) == 2)
    check("同一阶段不会被结算两次", sorted(e["payload"]["stage_id"] for e in seals2) == ["s1", "s2"])
    _, _, st2 = await eng.load(sid2)
    check("两阶段后演练结束", st2.status == "ended")
    check_log("并发结算后日志一致", ev2)

    # deadline timer vs host settle racing the same open stage: exactly one
    # of them seals s1; the loser either noops or settles the next stage
    sid3 = await eng.create_session("fast")
    start3 = await eng.start(sid3)
    await asyncio.sleep(1.2)
    await asyncio.gather(
        eng.settle(sid3, "deadline", expected_deadline=start3["deadline"]),
        eng.settle(sid3, "host"),
    )
    ev3 = await fetch_events(eng.db, sid3)
    seals3 = [e for e in ev3 if e["type"] == "stage_sealed"]
    s1_seals = [e for e in seals3 if e["payload"]["stage_id"] == "s1"]
    check("截止/手动竞争下 s1 只结算一次", len(s1_seals) == 1)
    check("s1 结算原因合法", s1_seals[0]["payload"]["reason"] in ("deadline", "host"))
    check("总结算数不超过阶段数", len(seals3) <= 2)
    check_log("截止/手动竞争日志一致", ev3)


# --------------------------------------------------------------------------- #
# [8] deterministic replay of every version prefix
# --------------------------------------------------------------------------- #
async def test_replay(eng):
    print("[8] 任意版本前缀的确定性回放")
    sid = await eng.create_session("duel")
    await eng.join(sid, "AAA", "张三")
    await eng.join(sid, "BBB", "李四")
    await eng.start(sid)
    await eng.submit(sid, "a", "s1", "x")
    await eng.submit(sid, "b", "s1", "p")   # auto-settle -> s2
    await eng.submit(sid, "a", "s2", "k")   # auto-settle -> end

    sess = await eng.get_session(sid)
    d = await eng.get_definition("duel")
    events = await fetch_events(eng.db, sid)
    final = max(e["version"] for e in events)
    check_log("完整日志一致", events)

    ok = True
    for v in range(0, final + 1):
        prefix = [e for e in events if e["version"] <= v]
        s1, s2 = reduce_events(prefix), reduce_events(prefix)
        if s1 != s2 or s1.last_version != v:
            ok = False
    check("每个前缀折叠确定且版本单调", ok)

    _, _, st_load = await eng.load(sid)
    check("load 与 reduce_events 结果一致", st_load == reduce_events(events))

    full = build_replay(sess, d, events, ("host", ""))
    frames = full["frames"]
    check("帧数 == 最终版本+1", len(frames) == final + 1)
    check("帧版本连续", [f["version"] for f in frames] == list(range(final + 1)))
    check("帧内快照版本单调", all(
        f["snapshot"]["session"]["version"] == f["version"] for f in frames
    ))

    ok = True
    for k in sorted({0, 1, final // 2, final - 1, final}):
        pref = build_replay(sess, d, [e for e in events if e["version"] <= k], ("host", ""))
        if pref["frames"] != frames[: k + 1] or pref["final_version"] != k:
            ok = False
    check("任意前缀回放与全量回放逐帧一致", ok)

    pa = build_replay(sess, d, events, ("role", "a"))
    dump_a = json.dumps(pa, ensure_ascii=False)
    check("参与者回放不含他人私有信息", "乙的隐秘s1" not in dump_a)
    check("参与者回放含本人私有信息", "甲的隐秘s1" in dump_a)
    check("参与者回放不暴露原始事件", pa["events"] is None)


# --------------------------------------------------------------------------- #
# [9] end_session allocation (regression: created-branch hardcoded version=1)
# --------------------------------------------------------------------------- #
async def test_end_session(eng):
    print("[9] 手动终止的版本分配")
    sid = await eng.create_session("duel")
    await eng.join(sid, "AAA", "张三")
    await eng.join(sid, "BBB", "李四")
    await eng.end_session(sid)
    ev = await fetch_events(eng.db, sid)
    check("未开始即终止：ended 落在 v3.0", (ev_at(ev, 3, 0) or {}).get("type") == "session_ended")
    check_log("未开始终止日志一致", ev)
    _, _, st = await eng.load(sid)
    check("状态已结束", st.status == "ended")

    n0 = len(ev)
    try:
        await eng.join(sid, "CCC", "王五")
        check("结束后加入被拒", False)
    except EngineError as exc:
        check("结束后加入被拒", exc.status == 409)
    await eng.end_session(sid)  # idempotent
    check("重复终止不产生事件", len(await fetch_events(eng.db, sid)) == n0)

    sid2 = await eng.create_session("duel")
    await eng.start(sid2)
    await eng.end_session(sid2)
    ev2 = await fetch_events(eng.db, sid2)
    check("进行中终止：v2.0 为 stage_sealed", (ev_at(ev2, 2, 0) or {}).get("type") == "stage_sealed")
    check("进行中终止：v2.1 为 stage_opened", (ev_at(ev2, 2, 1) or {}).get("type") == "stage_opened")
    check("进行中终止：v3.0 为 session_ended", (ev_at(ev2, 3, 0) or {}).get("type") == "session_ended")
    check("终止原因为 host", ev_at(ev2, 2, 0)["payload"]["reason"] == "host")
    check_log("进行中终止日志一致", ev2)


# --------------------------------------------------------------------------- #
# [10] end-to-end over HTTP + WebSocket through the real app
# --------------------------------------------------------------------------- #
def recv_until(ws, pred, what, limit=50):
    for _ in range(limit):
        msg = ws.receive_json()
        if pred(msg):
            return msg
    raise RuntimeError(f"未收到预期消息: {what}")


def is_snapshot(msg, **session):
    return msg.get("type") == "snapshot" and all(
        msg["snapshot"]["session"].get(k) == v for k, v in session.items()
    )


def test_http_ws_e2e():
    print("[10] HTTP + WebSocket 端到端")
    with TestClient(app) as client:
        check("健康检查", client.get("/api/health").json() == {"ok": True})
        check("首页可访问", client.get("/").status_code == 200)
        check("无密钥访问被拒", client.get("/api/drills").status_code == 401)

        r = client.post("/api/drills", json={
            "yaml": yaml.safe_dump(WS_DRILL, allow_unicode=True, sort_keys=False),
        }, headers=HOST)
        check("上传剧本生成草稿 v1", r.status_code == 200
              and r.json()["id"] == "wsdrill" and r.json()["version"] == 1
              and r.json()["status"] == "draft" and len(r.json()["sha256"]) == 64)

        # Drafts cannot back sessions until published.
        blocked = client.post("/api/sessions", params={"drill_id": "wsdrill"}, headers=HOST)
        check("草稿不能创建会话", blocked.status_code == 409)

        r = client.post("/api/drills/wsdrill/versions/1/publish",
                        json={"base_version": None}, headers=HOST)
        check("发布草稿 v1", r.status_code == 200 and r.json()["status"] == "published")

        # Version list + diff endpoints
        vl = client.get("/api/drills/wsdrill/versions", headers=HOST).json()
        check("版本列表含 v1 已发布", len(vl) == 1 and vl[0]["version"] == 1
              and vl[0]["status"] == "published" and vl[0]["yaml"])

        r = client.post("/api/sessions", params={"drill_id": "wsdrill"}, headers=HOST)
        check("创建会话", r.status_code == 201)
        sid = r.json()["session_id"]
        check("会话响应带剧本版本与摘要", r.json()["drill_version"] == 1
              and len(r.json()["drill_sha256"]) == 64
              and r.json()["digest_short"] == r.json()["drill_sha256"][:12])

        ja = client.post(f"/api/sessions/{sid}/join", json={"invite": "AAA", "name": "张三"})
        jb = client.post(f"/api/sessions/{sid}/join", json={"invite": "BBB", "name": "李四"})
        check("两名参与者加入", ja.status_code == 200 and jb.status_code == 200)
        ta, tb = ja.json()["token"], jb.json()["token"]
        check("加入版本递增", (ja.json()["version"], jb.json()["version"]) == (1, 2))
        check("重复加入 409", client.post(
            f"/api/sessions/{sid}/join", json={"invite": "AAA", "name": "x"}).status_code == 409)

        rp = client.get(f"/api/sessions/{sid}/replay", headers=HOST).json()
        check("启动前回放 final_version=2", rp["final_version"] == 2 and len(rp["frames"]) == 3)

        with ExitStack() as stack:
            bad = stack.enter_context(client.websocket_connect(f"/ws/{sid}?token=nope"))
            check("WS 认证失败", bad.receive_json()["type"] == "error")

            host_ws = stack.enter_context(client.websocket_connect(f"/ws/{sid}?host=test-key"))
            a_ws = stack.enter_context(client.websocket_connect(f"/ws/{sid}?token={ta}"))
            b_ws = stack.enter_context(client.websocket_connect(f"/ws/{sid}?token={tb}"))
            recv_until(host_ws, lambda m: m.get("type") == "snapshot", "host snapshot")
            snap_a = recv_until(a_ws, lambda m: m.get("type") == "snapshot", "a snapshot")
            recv_until(b_ws, lambda m: m.get("type") == "snapshot", "b snapshot")
            check("参与者快照按角色裁剪", snap_a["snapshot"]["me"]["role_id"] == "a")
            check("WS 快照带绑定版本与摘要",
                  snap_a["snapshot"]["drill"]["version"] == 1
                  and len(snap_a["snapshot"]["drill"]["sha256"]) == 64)

            host_ws.send_json({"type": "start"})
            recv_until(host_ws, lambda m: is_snapshot(m, status="running"), "host running")
            run_a = recv_until(a_ws, lambda m: is_snapshot(m, status="running"), "a running")
            recv_until(b_ws, lambda m: is_snapshot(m, status="running"), "b running")
            dump_a = json.dumps(run_a, ensure_ascii=False)
            check("运行快照不含他人私有视图", "乙私有一" not in dump_a)
            check("运行快照含本人私有视图", "甲私有一" in dump_a)

            a_ws.send_json({"type": "submit", "choice_id": "x", "client_stage": "s1"})
            ra = recv_until(a_ws, lambda m: m.get("type") == "submit_result", "a result")
            check("甲提交被接受", ra["accepted"] and not ra["settled"])

            b_ws.send_json({"type": "submit", "choice_id": "p", "client_stage": "s1"})
            rb = recv_until(b_ws, lambda m: m.get("type") == "submit_result", "b result")
            check("乙提交触发自动结算", rb["accepted"] and rb["settled"] and rb["next_stage"] == "s2")

            n_before = len(client.get(f"/api/sessions/{sid}/replay", headers=HOST).json()["events"])
            a_ws.send_json({"type": "submit", "choice_id": "x", "client_stage": "s1"})
            rd = recv_until(a_ws, lambda m: m.get("type") == "submit_result", "dup result")
            check("WS 重复提交幂等", rd["duplicate"] and rd["same"])
            n_after = len(client.get(f"/api/sessions/{sid}/replay", headers=HOST).json()["events"])
            check("重复提交不新增事件", n_before == n_after)

            host_ws.send_json({"type": "settle"})
            end_snap = recv_until(host_ws, lambda m: is_snapshot(m, status="ended"), "ended")
            check("主持人手动结算后结束", end_snap["snapshot"]["session"]["status"] == "ended")
            recv_until(a_ws, lambda m: is_snapshot(m, status="ended"), "a ended")

        rp = client.get(f"/api/sessions/{sid}/replay", headers=HOST).json()
        check("回放带剧本版本标识", rp["drill_version"] == 1 and len(rp["drill_sha256"]) == 64)
        check("端到端日志分配规则一致", log_error(rp["events"]) is None,
              log_error(rp["events"]) or "")
        check("端到端最终版本 v6", rp["final_version"] == 6)
        rp2 = client.get(f"/api/sessions/{sid}/replay", params={"version": 2}, headers=HOST).json()
        check("版本前缀回放 final_version=2", rp2["final_version"] == 2 and len(rp2["frames"]) == 3)
        check("前缀帧与全量一致", rp2["frames"] == rp["frames"][:3])

        rec_h = client.get(f"/api/sessions/{sid}/record", params={"fmt": "json"}, headers=HOST).json()
        check("主持人全量记录", rec_h["kind"] == "host" and len(rec_h["participants"]) == 2)
        check("主持人记录带剧本版本", rec_h["drill_version"] == 1
              and rec_h["drill"]["version"] == 1 and rec_h["drill_sha256"] == rp["drill_sha256"])
        rec_a = client.get(f"/api/sessions/{sid}/record", params={"fmt": "json", "token": ta}).json()
        check("个人记录带剧本版本", rec_a["drill_version"] == 1
              and rec_a["drill"]["digest_short"] == rp["drill_sha256"][:12])
        dump_rec_a = json.dumps(rec_a, ensure_ascii=False)
        check("个人记录不含他人私有信息", "乙私有一" not in dump_rec_a and "乙私有二" not in dump_rec_a)
        check("个人记录含本人选择", rec_a["stages"][0]["my_choice"] == "x")
        md = client.get(f"/api/sessions/{sid}/record", params={"fmt": "md", "token": tb})
        check("个人 Markdown 记录可导出", md.status_code == 200 and "个人记录" in md.text)
        check("Markdown 含剧本版本行", "剧本版本：v1" in md.text)
        check("无令牌导出被拒", client.get(f"/api/sessions/{sid}/record").status_code == 401)


# --------------------------------------------------------------------------- #
# [14] joint orchestration over HTTP + WebSocket through the real app
# --------------------------------------------------------------------------- #
def test_joint_http_ws():
    print("[14] 联合演练 HTTP + WebSocket 端到端")
    with TestClient(app) as client:
        # release both joint drills
        for dct in (JUP, JDOWN, JUP2):
            r = client.post("/api/drills", headers=HOST,
                            json={"yaml": _dump(dct), "base_version": None})
            assert r.status_code == 200, r.text
            client.post(f"/api/drills/{dct['id']}/versions/1/publish",
                        headers=HOST, json={"base_version": None})
        su = client.post("/api/sessions", params={"drill_id": "jup"}, headers=HOST).json()["session_id"]
        su2 = client.post("/api/sessions", params={"drill_id": "jup2"}, headers=HOST).json()["session_id"]
        sd = client.post("/api/sessions", params={"drill_id": "jdown"}, headers=HOST).json()["session_id"]

        # pinned script reference for the editor
        scr = client.get(f"/api/sessions/{sd}/script", headers=HOST).json()
        check("剧本引用按会话绑定版本返回",
              scr["drill_id"] == "jdown" and {r["id"] for r in scr["roles"]} == {"b", "x"}
              and any(s["id"] == "t1" for s in scr["stages"]))
        check("联演接口需主持人密钥",
              client.get("/api/joints").status_code == 401
              and client.get(f"/api/sessions/{sd}/joints").status_code == 401)

        spec = {
            "name": "HTTP 联演", "description": "端到端",
            "members": [su, su2, sd],
            "gates": [{
                "id": "g1", "target_session": sd, "target_stage": "t1",
                "title": "联动关阀", "public": "友邻单位请求联动",
                "views": {"b": "乙才知道的联动细节"},
                "prerequisites": [
                    {"session_id": su, "stage_id": "s1", "kind": "choice",
                     "role_id": "a", "value": "go"},
                    {"session_id": su2, "stage_id": "s1", "kind": "choice",
                     "role_id": "c", "value": "go"},
                ],
            }],
        }
        r = client.post("/api/joints", headers=HOST, json=spec)
        check("创建联演 201", r.status_code == 201 and r.json()["gates"][0]["status"] == "waiting")
        jid = r.json()["id"]
        check("联演列表含计数", any(j["id"] == jid for j in client.get("/api/joints", headers=HOST).json()))
        check("会话维度可查联演", len(client.get(f"/api/sessions/{sd}/joints", headers=HOST).json()) == 1)

        # invalid spec -> 400, nothing stored
        bad = dict(spec, name="坏联演", gates=[dict(spec["gates"][0], target_stage="zzz")])
        r = client.post("/api/joints", headers=HOST, json=bad)
        check("保存前校验（无效阶段）400", r.status_code == 400 and "不存在阶段" in r.json()["detail"])
        cyc = {
            "name": "环", "members": [su, sd],
            "gates": [
                {"id": "a", "target_session": sd, "target_stage": "t1",
                 "prerequisites": [{"session_id": su, "stage_id": "s1",
                                    "kind": "next_stage", "value": "s2"}]},
                {"id": "b", "target_session": su, "target_stage": "s1",
                 "prerequisites": [{"session_id": sd, "stage_id": "t1",
                                    "kind": "next_stage", "value": "t2"}]},
            ],
        }
        check("保存前校验（有环）400",
              client.post("/api/joints", headers=HOST, json=cyc).status_code == 400)

        # join + start all sessions
        ta = client.post(f"/api/sessions/{su}/join", json={"invite": "AAA", "name": "甲"}).json()["token"]
        tc = client.post(f"/api/sessions/{su2}/join", json={"invite": "CCC", "name": "丙"}).json()["token"]
        tb = client.post(f"/api/sessions/{sd}/join", json={"invite": "BBB", "name": "乙"}).json()["token"]
        tx = client.post(f"/api/sessions/{sd}/join", json={"invite": "XXX", "name": "旁观"}).json()["token"]

        with ExitStack() as stack:
            # Prime the TestClient websocket portal with a short-lived failed
            # handshake first (same harness quirk the [10] test relies on).
            prime = stack.enter_context(client.websocket_connect(f"/ws/{sd}?token=nope"))
            recv_until(prime, lambda m: m.get("type") == "error", "prime error")
            wa = stack.enter_context(client.websocket_connect(f"/ws/{su}?token={ta}"))
            wc = stack.enter_context(client.websocket_connect(f"/ws/{su2}?token={tc}"))
            wb = stack.enter_context(client.websocket_connect(f"/ws/{sd}?token={tb}"))
            wx = stack.enter_context(client.websocket_connect(f"/ws/{sd}?token={tx}"))
            host_up = stack.enter_context(client.websocket_connect(f"/ws/{su}?host=test-key"))
            host_dn = stack.enter_context(client.websocket_connect(f"/ws/{sd}?host=test-key"))
            for ws in (wa, wc, wb, wx, host_up, host_dn):
                recv_until(ws, lambda m: m.get("type") == "snapshot", "snap")
            host_up.send_json({"type": "start"})
            host_dn.send_json({"type": "start"})
            recv_until(wa, lambda m: is_snapshot(m, status="running"), "up running")
            recv_until(wb, lambda m: is_snapshot(m, status="running"), "down running")

            # first branch only -> still blocked
            wa.send_json({"type": "submit", "choice_id": "go", "client_stage": "s1"})
            recv_until(wa, lambda m: m.get("type") == "submit_result", "a result")
            g = client.get(f"/api/joints/{jid}", headers=HOST).json()["gates"][0]
            check("HTTP 监控显示单分支已满足、整体等待",
                  [p["satisfied"] for p in g["prerequisites"]] == [True, False]
                  and g["status"] == "waiting" and "等待 1 个分支" in g["block_reason"])

            # start second upstream + satisfy the second branch
            host2 = stack.enter_context(client.websocket_connect(f"/ws/{su2}?host=test-key"))
            recv_until(host2, lambda m: m.get("type") == "snapshot", "snap")
            host2.send_json({"type": "start"})
            recv_until(wc, lambda m: is_snapshot(m, status="running"), "up2 running")
            wc.send_json({"type": "submit", "choice_id": "go", "client_stage": "s1"})
            recv_until(wc, lambda m: m.get("type") == "submit_result", "c result")

            # the downstream participants must receive the injection via push
            def has_injection(msg):
                if msg.get("type") != "snapshot":
                    return False
                hist = msg["snapshot"].get("history") or []
                return any(h.get("injections") for h in hist)

            snap_b = recv_until(wb, has_injection, "b injection push")
            snap_x = recv_until(wx, has_injection, "x injection push")
            inj_b = snap_b["snapshot"]["history"][0]["injections"][0]
            inj_x = snap_x["snapshot"]["history"][0]["injections"][0]
            check("WS 推送注入到目标角色（公开+私有）",
                  inj_b["public"] == "友邻单位请求联动" and inj_b["private"] == "乙才知道的联动细节")
            check("WS 推送给无关角色仅公开、无私有",
                  inj_x["public"] == "友邻单位请求联动" and inj_x["private"] == "")
            check("无关角色推送不含他人私有文本",
                  "乙才知道的联动细节" not in json.dumps(snap_x, ensure_ascii=False))

            # host console of the target session shows the joint panel data
            g = client.get(f"/api/sessions/{sd}/joints", headers=HOST).json()[0]["gates"][0]
            check("主持台联演状态为已投递", g["status"] == "released" and g["event_version"])

        # target session records carry the injection with permission trimming
        rec_b = client.get(f"/api/sessions/{sd}/record", params={"fmt": "json", "token": tb}).json()
        rec_x = client.get(f"/api/sessions/{sd}/record", params={"fmt": "json", "token": tx}).json()
        check("个人记录含公开注入", any(
            s["joint_injections"] and s["joint_injections"][0]["public"] == "友邻单位请求联动"
            for s in rec_b["stages"]))
        check("目标角色个人记录含私有注入", any(
            inj.get("my_view") == "乙才知道的联动细节"
            for s in rec_b["stages"] for inj in s.get("joint_injections", [])))
        check("无关角色个人记录无私有注入",
              all(not inj.get("my_view")
                  for s in rec_x["stages"] for inj in s.get("joint_injections", [])))
        md = client.get(f"/api/sessions/{sd}/record", params={"fmt": "md", "token": tb}).text
        check("个人 Markdown 含联演注入段", "联演注入" in md)
        host_md = client.get(f"/api/sessions/{sd}/record", params={"fmt": "md"}, headers=HOST).text
        check("主持人 Markdown 含注入与角色视图",
              "联演注入" in host_md and "乙才知道的联动细节" in host_md)

        # upstream sessions' own records contain no injections
        for sid in (su, su2):
            rec = client.get(f"/api/sessions/{sid}/record", params={"fmt": "json"}, headers=HOST).json()
            check(f"上游 {sid} 记录保持独立、无注入",
                  all(not s.get("joint_injections") for s in rec["stages"]))

        # replace rejects changes to the now-released gate (409), list shows counts
        r = client.put(f"/api/joints/{jid}", headers=HOST, json=spec)
        check("原样 PUT 已投递门禁成功（幂等定义）", r.status_code == 200)
        changed = dict(spec)
        changed["gates"] = [dict(spec["gates"][0], title="改过标题")]
        r = client.put(f"/api/joints/{jid}", headers=HOST, json=changed)
        check("PUT 修改已投递门禁 409", r.status_code == 409)
        r = client.delete(f"/api/joints/{jid}", headers=HOST)
        check("删除含已投递门禁联演 409", r.status_code == 409)

        # ---- causal-timeline postmortem over the delivered joint ----------
        pm = client.post(f"/api/joints/{jid}/postmortem", headers=HOST,
                         json={"mode": "as_of", "as_of": time.time() + 3600})
        check("复盘接口 200 且为只读报告",
              pm.status_code == 200 and pm.json()["kind"] == "joint_postmortem"
              and pm.json()["joint_id"] == jid)
        pmj = pm.json()
        g = pmj["gates"][0]
        check("复盘状态：已投递（含注入引用）",
              g["status"] == "delivered" and g["injection_ref"]["session_id"] == sd
              and g["injection_ref"]["type"] == "joint_injection")
        check("复盘前置带命中事件引用",
              all(p["satisfied"] and p["event_ref"]["type"] == "stage_sealed"
                  for p in g["prerequisites"]))
        check("复盘时间轴稳定合并且含注入标注",
              pmj["summary"]["timeline_events"] == len(pmj["timeline"])
              and any(
                  m.get("kind") == "injection" and m.get("gate_id") == "g1"
                  for e in pmj["timeline"] for m in e["marks"]))
        check("复盘带关键路径与瓶颈数组",
              "gate_ids" in pmj["critical_path"] and isinstance(pmj["bottleneck_gates"], list))
        # auth + validation
        check("复盘接口需主持人密钥",
              client.post(f"/api/joints/{jid}/postmortem",
                          json={"mode": "as_of", "as_of": 1}).status_code == 401)
        check("复盘非法 mode 400",
              client.post(f"/api/joints/{jid}/postmortem", headers=HOST,
                          json={"mode": "nope"}).status_code == 400)
        check("复盘 as_of 非数字被拒（422/400）",
              client.post(f"/api/joints/{jid}/postmortem", headers=HOST,
                          json={"mode": "as_of", "as_of": "soon"}).status_code in (400, 422))
        check("prefix 模式缺成员 400",
              client.post(f"/api/joints/{jid}/postmortem", headers=HOST,
                          json={"mode": "prefix", "prefixes": {su: 9}}).status_code == 400)
        pfx = {m["session_id"]: m["latest_version"] for m in pmj["members"]}
        rp = client.post(f"/api/joints/{jid}/postmortem", headers=HOST,
                         json={"mode": "prefix", "prefixes": pfx})
        check("prefix 模式 200 且一致",
              rp.status_code == 200 and rp.json()["mode"] == "prefix"
              and all(m["truncated"] is False for m in rp.json()["cutoff"]["members"].values()))
        check("不存在联演复盘 404",
              client.post("/api/joints/zzzzzzzz/postmortem", headers=HOST,
                          json={"mode": "as_of", "as_of": 1}).status_code == 404)


def test_http_version_api():
    print("[11b] HTTP 版本管理 API")
    http_drill = yaml.safe_load(yaml.safe_dump(WS_DRILL))
    http_drill["id"] = "httpdrill"
    with TestClient(app) as client:
        # unauthenticated
        check("版本接口需主持人密钥",
              client.get("/api/drills/httpdrill/versions").status_code == 401)

        # v1 draft
        r = client.post("/api/drills", headers=HOST,
                        json={"yaml": _dump(http_drill), "base_version": None})
        check("HTTP 保存 v1 草稿", r.status_code == 200 and r.json()["version"] == 1)
        check("未发布剧本不出现在可创建会话列表",
              any(d["id"] == "httpdrill" and d["published_version"] is None
                  for d in client.get("/api/drills", headers=HOST).json()))

        # publish
        r = client.post("/api/drills/httpdrill/versions/1/publish",
                        headers=HOST, json={"base_version": None})
        check("HTTP 发布 v1", r.status_code == 200 and r.json()["status"] == "published")

        # invalid YAML -> 400
        r = client.post("/api/drills", headers=HOST,
                        json={"yaml": "id: x\nstages: not-a-list"})
        check("非法 YAML 被拒 400", r.status_code == 400)

        # v2 draft based on v1
        v2 = yaml.safe_load(yaml.safe_dump(http_drill))
        v2["stages"][0]["title"] = "一（改）"
        r = client.post("/api/drills", headers=HOST,
                        json={"yaml": _dump(v2), "base_version": 1})
        check("HTTP 保存 v2 草稿", r.json()["version"] == 2 and r.json()["status"] == "draft")

        # diff endpoint
        d = client.get("/api/drills/httpdrill/diff?from=1&to=2", headers=HOST)
        check("HTTP 差异 200 且含阶段改动",
              d.status_code == 200 and d.json()["stages"]["changed"]
              and d.json()["stages"]["changed"][0]["id"] == "s1")
        check("HTTP 差异 404", client.get(
            "/api/drills/httpdrill/diff?from=1&to=99", headers=HOST).status_code == 404)

        # stale base -> 409, current release stays v1
        r = client.post("/api/drills/httpdrill/versions/2/publish",
                        headers=HOST, json={"base_version": 99})
        check("HTTP 基准冲突发布 409", r.status_code == 409)
        current = next(x for x in client.get("/api/drills", headers=HOST).json()
                       if x["id"] == "httpdrill")
        check("冲突后当前发布版不变", current["published_version"] == 1
              and current["draft_version"] == 2)

        # copy v1 as draft while v2 draft exists -> v3 identical to v1
        r = client.post("/api/drills/httpdrill/copy-draft", headers=HOST,
                        json={"source_version": 1, "base_version": 1})
        check("HTTP 复制历史版本为草稿", r.status_code == 200 and r.json()["version"] == 3
              and r.json()["sha256"] == client.get(
                  "/api/drills/httpdrill/versions/1", headers=HOST).json()["sha256"])
        # v2 got superseded
        vlist = {v["version"]: v["status"]
                 for v in client.get("/api/drills/httpdrill/versions", headers=HOST).json()}
        check("复制后 v2 草稿被顶为 superseded", vlist.get(2) == "superseded"
              and vlist.get(3) == "draft")

        # publish the copied draft (base v1) -> v3 released
        r = client.post("/api/drills/httpdrill/versions/3/publish",
                        headers=HOST, json={"base_version": 1})
        check("HTTP 发布复制草稿 v3", r.status_code == 200)
        vlist = {v["version"]: v["status"]
                 for v in client.get("/api/drills/httpdrill/versions", headers=HOST).json()}
        check("v3 发布、v1 历史", vlist.get(3) == "published" and vlist.get(1) == "history")

        # session pins v3; session list shows the pin
        sid = client.post("/api/sessions", params={"drill_id": "httpdrill"},
                          headers=HOST).json()["session_id"]
        srow = next(s for s in client.get("/api/sessions", headers=HOST).json()
                    if s["id"] == sid)
        check("会话列表标注绑定版本",
              srow["drill_version"] == 3 and len(srow["drill_sha256"]) == 64)


# --------------------------------------------------------------------------- #
# [11] script versions: immutable versions, digests, draft gating, session
#      pinning across later releases, structured diffs, copy-from-history and
#      optimistic publish concurrency
# --------------------------------------------------------------------------- #
VDRILL = {
    "id": "vdrill",
    "name": "版本演练",
    "roles": [
        {"id": "a", "name": "甲", "invite": "AAA"},
        {"id": "b", "name": "乙", "invite": "BBB"},
    ],
    "stages": [
        {
            "id": "s1", "title": "阶段一", "public": "公开一", "duration": 60,
            "default_next": "s2",
            "views": {"a": "甲的隐秘"},
            "options": {
                "a": [{"id": "x", "text": "甲选X"}, {"id": "y", "text": "甲选Y"}],
                "b": [{"id": "p", "text": "乙选P"}],
            },
            "transitions": [{"when": {"a": "y"}, "next": "s3"}],
        },
        {
            "id": "s2", "title": "阶段二", "public": "公开二", "duration": None,
            "default_next": None,
            "options": {"a": [{"id": "k", "text": "甲终选"}]},
        },
        {
            "id": "s3", "title": "阶段三", "public": "公开三", "duration": None,
            "default_next": None,
            "options": {"a": [{"id": "z", "text": "Z"}]},
        },
    ],
}


def _dump(dct):
    return yaml.safe_dump(dct, allow_unicode=True, sort_keys=False)


async def test_script_versions(eng):
    print("[11] 剧本版本与发布管理")

    d1 = parse_definition(yaml.safe_load(yaml.safe_dump(VDRILL)))
    digest1 = content_digest(d1)

    # --- brand-new upload: v1 is an immutable draft, not a release ---------
    r = await eng.save_draft(d1, _dump(VDRILL), None)
    check("首次上传生成 v1 草稿", r["version"] == 1 and r["status"] == "draft")
    check("返回不可变内容摘要（sha256）", r["sha256"] == digest1 and len(r["sha256"]) == 64)
    try:
        await eng.create_session("vdrill")
        check("草稿不能创建会话", False)
    except EngineError as exc:
        check("草稿不能创建会话", exc.status == 409)

    # Re-saving identical content is idempotent (same digest -> reused).
    r_same = await eng.save_draft(d1, _dump(VDRILL), None)
    check("相同内容重复保存复用版本", r_same["version"] == 1 and r_same.get("reused") is True)
    versions = await eng.list_versions("vdrill")
    check("版本列表仍只有一条", len(versions) == 1)

    # --- publishing makes it usable ----------------------------------------
    pub = await eng.publish_drill("vdrill", 1, None)
    check("发布 v1", pub["status"] == "published" and pub["published_at"] is not None)
    sid_old = await eng.create_session("vdrill")
    sess_old = await eng.get_session(sid_old)
    check("新会话绑定 v1 与摘要", sess_old["drill_version"] == 1
          and sess_old["drill_sha256"] == digest1)

    # Editing again must not be based on a stale (None) release; this mirrors
    # the "someone else released meanwhile" guard on save as well.
    d2_dict = yaml.safe_load(yaml.safe_dump(VDRILL))
    d2_dict["stages"][0]["title"] = "阶段一（修订）"
    d2_dict["stages"][0]["views"]["a"] = "甲的隐秘-改"
    d2_dict["stages"][0]["options"]["a"][0]["text"] = "甲选X-改"
    d2_dict["stages"][0]["transitions"][0]["next"] = "s2"
    d2_dict["stages"][1]["title"] = "阶段二（修订）"
    d2 = parse_definition(d2_dict)
    digest2 = content_digest(d2)
    check("修订内容摘要不同", digest2 != digest1)

    r2 = await eng.save_draft(d2, _dump(d2_dict), 1)
    check("编辑生成不可变 v2 草稿", r2["version"] == 2 and r2["status"] == "draft"
          and r2["base_version"] == 1 and r2["sha256"] == digest2)
    versions = await eng.list_versions("vdrill")
    check("版本列表两条（v2 草稿在前）", [v["version"] for v in versions] == [2, 1])
    check("v1 仍为已发布（草稿不影响发布版）",
          next(v for v in versions if v["version"] == 1)["status"] == "published")

    # Current release definition is still v1 while v2 is a draft.
    cur = await eng.get_definition("vdrill")
    check("发布前 get_definition 仍是 v1 内容", cur.stages["s1"].title == "阶段一")
    # New sessions while a draft is open still bind the released v1.
    sid_new = await eng.create_session("vdrill")
    check("草稿期间新会话仍绑定 v1",
          (await eng.get_session(sid_new))["drill_version"] == 1)

    # A third save supersedes the open v2 draft (row kept, marked superseded).
    d3_dict = yaml.safe_load(yaml.safe_dump(d2_dict))
    d3_dict["stages"][1]["public"] = "公开二-再改"
    d3 = parse_definition(d3_dict)
    r3 = await eng.save_draft(d3, _dump(d3_dict), 1)
    check("再次编辑生成 v3 并顶掉 v2 草稿", r3["version"] == 3)
    versions = await eng.list_versions("vdrill")
    st2 = next(v for v in versions if v["version"] == 2)
    check("旧草稿 v2 标记为 superseded 且保留", st2["status"] == "superseded")

    # --- structured diff v1 -> v3 ------------------------------------------
    diff = await eng.diff_versions("vdrill", 1, 3)
    check("差异存在", diff["has_changes"] is True)
    s1_change = next((c for c in diff["stages"]["changed"] if c["id"] == "s1"), None)
    check("差异含 s1", s1_change is not None)
    check("s1 标题字段差异",
          any(f["field"] == "title" and f["old"] == "阶段一" for f in s1_change["fields"]))
    check("s1 角色视图差异",
          any(v["role_id"] == "a" and v["old"] == "甲的隐秘"
              for v in s1_change["views"]["changed"]))
    check("s1 选项文本差异",
          any(c["role_id"] == "a"
              and any(oc["id"] == "x" and any(f["field"] == "text" for f in oc["fields"])
                      for oc in c["changed"])
              for c in s1_change["options"]["changed"]))
    check("s1 转移规则差异",
          len(s1_change["transitions"]["changed"]) == 1
          and s1_change["transitions"]["changed"][0]["new"]["next"] == "s2")
    s2_change = next((c for c in diff["stages"]["changed"] if c["id"] == "s2"), None)
    check("差异含 s2（公开信息/标题）", s2_change is not None)

    # Diff against a nonexistent version is a 404.
    try:
        await eng.diff_versions("vdrill", 1, 99)
        check("不存在版本的差异报 404", False)
    except EngineError as exc:
        check("不存在版本的差异报 404", exc.status == 404)

    # --- optimistic concurrency: publishing v3 based on a stale release ----
    # v3 is an open draft based on v1. Another host (a separate process /
    # transaction is a legal writer with BEGIN IMMEDIATE) releases v2 first.
    # Emulate that external commit directly, then publishing v3 with its stale
    # base must fail and leave the current release untouched.
    await eng.db.db.execute(
        "UPDATE drill_versions SET status='history' "
        "WHERE drill_id='vdrill' AND version=1 AND status='published'"
    )
    await eng.db.db.execute(
        "UPDATE drill_versions SET status='published', published_at=? "
        "WHERE drill_id=? AND version=2",
        (str(now_ts()), "vdrill"),
    )
    await eng.db.db.execute(
        "UPDATE drills SET published_version=2, draft_version=3 WHERE id='vdrill'"
    )
    await eng.db.commit()
    drill = await eng._drill_row("vdrill")
    check("他人已先发布 v2", drill["published_version"] == 2)
    try:
        await eng.publish_drill("vdrill", 3, 1)
        check("基准版本被更新后发布被拒", False)
    except EngineError as exc:
        check("基准版本被更新后发布被拒（409）", exc.status == 409)
    drill = await eng._drill_row("vdrill")
    check("发布失败不改变当前发布版（仍 v2）", drill["published_version"] == 2)
    v3row = await eng.get_version("vdrill", 3)
    check("失败后 v3 仍为草稿", v3row["status"] == "draft")
    # A save carrying the stale base is rejected as well.
    try:
        await eng.save_draft(d3, _dump(d3_dict), 1)
        check("基于旧基准保存被拒", False)
    except EngineError as exc:
        check("基于旧基准保存被拒（409）", exc.status == 409)

    # Recover from the current release: copy v2 into a fresh draft, edit it,
    # then publish against the current base v2.
    cp = await eng.copy_as_draft("vdrill", 2, 2)
    check("从当前发布版复制为新草稿", cp["status"] == "draft" and cp["version"] >= 4)
    d4_dict = yaml.safe_load(yaml.safe_dump(d2_dict))
    d4_dict["stages"][0]["public"] = "公开一-新编辑"
    d4 = parse_definition(d4_dict)
    r4 = await eng.save_draft(d4, _dump(d4_dict), 2)
    check("刷新后基于 v2 编辑生成新草稿", r4["base_version"] == 2)
    pub4 = await eng.publish_drill("vdrill", r4["version"], 2)
    check("基于最新版编辑后发布成功", pub4["status"] == "published")
    versions = await eng.list_versions("vdrill")
    check("旧发布版 v2 变 history",
          next(v for v in versions if v["version"] == 2)["status"] == "history")
    check("只有一个 published",
          [v["version"] for v in versions if v["status"] == "published"] == [r4["version"]])

    # Copy from an arbitrary *historical* version (v1) into a new draft.
    cp1 = await eng.copy_as_draft("vdrill", 1, r4["version"])
    check("从历史 v1 复制为新草稿", cp1["status"] == "draft" and cp1["sha256"] == digest1)
    v1copy = await eng.get_version_def("vdrill", cp1["version"])
    check("复制草稿内容与 v1 完全一致", content_digest(v1copy) == digest1)

    # --- the decisive property: the session created on v1 stays on v1 ------
    # Drive the old session through start + a branch, then prove its snapshot,
    # branching and replay still use v1 even though the release has moved on.
    await eng.join(sid_old, "AAA", "张三")
    await eng.start(sid_old)
    # In v1, a=y branches to s3. Submit a=x, b=p -> transition {a:y} fails,
    # a=x has no option next -> default_next s2. (Same shape in later content,
    # but the stage titles/views the snapshot renders must remain v1's.)
    await eng.submit(sid_old, "a", "s1", "x")
    res = await eng.submit(sid_old, "b", "s1", "p")
    check("v1 会话按 v1 规则分支到 s2（默认分支）", res["next_stage"] == "s2")
    sess_old2, d_old, state_old = await eng.load(sid_old)
    check("已建会话加载的定义仍是 v1",
          sess_old2["drill_version"] == 1 and d_old.stages["s1"].title == "阶段一"
          and d_old.stages["s1"].views["a"] == "甲的隐秘"
          and content_digest(d_old) == digest1)

    # The v1 transition {a:y}->s3 still exists for this session: sid_old
    # itself is the proof that a created/started session keeps v1 forever.
    # Confirm sid_new (created mid-draft on released v1) also stays v1 after
    # the later releases.
    sess_new2, d_new, _ = await eng.load(sid_new)
    check("进行前创建的会话同样锁定 v1",
          sess_new2["drill_version"] == 1 and content_digest(d_new) == digest1)

    # Replay of the old session contains v1 content only.
    rows = await eng.db.all(
        "SELECT version, seq, type, actor, at, payload FROM events "
        "WHERE session_id=? ORDER BY version, seq", (sid_old,))
    replay = build_replay(sess_old2, d_old, load_events(rows), ("host", ""))
    dump_replay = json.dumps(replay, ensure_ascii=False)
    check("回放不含后续版本文本", "阶段一（修订）" not in dump_replay
          and "公开一-新编辑" not in dump_replay)
    check("回放保留 v1 文本", "阶段一" in dump_replay and "甲的隐秘" in dump_replay)
    check("回放响应带 v1 版本标识", replay["drill_version"] == 1
          and replay["drill_sha256"] == digest1)

    # A brand-new session after release uses the newest release, not v1.
    sid_latest = await eng.create_session("vdrill")
    sess_latest = await eng.get_session(sid_latest)
    check("新会话使用当前发布版", sess_latest["drill_version"] == r4["version"])
    _, d_latest, _ = await eng.load(sid_latest)
    check("新会话内容为最新发布版", d_latest.stages["s1"].public == "公开一-新编辑")

    # Only a released version can be published again; history/draft rules.
    try:
        await eng.publish_drill("vdrill", 2, r4["version"])
        check("历史版本不能重复发布", False)
    except EngineError as exc:
        check("非草稿版本不能发布", exc.status == 409)


# --------------------------------------------------------------------------- #
# [12] migration of a pre-versioning database
# --------------------------------------------------------------------------- #
async def test_legacy_migration():
    print("[12] 旧版数据库迁移")
    import aiosqlite
    import dataclasses

    legacy_path = str(Path(_TMP) / "legacy.db")
    d = parse_definition(yaml.safe_load(yaml.safe_dump(DUEL)))
    conn = await aiosqlite.connect(legacy_path)
    await conn.executescript("""
    CREATE TABLE drills (
        id TEXT PRIMARY KEY, name TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '', definition TEXT NOT NULL);
    CREATE TABLE sessions (
        id TEXT PRIMARY KEY, drill_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'created',
        reason TEXT, created_at TEXT NOT NULL, started_at TEXT, ended_at TEXT);
    CREATE TABLE participants (
        session_id TEXT NOT NULL, role_id TEXT NOT NULL, display_name TEXT NOT NULL,
        token TEXT NOT NULL, joined_at TEXT NOT NULL,
        PRIMARY KEY (session_id, role_id), UNIQUE (session_id, token));
    CREATE TABLE events (
        session_id TEXT NOT NULL, version INTEGER NOT NULL, seq INTEGER NOT NULL,
        type TEXT NOT NULL, actor TEXT NOT NULL DEFAULT '', at TEXT NOT NULL,
        payload TEXT NOT NULL DEFAULT '{}',
        PRIMARY KEY (session_id, version, seq));
    CREATE TABLE submissions (
        session_id TEXT NOT NULL, version INTEGER NOT NULL, role_id TEXT NOT NULL,
        stage_id TEXT NOT NULL, choice_id TEXT NOT NULL, at TEXT NOT NULL,
        PRIMARY KEY (session_id, stage_id, role_id));
    """)
    await conn.execute(
        "INSERT INTO drills(id,name,description,definition) VALUES(?,?,?,?)",
        (d.id, d.name, d.description,
         json.dumps(dataclasses.asdict(d), ensure_ascii=False)),
    )
    await conn.execute(
        "INSERT INTO sessions(id,drill_id,status,created_at) VALUES(?,?, 'created', ?)",
        ("legacys1", d.id, "1.0"),
    )
    await conn.commit()
    await conn.close()

    mdb = Database(legacy_path)
    await mdb.connect()
    try:
        meng = Engine(mdb)
        drill = await meng._drill_row("duel")
        check("迁移后 drill 有发布版本", drill and drill["published_version"] == 1)
        versions = await meng.list_versions("duel")
        check("迁移生成不可变 v1 已发布",
              len(versions) == 1 and versions[0]["version"] == 1
              and versions[0]["status"] == "published"
              and versions[0]["sha256"] == content_digest(d))
        sess = await meng.get_session("legacys1")
        check("旧会话绑定 v1 与摘要",
              sess["drill_version"] == 1 and sess["drill_sha256"] == content_digest(d))
        _, d_loaded, st = await meng.load("legacys1")
        check("旧会话可按绑定版本重建快照",
              d_loaded.stages["s1"].title == "阶段一" and st.status == "created")
        # Idempotent: reconnecting must not create a v2.
        await mdb.close()
        mdb2 = Database(legacy_path)
        await mdb2.connect()
        meng2 = Engine(mdb2)
        versions2 = await meng2.list_versions("duel")
        check("迁移幂等：不重复生成版本", len(versions2) == 1)
        await mdb2.close()
    finally:
        if mdb.db is not None:
            await mdb.close()


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# [13] joint exercise orchestration
# --------------------------------------------------------------------------- #
async def test_joint_orchestration(eng):
    print("[13] 联合演练编排（依赖/AND/一次性/乱序/权限/独立版本）")
    jo = JointOrchestrator(eng.db, eng)

    # ------------------------------------------------------------------ #
    # validation: acyclic, valid sessions/stages/outcomes/roles
    # ------------------------------------------------------------------ #
    try:
        assert_acyclic([
            {"target_session": "sA", "target_stage": "1",
             "prerequisites": [{"session_id": "sB", "stage_id": "1", "kind": "next_stage", "value": ""}]},
            {"target_session": "sB", "target_stage": "1",
             "prerequisites": [{"session_id": "sA", "stage_id": "1", "kind": "next_stage", "value": ""}]},
        ])
        check("跨会话循环依赖被拒", False)
    except JointError:
        check("跨会话循环依赖被拒", True)
    try:
        assert_acyclic([
            {"target_session": "sA", "target_stage": "1",
             "prerequisites": [{"session_id": "sA", "stage_id": "1", "kind": "next_stage", "value": ""}]},
        ])
        check("自环依赖被拒", False)
    except JointError:
        check("自环依赖被拒", True)
    # a DAG passes
    assert_acyclic([
        {"target_session": "sA", "target_stage": "2",
         "prerequisites": [{"session_id": "sA", "stage_id": "1", "kind": "next_stage", "value": "2"}]},
    ])
    check("无环 DAG 通过校验", True)

    su = await eng.create_session("jup")
    sd = await eng.create_session("jdown")
    su2 = await eng.create_session("jup2")

    base_spec = {
        "name": "厂际联演", "members": [su, sd, su2],
        "gates": [{
            "id": "g1", "target_session": sd, "target_stage": "t1",
            "title": "联动请求", "public": "请配合关阀", "views": {"b": "乙的专属联演情报"},
            "prerequisites": [
                {"session_id": su, "stage_id": "s1", "kind": "choice",
                 "role_id": "a", "value": "go"},
                {"session_id": su2, "stage_id": "s1", "kind": "choice",
                 "role_id": "c", "value": "go"},
            ],
        }],
    }
    bad_cases = [
        ({"name": "x", "members": ["deadbeef"], "gates": []}, "成员会话不存在"),
        ({"name": "x", "members": [sd], "gates": [{
            "id": "g", "target_session": sd, "target_stage": "nope",
            "prerequisites": [{"session_id": sd, "stage_id": "t1",
                               "kind": "next_stage", "value": ""}]}]}, "目标阶段不存在"),
        ({"name": "x", "members": [sd], "gates": [{
            "id": "g", "target_session": sd, "target_stage": "t1",
            "prerequisites": [{"session_id": sd, "stage_id": "t1",
                               "kind": "choice", "role_id": "b", "value": "nope"}]}]},
         "结局选项不存在"),
        ({"name": "x", "members": [sd], "gates": [{
            "id": "g", "target_session": sd, "target_stage": "t1",
            "prerequisites": [{"session_id": sd, "stage_id": "t1",
                               "kind": "choice", "role_id": "ghost", "value": "k"}]}]},
         "角色不存在"),
        ({"name": "x", "members": [sd], "gates": [{
            "id": "g", "target_session": sd, "target_stage": "t1",
            "views": {"ghost": "x"},
            "prerequisites": [{"session_id": sd, "stage_id": "t1",
                               "kind": "next_stage", "value": ""}]}]}, "推送角色不存在"),
        ({"name": "x", "members": [sd], "gates": [{
            "id": "g", "target_session": sd, "target_stage": "t1",
            "prerequisites": [
                {"session_id": sd, "stage_id": "t1", "kind": "next_stage", "value": ""},
                {"session_id": sd, "stage_id": "t1", "kind": "next_stage", "value": ""},
            ]}]}, "重复前置分支"),
        ({"name": "x", "members": [sd], "gates": [{
            "id": "g", "target_session": sd, "target_stage": "t1",
            "prerequisites": []}]}, "空前置被拒"),
        ({"name": "x", "members": [sd, su], "gates": [{
            "id": "g", "target_session": su, "target_stage": "s1",
            "prerequisites": [{"session_id": sd, "stage_id": "t1",
                               "kind": "next_stage", "value": "t2"},
                              {"session_id": su, "stage_id": "s1",
                               "kind": "next_stage", "value": "s2"}]}]}, "有环被拒"),
    ]
    for raw, label in bad_cases:
        try:
            await jo.validate_spec(raw)
            check(f"校验拒绝：{label}", False)
        except JointError:
            check(f"校验拒绝：{label}", True)

    # ------------------------------------------------------------------ #
    # happy path: AND join, one-shot delivery, role-permission trimming
    # ------------------------------------------------------------------ #
    await eng.join(sd, "BBB", "下游乙")
    await eng.join(sd, "XXX", "旁观者")
    await eng.start(sd)  # target session already at t1
    jid = (await jo.create_joint(base_spec))["id"]
    detail = await jo.get_joint(jid)
    check("新联演门禁等待中", detail["gates"][0]["status"] == WAITING
          and detail["gates"][0]["ready"] is False)
    check("阻塞原因含等待分支", "等待 2 个分支" in detail["gates"][0]["block_reason"])

    await eng.join(su, "AAA", "上游甲")
    await eng.start(su)
    await eng.submit(su, "a", "s1", "go")  # default_next s2
    await jo.reconcile_session(su)
    detail = await jo.get_joint(jid)
    sat = [p["satisfied"] for p in detail["gates"][0]["prerequisites"]]
    check("首个分支满足、整体仍等待", sat == [True, False])
    _, d_down, st_down = await eng.load(sd)
    snap_b = build_snapshot(
        await eng.get_session(sd), d_down, st_down, ("role", "b"))
    check("未释放前参与者快照无注入",
          all(not h.get("injections") for h in snap_b["history"]))

    await eng.join(su2, "CCC", "上游丙")
    await eng.start(su2)
    await eng.submit(su2, "c", "s1", "go")
    delivered = await jo.reconcile_session(su2)
    check("AND 汇合后注入投递到目标会话", delivered == {sd})
    detail = await jo.get_joint(jid)
    check("门禁状态 released", detail["gates"][0]["status"] == RELEASED
          and detail["gates"][0]["event_version"] is not None)

    # Idempotency: repeated/out-of-order reconciliation never re-delivers.
    check("重复仲裁不重复投递", await jo.reconcile_session(su) == set())
    check("再次仲裁不重复投递", await jo.reconcile_session(su2) == set())
    dup = await eng.append_injection(
        sd, f"inj_{jid}_g1", jid, "g1", "t1", "联动请求", "请配合关阀",
        {"b": "乙的专属联演情报"})
    check("同一注入重复追加被幂等拒绝", dup == {"delivered": False, "reason": "duplicate"})
    rows = await eng.db.all(
        "SELECT COUNT(*) AS n FROM events WHERE session_id=? AND type='joint_injection'",
        (sd,))
    check("目标会话仅有一条注入事件", rows[0]["n"] == 1)
    check_log("目标会话日志分配规则一致", await fetch_events(eng.db, sd))

    # The injection is in the TARGET session's independent event stream; the
    # upstream sessions know nothing about it.
    for other in (su, su2):
        n = await eng.db.one(
            "SELECT COUNT(*) AS n FROM events WHERE session_id=? AND type='joint_injection'",
            (other,))
        check(f"上游会话 {other} 事件记录保持独立", n["n"] == 0)

    # Permission trimming.
    _, d_down, st_down = await eng.load(sd)
    sess_down = await eng.get_session(sd)
    sb = build_snapshot(sess_down, d_down, st_down, ("role", "b"))
    sx = build_snapshot(sess_down, d_down, st_down, ("role", "x"))
    sh = build_snapshot(sess_down, d_down, st_down, ("host", None))
    inj_b = sb["history"][0]["injections"]
    inj_x = sx["history"][0]["injections"]
    check("目标角色看到公开+私有注入",
          inj_b and inj_b[0]["public"] == "请配合关阀" and inj_b[0]["private"] == "乙的专属联演情报")
    check("无关角色只见公开注入、无私有文本",
          inj_x and inj_x[0]["private"] == ""
          and "乙的专属联演情报" not in json.dumps(sx, ensure_ascii=False))
    check("主持人快照可见全部角色注入视图",
          sh["history"][0]["injections"][0]["views"] == {"b": "乙的专属联演情报"})

    # next_stage outcome + ready-before-open ordering on a second joint.
    sd2 = await eng.create_session("jdown")
    spec2 = {
        "name": "跳转结局联演", "members": [su, sd2],
        "gates": [{
            "id": "h1", "target_session": sd2, "target_stage": "t1",
            "title": "注入二", "public": "公开二",
            "prerequisites": [{"session_id": su, "stage_id": "s1",
                               "kind": "next_stage", "value": "s2"}],
        }],
    }
    jid2 = (await jo.create_joint(spec2))["id"]
    d2 = await jo.get_joint(jid2)
    # su already sealed s1 -> s2 earlier; creation sweep should observe it,
    # but sd2 is not started, so ready-but-waiting.
    check("前置先满足时门禁就绪等待目标阶段",
          d2["gates"][0]["ready"] is True and d2["gates"][0]["status"] == WAITING)
    check("阻塞原因提示等待阶段开放", "等待会话进入阶段" in d2["gates"][0]["block_reason"])
    await eng.join(sd2, "BBB", "乙二")
    await eng.start(sd2)
    delivered = await jo.reconcile_session(sd2)
    check("目标阶段开放后释放", delivered == {sd2})

    # Outcome mismatch: stop ends the drill; a next_stage->s2 gate stays.
    su3 = await eng.create_session("jup")
    sd3 = await eng.create_session("jdown")
    await eng.join(su3, "AAA", "甲"); await eng.join(sd3, "BBB", "乙")
    await eng.start(su3); await eng.start(sd3)
    spec3 = {
        "name": "结局不符联演", "members": [su3, sd3],
        "gates": [{
            "id": "h2", "target_session": sd3, "target_stage": "t1",
            "public": "x",
            "prerequisites": [{"session_id": su3, "stage_id": "s1",
                               "kind": "next_stage", "value": "s2"}],
        }],
    }
    jid3 = (await jo.create_joint(spec3))["id"]
    await eng.submit(su3, "a", "s1", "stop")  # transition next=None -> ends
    await jo.reconcile_session(su3)
    check("结局不符不满足分支",
          all(not p["satisfied"] for p in (await jo.get_joint(jid3))["gates"][0]["prerequisites"]))
    # Target stage seals before the gate can release -> missed, no retro-deliver.
    await eng.settle(sd3, "host")
    await jo.reconcile_session(sd3)
    check("目标阶段先结算记为错过",
          (await jo.get_joint(jid3))["gates"][0]["status"] == MISSED)
    n = await eng.db.one(
        "SELECT COUNT(*) AS n FROM events WHERE session_id=? AND type='joint_injection'",
        (sd3,))
    check("错过的门禁不投递任何注入", n["n"] == 0)

    # Frozen gate cannot be modified; a waiting gate can.
    try:
        await jo.replace_joint(jid3, {
            "name": "结局不符联演改", "members": [su3, sd3], "gates": [{
                "id": "h2", "target_session": sd3, "target_stage": "t1",
                "public": "CHANGED",
                "prerequisites": [{"session_id": su3, "stage_id": "s1",
                                   "kind": "next_stage", "value": "s2"}],
            }]})
        check("已错过门禁修改被拒", False)
    except JointError as exc:
        check("已错过门禁修改被拒（409）", exc.status == 409)
    try:
        await jo.delete_joint(jid)
        check("含已投递门禁的联演删除被拒", False)
    except JointError as exc:
        check("含已投递门禁的联演删除被拒（409）", exc.status == 409)

    # Replay determinism of an injected session includes the injection frame.
    sess_d, def_d, _ = await eng.load(sd)
    rows = await eng.db.all(
        "SELECT version, seq, type, actor, at, payload FROM events "
        "WHERE session_id=? ORDER BY version, seq", (sd,))
    replay = build_replay(sess_d, def_d, load_events(rows), ("host", ""))
    dump = json.dumps(replay, ensure_ascii=False)
    check("回放含注入事件标签", "联演注入追加到阶段" in dump)
    check("回放帧包含注入快照", "请配合关阀" in dump)
    last = replay["frames"][-1]["snapshot"]
    check("末帧快照携带注入", last["history"][0]["injections"])


# --------------------------------------------------------------------------- #
# [15] joint causal-timeline postmortem (联演因果时序复盘)
# --------------------------------------------------------------------------- #
async def test_joint_postmortem(eng):
    print("[15] 联演因果时序复盘（稳定合并/等待秒数/关键路径/截止前缀一致性）")
    import app.engine as emod
    from app.postmortem import (
        AWAITING_TARGET,
        DELIVERED,
        REVIEW_MISSED,
        UNSATISFIED,
        filter_prefix_events,
        parse_review_request,
    )

    # Pure prefix rules: whole-version atomicity under as_of (strict <), and
    # exact version inclusion under prefix.
    evs = [
        {"version": 1, "seq": 0, "at": 10.0},
        {"version": 2, "seq": 0, "at": 20.0},
        {"version": 2, "seq": 1, "at": 20.0},
        {"version": 3, "seq": 0, "at": 30.0},
    ]
    check("as_of 严格小于：同秒整版排除",
          [e["version"] for e in filter_prefix_events(evs, "as_of", 20.0, None)] == [1])
    check("as_of 含早于截止的整版",
          [e["version"] for e in filter_prefix_events(evs, "as_of", 20.5, None)] == [1, 2, 2])
    check("prefix 按版本前缀",
          [e["version"] for e in filter_prefix_events(evs, "prefix", None, 2)] == [1, 2, 2])
    jo = JointOrchestrator(eng.db, eng)
    real_now = emod.now_ts
    clock = {"t": 5000.0}
    emod.now_ts = lambda: clock["t"]
    try:
        su = await eng.create_session("jup")
        su2 = await eng.create_session("jup2")
        sd = await eng.create_session("jdown")
        for sid, invite in ((su, "AAA"), (su2, "CCC"), (sd, "BBB")):
            await eng.join(sid, invite, "n")
            await eng.start(sid)

        # g1: AND(su/s1 a=go @ t1, su2/s1 c=go @ t1+20) -> sd/t1 (delivered)
        # g2: sd/t1 next=t2 -> su/s2 (delivered once downstream seals), which
        #     chains after g1: su/s1 -> sd/t1 -> su/s2.
        spec = {
            "name": "复盘链演", "members": [su, su2, sd],
            "gates": [
                {"id": "g1", "target_session": sd, "target_stage": "t1",
                 "title": "一级注入", "public": "P",
                 "prerequisites": [
                     {"session_id": su, "stage_id": "s1", "kind": "choice",
                      "role_id": "a", "value": "go"},
                     {"session_id": su2, "stage_id": "s1", "kind": "choice",
                      "role_id": "c", "value": "go"}]},
                {"id": "g2", "target_session": su, "target_stage": "s2",
                 "title": "二级注入", "public": "Q",
                 "prerequisites": [{"session_id": sd, "stage_id": "t1",
                                    "kind": "next_stage", "value": "t2"}]},
            ],
        }
        jid = (await jo.create_joint(spec))["id"]

        # g3: a gate whose outcome never matches and whose target seals early
        # -> missed with a traceable close ref.
        sd2 = await eng.create_session("jdown")
        await eng.join(sd2, "BBB", "n")
        await eng.start(sd2)
        spec_m = {
            "name": "错过复盘演", "members": [su, sd2],
            "gates": [{"id": "m1", "target_session": sd2, "target_stage": "t1",
                       "public": "x",
                       "prerequisites": [{"session_id": su, "stage_id": "s1",
                                          "kind": "next_stage", "value": ""}]}],
        }
        jidm = (await jo.create_joint(spec_m))["id"]

        rep = await jo.review_joint(jid, {"mode": "as_of", "as_of": 5001.0})
        g1 = rep["gates"][0]
        check("开局：编排尚未满足", g1["status"] == UNSATISFIED
              and len(g1["active_blockers"]) == 2 and g1["wait_seconds"] is None)

        # first branch at 5010
        clock["t"] = 5010.0
        await eng.submit(su, "a", "s1", "go")
        await jo.reconcile_session(su)
        rep = await jo.review_joint(jid, {"mode": "as_of", "as_of": 5015.0})
        g1 = rep["gates"][0]
        check("单分支命中后仍尚未满足、按截止时刻计等待",
              g1["status"] == UNSATISFIED and g1["age_seconds"] == 5.0)
        check("成员前缀按统一时刻裁剪（su 结算含入、su2 尚未动作）",
              next(m for m in rep["members"] if m["session_id"] == su)["include_version"] >= 3
              and next(m for m in rep["members"] if m["session_id"] == su2)["include_version"] == 2)

        # strict cutoff: same-second events must not enter the prefix
        rep_eq = await jo.review_joint(jid, {"mode": "as_of", "as_of": 5010.0})
        g1_eq = rep_eq["gates"][0]
        check("同秒事件不改写前缀（严格小于）",
              g1_eq["status"] == UNSATISFIED and all(
                  not p["satisfied"] for p in g1_eq["prerequisites"]))

        # second branch at 5030 -> AND join; target t1 open -> g1 delivered
        clock["t"] = 5030.0
        await eng.submit(su2, "c", "u1", "go") if False else await eng.submit(su2, "c", "s1", "go")
        delivered = await jo.reconcile_session(su2)
        check("AND 汇合后投递", delivered == {sd})
        rep = await jo.review_joint(jid, {"mode": "as_of", "as_of": 6000.0})
        g1, g2 = rep["gates"]
        check("g1 已投递、汇合后零等待", g1["status"] == DELIVERED
              and g1["wait_seconds"] == 0.0)
        check("AND 汇合耗时=慢分支滞后 20 秒", g1["assembly_seconds"] == 20.0
              and g1["bottleneck_branch"]["earlier_branch_lead_seconds"] == [20.0])
        check("可追溯注入事件引用", g1["injection_ref"]
              and g1["injection_ref"]["type"] == "joint_injection"
              and g1["injection_ref"]["ref"].startswith(sd + "#v"))
        check("g2 前置未满足（下游 t1 未结算）", g2["status"] == UNSATISFIED)

        # downstream seals t1 -> t2 at 5050: g2 releases into su/s2 (already
        # open since 5010); critical path g1(20s assembly) -> g2(0).
        clock["t"] = 5050.0
        await eng.submit(sd, "b", "t1", "k")
        await jo.reconcile_session(sd)
        rep = await jo.review_joint(jid, {"mode": "as_of", "as_of": 6000.0})
        g1, g2 = rep["gates"]
        check("g2 已投递", g2["status"] == DELIVERED and g2["injection_ref"]
              and g2["injection_ref"]["session_id"] == su)
        check("g2 关键路径沿分支链汇总为 g1→g2",
              g2["critical_path"]["gate_ids"] == ["g1", "g2"]
              and g2["critical_path"]["cumulative_wait_seconds"] == 20.0)
        check("整体关键路径与瓶颈门为 g1（20 秒汇合）",
              rep["critical_path"]["gate_ids"] == ["g1"]
              and rep["critical_path"]["total_wait_seconds"] == 20.0
              and rep["bottleneck_gates"][0]["gate_id"] == "g1")

        # timeline: stable merge order + every required mark present
        marks = [(m["kind"], m.get("gate_id")) for e in rep["timeline"] for m in e["marks"]]
        for want in ("prereq_outcome", "branch_hit", "target_open", "injection",
                     "target_close", "stage_settlement"):
            check(f"时间轴标注含 {want}", any(k == want for k, _ in marks))
        keys = [(e["at"], e["member_ord"], e["version"], e["seq"]) for e in rep["timeline"]]
        check("时间轴按时间/会话顺序号/版本/事件序号稳定合并", keys == sorted(keys))
        check("时间轴事件带会话与 v.seq 引用",
              all(e["event_ref"].count("#v") == 1 for e in rep["timeline"]))

        # determinism: re-running the same cutoff is byte-identical
        rep2 = await jo.review_joint(jid, {"mode": "as_of", "as_of": 6000.0})
        check("同一截止时刻重复生成报告完全一致",
              json.dumps(rep, sort_keys=True) == json.dumps(rep2, sort_keys=True))

        # late event safety: the earlier 5015 report stays identical after new
        # events land (it only depends on the immutable prefix).
        rep_old = await jo.review_joint(jid, {"mode": "as_of", "as_of": 5015.0})
        rep_old_b = json.loads(json.dumps(rep_old))
        await eng.settle(su, "host")
        check("迟到事件不改写既有截止前缀",
              json.dumps(rep_old, sort_keys=True) == json.dumps(rep_old_b, sort_keys=True))

        # prefix mode: freeze su2 one version before its satisfying seal AND
        # the target session before the injection. g1 must be unsatisfied even
        # at "now", with per-session truncation shown.
        full = await jo.review_joint(jid, {"mode": "as_of", "as_of": 9e9})
        prefs = {}
        for m in full["members"]:
            prefs[m["session_id"]] = m["latest_version"]
        su2_full = next(m for m in full["members"] if m["session_id"] == su2)
        prefs[su2] = su2_full["latest_version"] - 1
        # Truncate the target session before its injection version too, so the
        # prefix genuinely reflects "the slow branch had not hit yet".
        prefs[sd] = 2
        rp = await jo.review_joint(jid, {"mode": "prefix", "prefixes": prefs})
        check("按成员版本前缀：慢分支前缀外时 g1 尚未满足",
              rp["gates"][0]["status"] == UNSATISFIED
              and rp["cutoff"]["members"][su2]["truncated"] is True
              and rp["gates"][0]["injection_ref"] is None)
        # If only the upstream branch is truncated but the target keeps the
        # delivered injection, it stays delivered — with evidence_partial
        # flagging the truncated causal evidence.
        prefs2 = dict(prefs)
        sd_full = next(m for m in full["members"] if m["session_id"] == sd)
        prefs2[sd] = sd_full["latest_version"]
        rp3 = await jo.review_joint(jid, {"mode": "prefix", "prefixes": prefs2})
        check("目标前缀含注入而上游前缀外：已投递且标注证据不完整",
              rp3["gates"][0]["status"] == DELIVERED
              and rp3["gates"][0]["evidence_partial"] is True
              and rp3["summary"]["evidence_partial"] == 1)
        rp2 = await jo.review_joint(jid, {"mode": "prefix", "prefixes": dict(prefs)})
        check("同一组版本前缀报告一致",
              json.dumps(rp, sort_keys=True) == json.dumps(rp2, sort_keys=True))
        try:
            parse_review_request({"mode": "prefix", "prefixes": {su: 1}}, [su, sd])
            check("前缀缺少成员被拒", False)
        except Exception:
            check("前缀缺少成员被拒", True)

        # missed gate: outcome never matches (su/s1 sealed -> s2, gate wants
        # the drill end "") and target t1 seals at 5060 -> missed.
        clock["t"] = 5060.0
        await eng.settle(sd2, "host")
        await jo.reconcile_session(sd2)
        rmp = await jo.review_joint(jidm, {"mode": "as_of", "as_of": 6000.0})
        m1 = rmp["gates"][0]
        check("目标窗口先关：已错过且不补投", m1["status"] == REVIEW_MISSED
              and m1["injection_ref"] is None and m1["close_ref"] is not None)
        check("错过原因含可追溯结算引用与未满足前置",
              m1["close_ref"]["type"] == "stage_sealed" and "未满足" in m1["block_reason"])
        n = await eng.db.one(
            "SELECT COUNT(*) AS n FROM events WHERE session_id=? AND type='joint_injection'",
            (sd2,))
        check("复盘只读：不错生任何注入事件", n["n"] == 0)

        # awaiting-target gate (ready before the target stage ever opens)
        sd3 = await eng.create_session("jdown")
        jidw = (await jo.create_joint({
            "name": "等开放", "members": [su, sd3],
            "gates": [{"id": "w1", "target_session": sd3, "target_stage": "t1",
                       "public": "x",
                       "prerequisites": [{"session_id": su, "stage_id": "s1",
                                          "kind": "choice", "role_id": "a",
                                          "value": "go"}]}],
        }))["id"]
        rw = await jo.review_joint(jidw, {"mode": "as_of", "as_of": 6000.0})
        check("前置已齐但目标未开放", rw["gates"][0]["status"] == AWAITING_TARGET
              and "等待会话进入阶段" in rw["gates"][0]["block_reason"]
              and "尚未开放" in rw["gates"][0]["block_reason"])
    finally:
        emod.now_ts = real_now


async def test_cross_session_write_serialization(eng):
    print("[13b] 跨会话写事务在共享连接上串行化")
    # Two DIFFERENT sessions hold two different session locks; their write
    # coroutines interleave at awaits but must never corrupt the shared
    # SQLite connection ("cannot start a transaction within a transaction").
    su = await eng.create_session("jup")
    sd = await eng.create_session("jdown")

    async def drive(sid, invite):
        await eng.join(sid, invite, "n")
        r = await eng.start(sid)
        return r

    for _ in range(5):
        su = await eng.create_session("jup")
        sd = await eng.create_session("jdown")
        results = await asyncio.gather(
            drive(su, "AAA"), drive(sd, "BBB"), return_exceptions=True)
        check("并发跨会话 start 均成功",
              all(not isinstance(r, Exception) for r in results), str(results))
        check_log("会话一日志一致", await fetch_events(eng.db, su))
        check_log("会话二日志一致", await fetch_events(eng.db, sd))
        await eng.end_session(su)
        await eng.end_session(sd)


async def engine_tests():
    db = Database(str(Path(_TMP) / "t.db"))
    await db.connect()
    eng = Engine(db)
    for dct in (DUEL, FAST, TIMED2, JUP, JDOWN, JUP2):
        d = parse_definition(yaml.safe_load(yaml.safe_dump(dct)))
        # Unknown drills seed as an immediately released v1.
        await eng.seed_from_file(d, yaml.safe_dump(dct, allow_unicode=True, sort_keys=False))
    try:
        await test_join_then_start(eng)
        await test_auto_settle(eng)
        await test_deadline_race(eng)
        await test_stale_deadline_timer(eng)
        await test_timer_bookkeeping()
        await test_concurrent_idempotency(eng)
        await test_replay(eng)
        await test_end_session(eng)
        await test_script_versions(eng)
        await test_joint_orchestration(eng)
        await test_joint_postmortem(eng)
        await test_cross_session_write_serialization(eng)
    finally:
        await db.close()


def main():
    signal.alarm(360)  # watchdog: a hung websocket read must not block CI
    test_definitions()
    asyncio.run(engine_tests())
    asyncio.run(_migration_test())
    test_http_ws_e2e()
    test_http_version_api()
    test_joint_http_ws()
    print(f"\n{'全部通过' if FAIL == 0 else '存在失败'}: {PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


async def _migration_test():
    await test_legacy_migration()


if __name__ == "__main__":
    sys.exit(main())
