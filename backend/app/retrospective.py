"""Joint-exercise causal timeline retrospective ("联演因果时序复盘").

Given a saved joint and a unified review horizon, the presenter gets one
deterministic report answering: *which prerequisites fired when, how long each
injection waited behind its AND-join, which dependency chain was the critical
path, and what blocked every gate that never delivered?*

Two equivalent ways to fix the report horizon (at least one is required; they
can also be combined):

* ``cutoff`` — one unified POSIX timestamp applied to **every** member
  session. A committed version is included only when its *commit instant*
  (the maximum ``at`` of the events inside that atomic version) is **strictly
  less** than the cutoff. An event at exactly the cutoff second (or arriving
  later, e.g. while the report is open) therefore belongs to the next prefix
  and can never rewrite the already-observed one.
* ``version_prefix`` — per member session, an explicit ``session_id ->
  version`` cap: include that session's versions ``<= n``. This reproduces a
  concrete "each member session as of event version n" snapshot exactly.

Everything in the report is reconstructed by folding each member session's
**pinned** script over the included event prefix — the live joint runtime
tables are only used to read the frozen gate definitions, never to decide a
status — so the same horizon always yields byte-stable gate states, waiting
seconds and timeline ordering.

Stable merge order for the cross-session timeline is
``(at, session_ord, version, seq)``: wall-clock time first, then the joint
membership order for same-instant ties (injecting across sessions in the same
second cannot reorder between reports), then the session-local causal order.

Gate status vocabulary used here (deliberately aligned with the runtime):

* ``delivered``    — the one-shot ``joint_injection`` event is in the target
                     session's included prefix; wait measured from the last
                     prerequisite satisfied to the injection;
* ``awaiting_open``— every branch is met inside the prefix but the target
                     stage is not open at the horizon (not reached yet /
                     already sealed / target session ended);
* ``missed``       — the delivery window is irrecoverably closed inside the
                     prefix: the target stage sealed or the target session
                     ended while branches were still unmet;
* ``unmet``        — one or more prerequisite branches have no observed
                     outcome in the included prefix yet.
"""
from __future__ import annotations

import json
from typing import Any

from .db import Database
from .definitions import DrillDef
from .engine import (
    E_JOINT_INJECTION,
    E_PARTICIPANT_JOINED,
    E_SESSION_ENDED,
    E_SESSION_STARTED,
    E_STAGE_CHOICE,
    E_STAGE_OPENED,
    E_STAGE_SEALED,
    SealedStage,
    State,
    now_ts,
    reduce_events,
)
from .joints import JointError, branch_satisfied

DELIVERED = "delivered"
AWAITING_OPEN = "awaiting_open"
MISSED = "missed"
UNMET = "unmet"

STATUS_LABELS = {
    DELIVERED: "已投递",
    AWAITING_OPEN: "目标未开放",
    MISSED: "已错过",
    UNMET: "尚未满足",
}

MARK_PREREQ = "prerequisite_outcome"   # 前置结局
MARK_BRANCH = "branch_hit"             # 分支命中
MARK_OPEN = "target_stage_open"        # 目标阶段开放
MARK_INJECTION = "injection"           # 注入
MARK_SETTLE = "stage_settled"          # 阶段结算


class RetroError(JointError):
    """400-class error while building a retrospective."""


# --------------------------------------------------------------------------- #
# pure helpers
# --------------------------------------------------------------------------- #
def version_commit_at(events: list[dict[str, Any]]) -> dict[int, float]:
    """Commit instant of every version: the maximum event ``at`` inside that
    atomic commit (a commit's events are causally one instant — seal/open in
    one version carry the same server timestamp)."""
    out: dict[int, float] = {}
    for e in events:
        v = e["version"]
        if v not in out or e["at"] > out[v]:
            out[v] = e["at"]
    return out


def included_versions(
    events: list[dict[str, Any]],
    cutoff: float | None,
    prefix_cap: int | None,
) -> list[int]:
    """Versions admitted into the review prefix.

    * explicit ``prefix_cap``: version <= cap;
    * unified ``cutoff``: the version's commit instant must be strictly less
      than the cutoff, so same-second/late commits can never enter the prefix.
    Both constraints apply when both are given (intersection)."""
    commit_at = version_commit_at(events)
    out = []
    for v in sorted(commit_at):
        if prefix_cap is not None and v > prefix_cap:
            continue
        if cutoff is not None and commit_at[v] >= cutoff:
            continue
        out.append(v)
    return out


def find_event(
    events: list[dict[str, Any]], etype: str, predicate
) -> dict[str, Any] | None:
    for e in events:
        if e["type"] == etype and predicate(e["payload"]):
            return e
    return None


def event_ref(sid: str, ord_: int, e: dict[str, Any] | None) -> dict[str, Any] | None:
    """Stable traceable reference to one event in a member session's log."""
    if e is None:
        return None
    return {
        "session_id": sid,
        "session_ord": ord_,
        "version": e["version"],
        "seq": e["seq"],
        "at": e["at"],
        "type": e["type"],
    }


def _stage_title(d: DrillDef, stage_id: str) -> str:
    st = d.stages.get(stage_id)
    return st.title if st else stage_id


def _role_name(d: DrillDef, role_id: str) -> str:
    r = d.roles.get(role_id)
    return r.name if r else role_id


def branch_label(b: dict[str, Any], d: DrillDef) -> str:
    stage = d.stages.get(b["stage_id"])
    stage_title = stage.title if stage else b["stage_id"]
    if b["kind"] == "next_stage":
        if b.get("value"):
            value_label = _stage_title(d, b["value"])
        else:
            value_label = "（演练结束）"
        return f"阶段「{stage_title}」结算跳转＝{value_label}"
    value_label = b.get("value", "")
    if stage is not None:
        for o in stage.options.get(b.get("role_id", ""), []):
            if o.id == b["value"]:
                value_label = o.text
    return f"阶段「{stage_title}」{_role_name(d, b.get('role_id', ''))} 选择＝{value_label}"


def gate_title_of(target_title: str, raw_title: str) -> str:
    return raw_title or target_title


# --------------------------------------------------------------------------- #
# report builder
# --------------------------------------------------------------------------- #
class RetroBuilder:
    """Reads the immutable joint definition rows + member event logs and
    assembles a causal timeline report as of the requested horizon. Read-only:
    building a report never mutates the database or arms any delivery."""

    def __init__(self, db: Database, engine: Any):
        self.db = db
        self.engine = engine

    async def _load_rows(self, joint_id: str) -> dict[str, Any]:
        jrow = await self.db.one(
            "SELECT id, name, description, created_at, updated_at FROM joints WHERE id=?",
            (joint_id,),
        )
        if jrow is None:
            raise RetroError("联演不存在", 404)
        member_rows = await self.db.all(
            "SELECT session_id, ord FROM joint_members WHERE joint_id=? ORDER BY ord, rowid",
            (joint_id,),
        )
        gate_rows = await self.db.all(
            "SELECT gate_id, target_session, target_stage, title, public_text, views, status, "
            "delivered_at, event_version FROM joint_gates WHERE joint_id=? ORDER BY rowid",
            (joint_id,),
        )
        branches = {}
        for gr in gate_rows:
            brows = await self.db.all(
                "SELECT branch_no, session_id, stage_id, kind, role_id, value "
                "FROM joint_branches WHERE joint_id=? AND gate_id=? ORDER BY branch_no",
                (joint_id, gr["gate_id"]),
            )
            branches[gr["gate_id"]] = [dict(b) for b in brows]
        return {"joint": dict(jrow), "members": [dict(r) for r in member_rows],
                "gates": [dict(r) for r in gate_rows], "branches": branches}

    async def build(
        self,
        joint_id: str,
        cutoff: float | None = None,
        version_prefix: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        version_prefix = version_prefix or {}
        rows = await self._load_rows(joint_id)
        member_ids = [m["session_id"] for m in rows["members"]]
        ord_of = {m["session_id"]: m["ord"] for m in rows["members"]}

        unknown = sorted(set(version_prefix) - set(member_ids))
        if unknown:
            raise RetroError(f"版本前缀包含非成员会话：{'、'.join(unknown)}", 400)
        for sid, cap in version_prefix.items():
            if not isinstance(cap, int) or isinstance(cap, bool) or cap < 0:
                raise RetroError(f"会话 {sid} 的版本前缀必须是非负整数", 400)
        if cutoff is None and not version_prefix:
            raise RetroError("请提供统一复盘截止时刻或至少一个成员会话的事件版本前缀", 400)

        # --- load every member's pinned script + full event log, then fold
        # only the prefix admitted by the horizon ---------------------------
        loaded: dict[str, dict[str, Any]] = {}
        for m in rows["members"]:
            sid = m["session_id"]
            try:
                sess, d, _ = await self.engine.load(sid)
            except Exception as exc:  # a vanished member must not crash review
                raise RetroError(f"成员会话 {sid} 无法读取：{exc}", 500) from exc
            all_events = await self._events(sid)
            cap = version_prefix.get(sid)
            versions = included_versions(all_events, cutoff, cap)
            included = [e for e in all_events if e["version"] in set(versions)]
            state = reduce_events(included)
            commit_at = version_commit_at(all_events)
            cap = version_prefix.get(sid)
            # The exclusive horizon bound of this member:
            #  * cutoff mode        -> the unified cutoff timestamp;
            #  * version-prefix mode-> the commit instant of the first excluded
            #    version (cap+1); None when the cap already covers the full log
            #    (this member imposes no time bound).
            if cutoff is not None:
                bound = cutoff
            elif cap is not None:
                bound = commit_at.get(cap + 1)
            else:
                bound = None
            loaded[sid] = {
                "sess": sess, "d": d, "events": included, "all": all_events,
                "state": state, "ord": m["ord"],
                "included_version": max(versions, default=0),
                "max_version": max(commit_at, default=0),
                "commit_at": commit_at,
                "bound_at": bound,
                "horizon_at": commit_at.get(max(versions, default=0))
                if versions else None,
            }

        # Effective joint horizon T*: the smallest exclusive member bound. A
        # per-session version-prefix request is "consistent" exactly when it
        # equals every session's cutoff prefix at T* (the report flags gates
        # whose effect is present but whose causal hit was capped away).
        finite_bounds = [loaded[s]["bound_at"] for s in member_ids
                         if loaded[s]["bound_at"] is not None]
        t_star = min(finite_bounds, default=None)
        data_now = max(
            (info["horizon_at"] for info in loaded.values()
             if info["horizon_at"] is not None),
            default=None,
        )
        horizon_t = t_star if t_star is not None else data_now

        # --- per-gate causal analysis ---------------------------------------
        gates_out: list[dict[str, Any]] = []
        for g_ord, gr in enumerate(rows["gates"]):
            gr["_ord"] = g_ord
            gates_out.append(
                self._analyze_gate(
                    gr, rows["branches"][gr["gate_id"]], loaded, ord_of, horizon_t
                )
            )

        # Critical path: the longest accumulated-wait chain of delivered
        # gates along the injection dependency edges. path_seconds is a
        # gate's own wait plus all predecessor waits in its chain.
        by_id = {g["id"]: g for g in gates_out}
        for g in gates_out:
            chain = self._predecessor_chain(g, by_id)
            g["predecessor_chain"] = [cid for cid, _ in chain]
            g["path_seconds"] = round(
                sum(by_id[cid]["wait_seconds"] for cid, _ in chain)
                + (g["wait_seconds"] or 0.0),
                3,
            )
        delivered = [g for g in gates_out if g["status"] == DELIVERED]
        terminal = max(delivered, key=lambda g: (g["path_seconds"], -g["ord"]),
                       default=None)
        if terminal is not None:
            critical_ids = terminal["predecessor_chain"] + [terminal["id"]]
        else:
            critical_ids = []
        for g in gates_out:
            g["critical"] = g["id"] in critical_ids
        critical_path = critical_ids
        # Bottleneck gate: the single delivered gate whose own join wait is the
        # longest — widening that branch would shorten the whole chain most.
        bottleneck = max(
            delivered,
            key=lambda g: (g["wait_seconds"] or -1.0, -g["ord"]),
            default=None,
        )

        timeline = self._timeline(loaded, rows["gates"], gates_out, ord_of)
        members_out = [self._member_meta(sid, loaded[sid], horizon_t) for sid in member_ids]

        return {
            "kind": "joint_retrospective",
            "joint_id": joint_id,
            "joint_name": rows["joint"]["name"],
            "description": rows["joint"]["description"],
            "generated_at": now_ts(),
            "mode": "cutoff" if cutoff is not None else "version_prefix",
            "cutoff": cutoff,
            "version_prefix": version_prefix,
            # Exclusive effective horizon T*: only versions committed strictly
            # before T* are in the report. Equal to the unified cutoff in
            # cutoff mode; in prefix mode it is the earliest first-excluded
            # commit among members (null only when every member log is fully
            # included).
            "effective_as_of": horizon_t,
            "horizon_consistent": all(g["horizon_consistent"] for g in gates_out),
            "members": members_out,
            "gates": gates_out,
            "timeline": timeline,
            "critical_path": critical_path,
            "bottleneck_gate": bottleneck["id"] if bottleneck else None,
            "counts": {
                "total": len(gates_out),
                DELIVERED: sum(g["status"] == DELIVERED for g in gates_out),
                AWAITING_OPEN: sum(g["status"] == AWAITING_OPEN for g in gates_out),
                MISSED: sum(g["status"] == MISSED for g in gates_out),
                UNMET: sum(g["status"] == UNMET for g in gates_out),
            },
        }

    async def _events(self, sid: str) -> list[dict[str, Any]]:
        rows = await self.db.all(
            "SELECT version, seq, type, actor, at, payload FROM events "
            "WHERE session_id=? ORDER BY version, seq",
            (sid,),
        )
        return [
            {"version": r["version"], "seq": r["seq"], "type": r["type"],
             "actor": r["actor"], "at": float(r["at"]),
             "payload": json.loads(r["payload"])}
            for r in rows
        ]

    # ------------------------------------------------------------------ #
    # gate analysis
    # ------------------------------------------------------------------ #
    def _analyze_gate(
        self,
        gr: dict[str, Any],
        branches: list[dict[str, Any]],
        loaded: dict[str, dict[str, Any]],
        ord_of: dict[str, int],
        horizon_t: float | None,
    ) -> dict[str, Any]:
        gid = gr["gate_id"]
        target_sid = gr["target_session"]
        target_stage = gr["target_stage"]
        t = loaded[target_sid]
        td: DrillDef = t["d"]
        target_state: State = t["state"]

        injection = find_event(
            t["events"], E_JOINT_INJECTION,
            lambda p, _gid=gid: p.get("gate_id") == _gid,
        )

        # branch hits as of the included prefix
        bstates = {sid: loaded[sid]["state"] for sid in loaded}
        pre_out: list[dict[str, Any]] = []
        met_at_all: list[tuple[float, int, int, int]] = []
        for b in branches:
            sid = b["session_id"]
            d = loaded[sid]["d"]
            met = branch_satisfied(b, bstates)
            sealed: SealedStage | None = (
                bstates[sid].sealed.get(b["stage_id"]) if met else None
            )
            hit_ev = None
            if met:
                hit_ev = find_event(
                    loaded[sid]["events"], E_STAGE_SEALED,
                    lambda p, s=b["stage_id"]: p.get("stage_id") == s,
                )
                met_at_all.append((
                    sealed.at, ord_of[sid], sealed.version,
                    hit_ev["seq"] if hit_ev else 0,
                ))
            pre_out.append({
                **b,
                "label": branch_label(b, d),
                "satisfied": met,
                "hit_at": sealed.at if met else None,
                "hit_event": event_ref(sid, ord_of[sid], hit_ev),
            })

        last_met = max(met_at_all, key=lambda x: (x[0], -x[1], -x[2], -x[3]),
                       default=None)
        all_met = bool(branches) and all(p["satisfied"] for p in pre_out)

        open_ev = find_event(
            t["events"], E_STAGE_OPENED,
            lambda p, s=target_stage: p.get("stage_id") == s,
        )
        seal_ev = find_event(
            t["events"], E_STAGE_SEALED,
            lambda p, s=target_stage: p.get("stage_id") == s,
        )
        end_ev = find_event(t["events"], E_SESSION_ENDED, lambda p: True)
        sealed_now = target_state.sealed.get(target_stage)

        status: str
        wait_seconds: float | None = None
        block_reason: str | None = None
        last_pre_ref = None
        bottleneck_branch = None
        horizon_consistent = True

        if injection is not None:
            # The effect is objectively inside the target session's requested
            # prefix. When an upstream prefix was capped before the causal hit
            # (an intentionally mixed horizon), the branch rows stay honest
            # (satisfied=False for excluded hits) and horizon_consistent flags
            # the inconsistency instead of rewriting either prefix.
            status = DELIVERED
            horizon_consistent = all(p["satisfied"] for p in pre_out)
            wait_seconds = round(injection["at"] - last_met[0], 3) if last_met else 0.0
            if last_met is not None:
                last_pre_ref = self._ref_for_met(last_met, pre_out, loaded, ord_of)
                bottleneck_branch = self._branch_of_met(last_met, pre_out)
            if not horizon_consistent:
                block_reason = (
                    "目标会话前缀已包含注入事件，但部分上游前缀早于其因果命中版本，"
                    "该混合前缀不是一致的截至时刻（请调小目标前缀或调大上游前缀）"
                )
        elif sealed_now is not None or target_state.status == "ended":
            # The one-shot window has closed inside the prefix and no delivery
            # made it in: the gate was / is missed as of this horizon.
            status = MISSED
            window_ev = seal_ev or end_ev
            if all_met and last_met is not None:
                last_pre_ref = self._ref_for_met(last_met, pre_out, loaded, ord_of)
                bottleneck_branch = self._branch_of_met(last_met, pre_out)
                wait_seconds = (
                    round(window_ev["at"] - last_met[0], 3) if window_ev else None
                )
                horizon_consistent = False
                if window_ev is not None and last_met[0] <= window_ev["at"] + 1e-6:
                    block_reason = (
                        f"最后前置于 {self._ts(last_met[0])} 满足，未早于目标窗口关闭"
                        f"（{self._ts(window_ev['at'])}），且目标前缀内没有注入事件；"
                        "若采用的是各会话独立版本前缀，该前缀不是一致的截至时刻"
                    )
                else:
                    block_reason = (
                        f"全部前置最终于 {self._ts(last_met[0])} 才满足，晚于目标窗口"
                        f"关闭（{self._ts(window_ev['at'])}），一次性注入窗口已错过"
                    )
            else:
                horizon_consistent = True
                if seal_ev is not None:
                    block_reason = (
                        f"目标阶段「{_stage_title(td, target_stage)}」已于 "
                        f"{self._ts(seal_ev['at'])} 结算，前置尚未全部满足，"
                        "一次性注入窗口已错过"
                    )
                else:
                    block_reason = "目标会话已结束而前置尚未全部满足，一次性注入窗口已错过"
        elif all_met:
            status = AWAITING_OPEN
            last_pre_ref = self._ref_for_met(last_met, pre_out, loaded, ord_of)
            bottleneck_branch = self._branch_of_met(last_met, pre_out)
            # Waiting is measured from the last satisfied prerequisite. If the
            # target stage has not opened at T* the wait extends to the
            # effective horizon; if it is open and all branches are met but the
            # injection commit is outside the prefix, the release simply lands
            # at the cutoff second or later.
            wait_seconds = round(horizon_t - last_met[0], 3) if horizon_t is not None else None
            if open_ev is None:
                block_reason = (
                    f"前置已全部满足（最后满足于 {self._ts(last_met[0])}），"
                    f"目标会话尚未进入阶段「{_stage_title(td, target_stage)}」，"
                    "截至时刻仍在等待目标阶段开放"
                )
            else:
                block_reason = (
                    f"前置已全部满足、目标阶段「{_stage_title(td, target_stage)}」"
                    "已开放，但注入事件落于截止时刻同秒或之后，未进入本次截至前缀"
                )
        else:
            status = UNMET
            waiting = [p["label"] for p in pre_out if not p["satisfied"]]
            block_reason = f"仍有 {len(waiting)} 个前置未满足：" + "；".join(waiting)

        target_title = _stage_title(td, target_stage)
        return {
            "id": gid,
            "ord": gr.get("_ord", 0),
            "target_session": target_sid,
            "target_session_name": td.name,
            "target_stage": target_stage,
            "target_stage_title": target_title,
            "title": gate_title_of(target_title, gr["title"] or ""),
            "public": gr["public_text"],
            "views": json.loads(gr["views"] or "{}"),
            "status": status,
            "status_label": STATUS_LABELS[status],
            "horizon_consistent": horizon_consistent,
            "prerequisites": pre_out,
            "wait_seconds": wait_seconds,
            "block_reason": block_reason,
            "last_prerequisite_event": last_pre_ref,
            "bottleneck_branch": bottleneck_branch,
            "target_open_event": event_ref(target_sid, ord_of[target_sid], open_ev),
            "target_settle_event": event_ref(target_sid, ord_of[target_sid], seal_ev),
            "injection_event": event_ref(target_sid, ord_of[target_sid], injection),
            "delivered_at": injection["at"] if injection else None,
            "injection_version": injection["version"] if injection else None,
            # filled in after every gate is analyzed:
            "predecessor_chain": [],
            "path_seconds": wait_seconds if status == DELIVERED else None,
            "critical": False,
        }

    @staticmethod
    def _ts(v: float) -> str:
        import datetime as _dt
        return _dt.datetime.fromtimestamp(v).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _branch_of_met(
        met: tuple[float, int, int, int], pre_out: list[dict[str, Any]]
    ) -> dict[str, Any] | None:
        """The prerequisite branch whose hit is the tuple ``met``: the last
        satisfied branch is the gate's join bottleneck."""
        at, ord_, version, _seq = met
        for p in pre_out:
            ref = p.get("hit_event")
            if (p["satisfied"] and abs((p["hit_at"] or 0) - at) < 1e-6
                    and ref is not None and ref["session_ord"] == ord_
                    and ref["version"] == version):
                return {
                    "session_id": p["session_id"],
                    "stage_id": p["stage_id"],
                    "kind": p["kind"],
                    "role_id": p.get("role_id", ""),
                    "value": p.get("value", ""),
                    "label": p["label"],
                    "hit_at": p["hit_at"],
                    "hit_event": p["hit_event"],
                }
        return None

    @staticmethod
    def _ref_for_met(met, pre_out, loaded, ord_of) -> dict[str, Any] | None:
        at, ord_, version, seq = met
        sid = next((s for s, info in loaded.items() if info["ord"] == ord_), None)
        if sid is None:
            return None
        ev = next((e for e in loaded[sid]["events"]
                   if e["version"] == version and e["seq"] == seq), None)
        return event_ref(sid, ord_, ev)

    def _predecessor_chain(
        self, gate: dict[str, Any], by_id: dict[str, dict[str, Any]]
    ) -> list[tuple[str, int]]:
        """Delivered gates whose injection targets a stage that one of this
        gate's prerequisite stages causally follows: the last injection into
        the watched upstream session whose delivery precedes the bottleneck
        branch hit. Chained gates form the join's critical path."""
        chain: list[tuple[str, int]] = []
        seen: set[str] = set()
        cur = gate
        while True:
            pred = None
            bottleneck = cur.get("bottleneck_branch")
            if bottleneck is not None:
                up_sid = bottleneck["session_id"]
                hit_at = bottleneck["hit_at"]
                candidates = [
                    g for g in by_id.values()
                    if g["status"] == DELIVERED
                    and g["target_session"] == up_sid
                    and g["id"] != cur["id"]
                    and g["id"] not in seen
                    and (g["delivered_at"] or 0) <= (hit_at or 0) + 1e-6
                ]
                pred = max(candidates, key=lambda g: (g["delivered_at"], -g["ord"]),
                           default=None)
            if pred is None:
                break
            chain.append((pred["id"], pred["ord"]))
            seen.add(pred["id"])
            cur = pred
            if len(seen) > len(by_id):  # defensive: DAG is validated anyway
                break
        chain.reverse()
        return chain

    # ------------------------------------------------------------------ #
    # merged timeline
    # ------------------------------------------------------------------ #
    def _timeline(
        self,
        loaded: dict[str, dict[str, Any]],
        gate_rows: list[dict[str, Any]],
        gates_out: list[dict[str, Any]],
        ord_of: dict[str, int],
    ) -> list[dict[str, Any]]:
        gates_by_target_stage: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for g in gates_out:
            gates_by_target_stage.setdefault(
                (g["target_session"], g["target_stage"]), []
            ).append(g)

        entries: list[dict[str, Any]] = []
        for sid, info in loaded.items():
            d: DrillDef = info["d"]
            for e in info["events"]:
                p = e["payload"]
                kind = e["type"]
                title = None
                stage_id = p.get("stage_id")
                marks: list[dict[str, Any]] = []
                detail: dict[str, Any] = {}

                if kind == E_SESSION_STARTED:
                    title = "演练开始"
                    stage_id = p.get("stage_id")
                elif kind == E_PARTICIPANT_JOINED:
                    title = f"{p.get('display_name')}（{_role_name(d, p.get('role_id', ''))}）加入"
                elif kind == E_STAGE_CHOICE:
                    role = _role_name(d, p.get("role_id", ""))
                    label = p.get("choice_id")
                    stage = d.stages.get(p.get("stage_id", ""))
                    if stage:
                        for o in stage.options.get(p.get("role_id", ""), []):
                            if o.id == p["choice_id"]:
                                label = o.text
                    title = f"{role} 提交选择：{label}"
                    detail = {"role_id": p.get("role_id"), "choice_id": p.get("choice_id")}
                elif kind == E_STAGE_SEALED:
                    title = f"阶段「{_stage_title(d, stage_id)}」结算"
                    marks.append({"kind": MARK_SETTLE, "label": "阶段结算",
                                  "next": p.get("next"), "reason": p.get("reason")})
                    detail = {"reason": p.get("reason"), "next": p.get("next"),
                              "choices": p.get("choices")}
                    # prerequisite outcomes + AND-join branch hits observed by
                    # this seal — one mark per gate branch watching this stage.
                    for g in gates_out:
                        for b in g["prerequisites"]:
                            if b["session_id"] != sid or b["stage_id"] != stage_id:
                                continue
                            if not b["satisfied"]:
                                continue
                            ref = b.get("hit_event") or {}
                            if ref.get("version") != e["version"]:
                                continue
                            marks.append({
                                "kind": MARK_PREREQ,
                                "label": "前置结局",
                                "gate_id": g["id"],
                                "gate_title": g["title"],
                                "branch_kind": b["kind"],
                                "branch_label": b["label"],
                            })
                            marks.append({
                                "kind": MARK_BRANCH,
                                "label": "分支命中",
                                "gate_id": g["id"],
                                "gate_title": g["title"],
                                "branch_label": b["label"],
                            })
                elif kind == E_STAGE_OPENED:
                    title = f"目标阶段开放：{_stage_title(d, stage_id)}" \
                        if gates_by_target_stage.get((sid, stage_id)) \
                        else f"进入阶段「{_stage_title(d, stage_id)}」"
                    for g in gates_by_target_stage.get((sid, stage_id), ()):
                        marks.append({
                            "kind": MARK_OPEN,
                            "label": "目标阶段开放",
                            "gate_id": g["id"],
                            "gate_title": g["title"],
                            "gate_status": g["status"],
                        })
                    detail = {"deadline": p.get("deadline")}
                elif kind == E_SESSION_ENDED:
                    title = "演练结束"
                    detail = {"reason": p.get("reason")}
                elif kind == E_JOINT_INJECTION:
                    title = f"联演注入：{p.get('title') or '（无标题）'}"
                    marks.append({
                        "kind": MARK_INJECTION,
                        "label": "注入",
                        "gate_id": p.get("gate_id"),
                        "gate_title": next(
                            (g["title"] for g in gates_out if g["id"] == p.get("gate_id")),
                            p.get("title"),
                        ),
                        "injection_id": p.get("injection_id"),
                    })
                    detail = {"injection_id": p.get("injection_id"),
                              "joint_id": p.get("joint_id"), "gate_id": p.get("gate_id")}
                else:
                    title = kind

                entries.append({
                    "session_id": sid,
                    "session_ord": ord_of[sid],
                    "drill_id": d.id,
                    "drill_name": d.name,
                    "drill_version": info["sess"]["drill_version"],
                    "version": e["version"],
                    "seq": e["seq"],
                    "at": e["at"],
                    "type": kind,
                    "actor": e.get("actor", ""),
                    "stage_id": stage_id,
                    "title": title,
                    "detail": detail,
                    "marks": marks,
                })

        # Stable cross-session merge: time, joint membership order, then the
        # session-local causal (version, seq) order. Same-second events across
        # sessions never reorder between two reports.
        entries.sort(key=lambda x: (x["at"], x["session_ord"], x["version"], x["seq"]))
        for i, x in enumerate(entries):
            x["index"] = i
        return entries

    # ------------------------------------------------------------------ #
    # member meta
    # ------------------------------------------------------------------ #
    def _member_meta(self, sid: str, info: dict[str, Any], horizon_t) -> dict[str, Any]:
        d: DrillDef = info["d"]
        st: State = info["state"]
        last_at = max((e["at"] for e in info["events"]), default=None)
        bound = info["bound_at"]
        return {
            "session_id": sid,
            "ord": info["ord"],
            "drill_id": d.id,
            "drill_name": d.name,
            "drill_version": info["sess"]["drill_version"],
            "drill_sha256": info["sess"].get("drill_sha256"),
            "digest_short": (info["sess"].get("drill_sha256") or "")[:12],
            "status": st.status,
            "current_stage": st.current,
            "sealed_stages": list(st.sealed.keys()),
            "included_version": info["included_version"],
            "max_version": info["max_version"],
            "prefix_truncated": info["included_version"] < info["max_version"],
            # Exclusive bound for this member; prefix_matches_horizon is true
            # when the per-session version prefix includes exactly the same
            # versions as a unified cutoff prefix at the effective T*.
            "bound_at": bound,
            "prefix_matches_horizon": bound is None or bound == horizon_t,
            "horizon_at": info["horizon_at"],
            "last_event_at": last_at,
        }
