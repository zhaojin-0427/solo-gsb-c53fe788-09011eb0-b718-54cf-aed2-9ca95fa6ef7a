"""Joint-exercise causal-timeline postmortem ("联演因果时序复盘").

A read-only, deterministic analyzer run by the presenter *after* (or during)
a saved joint exercise. The presenter submits one unified review cutoff and
the analyzer:

* loads every member session's **pinned** script version and immutable event
  log, then merges all events stably — by event time, member ordinal (会话顺序号)
  and (version, seq) (事件序号);
* marks prerequisite outcomes, branch hits, target-stage opens, injections and
  stage settlements (阶段结算);
* evaluates every gate purely from the chosen event prefix and classifies it
  into 尚未满足 / 目标未开放 / 已错过 / 已投递, with a blocking reason and
  traceable event references;
* measures the wait from the last satisfied prerequisite (the AND-join point)
  to delivery or to the one-shot window closing, flags the bottleneck branch
  and summarises the critical path along the gate dependency chain.

Two consistent cutoff modes produce an immutable "as-of" report:

* ``as_of``  — one unified POSIX cutoff ``T`` for every member. A version
  commit is included only when **all** of its events happened strictly before
  ``T``. Events at the same second as the cutoff, or late-arriving events that
  landed after an earlier review, therefore never rewrite the prefix: the
  report for a given ``T`` is a pure function of the append-only logs and
  reproduces byte-for-byte (modulus JSON formatting) on every re-run.
* ``prefix`` — one event-version prefix per member session. Each session is
  folded at exactly ``events.version <= prefixes[sid]``; this answers "what
  could be known from these frozen session versions".

Nothing here writes anything: joint gate rows and session event logs are the
only inputs, so a review can never perturb a live or finished exercise.
"""
from __future__ import annotations

from typing import Any

from .definitions import DrillDef
from .engine import (
    E_JOINT_INJECTION,
    E_SESSION_ENDED,
    E_SESSION_STARTED,
    E_STAGE_CHOICE,
    E_STAGE_OPENED,
    E_STAGE_SEALED,
    State,
    reduce_events,
)
from .joints import JointError, branch_satisfied
from .snapshots import REASON_LABELS

# Gate review statuses (four-valued, as required by the host console).
UNSATISFIED = "unsatisfied"          # 尚未满足：仍有前置分支未命中
AWAITING_TARGET = "awaiting_target"  # 目标未开放：前置已齐，等待目标阶段窗口
DELIVERED = "delivered"              # 已投递
REVIEW_MISSED = "missed"             # 已错过

STATUS_LABELS = {
    UNSATISFIED: "尚未满足",
    AWAITING_TARGET: "目标未开放",
    DELIVERED: "已投递",
    REVIEW_MISSED: "已错过",
}

MARK_OUTCOME = "prereq_outcome"
MARK_BRANCH_HIT = "branch_hit"
MARK_BRANCH_MISS = "branch_miss"
MARK_TARGET_OPEN = "target_open"
MARK_TARGET_CLOSE = "target_close"
MARK_INJECTION = "injection"
MARK_SETTLEMENT = "stage_settlement"
MARK_SESSION_END = "session_end"


class _Member:
    """One member session folded at the report's chosen prefix."""

    def __init__(
        self,
        ord_: int,
        sess: dict[str, Any],
        d: DrillDef,
        all_events: list[dict[str, Any]],
        events: list[dict[str, Any]],
        state: State,
    ):
        self.ord = ord_
        self.sess = sess
        self.d = d
        self.all_events = all_events
        self.events = events
        self.state = state
        self.latest_version = max((e["version"] for e in all_events), default=0)
        self.include_version = max((e["version"] for e in events), default=0)


def _round(x: float | None) -> float | None:
    if x is None:
        return None
    return round(float(x), 3)


def _event_ref(ev: dict[str, Any] | None, sid: str) -> dict[str, Any] | None:
    """Stable, traceable pointer into one session's immutable event log."""
    if ev is None:
        return None
    return {
        "session_id": sid,
        "version": ev["version"],
        "seq": ev["seq"],
        "type": ev["type"],
        "at": ev["at"],
        "ref": f"{sid}#v{ev['version']}.{ev['seq']}",
    }


def _branch_outcome_view(b: dict[str, Any], sealed) -> dict[str, Any]:
    """What outcome actually happened on a watched, sealed stage."""
    out: dict[str, Any] = {"next": sealed.next}
    if b["kind"] == "choice":
        out["role_id"] = b.get("role_id", "")
        out["choice"] = sealed.choices.get(b.get("role_id", ""))
    return out


def _branch_label(b: dict[str, Any], d: DrillDef) -> str:
    stage = d.stages.get(b["stage_id"])
    stage_title = stage.title if stage else b["stage_id"]
    if b["kind"] == "next_stage":
        if b.get("value"):
            t = d.stages.get(b["value"])
            want = t.title if t else b["value"]
        else:
            want = "（演练结束）"
        return f"[{d.name}] 阶段「{stage_title}」结算跳转＝{want}"
    role = d.roles.get(b.get("role_id", ""))
    role_name = role.name if role else b.get("role_id", "")
    value = b.get("value", "")
    if stage is not None:
        for o in stage.options.get(b.get("role_id", ""), []):
            if o.id == value:
                value = o.text
    return f"[{d.name}] 阶段「{stage_title}」{role_name} 选择＝{value}"


def build_postmortem(
    joint_row: dict[str, Any],
    members: list[_Member],
    gates_def: list[dict[str, Any]],
    mode: str,
    as_of: float | None,
) -> dict[str, Any]:
    """Pure analysis. ``gates_def`` rows carry definition + stored runtime
    columns; ``members`` are already folded at the requested prefix."""
    horizon = as_of if mode == "as_of" else None
    by_sid = {m.sess["id"]: m for m in members}

    # ---- event index per member ---------------------------------------- #
    def events_of(sid: str) -> list[dict[str, Any]]:
        return by_sid[sid].events

    def find_event(sid: str, etype: str, stage_id: str | None = None):
        for ev in events_of(sid):
            if ev["type"] != etype:
                continue
            if stage_id is not None and ev["payload"].get("stage_id") != stage_id:
                continue
            return ev
        return None

    def seal_event(sid: str, stage_id: str):
        return find_event(sid, E_STAGE_SEALED, stage_id)

    # ---- per-gate evaluation ------------------------------------------- #
    gates_out: list[dict[str, Any]] = []
    # gate ord -> analysis intermediates reused by the chain/path pass
    intermediates: dict[int, dict[str, Any]] = {}

    for ord_, g in enumerate(gates_def):
        tgt_sid = g["target_session"]
        tgt_stage = g["target_stage"]
        tgt_member = by_sid.get(tgt_sid)
        tgt_state = tgt_member.state if tgt_member else State()
        tgt_def = tgt_member.d if tgt_member else None

        injection_id = f"inj_{joint_row['id']}_{g['gate_id']}"
        # A session may be the target of several gates; match the injection by
        # its deterministic id.
        inj_ev: dict[str, Any] | None = None
        if tgt_member is not None:
            inj_ev = next(
                (e for e in events_of(tgt_sid)
                 if e["type"] == E_JOINT_INJECTION
                 and e["payload"].get("injection_id") == injection_id),
                None,
            )

        close_ev: dict[str, Any] | None = None
        close_kind = ""
        if inj_ev is None and tgt_member is not None:
            if tgt_stage in tgt_state.sealed:
                close_ev = seal_event(tgt_sid, tgt_stage)
                close_kind = MARK_TARGET_CLOSE
            elif tgt_state.status == "ended":
                close_ev = next(
                    (e for e in events_of(tgt_sid) if e["type"] == E_SESSION_ENDED),
                    None,
                )
                close_kind = MARK_SESSION_END

        # ---- prerequisites --------------------------------------------- #
        pre_out = []
        satisfied_times: list[float] = []
        unsatisfied_labels: list[str] = []
        hard_mismatches = 0
        for b in g["branches"]:
            m = by_sid.get(b["session_id"])
            st = m.state if m else State()
            d = m.d if m else None
            met = branch_satisfied(b, {b["session_id"]: st})
            sealed = st.sealed.get(b["stage_id"])
            sev = seal_event(b["session_id"], b["stage_id"]) if sealed else None
            label = _branch_label(b, d) if d else b["stage_id"]
            entry: dict[str, Any] = {
                "branch_no": b["branch_no"],
                "session_id": b["session_id"],
                "stage_id": b["stage_id"],
                "kind": b["kind"],
                "role_id": b.get("role_id", ""),
                "value": b.get("value", ""),
                "label": label,
                "satisfied": met,
                "satisfied_at": sealed.at if met else None,
                "event_ref": _event_ref(sev, b["session_id"]),
            }
            if sealed is not None:
                entry["sealed_at"] = sealed.at
                entry["actual"] = _branch_outcome_view(b, sealed)
                if not met:
                    hard_mismatches += 1
                    entry["outcome_match"] = False
            else:
                entry["outcome_match"] = None
            if met:
                satisfied_times.append(sealed.at)
            else:
                unsatisfied_labels.append(label)
            pre_out.append(entry)

        last_pre_at = max(satisfied_times) if satisfied_times else None
        first_pre_at = min(satisfied_times) if satisfied_times else None
        all_met = not unsatisfied_labels
        is_current = tgt_state.current == tgt_stage

        # ---- four-valued status ---------------------------------------- #
        # ``evidence_partial`` marks a prefix-mode anomaly: the target log
        # already carries the injection while one upstream seal that authorised
        # it lies outside its member's chosen version prefix. The injection is
        # still a fact in the target prefix (it is delivered), the report only
        # flags that the causal evidence is truncated — useful when replaying
        # inconsistent per-member cutoffs. In as_of mode this never happens,
        # because the one unified time cutoff keeps the causal ordering intact.
        evidence_partial = bool(inj_ev is not None and unsatisfied_labels)
        # Priority mirrors the live reconciler: delivered first, then the
        # closed one-shot window (a gate whose target sealed / session ended
        # can never be released, even while prerequisites remain unmet), then
        # the two waiting shades.
        if inj_ev is not None:
            status = DELIVERED
        elif close_ev is not None:
            status = REVIEW_MISSED
        elif not all_met:
            status = UNSATISFIED
        else:
            status = AWAITING_TARGET

        delivered_at = inj_ev["at"] if inj_ev else None
        closed_at = close_ev["at"] if close_ev else None

        # Wait from the AND-join point (last prerequisite satisfied) to the
        # injection, or to the one-shot window closing. None when the join
        # never completed within the prefix.
        wait_seconds: float | None = None
        age_seconds: float | None = None
        if status == DELIVERED and last_pre_at is not None:
            wait_seconds = delivered_at - last_pre_at
        elif status == REVIEW_MISSED and last_pre_at is not None and closed_at is not None:
            wait_seconds = closed_at - last_pre_at
        elif status == AWAITING_TARGET and last_pre_at is not None and horizon is not None:
            age_seconds = max(0.0, horizon - last_pre_at)
        elif status == UNSATISFIED and last_pre_at is not None and horizon is not None:
            age_seconds = max(0.0, horizon - last_pre_at)
        # AND assembly span: first branch hit -> last branch hit (the slack the
        # slower branches burned inside this gate's own prerequisite fan-in).
        assembly_seconds = (
            last_pre_at - first_pre_at
            if first_pre_at is not None and last_pre_at is not None
            and len(satisfied_times) > 1 else None
        )

        # ---- blocking reason ------------------------------------------- #
        stage_title = tgt_stage
        drill_name = tgt_sid
        if tgt_def is not None:
            tstage = tgt_def.stages.get(tgt_stage)
            stage_title = tstage.title if tstage else tgt_stage
            drill_name = tgt_def.name
        reason: str | None = None
        if status == UNSATISFIED:
            reason = f"等待 {len(unsatisfied_labels)} 个前置分支：" + "；".join(unsatisfied_labels)
            if hard_mismatches:
                reason += (
                    f"（其中 {hard_mismatches} 个上游阶段已结算但结局不符，"
                    "该分支在本会话内不会再命中）"
                )
        elif status == AWAITING_TARGET:
            if is_current:
                reason = (
                    "前置已全部满足且目标阶段已开放，但截至所选前缀注入事件尚未落账"
                    "（注入位于目标会话更新的版本前缀之外）"
                )
            elif tgt_state.current and tgt_def and tgt_state.current in tgt_def.stages:
                reason = (
                    f"前置已全部满足，等待会话进入阶段「{stage_title}」"
                    f"——会话「{drill_name}」当前处于阶段"
                    f"「{tgt_def.stages[tgt_state.current].title}」"
                )
            else:
                state_label = {"created": "尚未开始", "running": "进行中",
                               "ended": "已结束"}.get(tgt_state.status, tgt_state.status)
                reason = (
                    f"前置已全部满足，等待会话进入阶段「{stage_title}」"
                    f"——会话「{drill_name}」当前{state_label}，目标阶段尚未开放"
                )
        elif status == REVIEW_MISSED:
            ref = _event_ref(close_ev, tgt_sid)
            if close_kind == MARK_TARGET_CLOSE:
                payload = close_ev["payload"] if close_ev else {}
                rlabel = REASON_LABELS.get(payload.get("reason", ""), payload.get("reason", ""))
                reason = (
                    f"目标阶段「{stage_title}」已于 {ref['ref']} 结算（{rlabel}），"
                    "一次性注入窗口已关闭，不补投"
                )
            else:
                reason = f"目标会话已结束（{ref['ref'] if ref else ''}），注入窗口已关闭"
            if unsatisfied_labels:
                reason += "；且仍有 " + "；".join(unsatisfied_labels) + " 未满足"
        else:
            reason = (
                f"已在 {_event_ref(inj_ev, tgt_sid)['ref']} 投递"
                f"（最后前置汇合后等待 {_round(wait_seconds)} 秒）"
            )
            if evidence_partial:
                reason += (
                    "；注意：按当前各成员版本前缀，授权该注入的部分上游结算事件"
                    "尚在前缀之外（证据不完整），注入本身在目标会话前缀内已是事实"
                )

        # bottleneck branch: the one that completed the AND join (latest hit);
        # for an open gate the unsatisfied branches are the active blockers.
        bottleneck = None
        if satisfied_times:
            top = max(
                (e for e in pre_out if e["satisfied"]),
                key=lambda e: (e["satisfied_at"], -e["branch_no"]),
            )
            others = [
                _round(top["satisfied_at"] - e["satisfied_at"])
                for e in pre_out
                if e["satisfied"] and e is not top
            ]
            bottleneck = {
                "branch_no": top["branch_no"],
                "label": top["label"],
                "event_ref": top["event_ref"],
                "joined_at": top["satisfied_at"],
                "earlier_branch_lead_seconds": others,
            }
        active_blockers = [
            {"branch_no": e["branch_no"], "label": e["label"],
             "sealed_mismatch": e.get("outcome_match") is False,
             "event_ref": e["event_ref"]}
            for e in pre_out
            if not e["satisfied"]
        ]

        gates_out.append({
            "id": g["gate_id"],
            "title": g.get("title") or stage_title,
            "target_session": tgt_sid,
            "target_drill_name": drill_name,
            "target_stage": tgt_stage,
            "target_stage_title": stage_title,
            "public": g.get("public_text", ""),
            "views": g.get("views_obj", {}),
            "status": status,
            "status_label": STATUS_LABELS[status],
            "stored_status": g.get("status"),  # live gate row, for cross-check
            "prerequisites": pre_out,
            "all_prerequisites_met": all_met,
            "last_prerequisite_at": last_pre_at,
            "delivered_at": delivered_at,
            "closed_at": closed_at,
            "injection_ref": _event_ref(inj_ev, tgt_sid),
            "close_ref": _event_ref(close_ev, tgt_sid),
            "wait_seconds": _round(wait_seconds),
            "assembly_seconds": _round(assembly_seconds),
            "age_seconds": _round(age_seconds),
            "bottleneck_branch": bottleneck,
            "active_blockers": active_blockers,
            "evidence_partial": evidence_partial,
            "block_reason": reason,
        })
        intermediates[ord_] = {
            # Total causal delay introduced at this gate: the internal AND
            # assembly span plus the post-join wait until delivery/closing
            # (or the in-flight age at the review horizon).
            "wait": (
                (wait_seconds if wait_seconds is not None else (age_seconds or 0.0))
                + (assembly_seconds or 0.0)
            ),
            "status": status,
            "delivered": delivered_at,
        }

    # ---- gate dependency chain (injection causality) ------------------- #
    # Gate A -> Gate B when B watches the (session, stage) node that A's
    # one-shot injection targets. The saved-joint DAG validation makes this
    # subgraph acyclic as well.
    node_targets: dict[tuple[str, str], list[int]] = {}
    for i, g in enumerate(gates_def):
        node_targets.setdefault((g["target_session"], g["target_stage"]), []).append(i)
    edges: dict[int, list[int]] = {i: [] for i in range(len(gates_def))}
    for i, g in enumerate(gates_def):
        for b in g["branches"]:
            for up in node_targets.get((b["session_id"], b["stage_id"]), []):
                if up != i and i not in edges[up]:
                    edges[up].append(i)

    indeg = {i: 0 for i in range(len(gates_def))}
    for downs in edges.values():
        for j in downs:
            indeg[j] += 1
    # deterministic Kahn order
    queue = sorted(i for i, d in indeg.items() if d == 0)
    topo: list[int] = []
    while queue:
        n = queue.pop(0)
        topo.append(n)
        for m in sorted(edges[n]):
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
        queue.sort()
    if len(topo) != len(gates_def):  # defensive; validation guarantees a DAG
        topo = list(range(len(gates_def)))

    # best root -> gate path by cumulative delay; deterministic tie-break
    best: dict[int, dict[str, Any]] = {}

    def path_key(p: dict[str, Any]) -> tuple[float, list[int]]:
        # larger total wins; on a tie the path whose first differing gate ord
        # is smaller wins (stable across runs / hosts).
        return (-p["wait"], p["ords"])

    for i in topo:
        w = max(0.0, intermediates[i]["wait"])
        candidates = [{"wait": w, "ords": [i], "nodes": [i]}]
        for up in sorted(edges.keys()):
            if i in edges[up] and up in best:
                bp = best[up]
                candidates.append({
                    "wait": bp["wait"] + w,
                    "ords": bp["ords"] + [i],
                    "nodes": bp["nodes"] + [i],
                })
        winner = min(candidates, key=path_key)
        # min with negated wait => maximal wait; ords compared ascending
        best[i] = winner
        gates_out[i]["critical_path"] = {
            "gate_ids": [gates_def[n]["gate_id"] for n in winner["nodes"]],
            "gate_titles": [
                (gates_def[n].get("title") or gates_def[n]["target_stage"])
                for n in winner["nodes"]
            ],
            "cumulative_wait_seconds": _round(winner["wait"]),
        }

    if best:
        overall = min(best.values(), key=path_key)
        critical_path = {
            "gate_ids": [gates_def[n]["gate_id"] for n in overall["nodes"]],
            "gates": [
                {
                    "gate_id": gates_def[n]["gate_id"],
                    "title": gates_def[n].get("title") or gates_def[n]["target_stage"],
                    "status": gates_out[n]["status"],
                    "wait_seconds": gates_out[n]["wait_seconds"],
                    "assembly_seconds": gates_out[n]["assembly_seconds"],
                    "age_seconds": gates_out[n]["age_seconds"],
                    "bottleneck_branch": gates_out[n]["bottleneck_branch"],
                }
                for n in overall["nodes"]
            ],
            "total_wait_seconds": _round(overall["wait"]),
        }
    else:
        critical_path = {"gate_ids": [], "gates": [], "total_wait_seconds": None}

    # bottleneck ranking: delivered/missed waits first, then in-flight ages;
    # equal weights keep the saved gate order for a stable report.
    ranked = [
        {
            "gate_id": gg["id"],
            "title": gg["title"],
            "status": gg["status"],
            "wait_seconds": gg["wait_seconds"],
            "assembly_seconds": gg["assembly_seconds"],
            "age_seconds": gg["age_seconds"],
            "weight_seconds": _round(intermediates[i]["wait"]),
        }
        for i, gg in enumerate(gates_out)
    ]
    bottleneck_gates = sorted(
        (r for r in ranked if r["weight_seconds"] and r["weight_seconds"] > 0),
        key=lambda r: (-(r["weight_seconds"] or 0.0), ranked.index(r)),
    )

    # ---- stable merged timeline ----------------------------------------- #
    timeline: list[dict[str, Any]] = []
    # index marks by (sid, version, seq)
    mark_index: dict[tuple[str, int, int], list[dict[str, Any]]] = {}

    def add_mark(sid: str, ev: dict[str, Any], mark: dict[str, Any]) -> None:
        mark_index.setdefault((sid, ev["version"], ev["seq"]), []).append(mark)

    for i, g in enumerate(gates_def):
        # target open / close / injection marks
        m = by_sid.get(g["target_session"])
        if m is not None:
            ev_open = find_event(g["target_session"], E_STAGE_OPENED, g["target_stage"])
            if ev_open is not None:
                add_mark(g["target_session"], ev_open, {
                    "kind": MARK_TARGET_OPEN, "gate_id": g["gate_id"], "gate_ord": i,
                    "label": f"目标阶段开放：{g.get('title') or g['target_stage']}",
                })
            ev_seal = seal_event(g["target_session"], g["target_stage"])
            if ev_seal is not None:
                add_mark(g["target_session"], ev_seal, {
                    "kind": MARK_TARGET_CLOSE, "gate_id": g["gate_id"], "gate_ord": i,
                    "label": f"目标阶段结算：{g.get('title') or g['target_stage']}",
                })
            ev_inj = next(
                (e for e in events_of(g["target_session"])
                 if e["type"] == E_JOINT_INJECTION
                 and e["payload"].get("gate_id") == g["gate_id"]),
                None,
            )
            if ev_inj is not None:
                add_mark(g["target_session"], ev_inj, {
                    "kind": MARK_INJECTION, "gate_id": g["gate_id"], "gate_ord": i,
                    "label": f"联演注入：{ev_inj['payload'].get('title') or g['gate_id']}",
                })
        # prerequisite outcome / hit / miss marks
        for b in g["branches"]:
            mm = by_sid.get(b["session_id"])
            if mm is None:
                continue
            sealed = mm.state.sealed.get(b["stage_id"])
            sev = seal_event(b["session_id"], b["stage_id"]) if sealed else None
            if sev is None:
                continue
            met = branch_satisfied(b, {b["session_id"]: mm.state})
            add_mark(b["session_id"], sev, {
                "kind": MARK_OUTCOME, "gate_id": g["gate_id"], "gate_ord": i,
                "branch_no": b["branch_no"], "hit": met,
                "label": f"前置结局：{_branch_label(b, mm.d)}",
            })
            add_mark(b["session_id"], sev, {
                "kind": MARK_BRANCH_HIT if met else MARK_BRANCH_MISS,
                "gate_id": g["gate_id"], "gate_ord": i, "branch_no": b["branch_no"],
                "label": ("分支命中：" if met else "分支结局不符：")
                + _branch_label(b, mm.d),
            })

    for m in members:
        for ev in m.events:
            timeline.append({
                "at": ev["at"],
                "member_ord": m.ord,
                "session_id": m.sess["id"],
                "drill_id": m.d.id,
                "drill_name": m.d.name,
                "version": ev["version"],
                "seq": ev["seq"],
                "type": ev["type"],
                "actor": ev["actor"],
                "stage_id": ev["payload"].get("stage_id"),
                "label": _event_label(ev, m.d),
                "event_ref": f"{m.sess['id']}#v{ev['version']}.{ev['seq']}",
                "marks": sorted(
                    mark_index.get((m.sess["id"], ev["version"], ev["seq"]), []),
                    key=lambda mk: (mk["kind"], mk.get("gate_ord", 0), mk.get("branch_no", 0)),
                ),
            })
            if ev["type"] == E_STAGE_SEALED:
                timeline[-1]["marks"].append({"kind": MARK_SETTLEMENT})
            elif ev["type"] == E_SESSION_ENDED:
                timeline[-1]["marks"].append({"kind": MARK_SESSION_END})
    timeline.sort(key=lambda e: (e["at"], e["member_ord"], e["version"], e["seq"]))
    for i, e in enumerate(timeline):
        e["ord"] = i

    # ---- members block + envelope --------------------------------------- #
    wait_values = [
        g["wait_seconds"] or 0.0
        for g in gates_out
        if g["status"] in (DELIVERED, REVIEW_MISSED)
    ]
    max_wait = _round(max(wait_values)) if wait_values else None
    members_out = []
    cutoff_members = {}
    for m in members:
        sid = m.sess["id"]
        members_out.append({
            "ord": m.ord,
            "session_id": sid,
            "drill_id": m.d.id,
            "drill_name": m.d.name,
            "drill_version": m.sess["drill_version"],
            "drill_sha256": m.sess.get("drill_sha256"),
            "digest_short": (m.sess.get("drill_sha256") or "")[:12],
            "status": m.state.status,
            "current_stage": m.state.current,
            "current_stage_title": (
                m.d.stages[m.state.current].title
                if m.state.current and m.state.current in m.d.stages else None
            ),
            "include_version": m.include_version,
            "latest_version": m.latest_version,
            "events_included": len(m.events),
            "started_at": m.state.started_at,
            "ended_at": m.state.ended_at,
        })
        cutoff_members[sid] = {
            "include_version": m.include_version,
            "latest_version": m.latest_version,
            "truncated": m.include_version < m.latest_version,
        }

    summary = {
        "total": len(gates_out),
        "delivered": sum(1 for g in gates_out if g["status"] == DELIVERED),
        "missed": sum(1 for g in gates_out if g["status"] == REVIEW_MISSED),
        "awaiting_target": sum(1 for g in gates_out if g["status"] == AWAITING_TARGET),
        "unsatisfied": sum(1 for g in gates_out if g["status"] == UNSATISFIED),
        "total_delivered_wait_seconds": _round(sum(
            g["wait_seconds"] or 0.0
            for g in gates_out if g["status"] == DELIVERED
        )),
        "total_assembly_seconds": _round(sum(
            g["assembly_seconds"] or 0.0 for g in gates_out
        )),
        "max_wait_seconds": max_wait,
        "timeline_events": len(timeline),
        "evidence_partial": sum(1 for g in gates_out if g["evidence_partial"]),
    }

    return {
        "kind": "joint_postmortem",
        "joint_id": joint_row["id"],
        "joint_name": joint_row["name"],
        "description": joint_row.get("description", ""),
        "mode": mode,
        "as_of": as_of,
        "horizon_label": (
            f"统一截止时刻 {as_of}（POSIX 秒，严格小于：同秒事件不计入前缀）"
            if mode == "as_of"
            else "按各成员会话事件版本前缀折叠"
        ),
        "members": members_out,
        "cutoff": {"mode": mode, "as_of": as_of, "members": cutoff_members},
        "summary": summary,
        "gates": gates_out,
        "critical_path": critical_path,
        "bottleneck_gates": bottleneck_gates,
        "timeline": timeline,
    }


def _event_label(ev: dict[str, Any], d: DrillDef) -> str:
    p = ev["payload"]
    t = ev["type"]
    if t == E_SESSION_STARTED:
        return "演练开始"
    if t == "participant_joined":
        role = d.roles.get(p["role_id"])
        return f"{p.get('display_name')}（{role.name if role else p['role_id']}）加入"
    if t == E_STAGE_CHOICE:
        role = d.roles.get(p["role_id"])
        stage = d.stages.get(p["stage_id"])
        label = None
        if stage:
            for o in stage.options.get(p["role_id"], []):
                if o.id == p["choice_id"]:
                    label = o.text
        return f"{role.name if role else p['role_id']} 选择：{label or p['choice_id']}"
    if t == E_STAGE_SEALED:
        stage = d.stages.get(p["stage_id"])
        return (
            f"阶段「{stage.title if stage else p['stage_id']}」结算"
            f"（{REASON_LABELS.get(p.get('reason'), p.get('reason'))}）"
            f" → {p.get('next') or '（演练结束）'}"
        )
    if t == E_STAGE_OPENED:
        stage = d.stages.get(p["stage_id"])
        return f"进入阶段「{stage.title if stage else p['stage_id']}」"
    if t == E_SESSION_ENDED:
        return f"演练结束（{REASON_LABELS.get(p.get('reason'), p.get('reason'))}）"
    if t == E_JOINT_INJECTION:
        return f"联演注入追加到阶段「{p.get('stage_id')}」：{p.get('title') or '（无标题）'}"
    return t


# --------------------------------------------------------------------------- #
# request validation
# --------------------------------------------------------------------------- #
def parse_review_request(raw: Any, member_ids: list[str]) -> tuple[str, float | None]:
    """Validate the report request. Returns (mode, as_of)."""
    if not isinstance(raw, dict):
        raise JointError("请求体必须是对象", 400)
    mode = str(raw.get("mode") or "").strip()
    if mode not in ("as_of", "prefix"):
        raise JointError("mode 必须是 as_of（统一截止时刻）或 prefix（成员版本前缀）", 400)

    if mode == "as_of":
        as_of = raw.get("as_of")
        if isinstance(as_of, bool) or not isinstance(as_of, (int, float)):
            raise JointError("as_of 必须是 POSIX 秒数字", 400)
        as_of = float(as_of)
        if as_of != as_of or as_of in (float("inf"), float("-inf")):
            raise JointError("as_of 必须是有限数字", 400)
        return mode, as_of

    prefixes = raw.get("prefixes")
    if not isinstance(prefixes, dict) or not prefixes:
        raise JointError("prefix 模式需要 prefixes：{会话编号: 事件版本号}", 400)
    missing = [sid for sid in member_ids if sid not in prefixes]
    if missing:
        raise JointError(f"prefixes 缺少成员会话：{'、'.join(missing)}", 400)
    extra = [sid for sid in prefixes if sid not in member_ids]
    if extra:
        raise JointError(f"prefixes 含非成员会话：{'、'.join(extra)}", 400)
    for sid, v in prefixes.items():
        if isinstance(v, bool) or not isinstance(v, int) or v < 0:
            raise JointError(f"会话 {sid} 的版本前缀必须是非负整数", 400)
    return mode, None


def filter_prefix_events(
    all_events: list[dict[str, Any]],
    mode: str,
    as_of: float | None,
    prefix_version: int | None,
) -> list[dict[str, Any]]:
    """Apply the consistent cutoff to one session's log.

    * prefix mode — every event with ``version <= prefix_version``;
    * as_of mode — a whole version commit counts only when **every** event of
      that commit happened strictly before the cutoff (events in one commit
      share one logical instant). Same-second / late events therefore never
      rewrite an earlier prefix.
    """
    if mode == "prefix":
        return [e for e in all_events if e["version"] <= (prefix_version or 0)]
    commit_at: dict[int, float] = {}
    for e in all_events:
        v = e["version"]
        if v not in commit_at or e["at"] > commit_at[v]:
            commit_at[v] = e["at"]
    kept_versions = {v for v, t in commit_at.items() if t < (as_of or 0.0)}
    return [e for e in all_events if e["version"] in kept_versions]
