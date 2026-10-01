"""Build permission-trimmed snapshots from event-sourced state.

A participant snapshot contains only:
* public stage text + that role's own private view/options/choices
* outcomes of stages that have already been sealed (public announcement of the
  stage reached, never other roles' private views or unannounced option text)
The presenter snapshot additionally contains every role's view, every choice,
the raw definition and the full event log.

Joint-exercise injections (``joint_injection`` events) follow the same
permission model: the injection's public text goes to everyone watching the
stage, each role-specific view only to that role.
"""
from __future__ import annotations

from typing import Any

from .definitions import DrillDef, Stage
from .engine import Injection, State

REASON_LABELS = {
    "complete": "全部提交，自动结算",
    "deadline": "到达截止时刻",
    "host": "主持人手动结算",
    "terminal": "自然结束",
}


def _iso(ts: float | None) -> float | None:
    return ts  # epoch seconds; the client renders local time


def _choice_label(stage: Stage, role_id: str, choice_id: str | None) -> str | None:
    if choice_id is None:
        return None
    for o in stage.options.get(role_id, []):
        if o.id == choice_id:
            return o.text
    return choice_id


def _injection_view(inj: Injection, role_id: str | None, host: bool) -> dict[str, Any]:
    """Permission-trimmed view of one delivered joint injection."""
    out: dict[str, Any] = {
        "injection_id": inj.injection_id,
        "joint_id": inj.joint_id,
        "gate_id": inj.gate_id,
        "stage_id": inj.stage_id,
        "title": inj.title,
        "public": inj.public,
        "version": inj.version,
        "at": _iso(inj.at),
    }
    if host:
        out["views"] = dict(inj.views)
    elif role_id is not None:
        out["private"] = inj.views.get(role_id, "")
    return out


def _injections_for(state: State, stage_id: str) -> list[Injection]:
    return [
        inj for inj in state.injections.values()
        if inj.stage_id == stage_id
    ]


def _block_base(stage: Stage, state: State) -> dict[str, Any]:
    sealed = state.sealed.get(stage.id)
    return {
        "stage_id": stage.id,
        "title": stage.title,
        "public": stage.public,
        "announcement": stage.announcement,
        "duration": stage.duration,
        "kind": "sealed" if sealed else ("current" if state.current == stage.id else "pending"),
        "sealed_at": _iso(sealed.at) if sealed else None,
        "reason": sealed.reason if sealed else None,
        "reason_label": REASON_LABELS.get(sealed.reason, sealed.reason) if sealed else None,
    }


def _role_history_block(stage: Stage, state: State, role_id: str) -> dict[str, Any]:
    """What one role may see about one stage (past or current)."""
    block = _block_base(stage, state)
    deciding = role_id in stage.options
    block["deciding"] = deciding
    block["private"] = stage.views.get(role_id, "") if state.order and _reached(stage, state) else ""
    if deciding and _reached(stage, state):
        cid = state.choices.get((stage.id, role_id))
        block["my_choice"] = cid
        block["my_choice_label"] = _choice_label(stage, role_id, cid)
        block["options"] = [{"id": o.id, "text": o.text} for o in stage.options[role_id]]
    else:
        block["my_choice"] = None
        block["options"] = []
    # Only reveal the path actually taken once sealed.
    sealed = state.sealed.get(stage.id)
    if sealed:
        block["next"] = sealed.next
    block["injections"] = [
        _injection_view(inj, role_id, host=False)
        for inj in sorted(_injections_for(state, stage.id), key=lambda i: (i.at, i.injection_id))
    ]
    return block


def _reached(stage: Stage, state: State) -> bool:
    return stage.id in state.order


def _host_history_block(stage: Stage, state: State) -> dict[str, Any]:
    block = _block_base(stage, state)
    sealed = state.sealed.get(stage.id)
    block["views"] = dict(stage.views)
    block["deciding_roles"] = list(stage.options.keys())
    options_out: dict[str, list[dict[str, Any]]] = {}
    choices_out: dict[str, Any] = {}
    for role_id, olist in stage.options.items():
        options_out[role_id] = [
            {"id": o.id, "text": o.text, "next": o.next} for o in olist
        ]
        cid = state.choices.get((stage.id, role_id))
        if cid is None and sealed is not None:
            cid = sealed.choices.get(role_id)  # None == timed out
            choices_out[role_id] = None if cid is None else {
                "id": cid,
                "label": _choice_label(stage, role_id, cid),
                "timeout": cid is None,
            }
        elif cid is not None:
            choices_out[role_id] = {"id": cid, "label": _choice_label(stage, role_id, cid)}
        else:
            choices_out[role_id] = None
    block["options"] = options_out
    block["choices"] = choices_out
    block["transitions"] = [{"when": t.when, "next": t.next} for t in stage.transitions]
    block["default_next"] = stage.default_next
    block["reached"] = _reached(stage, state)
    block["injections"] = [
        _injection_view(inj, None, host=True)
        for inj in sorted(_injections_for(state, stage.id), key=lambda i: (i.at, i.injection_id))
    ]
    return block


def build_snapshot(
    sess: dict[str, Any],
    d: DrillDef,
    state: State,
    viewer: tuple[str, str] | None,
    online: dict[str, bool] | None = None,
) -> dict[str, Any]:
    """viewer: ("host", None) or ("role", role_id) or ("none", None)."""
    kind, role_id = viewer or ("none", None)
    is_host = kind == "host"

    base: dict[str, Any] = {
        "kind": "host" if is_host else "role",
        "server_time": 0,  # replaced by the caller with the authoritative time
        "session": {
            "id": sess["id"],
            "status": state.status,
            "end_reason": state.end_reason,
            "end_reason_label": REASON_LABELS.get(state.end_reason or "", state.end_reason),
            "started_at": _iso(state.started_at),
            "ended_at": _iso(state.ended_at),
            "version": state.last_version,
            "current_stage": state.current,
            "deadline": _iso(state.deadline),
        },
        "drill": {
            "id": d.id,
            "name": d.name,
            "description": d.description,
            # Immutable script version pinned at session creation. Later
            # drafts/releases never alter this snapshot, branching or replay.
            "version": sess.get("drill_version"),
            "sha256": sess.get("drill_sha256"),
            "digest_short": (sess.get("drill_sha256") or "")[:12],
        },
        "participants": [],
        "history": [],
    }

    roster = []
    for rid, info in sorted(state.roster.items()):
        role = d.roles.get(rid)
        entry = {
            "role_id": rid,
            "role_name": role.name if role else rid,
            "display_name": info["display_name"],
            "joined_at": _iso(info["joined_at"]),
            "online": bool((online or {}).get(rid, False)),
        }
        roster.append(entry)
    base["participants"] = roster

    if is_host:
        base["history"] = [
            _host_history_block(d.stages[sid], state) for sid in d.stages
        ]
        base["roles"] = [
            {"id": r.id, "name": r.name, "invite": r.invite} for r in d.roles.values()
        ]
        if state.current:
            stage = d.stages[state.current]
            submitted = [
                rid for rid in stage.options
                if (stage.id, rid) in state.choices
            ]
            base["current_detail"] = {
                "deciding_roles": list(stage.options.keys()),
                "submitted_roles": submitted,
            }
    else:
        assert role_id is not None
        role = d.roles[role_id]
        base["me"] = {
            "role_id": role.id,
            "role_name": role.name,
        }
        # Only expose stages in definition order, with private data present
        # solely for stages that have opened.
        blocks = []
        for sid, stage in d.stages.items():
            if _reached(stage, state):
                blocks.append(_role_history_block(stage, state, role_id))
        base["history"] = blocks

    return base
