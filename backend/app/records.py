"""Deterministic replay and record export.

Replay: fold the immutable event log up to each version and snapshot the state.
The same log always yields the same frames — the server keeps no hidden state
that affects progression (all timers only trigger settle events).
"""
from __future__ import annotations

import json
from typing import Any

from .definitions import DrillDef
from .engine import (
    E_JOINT_INJECTION,
    E_PARTICIPANT_JOINED,
    E_SESSION_ENDED,
    E_SESSION_STARTED,
    E_STAGE_CHOICE,
    E_STAGE_OPENED,
    E_STAGE_SEALED,
    State,
    reduce_events,
)
from .snapshots import REASON_LABELS, build_snapshot


def load_events(db_rows: list[Any]) -> list[dict[str, Any]]:
    return [
        {
            "version": r["version"],
            "seq": r["seq"],
            "type": r["type"],
            "actor": r["actor"],
            "at": float(r["at"]),
            "payload": json.loads(r["payload"]),
        }
        for r in db_rows
    ]


def build_replay(
    sess: dict[str, Any],
    d: DrillDef,
    events: list[dict[str, Any]],
    viewer: tuple[str, str] | None,
) -> dict[str, Any]:
    """One frame per committed version (plus v0 = pre-start)."""
    frames: list[dict[str, Any]] = []
    versions = sorted({e["version"] for e in events})

    # frame 0: before anything happened
    st0 = State(status="created")
    frames.append({"version": 0, "changes": [], "snapshot": _snap(sess, d, st0, viewer)})

    for v in versions:
        upto = [e for e in events if e["version"] <= v]
        st = reduce_events(upto)
        evv = [e for e in events if e["version"] == v]
        frames.append(
            {
                "version": v,
                "changes": [_change(e, d) for e in sorted(evv, key=lambda e: e["seq"])],
                "snapshot": _snap(sess, d, st, viewer),
            }
        )
    return {
        "session_id": sess["id"],
        "drill_id": d.id,
        "drill_name": d.name,
        "drill_version": sess.get("drill_version"),
        "drill_sha256": sess.get("drill_sha256"),
        "viewer": "host" if (viewer and viewer[0] == "host") else viewer[1],
        "final_version": max(versions, default=0),
        "frames": frames,
        "events": events if (viewer and viewer[0] == "host") else None,
    }


def _snap(sess: dict[str, Any], d: DrillDef, st: State, viewer) -> dict[str, Any]:
    return build_snapshot(sess, d, st, viewer, online=None)


def _change(ev: dict[str, Any], d: DrillDef) -> dict[str, Any]:
    p = ev["payload"]
    t = ev["type"]
    base = {"seq": ev["seq"], "type": t, "at": ev["at"], "actor": ev["actor"]}
    if t == E_SESSION_STARTED:
        base["label"] = "演练开始"
        base["stage_id"] = p["stage_id"]
    elif t == E_PARTICIPANT_JOINED:
        role = d.roles.get(p["role_id"])
        base["label"] = f"{p['display_name']}（{role.name if role else p['role_id']}）加入"
    elif t == E_STAGE_CHOICE:
        stage = d.stages.get(p["stage_id"])
        label = None
        if stage:
            for o in stage.options.get(p["role_id"], []):
                if o.id == p["choice_id"]:
                    label = o.text
        role = d.roles.get(p["role_id"])
        base["label"] = f"{role.name if role else p['role_id']} 选择：{label or p['choice_id']}"
        base["stage_id"] = p["stage_id"]
        base["role_id"] = p["role_id"]
        base["choice_id"] = p["choice_id"]
    elif t == E_STAGE_SEALED:
        stage = d.stages.get(p["stage_id"])
        base["label"] = (
            f"阶段「{stage.title if stage else p['stage_id']}」结算"
            f"（{REASON_LABELS.get(p['reason'], p['reason'])}）"
        )
        base["stage_id"] = p["stage_id"]
        base["reason"] = p["reason"]
        base["next"] = p["next"]
    elif t == E_STAGE_OPENED:
        stage = d.stages.get(p["stage_id"])
        base["label"] = f"进入阶段「{stage.title if stage else p['stage_id']}」"
        base["stage_id"] = p["stage_id"]
        base["deadline"] = p.get("deadline")
    elif t == E_SESSION_ENDED:
        base["label"] = f"演练结束（{REASON_LABELS.get(p['reason'], p['reason'])}）"
        base["reason"] = p["reason"]
    elif t == E_JOINT_INJECTION:
        base["label"] = f"联演注入追加到阶段「{p.get('stage_id')}」：{p.get('title') or '（无标题）'}"
        base["stage_id"] = p["stage_id"]
        base["joint_id"] = p.get("joint_id")
        base["gate_id"] = p.get("gate_id")
        base["injection_id"] = p.get("injection_id")
    return base


# --------------------------------------------------------------------------- #
# records
# --------------------------------------------------------------------------- #
def personal_record(
    sess: dict[str, Any], d: DrillDef, events: list[dict[str, Any]], role_id: str
) -> dict[str, Any]:
    """A participant's record. Contains only that role's own private views and
    choices plus public information; other roles' private text never appears."""
    st = reduce_events(events)
    role = d.roles[role_id]
    stages_out = []
    for sid in d.stages:
        stage = d.stages[sid]
        if sid not in st.order:
            continue
        sealed = st.sealed.get(sid)
        cid = st.choices.get((sid, role_id))
        if cid is None and sealed is not None and role_id in stage.options:
            cid = sealed.choices.get(role_id)
        label = None
        if cid is not None:
            for o in stage.options.get(role_id, []):
                if o.id == cid:
                    label = o.text
        entry: dict[str, Any] = {
            "stage_id": sid,
            "title": stage.title,
            "public": stage.public,
            "announcement": stage.announcement,
            "my_view": stage.views.get(role_id, ""),
            "deciding": role_id in stage.options,
            "my_choice": cid,
            "my_choice_label": label,
            "timed_out": sealed is not None
            and role_id in stage.options
            and sealed.choices.get(role_id) is None,
            "settled_reason": sealed.reason if sealed else None,
            "next_stage": sealed.next if sealed else None,
            # Joint injections are part of THIS session's own event record;
            # only the public text + the viewer's own private view appear.
            "joint_injections": [
                {
                    "injection_id": inj.injection_id,
                    "joint_id": inj.joint_id,
                    "gate_id": inj.gate_id,
                    "title": inj.title,
                    "public": inj.public,
                    "my_view": inj.views.get(role_id, ""),
                    "at": inj.at,
                    "version": inj.version,
                }
                for inj in sorted(st.injections.values(), key=lambda i: (i.at, i.injection_id))
                if inj.stage_id == sid
            ],
        }
        stages_out.append(entry)

    roster_self = st.roster.get(role_id)
    return {
        "kind": "personal",
        "drill": {
            "id": d.id, "name": d.name, "description": d.description,
            "version": sess.get("drill_version"),
            "sha256": sess.get("drill_sha256"),
            "digest_short": (sess.get("drill_sha256") or "")[:12],
        },
        "session_id": sess["id"],
        "drill_version": sess.get("drill_version"),
        "drill_sha256": sess.get("drill_sha256"),
        "role": {"id": role.id, "name": role.name},
        "display_name": roster_self["display_name"] if roster_self else role.name,
        "joined_at": roster_self["joined_at"] if roster_self else None,
        "started_at": st.started_at,
        "ended_at": st.ended_at,
        "end_reason": st.end_reason,
        "stages": stages_out,
    }


def host_record(
    sess: dict[str, Any], d: DrillDef, events: list[dict[str, Any]]
) -> dict[str, Any]:
    st = reduce_events(events)
    stages_out = []
    for sid in d.stages:
        stage = d.stages[sid]
        sealed = st.sealed.get(sid)
        choices = {}
        if sealed is not None:
            for rid, cid in sealed.choices.items():
                label = None
                if cid is not None:
                    for o in stage.options.get(rid, []):
                        if o.id == cid:
                            label = o.text
                choices[rid] = {"choice_id": cid, "label": label, "timeout": cid is None}
        stages_out.append(
            {
                "stage_id": sid,
                "title": stage.title,
                "public": stage.public,
                "announcement": stage.announcement,
                "duration": stage.duration,
                "reached": sid in st.order,
                "views": dict(stage.views),
                "options": {
                    rid: [{"id": o.id, "text": o.text, "next": o.next} for o in olist]
                    for rid, olist in stage.options.items()
                },
                "transitions": [
                    {"when": t.when, "next": t.next} for t in stage.transitions
                ],
                "default_next": stage.default_next,
                "choices": choices,
                "joint_injections": [
                    {
                        "injection_id": inj.injection_id,
                        "joint_id": inj.joint_id,
                        "gate_id": inj.gate_id,
                        "title": inj.title,
                        "public": inj.public,
                        "views": dict(inj.views),
                        "at": inj.at,
                        "version": inj.version,
                    }
                    for inj in sorted(st.injections.values(), key=lambda i: (i.at, i.injection_id))
                    if inj.stage_id == sid
                ],
                "sealed": {
                    "reason": sealed.reason,
                    "reason_label": REASON_LABELS.get(sealed.reason, sealed.reason),
                    "next": sealed.next,
                    "at": sealed.at,
                    "version": sealed.version,
                }
                if sealed
                else None,
            }
        )
    return {
        "kind": "host",
        "drill": {
            "id": d.id,
            "name": d.name,
            "description": d.description,
            "version": sess.get("drill_version"),
            "sha256": sess.get("drill_sha256"),
            "digest_short": (sess.get("drill_sha256") or "")[:12],
            "roles": [
                {"id": r.id, "name": r.name, "invite": r.invite}
                for r in d.roles.values()
            ],
        },
        "session_id": sess["id"],
        "drill_version": sess.get("drill_version"),
        "drill_sha256": sess.get("drill_sha256"),
        "status": st.status,
        "started_at": st.started_at,
        "ended_at": st.ended_at,
        "end_reason": st.end_reason,
        "participants": [
            {
                "role_id": rid,
                "role_name": d.roles[rid].name if rid in d.roles else rid,
                "display_name": info["display_name"],
                "joined_at": info["joined_at"],
            }
            for rid, info in sorted(st.roster.items())
        ],
        "stages": stages_out,
        "events": events,
        "final_version": st.last_version,
    }


# --------------------------------------------------------------------------- #
# markdown rendering
# --------------------------------------------------------------------------- #
def _md_escape(text: Any) -> str:
    return str(text if text is not None else "").replace("\r\n", "\n")


def render_personal_md(rec: dict[str, Any]) -> str:
    lines = [
        f"# 应急演练个人记录 — {_md_escape(rec['drill']['name'])}",
        "",
        f"- 演练：{_md_escape(rec['drill']['name'])}（`{rec['drill']['id']}`）",
        f"- 剧本版本：v{rec.get('drill_version')}（`{_md_escape((rec.get('drill_sha256') or '')[:12])}`）",
        f"- 会话：`{rec['session_id']}`",
        f"- 角色：{_md_escape(rec['role']['name'])}",
        f"- 姓名：{_md_escape(rec['display_name'])}",
        f"- 结束原因：{REASON_LABELS.get(rec.get('end_reason') or '', rec.get('end_reason') or '—')}",
        "",
        "> 本记录仅包含您本人的私有信息与公开信息，不含其他角色的隐藏内容。",
        "",
    ]
    for i, s in enumerate(rec["stages"], 1):
        lines += [
            f"## {i}. {_md_escape(s['title'])}  `{s['stage_id']}`",
            "",
            f"**公开信息：** {_md_escape(s['public'])}",
            "",
        ]
        if s["my_view"]:
            lines += [f"**本角色可见：** {_md_escape(s['my_view'])}", ""]
        if s["deciding"]:
            if s["timed_out"]:
                lines += ["**我的决定：** ⏱ 未在截止前提交（超时）", ""]
            elif s["my_choice"]:
                lines += [f"**我的决定：** {_md_escape(s['my_choice_label'])}", ""]
        for inj in s.get("joint_injections", []):
            lines += [
                "### 🔗 联演注入：" + _md_escape(inj["title"]),
                "",
                f"公开信息：{_md_escape(inj['public'])}",
                "",
            ]
            if inj["my_view"]:
                lines += [f"本角色可见：{_md_escape(inj['my_view'])}", ""]
        if s["settled_reason"]:
            lines += [
                f"*结算方式：{REASON_LABELS.get(s['settled_reason'], s['settled_reason'])}*",
                "",
            ]
    return "\n".join(lines)


def render_host_md(rec: dict[str, Any]) -> str:
    lines = [
        f"# 应急演练主持人全量记录 — {_md_escape(rec['drill']['name'])}",
        "",
        f"- 演练：{_md_escape(rec['drill']['name'])}（`{rec['drill']['id']}`）",
        f"- 剧本版本：v{rec.get('drill_version')}（`{_md_escape((rec.get('drill_sha256') or '')[:12])}`，会话创建时固化）",
        f"- 会话：`{rec['session_id']}`",
        f"- 状态：{rec['status']}",
        f"- 结束原因：{REASON_LABELS.get(rec.get('end_reason') or '', rec.get('end_reason') or '—')}",
        f"- 最终版本号：{rec['final_version']}",
        "",
        "## 角色与邀请码",
        "",
        "| 角色 | 邀请码 | 参与者 | 加入时间 |",
        "| --- | --- | --- | --- |",
    ]
    pmap = {p["role_id"]: p for p in rec["participants"]}
    for r in rec["drill"]["roles"]:
        p = pmap.get(r["id"])
        lines.append(
            f"| {_md_escape(r['name'])} | `{r['invite']}` | "
            f"{_md_escape(p['display_name']) if p else '—'} | "
            f"{p['joined_at'] if p else '—'} |"
        )
    lines.append("")

    n = 0
    for s in rec["stages"]:
        if not s["reached"]:
            continue
        n += 1
        lines += [f"## {n}. {_md_escape(s['title'])}  `{s['stage_id']}`", ""]
        lines.append(f"**公开信息：** {_md_escape(s['public'])}")
        lines.append("")
        if s["duration"]:
            lines.append(f"**限时：** {s['duration']} 秒")
            lines.append("")
        for rid, text in s["views"].items():
            rname = _role_name(rec, rid)
            lines += [f"**{rname} 可见：** {_md_escape(text)}", ""]
        if s["options"]:
            lines += ["**限时选项：**", ""]
            for rid, olist in s["options"].items():
                rname = _role_name(rec, rid)
                lines.append(f"- {rname}:")
                for o in olist:
                    nxt = f" → `{o['next']}`" if o["next"] else ""
                    lines.append(f"  - `{o['id']}` {_md_escape(o['text'])}{nxt}")
            lines.append("")
        if s["transitions"]:
            lines.append("**条件分支：**")
            for t in s["transitions"]:
                cond = ", ".join(f"{_role_name(rec, k)}={v}" for k, v in t["when"].items())
                lines.append(f"- 当 {cond} → `{t['next'] or '（结束）'}`")
            lines.append("")
        if s["choices"]:
            lines += ["**实际决定：**", ""]
            for rid, c in s["choices"].items():
                rname = _role_name(rec, rid)
                if c["timeout"]:
                    lines.append(f"- {rname}: ⏱ 超时未提交")
                else:
                    lines.append(f"- {rname}: {_md_escape(c['label'] or c['choice_id'])}")
            lines.append("")
        for inj in s.get("joint_injections", []):
            lines += [
            f"### 🔗 联演注入（v{inj['version']}）：{_md_escape(inj['title'])}",
            "",
            f"公开信息：{_md_escape(inj['public'])}",
            ]
            for rid, text in inj["views"].items():
                lines.append(f"- 仅 {_role_name(rec, rid)} 可见：{_md_escape(text)}")
            lines.append("")
        if s["sealed"]:
            lines += [
                f"*结算方式：{s['sealed']['reason_label']}；下一阶段："
                f"`{s['sealed']['next'] or '（结束）'}`；版本 v{s['sealed']['version']}*",
                "",
            ]

    lines += ["## 事件日志", ""]
    for e in rec["events"]:
        lines.append(
            f"- v{e['version']}.{e['seq']} {e['type']} · {e['actor']} · {e['at']}"
        )
    return "\n".join(lines)


def _role_name(rec: dict[str, Any], role_id: str) -> str:
    for r in rec["drill"]["roles"]:
        if r["id"] == role_id:
            return r["name"]
    return role_id
