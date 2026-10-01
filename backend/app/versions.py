"""Immutable script versions: canonical content digests and structural diffs.

Every saved script version gets a SHA-256 *content digest* computed over the
canonical JSON form of the parsed definition. Semantically identical scripts
therefore always share a digest, regardless of YAML comments, key order or
whitespace; the digest is what proves that a session-bound snapshot can never
silently change.

``diff_definitions`` renders a structured comparison (meta fields / roles /
stages, and per-stage views, options and transition rules) used by the host
console to show what changed between any two versions.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
from typing import Any

from .definitions import DrillDef, Option, Role, Stage, Transition

FIELD_LABELS = {
    "name": "剧本名称",
    "description": "剧本说明",
    "initial_stage": "初始阶段",
    "title": "阶段标题",
    "public": "公开信息",
    "announcement": "阶段公告",
    "duration": "限时（秒）",
    "default_next": "默认下一阶段",
    "text": "文本",
    "next": "跳转目标",
    "invite": "邀请码",
}


# --------------------------------------------------------------------------- #
# digest
# --------------------------------------------------------------------------- #
def canonical_text(d: DrillDef) -> str:
    return json.dumps(
        dataclasses.asdict(d), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def canonical_dict_text(obj: dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def content_digest(d: DrillDef) -> str:
    return hashlib.sha256(canonical_text(d).encode("utf-8")).hexdigest()


def dict_digest(obj: dict[str, Any]) -> str:
    """Digest of a serialized definition dict (used when migrating old rows)."""
    return hashlib.sha256(canonical_dict_text(obj).encode("utf-8")).hexdigest()


def short_digest(sha: str | None) -> str:
    return (sha or "")[:12]


# --------------------------------------------------------------------------- #
# structured diff
# --------------------------------------------------------------------------- #
def _change(field: str, old: Any, new: Any) -> dict[str, Any]:
    return {"field": field, "label": FIELD_LABELS.get(field, field), "old": old, "new": new}


def _role_brief(role: Role) -> dict[str, Any]:
    return {"id": role.id, "name": role.name, "invite": role.invite}


def _option_json(o: Option) -> dict[str, Any]:
    return {"id": o.id, "text": o.text, "next": o.next}


def _role_name(d: DrillDef | None, rid: str) -> str:
    r = d.roles.get(rid) if d else None
    return r.name if r else rid


def _diff_meta(old: DrillDef, new: DrillDef) -> list[dict[str, Any]]:
    out = []
    for f in ("name", "description", "initial_stage"):
        ov, nv = getattr(old, f), getattr(new, f)
        if ov != nv:
            out.append(_change(f, ov, nv))
    return out


def _diff_roles(old: DrillDef, new: DrillDef) -> dict[str, Any]:
    old_ids = set(old.roles)
    new_ids = set(new.roles)
    changed: list[dict[str, Any]] = []
    for rid in sorted(old_ids & new_ids):
        fields: list[dict[str, Any]] = []
        if old.roles[rid].name != new.roles[rid].name:
            fields.append(_change("name", old.roles[rid].name, new.roles[rid].name))
        if old.roles[rid].invite != new.roles[rid].invite:
            fields.append(_change("invite", old.roles[rid].invite, new.roles[rid].invite))
        if fields:
            changed.append({"id": rid, "name": new.roles[rid].name, "fields": fields})
    return {
        "added": [_role_brief(new.roles[r]) for r in sorted(new_ids - old_ids)],
        "removed": [_role_brief(old.roles[r]) for r in sorted(old_ids - new_ids)],
        "changed": changed,
    }


def _diff_views(old_stage: Stage, new_stage: Stage, old: DrillDef, new: DrillDef) -> dict[str, Any]:
    old_rids = set(old_stage.views)
    new_rids = set(new_stage.views)
    changed = []
    for rid in sorted(old_rids & new_rids):
        if old_stage.views[rid] != new_stage.views[rid]:
            changed.append({
                "role_id": rid,
                "role_name": _role_name(new, rid) or _role_name(old, rid),
                "old": old_stage.views[rid],
                "new": new_stage.views[rid],
            })
    return {
        "added": [
            {"role_id": rid, "role_name": _role_name(new, rid), "text": new_stage.views[rid]}
            for rid in sorted(new_rids - old_rids)
        ],
        "removed": [
            {"role_id": rid, "role_name": _role_name(old, rid), "text": old_stage.views[rid]}
            for rid in sorted(old_rids - new_rids)
        ],
        "changed": changed,
    }


def _diff_options(
    old_stage: Stage, new_stage: Stage, old: DrillDef, new: DrillDef
) -> dict[str, Any]:
    """Per deciding role: added/removed roles and per-option text/next changes."""
    old_roles = set(old_stage.options)
    new_roles = set(new_stage.options)

    def role_entry(d: DrillDef, stage: Stage, rid: str) -> dict[str, Any]:
        return {
            "role_id": rid,
            "role_name": _role_name(d, rid),
            "options": [_option_json(o) for o in stage.options[rid]],
        }

    changed: list[dict[str, Any]] = []
    for rid in sorted(old_roles & new_roles):
        old_opts = {o.id: o for o in old_stage.options[rid]}
        new_opts = {o.id: o for o in new_stage.options[rid]}
        added = [_option_json(new_opts[i]) for i in sorted(set(new_opts) - set(old_opts))]
        removed = [_option_json(old_opts[i]) for i in sorted(set(old_opts) - set(new_opts))]
        opt_changed = []
        for oid in sorted(set(old_opts) & set(new_opts)):
            fields: list[dict[str, Any]] = []
            if old_opts[oid].text != new_opts[oid].text:
                fields.append(_change("text", old_opts[oid].text, new_opts[oid].text))
            if old_opts[oid].next != new_opts[oid].next:
                fields.append(_change("next", old_opts[oid].next, new_opts[oid].next))
            if fields:
                opt_changed.append({"id": oid, "fields": fields})
        if added or removed or opt_changed:
            changed.append({
                "role_id": rid,
                "role_name": _role_name(new, rid) or _role_name(old, rid),
                "added": added,
                "removed": removed,
                "changed": opt_changed,
            })
    return {
        "added_roles": [role_entry(new, new_stage, r) for r in sorted(new_roles - old_roles)],
        "removed_roles": [role_entry(old, old_stage, r) for r in sorted(old_roles - new_roles)],
        "changed": changed,
    }


def _tkey(t: Transition) -> str:
    return json.dumps(t.when, ensure_ascii=False, sort_keys=True)


def _tjson(t: Transition) -> dict[str, Any]:
    return {"when": dict(t.when), "next": t.next}


def _diff_transitions(old_stage: Stage, new_stage: Stage) -> dict[str, Any]:
    old_map = {_tkey(t): t for t in old_stage.transitions}
    new_map = {_tkey(t): t for t in new_stage.transitions}
    added = [_tjson(new_map[k]) for k in sorted(set(new_map) - set(old_map))]
    removed = [_tjson(old_map[k]) for k in sorted(set(old_map) - set(new_map))]
    changed = []
    for k in sorted(set(old_map) & set(new_map)):
        if old_map[k].next != new_map[k].next:
            changed.append({"old": _tjson(old_map[k]), "new": _tjson(new_map[k])})
    return {"added": added, "removed": removed, "changed": changed}


def _stage_brief(stage: Stage) -> dict[str, Any]:
    return {"id": stage.id, "title": stage.title}


def _diff_stage(old_stage: Stage, new_stage: Stage, old: DrillDef, new: DrillDef) -> dict[str, Any]:
    fields: list[dict[str, Any]] = []
    for f in ("title", "public", "announcement", "duration", "default_next"):
        ov, nv = getattr(old_stage, f), getattr(new_stage, f)
        if ov != nv:
            fields.append(_change(f, ov, nv))
    views = _diff_views(old_stage, new_stage, old, new)
    options = _diff_options(old_stage, new_stage, old, new)
    transitions = _diff_transitions(old_stage, new_stage)
    return {
        "id": new_stage.id,
        "title": new_stage.title,
        "fields": fields,
        "views": views,
        "options": options,
        "transitions": transitions,
    }


def _stage_has_changes(c: dict[str, Any]) -> bool:
    return bool(
        c["fields"]
        or c["views"]["added"] or c["views"]["removed"] or c["views"]["changed"]
        or c["options"]["added_roles"] or c["options"]["removed_roles"] or c["options"]["changed"]
        or c["transitions"]["added"] or c["transitions"]["removed"] or c["transitions"]["changed"]
    )


def diff_definitions(
    old: DrillDef | None, new: DrillDef | None
) -> dict[str, Any]:
    """Structured diff of two parsed definitions. Either side may be None
    (everything is added/removed respectively)."""
    if old is None and new is None:
        raise ValueError("diff needs at least one side")

    if old is None:
        assert new is not None
        return _all_added(new)
    if new is None:
        return _all_removed(old)

    meta = _diff_meta(old, new)
    roles = _diff_roles(old, new)

    old_ids = set(old.stages)
    new_ids = set(new.stages)
    stages_changed = []
    for sid in sorted(old_ids & new_ids):
        c = _diff_stage(old.stages[sid], new.stages[sid], old, new)
        if _stage_has_changes(c):
            stages_changed.append(c)
    stages = {
        "added": [_stage_brief(new.stages[s]) for s in sorted(new_ids - old_ids)],
        "removed": [_stage_brief(old.stages[s]) for s in sorted(old_ids - new_ids)],
        "changed": stages_changed,
    }

    has_changes = bool(
        meta
        or roles["added"] or roles["removed"] or roles["changed"]
        or stages["added"] or stages["removed"] or stages["changed"]
    )
    summary = {
        "meta_changed": len(meta),
        "roles_added": len(roles["added"]),
        "roles_removed": len(roles["removed"]),
        "roles_changed": len(roles["changed"]),
        "stages_added": len(stages["added"]),
        "stages_removed": len(stages["removed"]),
        "stages_changed": len(stages["changed"]),
    }
    return {
        "has_changes": has_changes,
        "summary": summary,
        "meta": meta,
        "roles": roles,
        "stages": stages,
    }


def _all_added(d: DrillDef) -> dict[str, Any]:
    return {
        "has_changes": True,
        "summary": {
            "meta_changed": 0,
            "roles_added": len(d.roles),
            "roles_removed": 0,
            "roles_changed": 0,
            "stages_added": len(d.stages),
            "stages_removed": 0,
            "stages_changed": 0,
        },
        "meta": [],
        "roles": {
            "added": [_role_brief(r) for r in d.roles.values()],
            "removed": [],
            "changed": [],
        },
        "stages": {
            "added": [_stage_brief(s) for s in d.stages.values()],
            "removed": [],
            "changed": [],
        },
    }


def _all_removed(d: DrillDef) -> dict[str, Any]:
    return {
        "has_changes": True,
        "summary": {
            "meta_changed": 0,
            "roles_added": 0,
            "roles_removed": len(d.roles),
            "roles_changed": 0,
            "stages_added": 0,
            "stages_removed": len(d.stages),
            "stages_changed": 0,
        },
        "meta": [],
        "roles": {
            "added": [],
            "removed": [_role_brief(r) for r in d.roles.values()],
            "changed": [],
        },
        "stages": {
            "added": [],
            "removed": [_stage_brief(s) for s in d.stages.values()],
            "changed": [],
        },
    }
