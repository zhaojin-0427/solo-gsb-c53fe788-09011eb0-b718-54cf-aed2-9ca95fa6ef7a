"""Loading and validation of presenter-authored drill definitions (YAML)."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


class DrillError(ValueError):
    """Raised when a drill definition fails validation."""


@dataclass
class Option:
    id: str
    text: str
    next: str | None = None
    timeout: bool = False  # this option represents "no choice in time"


@dataclass
class Transition:
    when: dict[str, Any]
    next: str | None  # None => end the drill


@dataclass
class Stage:
    id: str
    title: str
    public: str
    announcement: str
    duration: int | None  # seconds; None => presenter must advance manually
    default_next: str | None
    options: dict[str, list[Option]] = field(default_factory=dict)  # role_id -> options
    views: dict[str, str] = field(default_factory=dict)  # role_id -> private text
    transitions: list[Transition] = field(default_factory=list)


@dataclass
class Role:
    id: str
    name: str
    invite: str  # invite code, unique within the drill


@dataclass
class DrillDef:
    id: str
    name: str
    description: str
    roles: dict[str, Role]
    stages: dict[str, Stage]
    initial_stage: str

    def deciding_roles(self, stage: Stage) -> list[str]:
        return list(stage.options.keys())


def _require(obj: dict[str, Any], key: str, ctx: str) -> Any:
    if key not in obj or obj[key] in (None, ""):
        raise DrillError(f"{ctx}: missing required field '{key}'")
    return obj[key]


def parse_definition(raw: dict[str, Any]) -> DrillDef:
    if not isinstance(raw, dict):
        raise DrillError("top level must be a mapping")

    drill_id = str(_require(raw, "id", "drill"))
    name = str(_require(raw, "name", f"drill {drill_id}"))
    description = str(raw.get("description", ""))

    roles: dict[str, Role] = {}
    for r in raw.get("roles") or []:
        rid = str(_require(r, "id", "role"))
        if rid in roles:
            raise DrillError(f"drill {drill_id}: duplicate role id '{rid}'")
        roles[rid] = Role(
            id=rid,
            name=str(_require(r, "name", f"role {rid}")),
            invite=str(_require(r, "invite", f"role {rid}")),
        )
    if not roles:
        raise DrillError(f"drill {drill_id}: at least one role is required")
    invites = [r.invite for r in roles.values()]
    if len(set(invites)) != len(invites):
        raise DrillError(f"drill {drill_id}: invite codes must be unique")

    stages: dict[str, Stage] = {}
    first_stage: str | None = None
    for s in raw.get("stages") or []:
        sid = str(_require(s, "id", "stage"))
        if sid in stages:
            raise DrillError(f"drill {drill_id}: duplicate stage id '{sid}'")
        if first_stage is None:
            first_stage = sid

        duration = s.get("duration")
        if duration is not None:
            if not isinstance(duration, int) or isinstance(duration, bool) or duration <= 0:
                raise DrillError(f"stage {sid}: duration must be a positive integer (seconds)")

        stage = Stage(
            id=sid,
            title=str(_require(s, "title", f"stage {sid}")),
            public=str(s.get("public", "")),
            announcement=str(s.get("announcement", s.get("public", ""))),
            duration=duration,
            default_next=s.get("default_next"),
        )

        opts = s.get("options") or {}
        if not isinstance(opts, dict):
            raise DrillError(f"stage {sid}: options must be a mapping of role id to list")
        for role_id, olist in opts.items():
            if role_id not in roles:
                raise DrillError(f"stage {sid}: options reference unknown role '{role_id}'")
            if not olist:
                raise DrillError(f"stage {sid}: role '{role_id}' has an empty option list")
            seen: set[str] = set()
            parsed: list[Option] = []
            for o in olist:
                oid = str(_require(o, "id", f"stage {sid} option"))
                if oid in seen:
                    raise DrillError(f"stage {sid}: duplicate option id '{oid}' for role '{role_id}'")
                seen.add(oid)
                parsed.append(
                    Option(
                        id=oid,
                        text=str(_require(o, "text", f"stage {sid} option {oid}")),
                        next=o.get("next"),
                        timeout=bool(o.get("timeout", False)),
                    )
                )
            stage.options[role_id] = parsed

        views = s.get("views") or {}
        if not isinstance(views, dict):
            raise DrillError(f"stage {sid}: views must be a mapping of role id to text")
        for role_id, text in views.items():
            if role_id not in roles:
                raise DrillError(f"stage {sid}: view references unknown role '{role_id}'")
            stage.views[role_id] = str(text)

        for t in s.get("transitions") or []:
            when = _require(t, "when", f"stage {sid} transition")
            if not isinstance(when, dict) or not when:
                raise DrillError(f"stage {sid}: transition 'when' must be a non-empty mapping")
            for role_id in when:
                if role_id not in roles:
                    raise DrillError(f"stage {sid}: transition references unknown role '{role_id}'")
            stage.transitions.append(Transition(when=dict(when), next=t.get("next")))

        stages[sid] = stage

    if not stages:
        raise DrillError(f"drill {drill_id}: at least one stage is required")
    assert first_stage is not None

    initial = str(raw.get("initial_stage", first_stage))
    if initial not in stages:
        raise DrillError(f"drill {drill_id}: initial_stage '{initial}' does not exist")

    # Validate every stage target exists.
    for stage in stages.values():
        if stage.default_next is not None and stage.default_next not in stages:
            raise DrillError(f"stage {stage.id}: default_next '{stage.default_next}' unknown")
        for i, t in enumerate(stage.transitions):
            if t.next is not None and t.next not in stages:
                raise DrillError(f"stage {stage.id}: transition[{i}] target '{t.next}' unknown")
        for role_id, olist in stage.options.items():
            for o in olist:
                if o.next is not None and o.next not in stages:
                    raise DrillError(
                        f"stage {stage.id}: option {role_id}/{o.id} next '{o.next}' unknown"
                    )

    return DrillDef(
        id=drill_id,
        name=name,
        description=description,
        roles=roles,
        stages=stages,
        initial_stage=initial,
    )


def def_from_json(data: dict[str, Any]) -> DrillDef:
    """Reconstruct a DrillDef from its ``dataclasses.asdict`` JSON form.

    Lives here (rather than next to the engine) so schema migration code can
    deserialize legacy definitions without importing the engine layer.
    """
    roles = {rid: Role(**r) for rid, r in data["roles"].items()}
    stages: dict[str, Stage] = {}
    for sid, s in data["stages"].items():
        s = dict(s)
        s["options"] = {
            rid: [Option(**o) for o in olist] for rid, olist in s["options"].items()
        }
        s["transitions"] = [Transition(**t) for t in s["transitions"]]
        stages[sid] = Stage(**s)
    return DrillDef(
        id=data["id"],
        name=data["name"],
        description=data["description"],
        roles=roles,
        stages=stages,
        initial_stage=data["initial_stage"],
    )


def load_file(path: Path) -> DrillDef:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise DrillError(f"{path.name}: invalid YAML ({exc})") from exc
    d = parse_definition(raw or {})
    if d.id != path.stem:
        raise DrillError(f"{path.name}: drill id '{d.id}' must match file name '{path.stem}'")
    return d


def load_directory(directory: Path) -> dict[str, DrillDef]:
    out: dict[str, DrillDef] = {}
    if not directory.exists():
        return out
    for path in sorted(directory.glob("*.y*ml")):
        d = load_file(path)
        out[d.id] = d
    return out
