"""Drill engine: pure event-sourced state reducer + atomic SQLite commits.

Event log (all timestamps are POSIX seconds, UTC)
-------------------------------------------------
A "version" is one atomic commit; ``seq`` orders the events inside it.
Every commit takes ``version = last_version + 1`` (never hardcoded) and
assigns ``seq`` from 0 upwards in a fixed causal order, so the primary key
(session_id, version, seq) can never collide and any version prefix replays
deterministically:

  join            v+1:  0 participant_joined {role_id, display_name, at}
  start           v+1:  0 session_started {at, stage_id, deadline}
                        1 stage_opened {stage_id, deadline, at}
  submit          v+1:  0 stage_choice {stage_id, role_id, choice_id, at}
    …last choice       1 stage_sealed {stage_id, reason, choices, next, at}
    (auto-settle)      2 stage_opened | session_ended
  submit rejected v+1:  0 stage_sealed, 1 stage_opened | session_ended
  (past deadline)
  settle          v+1:  0 stage_sealed, 1 stage_opened | session_ended
  (deadline/host)      A deadline callback carries the deadline captured when
                       its timer was armed (expected_deadline); inside the
                       transaction it must equal state.deadline exactly,
                       otherwise the stale callback idles with no events.
  end (created)   v+1:  0 session_ended {reason, at}
  end (running)   v+1:  0 stage_sealed, 1 stage_opened | session_ended
                    (if the seal only advanced the stage)
                  v+2:  0 session_ended {reason, at}

Replay rebuilds state by folding events in (version, seq) order —
deterministic for any version prefix.
"""
from __future__ import annotations

import asyncio
import json
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

from .db import Database, dumps
from .definitions import DrillDef, Option, Stage, def_from_json
from .versions import content_digest, diff_definitions

E_SESSION_STARTED = "session_started"
E_PARTICIPANT_JOINED = "participant_joined"
E_STAGE_CHOICE = "stage_choice"
E_STAGE_SEALED = "stage_sealed"
E_STAGE_OPENED = "stage_opened"
E_SESSION_ENDED = "session_ended"
# A one-shot joint-exercise injection appended to the TARGET session's own
# event log by the orchestrator. It occupies a normal monotonic version, so
# every session keeps an independent, deterministically replayable record.
E_JOINT_INJECTION = "joint_injection"


class EngineError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


# --------------------------------------------------------------------------- #
# pure helpers
# --------------------------------------------------------------------------- #
def now_ts() -> float:
    return time.time()


def new_id(nbytes: int = 4) -> str:
    return secrets.token_hex(nbytes)  # 8-char session id


def new_token() -> str:
    return secrets.token_urlsafe(18)


def dataclasses_asdict(d: DrillDef) -> dict[str, Any]:
    import dataclasses

    return dataclasses.asdict(d)


def _ts_or_none(value: str | float | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def find_option(stage: Stage, role_id: str, choice_id: str) -> Option | None:
    for o in stage.options.get(role_id, []):
        if o.id == choice_id:
            return o
    return None


def evaluate(stage: Stage, choices: dict[str, str | None]) -> str | None:
    """Decide the next stage id from sealed choices.

    Priority: explicit option ``next`` of single decision holder, then ordered
    conditional transitions (first fully matching wins), then default_next.
    ``None`` means the drill ends.
    """
    for role_id, olist in stage.options.items():
        cid = choices.get(role_id)
        if cid is not None:
            opt = find_option(stage, role_id, cid)
            if opt is not None and opt.next is not None:
                return opt.next

    for t in stage.transitions:
        if all(choices.get(role) == want for role, want in t.when.items()):
            return t.next
    return stage.default_next


@dataclass
class SealedStage:
    stage_id: str
    version: int
    at: float
    reason: str
    choices: dict[str, str | None]
    next: str | None


@dataclass
class Injection:
    """A joint-exercise injection delivered into this session."""
    injection_id: str
    joint_id: str
    gate_id: str
    stage_id: str
    title: str
    public: str
    views: dict[str, str] = field(default_factory=dict)  # role_id -> private text
    version: int = 0
    at: float = 0.0


@dataclass
class State:
    status: str = "created"  # created|running|ended
    end_reason: str | None = None
    current: str | None = None
    deadline: float | None = None
    started_at: float | None = None
    ended_at: float | None = None
    last_version: int = 0
    choices: dict[tuple[str, str], str] = field(default_factory=dict)
    sealed: dict[str, SealedStage] = field(default_factory=dict)
    order: list[str] = field(default_factory=list)
    roster: dict[str, dict[str, Any]] = field(default_factory=dict)
    # joint_injection event payloads keyed by injection id; the dict shape is
    # reconstructed by reduce_events for deterministic replay/equality.
    injections: dict[str, Injection] = field(default_factory=dict)


def reduce_events(events: list[dict[str, Any]]) -> State:
    st = State()
    for ev in events:
        st.last_version = ev["version"]
        t = ev["type"]
        p = ev["payload"]
        if t == E_SESSION_STARTED:
            st.status = "running"
            st.started_at = p["at"]
        elif t == E_PARTICIPANT_JOINED:
            st.roster[p["role_id"]] = {
                "display_name": p["display_name"],
                "joined_at": p["at"],
            }
        elif t == E_STAGE_CHOICE:
            st.choices[(p["stage_id"], p["role_id"])] = p["choice_id"]
        elif t == E_STAGE_OPENED:
            st.current = p["stage_id"]
            st.deadline = p.get("deadline")
            if p["stage_id"] not in st.order:
                st.order.append(p["stage_id"])
        elif t == E_STAGE_SEALED:
            st.current = None
            st.deadline = None
            st.sealed[p["stage_id"]] = SealedStage(
                stage_id=p["stage_id"],
                version=ev["version"],
                at=p["at"],
                reason=p["reason"],
                choices=dict(p["choices"]),
                next=p["next"],
            )
        elif t == E_SESSION_ENDED:
            st.status = "ended"
            st.end_reason = p["reason"]
            st.ended_at = p["at"]
            st.current = None
            st.deadline = None
        elif t == E_JOINT_INJECTION:
            st.injections[p["injection_id"]] = Injection(
                injection_id=p["injection_id"],
                joint_id=p.get("joint_id", ""),
                gate_id=p.get("gate_id", ""),
                stage_id=p["stage_id"],
                title=p.get("title", ""),
                public=p.get("public", ""),
                views=dict(p.get("views", {})),
                version=ev["version"],
                at=p["at"],
            )
    return st


# --------------------------------------------------------------------------- #
# engine
# --------------------------------------------------------------------------- #
class Engine:
    def __init__(self, db: Database):
        self.db = db
        self._locks: dict[str, asyncio.Lock] = {}
        # Serialise draft/publish edits per drill. Single process anyway; the
        # BEGIN IMMEDIATE transaction additionally guards external writers.
        self._drill_locks: dict[str, asyncio.Lock] = {}

    def lock_for(self, session_id: str) -> asyncio.Lock:
        lk = self._locks.get(session_id)
        if lk is None:
            lk = asyncio.Lock()
            self._locks[session_id] = lk
        return lk

    def drill_lock_for(self, drill_id: str) -> asyncio.Lock:
        lk = self._drill_locks.get(drill_id)
        if lk is None:
            lk = asyncio.Lock()
            self._drill_locks[drill_id] = lk
        return lk

    # ------------------------------------------------------------------ #
    # script versions
    # ------------------------------------------------------------------ #
    async def _drill_row(self, drill_id: str) -> dict[str, Any] | None:
        row = await self.db.one(
            "SELECT id, name, description, created_at, published_version, draft_version "
            "FROM drills WHERE id=?",
            (drill_id,),
        )
        return dict(row) if row else None

    async def _max_version(self, drill_id: str) -> int:
        row = await self.db.one(
            "SELECT MAX(version) AS m FROM drill_versions WHERE drill_id=?",
            (drill_id,),
        )
        return (row["m"] if row and row["m"] is not None else 0)

    async def _get_version_row(self, drill_id: str, version: int) -> dict[str, Any] | None:
        row = await self.db.one(
            "SELECT drill_id, version, status, definition, yaml, sha256, base_version, "
            "created_at, published_at FROM drill_versions WHERE drill_id=? AND version=?",
            (drill_id, version),
        )
        return dict(row) if row else None

    @staticmethod
    def _version_meta(row: dict[str, Any], with_yaml: bool = False) -> dict[str, Any]:
        meta = {
            "version": row["version"],
            "status": row["status"],
            "sha256": row["sha256"],
            "digest_short": (row["sha256"] or "")[:12],
            "base_version": row["base_version"],
            "created_at": _ts_or_none(row["created_at"]),
            "published_at": _ts_or_none(row["published_at"]),
        }
        if with_yaml:
            meta["yaml"] = row["yaml"]
        return meta

    async def list_drills(self) -> list[dict[str, Any]]:
        """Drill summary with the current release/draft pointers."""
        rows = await self.db.all(
            "SELECT id, name, description, created_at, published_version, draft_version "
            "FROM drills ORDER BY id"
        )
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append({
                "id": r["id"],
                "name": r["name"],
                "description": r["description"],
                "published_version": r["published_version"],
                "draft_version": r["draft_version"],
                "latest_version": max(
                    [v for v in (r["published_version"], r["draft_version"]) if v is not None],
                    default=0,
                ),
                "created_at": _ts_or_none(r["created_at"]),
            })
        return out

    async def list_versions(self, drill_id: str) -> list[dict[str, Any]]:
        if await self._drill_row(drill_id) is None:
            raise EngineError(f"drill '{drill_id}' not found", 404)
        rows = await self.db.all(
            "SELECT drill_id, version, status, definition, yaml, sha256, base_version, "
            "created_at, published_at FROM drill_versions WHERE drill_id=? "
            "ORDER BY version DESC",
            (drill_id,),
        )
        return [self._version_meta(dict(r), with_yaml=True) for r in rows]

    async def get_version(self, drill_id: str, version: int, with_yaml: bool = True) -> dict[str, Any]:
        row = await self._get_version_row(drill_id, version)
        if row is None:
            raise EngineError(f"版本 v{version} 不存在", 404)
        return self._version_meta(row, with_yaml=with_yaml)

    async def get_version_def(self, drill_id: str, version: int) -> DrillDef:
        row = await self._get_version_row(drill_id, version)
        if row is None:
            raise EngineError(f"版本 v{version} 不存在", 404)
        return def_from_json(json.loads(row["definition"]))

    async def save_draft(
        self,
        d: DrillDef,
        yaml_text: str,
        base_version: int | None,
    ) -> dict[str, Any]:
        """Save uploaded/edited YAML as a new immutable draft version.

        A drill only ever has one open draft: saving again supersedes the
        previous draft row (it stays on disk forever with status 'superseded').
        Semantically identical content reuses the existing version — drafts
        are deduplicated by SHA-256 so re-saving the same YAML is idempotent.
        ``base_version`` records what the author started from and must be the
        current release (or None for a brand-new drill).
        """
        async with self.drill_lock_for(d.id):
            await self.db.begin()
            try:
                drill = await self._drill_row(d.id)
                digest = content_digest(d)
                now = str(now_ts())

                if drill is None:
                    if base_version is not None:
                        raise EngineError("新剧本的基准版本必须为空", 400)
                    next_v = 1
                    await self.db.db.execute(
                        "INSERT INTO drills(id, name, description, definition, created_at, "
                        "published_version, draft_version) VALUES(?,?,?,?,?,NULL,1)",
                        (d.id, d.name, d.description, dumps(dataclasses_asdict(d)), now),
                    )
                else:
                    if base_version is not None and base_version != drill["published_version"]:
                        raise EngineError(
                            f"基准版本不是当前发布版（当前为 v{drill['published_version']}），"
                            "请从最新版本复制后再编辑",
                            409,
                        )
                    # Idempotent draft: identical content already stored?
                    dup = await self.db.one(
                        "SELECT version FROM drill_versions WHERE drill_id=? AND sha256=? "
                        "ORDER BY version DESC LIMIT 1",
                        (d.id, digest),
                    )
                    if dup is not None:
                        await self.db.commit()
                        meta = await self.get_version(d.id, dup["version"], with_yaml=False)
                        return {"id": d.id, **meta, "reused": True}

                    old_draft = drill["draft_version"]
                    next_v = (await self._max_version(d.id)) + 1
                    if old_draft is not None:
                        await self.db.db.execute(
                            "UPDATE drill_versions SET status='superseded' "
                            "WHERE drill_id=? AND version=? AND status='draft'",
                            (d.id, old_draft),
                        )
                    await self.db.db.execute(
                        "UPDATE drills SET name=?, description=?, draft_version=? WHERE id=?",
                        (d.name, d.description, next_v, d.id),
                    )

                await self.db.db.execute(
                    "INSERT INTO drill_versions"
                    "(drill_id, version, status, definition, yaml, sha256, base_version, "
                    "created_at, published_at) VALUES(?,?, 'draft', ?,?,?,?,?,NULL)",
                    (d.id, next_v, dumps(dataclasses_asdict(d)), yaml_text, digest,
                     base_version, now),
                )
                await self.db.commit()
            except Exception:
                await self.db.rollback()
                raise

        meta = await self.get_version(d.id, next_v, with_yaml=False)
        return {"id": d.id, **meta, "reused": False}

    async def publish_drill(
        self, drill_id: str, version: int, base_version: int | None
    ) -> dict[str, Any]:
        """Release a draft. Optimistic concurrency: the version it was based
        on must still be the current release, otherwise a 409 is raised and
        *nothing* changes (the current release stays in place)."""
        async with self.drill_lock_for(drill_id):
            await self.db.begin()
            try:
                drill = await self._drill_row(drill_id)
                if drill is None:
                    raise EngineError(f"drill '{drill_id}' not found", 404)
                row = await self._get_version_row(drill_id, version)
                if row is None:
                    raise EngineError(f"版本 v{version} 不存在", 404)
                if row["status"] != "draft":
                    raise EngineError("只有草稿可以发布", 409)
                current_pub = drill["published_version"]
                if base_version != current_pub:
                    raise EngineError(
                        f"基准版本 v{base_version} 已被他人更新"
                        f"（当前发布版为 v{current_pub}），请重新比对差异后再发布",
                        409,
                    )

                now = str(now_ts())
                if current_pub is not None:
                    await self.db.db.execute(
                        "UPDATE drill_versions SET status='history' "
                        "WHERE drill_id=? AND version=? AND status='published'",
                        (drill_id, current_pub),
                    )
                await self.db.db.execute(
                    "UPDATE drill_versions SET status='published', published_at=? "
                    "WHERE drill_id=? AND version=?",
                    (now, drill_id, version),
                )
                d = def_from_json(json.loads(row["definition"]))
                await self.db.db.execute(
                    "UPDATE drills SET name=?, description=?, definition=?, "
                    "published_version=?, draft_version=NULL WHERE id=?",
                    (d.name, d.description, row["definition"], version, drill_id),
                )
                await self.db.commit()
            except Exception:
                await self.db.rollback()
                raise

        return await self.get_version(drill_id, version, with_yaml=False)

    async def copy_as_draft(
        self, drill_id: str, source_version: int, base_version: int | None
    ) -> dict[str, Any]:
        """Copy any historical/published/draft version's content into a fresh
        draft. Releasing it still requires ``base_version`` to equal the
        current release, so a stale copy cannot overwrite someone else's
        release behind their back."""
        async with self.drill_lock_for(drill_id):
            drill = await self._drill_row(drill_id)
            if drill is None:
                raise EngineError(f"drill '{drill_id}' not found", 404)
            if base_version is not None and base_version != drill["published_version"]:
                raise EngineError(
                    f"基准版本不是当前发布版（当前为 v{drill['published_version']}）",
                    409,
                )
            src = await self._get_version_row(drill_id, source_version)
            if src is None:
                raise EngineError(f"版本 v{source_version} 不存在", 404)

            await self.db.begin()
            try:
                # Deduplicate: an identical open draft is reused.
                dup = await self.db.one(
                    "SELECT version FROM drill_versions WHERE drill_id=? AND sha256=? "
                    "AND status='draft' ORDER BY version DESC LIMIT 1",
                    (drill_id, src["sha256"]),
                )
                if dup is not None:
                    await self.db.commit()
                    meta = await self.get_version(drill_id, dup["version"], with_yaml=False)
                    return {"id": drill_id, **meta, "reused": True}

                now = str(now_ts())
                next_v = (await self._max_version(drill_id)) + 1
                old_draft = drill["draft_version"]
                if old_draft is not None:
                    await self.db.db.execute(
                        "UPDATE drill_versions SET status='superseded' "
                        "WHERE drill_id=? AND version=? AND status='draft'",
                        (drill_id, old_draft),
                    )
                await self.db.db.execute(
                    "INSERT INTO drill_versions"
                    "(drill_id, version, status, definition, yaml, sha256, base_version, "
                    "created_at, published_at) VALUES(?,?, 'draft', ?,?,?,?,?,NULL)",
                    (drill_id, next_v, src["definition"], src["yaml"], src["sha256"],
                     base_version, now),
                )
                await self.db.db.execute(
                    "UPDATE drills SET draft_version=? WHERE id=?",
                    (next_v, drill_id),
                )
                await self.db.commit()
            except Exception:
                await self.db.rollback()
                raise
        meta = await self.get_version(drill_id, next_v, with_yaml=False)
        return {"id": drill_id, **meta, "reused": False}

    async def diff_versions(self, drill_id: str, from_version: int, to_version: int) -> dict[str, Any]:
        rows = {
            v: await self._get_version_row(drill_id, v)
            for v in (from_version, to_version)
        }
        missing = [f"v{v}" for v, r in rows.items() if r is None]
        if missing:
            raise EngineError(f"版本 {'、'.join(missing)} 不存在", 404)
        old_d = def_from_json(json.loads(rows[from_version]["definition"]))
        new_d = def_from_json(json.loads(rows[to_version]["definition"]))
        return diff_definitions(old_d, new_d)

    async def seed_from_file(self, d: DrillDef, yaml_text: str) -> None:
        """Startup reconciliation of a YAML shipped in the drills directory.

        * unknown drill                -> immutable v1, released immediately;
        * file changed vs release      -> a new draft based on the release;
        * identical to release/draft   -> nothing.
        """
        async with self.drill_lock_for(d.id):
            drill = await self._drill_row(d.id)
            digest = content_digest(d)
            if drill is None:
                now = str(now_ts())
                await self.db.begin()
                try:
                    await self.db.db.execute(
                        "INSERT INTO drills(id, name, description, definition, created_at, "
                        "published_version, draft_version) VALUES(?,?,?,?,?,1,NULL)",
                        (d.id, d.name, d.description, dumps(dataclasses_asdict(d)), now),
                    )
                    await self.db.db.execute(
                        "INSERT INTO drill_versions"
                        "(drill_id, version, status, definition, yaml, sha256, base_version, "
                        "created_at, published_at) VALUES(?,1,'published',?,?,?,?,?,?)",
                        (d.id, dumps(dataclasses_asdict(d)), yaml_text, digest, None, now, now),
                    )
                    await self.db.commit()
                except Exception:
                    await self.db.rollback()
                    raise
                return

            if drill["published_version"] is not None:
                pub = await self._get_version_row(d.id, drill["published_version"])
                if pub and pub["sha256"] == digest:
                    return
            if drill["draft_version"] is not None:
                draft = await self._get_version_row(d.id, drill["draft_version"])
                if draft and draft["sha256"] == digest:
                    return
            # File newer than the DB: import as a draft keyed to the release.
        await self.save_draft(d, yaml_text, base_version=drill["published_version"])

    async def get_definition(self, drill_id: str) -> DrillDef:
        """The currently *released* definition. Raises 409 when the drill has
        no release yet (drafts cannot back a session)."""
        drill = await self._drill_row(drill_id)
        if drill is None:
            raise EngineError(f"drill '{drill_id}' not found", 404)
        if drill["published_version"] is None:
            raise EngineError("剧本尚未发布：草稿发布后才能创建会话", 409)
        return await self.get_version_def(drill_id, drill["published_version"])

    async def get_session_definition(self, session_id: str) -> tuple[dict[str, Any], DrillDef]:
        """Load the exact immutable script version a session was created with.
        Later drafts/releases can never affect this lookup."""
        sess = await self.get_session(session_id)
        row = await self.db.one(
            "SELECT definition FROM drill_versions WHERE drill_id=? AND version=?",
            (sess["drill_id"], sess["drill_version"]),
        )
        if row is None:
            raise EngineError(
                f"会话绑定的剧本版本 v{sess['drill_version']} 已丢失，无法重建快照", 500
            )
        return sess, def_from_json(json.loads(row["definition"]))

    async def list_sessions(self) -> list[dict[str, Any]]:
        rows = await self.db.all(
            "SELECT id, drill_id, drill_version, drill_sha256, status, reason, "
            "created_at, started_at, ended_at FROM sessions ORDER BY rowid DESC"
        )
        return [dict(r) for r in rows]

    async def get_session(self, session_id: str) -> dict[str, Any]:
        row = await self.db.one(
            "SELECT id, drill_id, drill_version, drill_sha256, status, reason, "
            "created_at, started_at, ended_at FROM sessions WHERE id=?",
            (session_id,),
        )
        if row is None:
            raise EngineError("session not found", 404)
        return dict(row)

    async def create_session(self, drill_id: str) -> str:
        # Drafts cannot back sessions: only the current release is allowed,
        # and the new row pins both its version number and content digest.
        drill = await self._drill_row(drill_id)
        if drill is None:
            raise EngineError(f"drill '{drill_id}' not found", 404)
        if drill["published_version"] is None:
            raise EngineError("剧本尚未发布：草稿发布后才能创建会话", 409)
        vrow = await self._get_version_row(drill_id, drill["published_version"])
        assert vrow is not None
        for _ in range(5):
            sid = new_id()
            try:
                await self.db.db.execute(
                    "INSERT INTO sessions(id, drill_id, drill_version, drill_sha256, created_at) "
                    "VALUES(?,?,?,?,?)",
                    (sid, drill_id, vrow["version"], vrow["sha256"], now_ts()),
                )
                await self.db.commit()
                return sid
            except Exception:
                await self.db.rollback()
        raise EngineError("could not allocate session id", 500)

    async def load(self, session_id: str) -> tuple[dict[str, Any], DrillDef, State]:
        """Fetch session row, its *pinned* drill definition and folded state."""
        sess, d = await self.get_session_definition(session_id)
        rows = await self.db.all(
            "SELECT version, seq, type, actor, at, payload FROM events "
            "WHERE session_id=? ORDER BY version, seq",
            (session_id,),
        )
        events = [
            {
                "version": r["version"],
                "seq": r["seq"],
                "type": r["type"],
                "actor": r["actor"],
                "at": float(r["at"]),
                "payload": json.loads(r["payload"]),
            }
            for r in rows
        ]
        return sess, d, reduce_events(events)

    async def get_participant(self, session_id: str, token: str) -> dict[str, Any] | None:
        row = await self.db.one(
            "SELECT session_id, role_id, display_name, token FROM participants "
            "WHERE session_id=? AND token=?",
            (session_id, token),
        )
        return dict(row) if row else None

    async def join(
        self, session_id: str, invite_code: str, display_name: str
    ) -> dict[str, Any]:
        async with self.lock_for(session_id):
            sess = await self.get_session(session_id)
            if sess["status"] == "ended":
                raise EngineError("演练已结束，无法加入", 409)
            d = await self.get_version_def(sess["drill_id"], sess["drill_version"])
            role = next((r for r in d.roles.values() if r.invite == invite_code.strip()), None)
            if role is None:
                raise EngineError("邀请码无效", 403)

            existing = await self.db.one(
                "SELECT token, display_name FROM participants WHERE session_id=? AND role_id=?",
                (session_id, role.id),
            )
            if existing is not None:
                raise EngineError(f"角色 {role.name} 已有人加入", 409)

            name = (display_name or "").strip() or role.name
            token = new_token()
            ts = now_ts()
            _, _, state = await self.load(session_id)
            version = state.last_version + 1
            await self.db.begin()
            try:
                await self.db.db.execute(
                    "INSERT INTO participants(session_id, role_id, display_name, token, joined_at) "
                    "VALUES(?,?,?,?,?)",
                    (session_id, role.id, name, token, ts),
                )
                await self._insert_event(
                    session_id, version, 0, E_PARTICIPANT_JOINED, role.id,
                    {"role_id": role.id, "display_name": name, "at": ts},
                )
                await self.db.commit()
            except Exception:
                await self.db.rollback()
                raise
            return {
                "role_id": role.id,
                "role_name": role.name,
                "display_name": name,
                "token": token,
                "version": version,
                "drill_version": sess["drill_version"],
                "drill_sha256": sess["drill_sha256"],
            }

    async def start(self, session_id: str) -> dict[str, Any]:
        async with self.lock_for(session_id):
            sess = await self.get_session(session_id)
            if sess["status"] != "created":
                raise EngineError("会话已经开始或已结束", 409)
            d = await self.get_version_def(sess["drill_id"], sess["drill_version"])
            stage = d.stages[d.initial_stage]
            ts = now_ts()
            deadline = ts + stage.duration if stage.duration else None
            # Participants may have joined before start: the start commit must
            # take the next free version, never a hardcoded one.
            _, _, state = await self.load(session_id)
            version = state.last_version + 1
            await self.db.begin()
            try:
                await self._insert_event(
                    session_id, version, 0, E_SESSION_STARTED, "host",
                    {"at": ts, "stage_id": stage.id, "deadline": deadline},
                )
                await self._insert_event(
                    session_id, version, 1, E_STAGE_OPENED, "system",
                    {"stage_id": stage.id, "deadline": deadline, "at": ts},
                )
                await self.db.db.execute(
                    "UPDATE sessions SET status='running', started_at=? WHERE id=?",
                    (ts, session_id),
                )
                await self.db.commit()
            except Exception:
                await self.db.rollback()
                raise
            return {
                "version": version,
                "stage_id": stage.id,
                "deadline": deadline,
                "ended": False,
            }

    async def submit(
        self,
        session_id: str,
        role_id: str,
        stage_id: str | None,
        choice_id: str,
        client_stage: str | None = None,
    ) -> dict[str, Any]:
        """Submit a choice. Atomic against deadlines: if the deadline has been
        reached the choice is rejected and the stage is sealed in the same
        transaction. When the last required choice arrives, the stage settles
        immediately ("complete")."""
        async with self.lock_for(session_id):
            await self.db.begin()
            try:
                sess, d, state = await self.load(session_id)
                target_stage_id = stage_id or client_stage or state.current

                # Idempotency first: a retried submission (possibly arriving
                # after the stage has settled) replays the original outcome.
                if target_stage_id is not None:
                    prior = await self.db.one(
                        "SELECT choice_id FROM submissions "
                        "WHERE session_id=? AND stage_id=? AND role_id=?",
                        (session_id, target_stage_id, role_id),
                    )
                    if prior is not None:
                        await self.db.commit()
                        return {
                            "accepted": False,
                            "duplicate": True,
                            "stage_id": target_stage_id,
                            "choice_id": prior["choice_id"],
                            "same": prior["choice_id"] == choice_id,
                            "current_stage": state.current,
                            "deadline": state.deadline,
                            "settled": state.current != target_stage_id,
                            "ended": state.status == "ended",
                        }

                if sess["status"] != "running" or state.current is None:
                    raise EngineError("当前没有进行中的阶段", 409)
                stage = d.stages[state.current]
                if target_stage_id is not None and target_stage_id != stage.id:
                    raise EngineError("阶段已变化，请刷新后重试", 409)
                if role_id not in stage.options:
                    raise EngineError("本角色在当前阶段没有待决策项", 403)
                if find_option(stage, role_id, choice_id) is None:
                    raise EngineError("选项不存在", 400)

                ts = now_ts()
                if state.deadline is not None and ts >= state.deadline:
                    # Deadline race: reject and seal atomically.
                    seal = await self._seal(
                        session_id, d, state, stage, "deadline",
                        version=state.last_version + 1, at=ts,
                    )
                    await self._finalize_session_row(session_id, state)
                    await self.db.commit()
                    return {
                        "accepted": False,
                        "duplicate": False,
                        "reason": "deadline",
                        "deadline": state.deadline,
                        "settled": True,
                        **seal,
                    }

                version = state.last_version + 1
                await self.db.db.execute(
                    "INSERT INTO submissions(session_id, version, role_id, stage_id, choice_id, at) "
                    "VALUES(?,?,?,?,?,?)",
                    (session_id, version, role_id, stage.id, choice_id, ts),
                )
                await self._insert_event(
                    session_id, version, 0, E_STAGE_CHOICE, role_id,
                    {"stage_id": stage.id, "role_id": role_id, "choice_id": choice_id, "at": ts},
                )
                state.choices[(stage.id, role_id)] = choice_id
                state.last_version = version

                seal_info: dict[str, Any] | None = None
                deciding = list(stage.options.keys())
                all_in = all((stage.id, r) in state.choices for r in deciding)
                if all_in:
                    # The choice event above holds seq 0 of this version; the
                    # seal events continue at seq 1 within the same commit.
                    seal_info = await self._seal(
                        session_id, d, state, stage, "complete",
                        version=version, at=ts, seq0=1,
                    )
                await self._finalize_session_row(session_id, state)
                await self.db.commit()
                return {
                    "accepted": True,
                    "duplicate": False,
                    "choice_id": choice_id,
                    "version": version,
                    "deadline": state.deadline,
                    "settled": seal_info is not None,
                    **(seal_info or {}),
                }
            except EngineError:
                await self.db.rollback()
                raise
            except Exception:
                await self.db.rollback()
                raise

    async def _seal(
        self,
        session_id: str,
        d: DrillDef,
        state: State,
        stage: Stage,
        reason: str,
        version: int,
        at: float,
        seq0: int = 0,
    ) -> dict[str, Any]:
        """Append stage_sealed and stage_opened/session_ended; mutate state.

        Must run inside an open transaction. ``reason`` is complete|deadline|host.
        ``seq0`` is the seq of the stage_sealed event inside this version's
        commit: 0 when the seal leads the commit (settle / rejected submit),
        1 when a stage_choice event already occupies seq 0 (auto-settle on
        the last submission).
        """
        choices = {r: state.choices.get((stage.id, r)) for r in stage.options}
        nxt = evaluate(stage, choices)
        await self._insert_event(
            session_id, version, seq0, E_STAGE_SEALED, "system",
            {
                "stage_id": stage.id,
                "reason": reason,
                "choices": choices,
                "next": nxt,
                "at": at,
            },
        )
        sealed = SealedStage(
            stage_id=stage.id, version=version, at=at, reason=reason,
            choices=choices, next=nxt,
        )
        state.sealed[stage.id] = sealed
        state.current = None
        state.deadline = None

        if nxt is not None and nxt in d.stages:
            nstage = d.stages[nxt]
            deadline = at + nstage.duration if nstage.duration else None
            await self._insert_event(
                session_id, version, seq0 + 1, E_STAGE_OPENED, "system",
                {"stage_id": nstage.id, "deadline": deadline, "at": at},
            )
            state.current = nstage.id
            state.deadline = deadline
            if nstage.id not in state.order:
                state.order.append(nstage.id)
        else:
            await self._insert_event(
                session_id, version, seq0 + 1, E_SESSION_ENDED,
                "host" if reason == "host" else "system",
                {"reason": reason, "at": at},
            )
            state.status = "ended"
            state.end_reason = reason
            state.ended_at = at
        state.last_version = version
        return {
            "sealed_stage": stage.id,
            "seal_reason": reason,
            "next_stage": state.current,
            "next_deadline": state.deadline,
            "ended": state.status == "ended",
            "seal_version": version,
        }

    async def settle(
        self,
        session_id: str,
        reason: str,
        expected_deadline: float | None = None,
    ) -> dict[str, Any]:
        """Advance the open stage now. Used by the deadline timer and the
        presenter's manual control. Returns {noop:True} if nothing is open.

        A deadline callback must pass ``expected_deadline``: the exact deadline
        its timer captured when armed. It is compared against the current
        ``state.deadline`` inside the same SQLite transaction. When the stage
        has since advanced (last submission or a host settle moved the session
        to another timed stage before this delayed callback ran), the identity
        check fails and the callback idles without adding events — it must not
        be mistaken for the new stage's timeout.
        """
        if reason not in ("deadline", "host"):
            reason = "host"
        async with self.lock_for(session_id):
            await self.db.begin()
            try:
                sess, d, state = await self.load(session_id)
                if sess["status"] != "running" or state.current is None:
                    await self.db.commit()
                    return {"noop": True}
                stage = d.stages[state.current]
                ts = now_ts()
                if reason == "deadline":
                    # A deadline settle is only valid for the stage whose timer
                    # actually fired:
                    #  * state.deadline is None  -> open stage has no timeout;
                    #  * expected != current     -> stale callback from an
                    #    earlier timed stage that was advanced before it could
                    #    run (lock contention / event-loop delay);
                    #  * ts < current            -> the armed timer fired early.
                    # Deadlines round-trip through JSON as IEEE doubles and the
                    # timer captures the very same float, so an exact compare
                    # is both sufficient and correct.
                    if state.deadline is None or expected_deadline != state.deadline:
                        await self.db.commit()
                        return {"noop": True, "stale": state.deadline != expected_deadline}
                    if ts < state.deadline:
                        await self.db.commit()
                        return {"noop": True, "early": True}
                info = await self._seal(
                    session_id, d, state, stage, reason,
                    version=state.last_version + 1, at=ts,
                )
                await self._finalize_session_row(session_id, state)
                await self.db.commit()
                return info
            except Exception:
                await self.db.rollback()
                raise

    async def end_session(self, session_id: str) -> dict[str, Any]:
        """Presenter terminates the whole drill: seal the open stage (host),
        then end the session. No-op if already ended."""
        async with self.lock_for(session_id):
            await self.db.begin()
            try:
                sess, d, state = await self.load(session_id)
                if state.status == "created":
                    ts = now_ts()
                    await self._insert_event(
                        session_id, state.last_version + 1, 0,
                        E_SESSION_ENDED, "host",
                        {"reason": "host", "at": ts},
                    )
                    state.status = "ended"
                    state.end_reason = "host"
                    state.ended_at = ts
                elif state.status == "running":
                    ts = now_ts()
                    if state.current is not None:
                        stage = d.stages[state.current]
                        await self._seal(
                            session_id, d, state, stage, "host",
                            version=state.last_version + 1, at=ts,
                        )
                    if state.status != "ended":
                        await self._insert_event(
                            session_id, state.last_version + 1, 0,
                            E_SESSION_ENDED, "host",
                            {"reason": "host", "at": ts},
                        )
                        state.status = "ended"
                        state.end_reason = "host"
                        state.ended_at = ts
                await self._finalize_session_row(session_id, state)
                await self.db.commit()
                return {"ended": True, "ended_at": state.ended_at}
            except Exception:
                await self.db.rollback()
                raise

    # ------------------------------------------------------------------ #
    # joint-exercise injections
    # ------------------------------------------------------------------ #
    async def append_injection(
        self,
        session_id: str,
        injection_id: str,
        joint_id: str,
        gate_id: str,
        stage_id: str,
        title: str,
        public_text: str,
        views: dict[str, str],
    ) -> dict[str, Any]:
        """Atomically append a one-shot joint injection to a session.

        The same safety rules as a normal state change apply:

        * the session lock serialises it against submissions/settles, so the
          check "stage still open" and the event insert are one atomic
          transaction — an injection can never unlock a stage that sealed
          first and can never be delivered twice;
        * the injection occupies exactly one new monotonic version, keeping
          the target session's event log independent and replayable;
        * ``views`` are permission-trimmed by the snapshot builder exactly
          like ordinary stage views.

        Returns ``{"delivered": True, ...}`` or
        ``{"delivered": False, "reason": "ended"|"stage_closed"|"duplicate"}``.
        """
        async with self.lock_for(session_id):
            await self.db.begin()
            try:
                sess, d, state = await self.load(session_id)
                if injection_id in state.injections:
                    await self.db.commit()
                    return {"delivered": False, "reason": "duplicate"}
                if sess["status"] != "running" or state.current != stage_id:
                    await self.db.commit()
                    return {
                        "delivered": False,
                        "reason": "stage_closed",
                        "current": state.current,
                        "status": sess["status"],
                    }
                ts = now_ts()
                version = state.last_version + 1
                payload = {
                    "injection_id": injection_id,
                    "joint_id": joint_id,
                    "gate_id": gate_id,
                    "stage_id": stage_id,
                    "title": title,
                    "public": public_text,
                    "views": views,
                    "at": ts,
                }
                await self._insert_event(
                    session_id, version, 0, E_JOINT_INJECTION, "joint", payload
                )
                state.injections[injection_id] = Injection(
                    injection_id=injection_id,
                    joint_id=joint_id,
                    gate_id=gate_id,
                    stage_id=stage_id,
                    title=title,
                    public=public_text,
                    views=dict(views),
                    version=version,
                    at=ts,
                )
                state.last_version = version
                await self.db.commit()
                return {
                    "delivered": True,
                    "version": version,
                    "at": ts,
                    "stage_id": stage_id,
                }
            except Exception:
                await self.db.rollback()
                raise

    # ---------- plumbing ----------
    async def _insert_event(
        self, session_id: str, version: int, seq: int, etype: str, actor: str, payload: dict
    ) -> None:
        await self.db.db.execute(
            "INSERT INTO events(session_id, version, seq, type, actor, at, payload) "
            "VALUES(?,?,?,?,?,?,?)",
            (
                session_id, version, seq, etype, actor,
                payload.get("at", now_ts()), dumps(payload),
            ),
        )

    async def _finalize_session_row(self, session_id: str, state: State) -> None:
        if state.status == "ended":
            await self.db.db.execute(
                "UPDATE sessions SET status='ended', reason=?, ended_at=? WHERE id=?",
                (state.end_reason, state.ended_at, session_id),
            )
