"""SQLite persistence. The event log is the source of truth; rows are append-only.

Concurrency model
-----------------
* One writer at a time, guarded by an asyncio.Lock inside Engine (same process).
* Every state-changing operation runs in a single BEGIN IMMEDIATE transaction,
  which gives cross-process atomicity as well.
* Readers (snapshots/replays/exports) never block thanks to WAL mode.
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import time
from typing import Any

import aiosqlite
import yaml

SCHEMA = """
-- Drill metadata. The published definition pointer lives on
-- drill_versions; this table only carries the identity and display name.
CREATE TABLE IF NOT EXISTS drills (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    definition  TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL DEFAULT '',
    published_version INTEGER,          -- current released version (NULL => draft only)
    draft_version     INTEGER           -- current editable draft (NULL => none)
);

-- Immutable script versions. A version row is never mutated after insert;
-- publishing only flips drills.published_version and marks the previous
-- released row 'history'. Content identical to an earlier version reuses the
-- same sha256 digest, so a session can always prove it runs the exact script
-- it was created with.
CREATE TABLE IF NOT EXISTS drill_versions (
    drill_id   TEXT NOT NULL,
    version    INTEGER NOT NULL,
    status     TEXT NOT NULL,           -- draft|published|history|superseded
    definition TEXT NOT NULL,           -- canonical parsed DrillDef JSON
    yaml       TEXT NOT NULL DEFAULT '',-- raw YAML the author uploaded
    sha256     TEXT NOT NULL,
    base_version INTEGER,               -- version this draft was based on
    created_at TEXT NOT NULL,
    published_at TEXT,
    PRIMARY KEY (drill_id, version)
);
CREATE INDEX IF NOT EXISTS idx_drill_versions_status
    ON drill_versions(drill_id, status);

CREATE TABLE IF NOT EXISTS sessions (
    id        TEXT PRIMARY KEY,
    drill_id  TEXT NOT NULL REFERENCES drills(id),
    -- The exact immutable script version this session is pinned to. Later
    -- edits, drafts and releases can never change a created/running session's
    -- snapshot, branching or replay.
    drill_version INTEGER NOT NULL DEFAULT 1,
    drill_sha256  TEXT NOT NULL DEFAULT '',
    status    TEXT NOT NULL DEFAULT 'created',   -- created|running|ended
    reason    TEXT,                              -- terminal|complete|deadline|host
    created_at TEXT NOT NULL,
    started_at TEXT,
    ended_at   TEXT
);

CREATE TABLE IF NOT EXISTS participants (
    session_id   TEXT NOT NULL REFERENCES sessions(id),
    role_id      TEXT NOT NULL,
    display_name TEXT NOT NULL,
    token        TEXT NOT NULL,
    joined_at    TEXT NOT NULL,
    PRIMARY KEY (session_id, role_id),
    UNIQUE (session_id, token)
);

CREATE TABLE IF NOT EXISTS events (
    session_id TEXT NOT NULL REFERENCES sessions(id),
    version    INTEGER NOT NULL,
    seq        INTEGER NOT NULL,               -- ordering inside one version commit
    type       TEXT NOT NULL,
    actor      TEXT NOT NULL DEFAULT '',       -- role id, 'host' or 'system'
    at         TEXT NOT NULL,
    payload    TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (session_id, version, seq)
);

-- Duplicate submit guard (choice events are also the audit record).
CREATE TABLE IF NOT EXISTS submissions (
    session_id TEXT NOT NULL,
    version    INTEGER NOT NULL,
    role_id    TEXT NOT NULL,
    stage_id   TEXT NOT NULL,
    choice_id  TEXT NOT NULL,
    at         TEXT NOT NULL,
    PRIMARY KEY (session_id, stage_id, role_id)
);

-- ---------------------------------------------------------------------------
-- Joint exercise orchestration ("联合演练编排"): several independent sessions
-- are grouped into one joint drill; gates carry directed stage dependencies.
-- A gate releases a one-shot injection into its target session only after
-- EVERY prerequisite branch has observed its specified outcome (AND join).
-- The delivered injection itself is appended to the TARGET session's own
-- event log (a joint_injection event), so each session keeps its pinned
-- script version and its independent, deterministic event record.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS joints (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS joint_members (
    joint_id   TEXT NOT NULL REFERENCES joints(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    ord        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (joint_id, session_id)
);
CREATE INDEX IF NOT EXISTS idx_joint_members_session
    ON joint_members(session_id);

-- One gate = one one-shot injection targeting one stage of one session.
-- status: waiting (prerequisites not all met / target stage not yet open) |
--         released (delivered exactly once) | missed (target stage sealed
--         or session ended before release — the delivery window is gone).
CREATE TABLE IF NOT EXISTS joint_gates (
    joint_id       TEXT NOT NULL REFERENCES joints(id) ON DELETE CASCADE,
    gate_id        TEXT NOT NULL,
    target_session TEXT NOT NULL,
    target_stage   TEXT NOT NULL,
    title          TEXT NOT NULL DEFAULT '',
    public_text    TEXT NOT NULL DEFAULT '',
    views          TEXT NOT NULL DEFAULT '{}',  -- role_id -> private text (JSON)
    status         TEXT NOT NULL DEFAULT 'waiting',
    delivered_at   TEXT,
    event_version  INTEGER,                      -- session version of the injection
    PRIMARY KEY (joint_id, gate_id)
);
CREATE INDEX IF NOT EXISTS idx_joint_gates_lookup
    ON joint_gates(target_session, target_stage, status);
CREATE INDEX IF NOT EXISTS idx_joint_gates_joint
    ON joint_gates(joint_id, status);

-- A prerequisite branch: "(session, stage) must reach the specified outcome".
-- kind 'next_stage': the sealed stage's resolved next must equal ``value``
--                    ('' denotes the drill ending, next == None);
-- kind 'choice':     ``role_id``'s sealed choice must equal ``value``.
CREATE TABLE IF NOT EXISTS joint_branches (
    joint_id   TEXT NOT NULL,
    gate_id    TEXT NOT NULL,
    branch_no  INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    stage_id   TEXT NOT NULL,
    kind       TEXT NOT NULL,
    role_id    TEXT NOT NULL DEFAULT '',
    value      TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (joint_id, gate_id, branch_no)
);
CREATE INDEX IF NOT EXISTS idx_joint_branches_watch
    ON joint_branches(session_id, stage_id);

-- A branch is recorded at most once (one-shot): the PK makes duplicate /
-- out-of-order observation idempotent.
CREATE TABLE IF NOT EXISTS joint_branch_hits (
    joint_id      TEXT NOT NULL,
    gate_id       TEXT NOT NULL,
    branch_no     INTEGER NOT NULL,
    event_version INTEGER NOT NULL,
    at            TEXT NOT NULL,
    PRIMARY KEY (joint_id, gate_id, branch_no)
);
"""


def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


class Database:
    def __init__(self, path: str):
        self.path = path
        self.db: aiosqlite.Connection | None = None
        # All writers share ONE underlying SQLite connection, and Python's
        # sqlite3 opens transactions implicitly before DML. Per-session locks
        # guard each session's invariants, but two DIFFERENT sessions (or a
        # session and the joint orchestrator) can hold different session locks
        # and interleave at an await in the middle of their transactions.
        # This lock serialises the SQL transaction boundary itself so one
        # transaction's begin..commit is never interleaved with another's.
        self._write_lock = asyncio.Lock()
        self._tx_owner: asyncio.Task | None = None

    def write_lock(self) -> asyncio.Lock:
        return self._write_lock

    async def connect(self) -> None:
        self.db = await aiosqlite.connect(self.path, isolation_level=None)
        self.db.row_factory = aiosqlite.Row
        # isolation_level=None puts the connection in SQLite autocommit mode,
        # so transactions are opened ONLY by our explicit BEGIN IMMEDIATE and
        # no implicit DML transaction can leak across an await.
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA foreign_keys=ON")
        await self.db.execute("PRAGMA busy_timeout=5000")
        await self.db.executescript(SCHEMA)
        await self._migrate()
        await self.db.commit()

    async def close(self) -> None:
        if self.db is not None:
            await self.db.close()

    # ---------- low level helpers (connection in autocommit mode) ----------
    async def begin(self) -> None:
        # Acquire the connection-wide write lock for the duration of the
        # transaction; released by commit()/rollback(). This serialises the
        # SQL of transactions that belong to different session locks.
        await self._write_lock.acquire()
        self._tx_owner = asyncio.current_task()
        try:
            await self.db.execute("BEGIN IMMEDIATE")
        except Exception:
            self._tx_owner = None
            self._write_lock.release()
            raise

    async def _end_tx(self, action) -> None:
        owner = getattr(self, "_tx_owner", None)
        try:
            await action()
        finally:
            # Only the task that opened this transaction releases the lock,
            # so a stray commit on an idle connection never unlocks another
            # coroutine's in-flight transaction.
            if owner is not None and owner is asyncio.current_task():
                self._tx_owner = None
                self._write_lock.release()

    async def commit(self) -> None:
        await self._end_tx(self.db.commit)

    async def rollback(self) -> None:
        await self._end_tx(self.db.rollback)

    async def all(self, sql: str, params: tuple = ()) -> list[aiosqlite.Row]:
        async with self.db.execute(sql, params) as cur:
            return list(await cur.fetchall())

    async def one(self, sql: str, params: tuple = ()) -> aiosqlite.Row | None:
        async with self.db.execute(sql, params) as cur:
            return await cur.fetchone()

    # ---------- migration from the pre-versioning schema ----------
    async def _table_columns(self, table: str) -> set[str]:
        async with self.db.execute(f"PRAGMA table_info({table})") as cur:
            rows = await cur.fetchall()
        return {r["name"] for r in rows}

    async def _migrate(self) -> None:
        """Upgrade a database created before script versioning.

        * drill_versions is populated from the legacy drills.definition blob
          (or freshly parsed from drills/*.yaml), as immutable v1 releases;
        * sessions gain drill_version/drill_sha256 pinned to v1 of their drill.
        The migration is idempotent.
        """
        from .definitions import DrillDef, DrillError, def_from_json, parse_definition
        from .versions import content_digest

        def restore(loaded: Any):
            """The legacy blob was dataclasses.asdict(DrillDef); a file parse
            yields raw YAML mappings. Accept either shape."""
            if not isinstance(loaded, dict):
                return None
            try:
                return def_from_json(loaded)
            except (KeyError, TypeError):
                pass
            try:
                return parse_definition(loaded)
            except DrillError:
                return None

        drill_cols = await self._table_columns("drills")
        for col, decl in (
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
            ("published_version", "INTEGER"),
            ("draft_version", "INTEGER"),
        ):
            if col not in drill_cols:
                await self.db.execute(f"ALTER TABLE drills ADD COLUMN {col} {decl}")

        sess_cols = await self._table_columns("sessions")
        if "drill_version" not in sess_cols:
            await self.db.execute(
                "ALTER TABLE sessions ADD COLUMN drill_version INTEGER NOT NULL DEFAULT 1"
            )
        if "drill_sha256" not in sess_cols:
            await self.db.execute(
                "ALTER TABLE sessions ADD COLUMN drill_sha256 TEXT NOT NULL DEFAULT ''"
            )

        now = str(time.time())
        async with self.db.execute(
            "SELECT id, name, description, definition FROM drills "
            "WHERE published_version IS NULL AND draft_version IS NULL"
        ) as cur:
            legacy = list(await cur.fetchall())

        for row in legacy:
            drill_id = row["id"]
            d: DrillDef | None = None
            raw_yaml = ""
            if row["definition"]:
                try:
                    loaded = json.loads(row["definition"])
                    d = restore(loaded)
                except (ValueError, TypeError):
                    d = None
            if d is None:
                # Last resort: re-parse the YAML file shipped on disk.
                from pathlib import Path

                from . import config

                for ext in ("yaml", "yml"):
                    p = Path(config.DRILLS_DIR) / f"{drill_id}.{ext}"
                    if p.exists():
                        try:
                            file_yaml = p.read_text(encoding="utf-8")
                            d = restore(yaml.safe_load(file_yaml))
                            if d is not None:
                                raw_yaml = file_yaml
                        except yaml.YAMLError:
                            d = None
                        break
            if d is None:
                # Nothing usable to reconstruct from: leave the drill visible
                # but without a release; the host must upload a fresh draft.
                await self.db.execute(
                    "UPDATE drills SET created_at=? WHERE id=?", (now, drill_id)
                )
                continue
            definition_json = dumps(dataclasses.asdict(d))
            digest = content_digest(d)
            if not raw_yaml:
                raw_yaml = yaml.safe_dump(
                    dataclasses.asdict(d), allow_unicode=True, sort_keys=False
                )
            await self.db.execute(
                "INSERT INTO drill_versions"
                "(drill_id, version, status, definition, yaml, sha256, base_version, "
                " created_at, published_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (drill_id, 1, "published", definition_json, raw_yaml, digest,
                 None, now, now),
            )
            await self.db.execute(
                "UPDATE drills SET name=?, description=?, created_at=?, "
                "published_version=1, draft_version=NULL WHERE id=?",
                (d.name, d.description, now, drill_id),
            )
            await self.db.execute(
                "UPDATE sessions SET drill_version=1, drill_sha256=? WHERE drill_id=? "
                "AND (drill_sha256 IS NULL OR drill_sha256='')",
                (digest, drill_id),
            )

        # Sessions whose drill somehow predates any version row stay runnable.
        await self.db.execute(
            "UPDATE sessions SET drill_version=COALESCE(drill_version, 1) "
            "WHERE drill_version IS NULL"
        )
