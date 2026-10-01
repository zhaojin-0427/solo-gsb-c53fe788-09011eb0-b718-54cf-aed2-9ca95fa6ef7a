"""Joint exercise orchestration ("联合演练编排").

A joint groups several *independent* drill sessions and lets the presenter
declare directed dependencies between their stages: when an upstream stage
reaches a specified outcome (a resolved transition, or a role's exact
choice), a one-shot **injection** is appended to the target session at a
specified stage — but only after **every** prerequisite branch of that gate
has been observed (an AND join).

Safety properties (all enforced here, on top of the event-sourced engine):

* Saving a joint is validated against each member session's **pinned**
  script version: referenced sessions / stages / outcomes / roles must
  exist, and the dependency graph over ``(session, stage)`` nodes must be
  acyclic. Sessions therefore keep their independent script versions;
  later releases never change what a gate refers to.
* A satisfied branch is recorded at most once (``joint_branch_hits``
  primary key), so duplicate / out-of-order observations are idempotent.
* An injection is delivered by ``Engine.append_injection`` inside the
  target session's own ``BEGIN IMMEDIATE`` transaction, which re-checks
  that the target stage is still open: an injection can never be delivered
  twice and can never unlock a stage that sealed first. A target stage
  that seals (or a target session that ends) before release marks the gate
  ``missed`` — the one-shot delivery window is gone.
* The delivered injection is a normal ``joint_injection`` event in the
  target session's event log (one monotonic version), so every session
  keeps an independent event stream and deterministic replay; private
  injection views are permission-trimmed by the snapshot builder.
"""
from __future__ import annotations

import asyncio
import json
import secrets
from collections import defaultdict, deque
from typing import Any

from .db import Database, dumps
from .definitions import DrillDef
from .engine import Engine, EngineError, now_ts, reduce_events

WAITING = "waiting"
RELEASED = "released"
MISSED = "missed"


class JointError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


# --------------------------------------------------------------------------- #
# pure helpers
# --------------------------------------------------------------------------- #
def new_joint_id() -> str:
    return secrets.token_hex(4)  # 8 chars, same shape as a session id


def _gate_hash(g: dict[str, Any]) -> str:
    """Stable identity of a gate's definition. Used to tell whether an
    already-released gate is being modified by a replace request."""
    canon = {
        "target_session": g["target_session"],
        "target_stage": g["target_stage"],
        "title": g.get("title", ""),
        "public": g.get("public", ""),
        "views": dict(sorted((g.get("views") or {}).items())),
        "prerequisites": sorted(
            (
                {
                    "session_id": b["session_id"],
                    "stage_id": b["stage_id"],
                    "kind": b["kind"],
                    "role_id": b.get("role_id", ""),
                    "value": b.get("value", ""),
                }
                for b in g["prerequisites"]
            ),
            key=lambda b: (b["session_id"], b["stage_id"], b["kind"], b["role_id"], b["value"]),
        ),
    }
    return json.dumps(canon, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def branch_satisfied(branch: dict[str, Any], states: dict[str, Any]) -> bool:
    """Has this prerequisite observed its specified outcome?

    * ``next_stage`` — the watched stage sealed and its resolved ``next``
      equals ``value`` ('' means the drill ended there);
    * ``choice`` — the watched stage sealed with ``role_id``'s choice equal
      to ``value`` (a timeout is recorded as None and therefore never
      matches a concrete option id).
    """
    st = states.get(branch["session_id"])
    if st is None:
        return False
    sealed = st.sealed.get(branch["stage_id"])
    if sealed is None:
        return False
    if branch["kind"] == "next_stage":
        return (sealed.next or "") == branch.get("value", "")
    return sealed.choices.get(branch.get("role_id", "")) == branch.get("value")


def assert_acyclic(gates: list[dict[str, Any]]) -> None:
    """Kahn topological sort over (session_id, stage_id) nodes. Every
    prerequisite edge points at the gate's target node. A residual means
    the directed dependency graph contains a cycle (self loops included)."""
    nodes: set[tuple[str, str]] = set()
    edges: dict[tuple[str, str], set[tuple[str, str]]] = defaultdict(set)
    indeg: dict[tuple[str, str], int] = defaultdict(int)
    for g in gates:
        tgt = (g["target_session"], g["target_stage"])
        nodes.add(tgt)
        indeg.setdefault(tgt, 0)
        for b in g["prerequisites"]:
            src = (b["session_id"], b["stage_id"])
            nodes.add(src)
            indeg.setdefault(src, 0)
            if tgt not in edges[src]:
                edges[src].add(tgt)
                indeg[tgt] += 1
    queue = deque([n for n in nodes if indeg[n] == 0])
    seen = 0
    while queue:
        n = queue.popleft()
        seen += 1
        for m in edges.get(n, ()):  # type: ignore[arg-type]
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
    if seen != len(nodes):
        cyclic = sorted(n for n in nodes if indeg[n] > 0)
        where = "、".join(f"{s}/{t}" for s, t in cyclic[:5])
        raise JointError(f"依赖存在环，联合演练依赖必须是有向无环图（涉及：{where}）", 400)


# --------------------------------------------------------------------------- #
# orchestrator
# --------------------------------------------------------------------------- #
class JointOrchestrator:
    def __init__(self, db: Database, engine: Engine):
        self.db = db
        self.engine = engine
        # One global writer for joint metadata/runtime: reconciliation only
        # ever acquires session locks *while holding this lock and no other
        # session lock*, so the lock order is always joint -> session and no
        # cycle with engine operations (session-only locks) is possible.
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # validation (against each session's PINNED drill version)
    # ------------------------------------------------------------------ #
    async def _load_session_defs(
        self, session_ids: list[str]
    ) -> dict[str, tuple[dict[str, Any], DrillDef]]:
        out: dict[str, tuple[dict[str, Any], DrillDef]] = {}
        for sid in session_ids:
            try:
                sess, d = await self.engine.get_session_definition(sid)
            except EngineError as exc:
                raise JointError(f"会话 {sid} 不存在", 404) from exc
            out[sid] = (sess, d)
        return out

    def _validate_branch(
        self,
        raw: Any,
        ctx: str,
        members: list[str],
        defs: dict[str, tuple[dict[str, Any], DrillDef]],
    ) -> dict[str, str]:
        if not isinstance(raw, dict):
            raise JointError(f"{ctx}：前置分支必须是对象", 400)
        sid = str(raw.get("session_id") or "").strip()
        stage_id = str(raw.get("stage_id") or "").strip()
        kind = str(raw.get("kind") or "").strip()
        if not sid:
            raise JointError(f"{ctx}：缺少上游会话 session_id", 400)
        if sid not in members:
            raise JointError(f"{ctx}：上游会话 {sid} 不在联演成员中", 400)
        if not stage_id:
            raise JointError(f"{ctx}：缺少上游阶段 stage_id", 400)
        _, d = defs[sid]
        stage = d.stages.get(stage_id)
        if stage is None:
            raise JointError(
                f"{ctx}：会话 {sid} 绑定的剧本 v{defs[sid][0]['drill_version']} "
                f"中不存在阶段 {stage_id}", 400,
            )
        if kind not in ("next_stage", "choice"):
            raise JointError(f"{ctx}：结局类型 kind 必须是 next_stage 或 choice", 400)

        role_id = ""
        if kind == "next_stage":
            value = raw.get("value")
            value = "" if value is None else str(value)
            if value and value not in d.stages:
                raise JointError(
                    f"{ctx}：指定结局阶段 {value} 在会话 {sid} 的剧本中不存在", 400,
                )
        else:
            role_id = str(raw.get("role_id") or "").strip()
            value = str(raw.get("value") or "").strip()
            if not role_id or not value:
                raise JointError(f"{ctx}：choice 结局需要 role_id 与选项 value", 400)
            if role_id not in d.roles:
                raise JointError(f"{ctx}：会话 {sid} 的剧本中不存在角色 {role_id}", 400)
            olist = stage.options.get(role_id)
            if not olist:
                raise JointError(
                    f"{ctx}：角色 {d.roles[role_id].name} 在阶段 {stage_id} 没有决策选项", 400,
                )
            if not any(o.id == value for o in olist):
                raise JointError(
                    f"{ctx}：阶段 {stage_id} 中角色 {d.roles[role_id].name} "
                    f"不存在选项 {value}", 400,
                )
        return {
            "session_id": sid,
            "stage_id": stage_id,
            "kind": kind,
            "role_id": role_id,
            "value": value,
        }

    async def validate_spec(self, raw: Any) -> dict[str, Any]:
        """Normalize and validate a joint create/replace payload.

        Every reference is checked against the immutable script version the
        referenced session is pinned to — never the latest release.
        """
        if not isinstance(raw, dict):
            raise JointError("请求体必须是对象", 400)
        name = str(raw.get("name") or "").strip()
        if not name:
            raise JointError("联演名称不能为空", 400)
        description = str(raw.get("description") or "")

        members_raw = raw.get("members") or []
        if not isinstance(members_raw, list) or not members_raw:
            raise JointError("至少选择一个成员会话", 400)
        members: list[str] = []
        for sid in members_raw:
            sid = str(sid or "").strip()
            if not sid:
                continue
            if sid in members:
                raise JointError(f"成员会话重复：{sid}", 400)
            members.append(sid)
        defs = await self._load_session_defs(members)

        gates_raw = raw.get("gates")
        if gates_raw is None:
            gates_raw = []
        if not isinstance(gates_raw, list):
            raise JointError("gates 必须是数组", 400)

        gates: list[dict[str, Any]] = []
        seen_gate_ids: set[str] = set()
        for i, graw in enumerate(gates_raw):
            ctx = f"编排 {i + 1}"
            if not isinstance(graw, dict):
                raise JointError(f"{ctx}：必须是对象", 400)
            gid = str(graw.get("id") or "").strip() or f"g{i + 1}"
            if not all(c.isalnum() or c in "_-" for c in gid) or len(gid) > 40:
                raise JointError(f"{ctx}：编排 id 只能含字母数字、下划线、连字符（≤40）", 400)
            if gid in seen_gate_ids:
                raise JointError(f"{ctx}：编排 id 重复：{gid}", 400)
            seen_gate_ids.add(gid)

            target_session = str(graw.get("target_session") or "").strip()
            target_stage = str(graw.get("target_stage") or "").strip()
            if not target_session:
                raise JointError(f"{ctx}：缺少目标会话 target_session", 400)
            if target_session not in members:
                raise JointError(f"{ctx}：目标会话 {target_session} 不在联演成员中", 400)
            _, td = defs[target_session]
            tstage = td.stages.get(target_stage)
            if tstage is None:
                raise JointError(
                    f"{ctx}：目标会话 {target_session} 绑定的剧本 "
                    f"v{defs[target_session][0]['drill_version']} 中不存在阶段 {target_stage}", 400,
                )
            views_raw = graw.get("views") or {}
            if not isinstance(views_raw, dict):
                raise JointError(f"{ctx}：views 必须是角色到文本的映射", 400)
            views: dict[str, str] = {}
            for rid, text in views_raw.items():
                rid = str(rid)
                if rid not in td.roles:
                    raise JointError(
                        f"{ctx}：目标剧本中不存在角色 {rid}，无法按该角色权限推送", 400,
                    )
                views[rid] = str(text or "")

            prereqs_raw = graw.get("prerequisites")
            if not isinstance(prereqs_raw, list) or not prereqs_raw:
                raise JointError(f"{ctx}：至少需要一个前置分支", 400)
            prereqs = [
                self._validate_branch(b, f"{ctx} 前置 {j + 1}", members, defs)
                for j, b in enumerate(prereqs_raw)
            ]
            bkeys = [
                (b["session_id"], b["stage_id"], b["kind"], b["role_id"], b["value"])
                for b in prereqs
            ]
            if len(set(bkeys)) != len(bkeys):
                raise JointError(f"{ctx}：存在重复前置分支（同会话/阶段/结局）", 400)

            gates.append({
                "id": gid,
                "target_session": target_session,
                "target_stage": target_stage,
                "title": str(graw.get("title") or "").strip() or tstage.title,
                "public": str(graw.get("public") or ""),
                "views": views,
                "prerequisites": prereqs,
            })

        assert_acyclic(gates)
        return {
            "name": name,
            "description": description,
            "members": members,
            "gates": gates,
        }

    # ------------------------------------------------------------------ #
    # CRUD
    # ------------------------------------------------------------------ #
    async def list_joints(self) -> list[dict[str, Any]]:
        rows = await self.db.all(
            "SELECT id, name, description, created_at, updated_at FROM joints ORDER BY rowid DESC"
        )
        out: list[dict[str, Any]] = []
        for r in rows:
            jid = r["id"]
            members = await self.db.all(
                "SELECT session_id FROM joint_members WHERE joint_id=? ORDER BY ord, rowid",
                (jid,),
            )
            gate_rows = await self.db.all(
                "SELECT status, COUNT(*) AS n FROM joint_gates WHERE joint_id=? GROUP BY status",
                (jid,),
            )
            counts = {g["status"]: g["n"] for g in gate_rows}
            out.append({
                "id": jid,
                "name": r["name"],
                "description": r["description"],
                "created_at": float(r["created_at"] or 0),
                "updated_at": float(r["updated_at"] or 0),
                "member_count": len(members),
                "members": [m["session_id"] for m in members],
                "gate_count": sum(counts.values()),
                "released_count": counts.get(RELEASED, 0),
                "missed_count": counts.get(MISSED, 0),
                "waiting_count": counts.get(WAITING, 0),
            })
        return out

    async def _insert_joint_rows(self, jid: str, spec: dict[str, Any], now: str) -> None:
        await self.db.db.execute(
            "INSERT INTO joints(id, name, description, created_at, updated_at) "
            "VALUES(?,?,?,?,?)",
            (jid, spec["name"], spec["description"], now, now),
        )
        for ord_, sid in enumerate(spec["members"]):
            await self.db.db.execute(
                "INSERT INTO joint_members(joint_id, session_id, ord) VALUES(?,?,?)",
                (jid, sid, ord_),
            )
        for g in spec["gates"]:
            await self.db.db.execute(
                "INSERT INTO joint_gates"
                "(joint_id, gate_id, target_session, target_stage, title, public_text, "
                " views, status) VALUES(?,?,?,?,?,?,?,?)",
                (jid, g["id"], g["target_session"], g["target_stage"], g["title"],
                 g["public"], dumps(g["views"]), WAITING),
            )
            for n, b in enumerate(g["prerequisites"]):
                await self.db.db.execute(
                    "INSERT INTO joint_branches"
                    "(joint_id, gate_id, branch_no, session_id, stage_id, kind, role_id, value) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (jid, g["id"], n, b["session_id"], b["stage_id"], b["kind"],
                     b["role_id"], b["value"]),
                )

    async def create_joint(self, raw: Any) -> dict[str, Any]:
        spec = await self.validate_spec(raw)
        jid = new_joint_id()
        now = str(now_ts())
        async with self._lock:
            await self.db.begin()
            try:
                await self._insert_joint_rows(jid, spec, now)
                await self.db.commit()
            except Exception:
                await self.db.rollback()
                raise
        # Gates whose prerequisites are already satisfied and whose target is
        # already open must release immediately on creation.
        await self.reconcile_joint(jid)
        return await self.get_joint(jid)

    async def _row(self, jid: str):
        return await self.db.one(
            "SELECT id, name, description, created_at, updated_at FROM joints WHERE id=?",
            (jid,),
        )

    async def delete_joint(self, jid: str) -> None:
        async with self._lock:
            if await self._row(jid) is None:
                raise JointError("联演不存在", 404)
            locked = await self.db.one(
                "SELECT COUNT(*) AS n FROM joint_gates "
                "WHERE joint_id=? AND status!=?",
                (jid, WAITING),
            )
            if locked and locked["n"] > 0:
                raise JointError(
                    "已有注入释放或错过窗口的联演不能删除（需保留事件追溯）", 409,
                )
            await self.db.begin()
            try:
                await self.db.db.execute("DELETE FROM joint_branches WHERE joint_id=?", (jid,))
                await self.db.db.execute("DELETE FROM joint_branch_hits WHERE joint_id=?", (jid,))
                await self.db.db.execute("DELETE FROM joint_gates WHERE joint_id=?", (jid,))
                await self.db.db.execute("DELETE FROM joint_members WHERE joint_id=?", (jid,))
                await self.db.db.execute("DELETE FROM joints WHERE id=?", (jid,))
                await self.db.commit()
            except Exception:
                await self.db.rollback()
                raise

    async def replace_joint(self, jid: str, raw: Any) -> dict[str, Any]:
        """Replace a joint's definition.

        Gates that already released or missed are immutable audit anchors:
        removing them or changing their definition is a 409. A waiting gate
        that keeps its identity (same canonical definition) keeps its branch
        hits; an edited waiting gate is reset (hits re-recorded on the next
        reconciliation).
        """
        spec = await self.validate_spec(raw)
        async with self._lock:
            row = await self._row(jid)
            if row is None:
                raise JointError("联演不存在", 404)
            old_gate_rows = await self.db.all(
                "SELECT gate_id, status FROM joint_gates WHERE joint_id=?", (jid,),
            )
            old_status = {r["gate_id"]: r["status"] for r in old_gate_rows}
            new_by_id = {g["id"]: g for g in spec["gates"]}

            for gid, status in old_status.items():
                if status == WAITING:
                    continue
                if gid not in new_by_id:
                    raise JointError(f"已释放/错过的编排 {gid} 不能删除", 409)
            # Hashes of frozen gates must match exactly; compare in Python so
            # the check never depends on DB-side normalization.
            old_hashes: dict[str, str] = {}
            if any(s != WAITING for s in old_status.values()):
                for g in (await self._gate_defs(jid)):
                    if old_status.get(g["id"]) != WAITING:
                        old_hashes[g["id"]] = _gate_hash(g)
                for gid, h in old_hashes.items():
                    if _gate_hash(new_by_id[gid]) != h:
                        raise JointError(f"已释放/错过的编排 {gid} 不能修改", 409)

            now = str(now_ts())
            await self.db.begin()
            try:
                await self.db.db.execute(
                    "UPDATE joints SET name=?, description=?, updated_at=? WHERE id=?",
                    (spec["name"], spec["description"], now, jid),
                )
                await self.db.db.execute("DELETE FROM joint_members WHERE joint_id=?", (jid,))
                for ord_, sid in enumerate(spec["members"]):
                    await self.db.db.execute(
                        "INSERT INTO joint_members(joint_id, session_id, ord) VALUES(?,?,?)",
                        (jid, sid, ord_),
                    )

                # Drop waiting gates that disappeared / were replaced; frozen
                # gates stay untouched (validated identical above).
                frozen_ids = {gid for gid, st in old_status.items() if st != WAITING}
                for gid, st in list(old_status.items()):
                    if st == WAITING:
                        await self.db.db.execute(
                            "DELETE FROM joint_branch_hits WHERE joint_id=? AND gate_id=?",
                            (jid, gid),
                        )
                        await self.db.db.execute(
                            "DELETE FROM joint_branches WHERE joint_id=? AND gate_id=?",
                            (jid, gid),
                        )
                        await self.db.db.execute(
                            "DELETE FROM joint_gates WHERE joint_id=? AND gate_id=?",
                            (jid, gid),
                        )
                for g in spec["gates"]:
                    if g["id"] in frozen_ids:
                        continue
                    await self.db.db.execute(
                        "INSERT INTO joint_gates"
                        "(joint_id, gate_id, target_session, target_stage, title, public_text, "
                        " views, status) VALUES(?,?,?,?,?,?,?,?)",
                        (jid, g["id"], g["target_session"], g["target_stage"], g["title"],
                         g["public"], dumps(g["views"]), WAITING),
                    )
                    for n, b in enumerate(g["prerequisites"]):
                        await self.db.db.execute(
                            "INSERT INTO joint_branches"
                            "(joint_id, gate_id, branch_no, session_id, stage_id, kind, "
                            " role_id, value) VALUES(?,?,?,?,?,?,?,?)",
                            (jid, g["id"], n, b["session_id"], b["stage_id"], b["kind"],
                             b["role_id"], b["value"]),
                        )
                await self.db.commit()
            except Exception:
                await self.db.rollback()
                raise
        await self.reconcile_joint(jid)
        return await self.get_joint(jid)

    async def _gate_defs(self, jid: str) -> list[dict[str, Any]]:
        """Gate definitions with branches (canonical shape, for hashing)."""
        gates = await self.db.all(
            "SELECT gate_id, target_session, target_stage, title, public_text, views "
            "FROM joint_gates WHERE joint_id=? ORDER BY rowid",
            (jid,),
        )
        out: list[dict[str, Any]] = []
        for r in gates:
            branches = await self.db.all(
                "SELECT session_id, stage_id, kind, role_id, value "
                "FROM joint_branches WHERE joint_id=? AND gate_id=? ORDER BY branch_no",
                (jid, r["gate_id"]),
            )
            out.append({
                "id": r["gate_id"],
                "target_session": r["target_session"],
                "target_stage": r["target_stage"],
                "title": r["title"],
                "public": r["public_text"],
                "views": json.loads(r["views"] or "{}"),
                "prerequisites": [dict(b) for b in branches],
            })
        return out

    # ------------------------------------------------------------------ #
    # runtime reconciliation
    # ------------------------------------------------------------------ #
    async def _member_states(
        self, member_ids: list[str]
    ) -> dict[str, tuple[dict[str, Any], DrillDef, Any]]:
        states: dict[str, tuple[dict[str, Any], DrillDef, Any]] = {}
        for sid in member_ids:
            try:
                states[sid] = await self.engine.load(sid)
            except EngineError:
                continue
        return states

    async def reconcile_session(self, session_id: str) -> set[str]:
        """Reconcile every joint containing ``session_id``. Returns the set
        of sessions that actually received an injection (callers broadcast
        their snapshots)."""
        rows = await self.db.all(
            "SELECT DISTINCT joint_id FROM joint_members WHERE session_id=?",
            (session_id,),
        )
        delivered: set[str] = set()
        for r in rows:
            delivered |= await self.reconcile_joint(r["joint_id"])
        return delivered

    async def reconcile_all(self) -> None:
        """Startup sweep: re-evaluate every joint after a restart."""
        rows = await self.db.all("SELECT id FROM joints ORDER BY rowid")
        for r in rows:
            try:
                await self.reconcile_joint(r["id"])
            except Exception as exc:  # never block startup on one bad joint
                print(f"[joint] reconcile {r['id']} failed: {exc}")

    async def reconcile_joint(self, jid: str) -> set[str]:
        """Idempotent sweep of one joint. Safe to run after ANY member
        session commit and as many times as needed:

        1. record one-shot hits for branches whose outcome is observed;
        2. mark gates ``missed`` when the target stage sealed / the target
           session ended before all branches were met;
        3. release gates whose branches all met while the target stage is
           open — the delivery itself re-checks openness atomically in the
           target session's transaction.
        """
        async with self._lock:
            if await self._row(jid) is None:
                return set()
            members = [
                r["session_id"]
                for r in await self.db.all(
                    "SELECT session_id FROM joint_members WHERE joint_id=? ORDER BY ord, rowid",
                    (jid,),
                )
            ]
            loaded = await self._member_states(members)

            candidates: list[dict[str, Any]] = []
            await self.db.begin()
            try:
                gate_rows = await self.db.all(
                    "SELECT gate_id, target_session, target_stage, status, title, public_text, "
                    "views FROM joint_gates WHERE joint_id=? ORDER BY rowid",
                    (jid,),
                )
                hit_rows = await self.db.all(
                    "SELECT gate_id, branch_no FROM joint_branch_hits WHERE joint_id=?",
                    (jid,),
                )
                hit_keys = {(h["gate_id"], h["branch_no"]) for h in hit_rows}

                for gr in gate_rows:
                    if gr["status"] != WAITING:
                        continue
                    gid = gr["gate_id"]
                    branches = await self.db.all(
                        "SELECT branch_no, session_id, stage_id, kind, role_id, value "
                        "FROM joint_branches WHERE joint_id=? AND gate_id=? ORDER BY branch_no",
                        (jid, gid),
                    )
                    bstates = {sid: t[2] for sid, t in loaded.items()}
                    all_met = True
                    for b in branches:
                        bdict = dict(b)
                        met = branch_satisfied(bdict, bstates)
                        if met and (gid, b["branch_no"]) not in hit_keys:
                            sealed = bstates[b["session_id"]].sealed.get(b["stage_id"])
                            await self.db.db.execute(
                                "INSERT OR IGNORE INTO joint_branch_hits"
                                "(joint_id, gate_id, branch_no, event_version, at) "
                                "VALUES(?,?,?,?,?)",
                                (jid, gid, b["branch_no"], sealed.version, str(sealed.at)),
                            )
                            hit_keys.add((gid, b["branch_no"]))
                        if not met:
                            all_met = False

                    tgt = loaded.get(gr["target_session"])
                    if tgt is None:
                        continue
                    tstate = tgt[2]
                    if tstate.status == "ended":
                        await self._set_gate_status(jid, gid, MISSED)
                        continue
                    if gr["target_stage"] in tstate.sealed:
                        # The one-shot delivery window closed before release.
                        await self._set_gate_status(jid, gid, MISSED)
                        continue
                    if all_met and tstate.current == gr["target_stage"]:
                        candidates.append({
                            "gate_id": gid,
                            "target_session": gr["target_session"],
                            "target_stage": gr["target_stage"],
                            "title": gr["title"],
                            "public": gr["public_text"],
                            "views": json.loads(gr["views"] or "{}"),
                        })
                await self.db.commit()
            except Exception:
                await self.db.rollback()
                raise

            delivered_sessions: set[str] = set()
            for g in candidates:
                target_sid = g["target_session"]
                _, td, _ = loaded[target_sid]
                # Permission filter: only roles of the target session's
                # pinned script may ever receive a private view.
                views = {r: t for r, t in g["views"].items() if r in td.roles}
                injection_id = f"inj_{jid}_{g['gate_id']}"
                resp = await self.engine.append_injection(
                    target_sid,
                    injection_id,
                    jid,
                    g["gate_id"],
                    g["target_stage"],
                    g["title"],
                    g["public"],
                    views,
                )
                await self.db.begin()
                try:
                    if resp.get("delivered"):
                        await self.db.db.execute(
                            "UPDATE joint_gates SET status=?, delivered_at=?, event_version=? "
                            "WHERE joint_id=? AND gate_id=? AND status=?",
                            (RELEASED, str(resp["at"]), resp["version"],
                             jid, g["gate_id"], WAITING),
                        )
                        delivered_sessions.add(target_sid)
                    elif resp.get("reason") == "duplicate":
                        # Crash recovery: the event exists but the gate row
                        # was never marked. Recover its version/at from the
                        # target session's log.
                        inj = loaded[target_sid][2].injections.get(injection_id)
                        if inj is None:
                            _, _, reloaded = await self.engine.load(target_sid)
                            inj = reloaded.injections.get(injection_id)
                        if inj is not None:
                            await self.db.db.execute(
                                "UPDATE joint_gates SET status=?, delivered_at=?, event_version=? "
                                "WHERE joint_id=? AND gate_id=? AND status=?",
                                (RELEASED, str(inj.at), inj.version,
                                 jid, g["gate_id"], WAITING),
                            )
                            delivered_sessions.add(target_sid)
                    else:
                        # stage_closed (raced with a seal) -> window missed.
                        await self._set_gate_status(jid, g["gate_id"], MISSED)
                    await self.db.commit()
                except Exception:
                    await self.db.rollback()
                    raise
            return delivered_sessions

    async def _set_gate_status(self, jid: str, gid: str, status: str) -> None:
        await self.db.db.execute(
            "UPDATE joint_gates SET status=? WHERE joint_id=? AND gate_id=? AND status=?",
            (status, jid, gid, WAITING),
        )

    # ------------------------------------------------------------------ #
    # read models (host console / monitoring)
    # ------------------------------------------------------------------ #
    async def get_joint(self, jid: str) -> dict[str, Any]:
        row = await self._row(jid)
        if row is None:
            raise JointError("联演不存在", 404)
        member_ids = [
            r["session_id"]
            for r in await self.db.all(
                "SELECT session_id FROM joint_members WHERE joint_id=? ORDER BY ord, rowid",
                (jid,),
            )
        ]
        loaded = await self._member_states(member_ids)
        sessions_meta = await self._sessions_meta(member_ids, loaded)

        gate_rows = await self.db.all(
            "SELECT gate_id, target_session, target_stage, title, public_text, views, status, "
            "delivered_at, event_version FROM joint_gates WHERE joint_id=? ORDER BY rowid",
            (jid,),
        )
        gates_out = []
        for gr in gate_rows:
            branches = await self.db.all(
                "SELECT branch_no, session_id, stage_id, kind, role_id, value "
                "FROM joint_branches WHERE joint_id=? AND gate_id=? ORDER BY branch_no",
                (jid, gr["gate_id"]),
            )
            bstates = {sid: t[2] for sid, t in loaded.items()}
            pre = []
            unsatisfied = []
            for b in branches:
                b = dict(b)
                met = branch_satisfied(b, bstates)
                pre.append(self._branch_view(b, met, loaded))
                if not met:
                    unsatisfied.append(pre[-1]["label"])
            tgt = loaded.get(gr["target_session"])
            tstage_title = gr["target_stage"]
            if tgt is not None:
                tstage_title = tgt[1].stages.get(gr["target_stage"])
                tstage_title = tstage_title.title if tstage_title else gr["target_stage"]

            status, reason = gr["status"], None
            if status == WAITING:
                if unsatisfied:
                    reason = f"等待 {len(unsatisfied)} 个分支：" + "；".join(unsatisfied)
                elif tgt is None or tgt[2].current != gr["target_stage"]:
                    tname = sessions_meta.get(gr["target_session"], {}).get("drill_name", gr["target_session"])
                    reason = f"前置已全部满足，等待会话进入阶段「{tstage_title}」（{tname}）"
            elif status == MISSED:
                reason = "目标阶段已结算或会话已结束，一次性注入窗口已错过"

            gates_out.append({
                "id": gr["gate_id"],
                "target_session": gr["target_session"],
                "target_stage": gr["target_stage"],
                "target_stage_title": tstage_title,
                "title": gr["title"],
                "public": gr["public_text"],
                "views": json.loads(gr["views"] or "{}"),
                "status": status,
                "status_label": {WAITING: "等待中", RELEASED: "已投递", MISSED: "已错过"}[status],
                "delivered_at": float(gr["delivered_at"]) if gr["delivered_at"] else None,
                "event_version": gr["event_version"],
                "prerequisites": pre,
                "block_reason": reason,
                "ready": status == WAITING and not unsatisfied,
            })

        return {
            "id": jid,
            "name": row["name"],
            "description": row["description"],
            "created_at": float(row["created_at"] or 0),
            "updated_at": float(row["updated_at"] or 0),
            "members": [sessions_meta[sid] for sid in member_ids if sid in sessions_meta],
            "gates": gates_out,
        }

    def _branch_view(
        self,
        b: dict[str, Any],
        met: bool,
        loaded: dict[str, tuple[dict[str, Any], DrillDef, Any]],
    ) -> dict[str, Any]:
        sess, d = loaded.get(b["session_id"], (None, None, None))[:2]
        stage = d.stages.get(b["stage_id"]) if d else None
        stage_title = stage.title if stage else b["stage_id"]
        if b["kind"] == "next_stage":
            if b["value"]:
                t = d.stages.get(b["value"]) if d else None
                value_label = t.title if t else b["value"]
            else:
                value_label = "（演练结束）"
            kind_label = "结局跳转"
            role_name = ""
        else:
            role = d.roles.get(b["role_id"]) if d else None
            role_name = role.name if role else b["role_id"]
            value_label = b["value"]
            if stage is not None:
                for o in stage.options.get(b["role_id"], []):
                    if o.id == b["value"]:
                        value_label = o.text
            kind_label = f"{role_name} 选择"
        drill_name = d.name if d else b["session_id"]
        label = f"[{drill_name}] 阶段「{stage_title}」{kind_label}＝{value_label}"
        sealed = None
        if met:
            st3 = loaded[b["session_id"]][2]
            sc = st3.sealed.get(b["stage_id"])
            if sc is not None:
                sealed = {"version": sc.version, "at": sc.at}
        return {
            "session_id": b["session_id"],
            "stage_id": b["stage_id"],
            "stage_title": stage_title,
            "kind": b["kind"],
            "role_id": b.get("role_id", ""),
            "role_name": role_name,
            "value": b.get("value", ""),
            "value_label": value_label,
            "satisfied": met,
            "sealed": sealed,
            "label": label,
        }

    async def _sessions_meta(
        self,
        member_ids: list[str],
        loaded: dict[str, tuple[dict[str, Any], DrillDef, Any]],
    ) -> dict[str, Any]:
        drill_names: dict[str, str] = {}
        try:
            for s in await self.engine.list_sessions():
                drill_names[s["id"]] = s.get("drill_id", "")
        except Exception:
            pass
        out: dict[str, Any] = {}
        for sid in member_ids:
            t = loaded.get(sid)
            if t is None:
                continue
            sess, d, st = t
            out[sid] = {
                "session_id": sid,
                "drill_id": d.id,
                "drill_name": d.name,
                "drill_version": sess["drill_version"],
                "digest_short": (sess.get("drill_sha256") or "")[:12],
                "status": st.status,
                "current_stage": st.current,
                "current_stage_title": (
                    d.stages[st.current].title if st.current and st.current in d.stages else None
                ),
                "sealed_stages": list(st.sealed.keys()),
                "stage_order": list(st.order),
                # Tip versions for the postmortem prefix inputs.
                "last_version": st.last_version,
                "last_event_at": max(
                    [s.at for s in st.sealed.values()]
                    + [i.at for i in st.injections.values()]
                    + [x for x in (st.ended_at, st.started_at) if x],
                    default=None,
                ),
            }
        return out

    async def joints_for_session(self, session_id: str) -> list[dict[str, Any]]:
        """Joint read models touching one session (host console of a
        session polls this to show branch waiting state and blockers)."""
        rows = await self.db.all(
            "SELECT joint_id FROM joint_members WHERE session_id=? ORDER BY rowid",
            (session_id,),
        )
        out = []
        for r in rows:
            try:
                detail = await self.get_joint(r["joint_id"])
            except JointError:
                continue
            out.append(detail)
        return out

    # ------------------------------------------------------------------ #
    # causal-timeline postmortem (read-only deterministic review)
    # ------------------------------------------------------------------ #
    async def review_joint(
        self, jid: str, raw: Any
    ) -> dict[str, Any]:
        """Build a consistent as-of postmortem report.

        Pure read: the frozen gate definitions, each member session's pinned
        script version and immutable event log are the only inputs; nothing in
        ``raw`` can mutate a live or finished exercise.
        """
        from .postmortem import (
            _Member,
            build_postmortem,
            filter_prefix_events,
            parse_review_request,
        )

        row = await self._row(jid)
        if row is None:
            raise JointError("联演不存在", 404)
        member_ids = [
            r["session_id"]
            for r in await self.db.all(
                "SELECT session_id FROM joint_members WHERE joint_id=? ORDER BY ord, rowid",
                (jid,),
            )
        ]
        mode, as_of = parse_review_request(raw, member_ids)
        prefixes = raw.get("prefixes") if mode == "prefix" else None

        # Frozen gate definitions + stored runtime columns (for cross-checking
        # the recomputed status against the live rows).
        gate_rows = await self.db.all(
            "SELECT gate_id, target_session, target_stage, title, public_text, views, "
            "status FROM joint_gates WHERE joint_id=? ORDER BY rowid",
            (jid,),
        )
        gates_def: list[dict[str, Any]] = []
        for gr in gate_rows:
            branches = await self.db.all(
                "SELECT branch_no, session_id, stage_id, kind, role_id, value "
                "FROM joint_branches WHERE joint_id=? AND gate_id=? ORDER BY branch_no",
                (jid, gr["gate_id"]),
            )
            gates_def.append({
                "gate_id": gr["gate_id"],
                "target_session": gr["target_session"],
                "target_stage": gr["target_stage"],
                "title": gr["title"],
                "public_text": gr["public_text"],
                "views_obj": json.loads(gr["views"] or "{}"),
                "status": gr["status"],
                "branches": [dict(b) for b in branches],
            })

        members: list[_Member] = []
        for ord_, sid in enumerate(member_ids):
            sess, d = await self.engine.get_session_definition(sid)
            rows = await self.db.all(
                "SELECT version, seq, type, actor, at, payload FROM events "
                "WHERE session_id=? ORDER BY version, seq",
                (sid,),
            )
            all_events = [
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
            prefix_v = prefixes[sid] if prefixes is not None else None
            events = filter_prefix_events(all_events, mode, as_of, prefix_v)
            members.append(
                _Member(ord_, sess, d, all_events, events, reduce_events(events))
            )

        return build_postmortem(dict(row), members, gates_def, mode, as_of)

    async def session_script(self, session_id: str) -> dict[str, Any]:
        """Pinned script reference data used by the joint editor when it
        offers stages/roles/options for prerequisite and target pickers."""
        try:
            sess, d = await self.engine.get_session_definition(session_id)
        except EngineError as exc:
            raise JointError("会话不存在", 404) from exc
        return {
            "session_id": session_id,
            "drill_id": d.id,
            "drill_name": d.name,
            "drill_version": sess["drill_version"],
            "digest_short": (sess.get("drill_sha256") or "")[:12],
            "roles": [{"id": r.id, "name": r.name, "invite": r.invite} for r in d.roles.values()],
            "stages": [
                {
                    "id": s.id,
                    "title": s.title,
                    "duration": s.duration,
                    "deciding_roles": [
                        {
                            "role_id": rid,
                            "role_name": d.roles[rid].name,
                            "options": [{"id": o.id, "text": o.text} for o in olist],
                        }
                        for rid, olist in s.options.items()
                    ],
                }
                for s in d.stages.values()
            ],
        }
