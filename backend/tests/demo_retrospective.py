"""Drive a realistic joint through the real app and print a real retrospective
request + response, for documentation/manual verification.

The drill lifecycle is driven in-process through the app's real Engine +
JointOrchestrator (same objects the lifespan wires up), and the retrospective
itself is fetched over real HTTP with TestClient. Runs against an isolated
temp DB — it never touches real data."""
import json
import os
import sys
import tempfile
from pathlib import Path

import yaml

TMP = tempfile.mkdtemp(prefix="retro-demo-")
os.environ["DATA_DIR"] = TMP
os.environ["DRILLS_DIR"] = str(Path(TMP) / "drills")
os.environ["STATIC_DIR"] = str(Path(__file__).resolve().parents[1] / "frontend")
os.environ["HOST_KEY"] = "host-1234"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from starlette.testclient import TestClient
from app.main import app, engine, joints

JUP = {
    "id": "jup", "name": "上游指挥中心",
    "roles": [{"id": "a", "name": "值班长甲", "invite": "AAA"}],
    "stages": [
        {"id": "s1", "title": "泄漏研判", "public": "反应釜泄漏", "duration": None,
         "default_next": "s2",
         "options": {"a": [{"id": "go", "text": "确认扩大预警"}, {"id": "stop", "text": "控制中"}]}},
        {"id": "s2", "title": "上游处置", "public": "p2", "duration": None,
         "default_next": None, "options": {"a": [{"id": "k", "text": "结束"}]}},
    ],
}
JDOWN = {
    "id": "jdown", "name": "下游厂区",
    "roles": [{"id": "b", "name": "值班长乙", "invite": "BBB"}],
    "stages": [
        {"id": "t1", "title": "下游警戒", "public": "下游待命", "duration": None,
         "default_next": "t2",
         "options": {"b": [{"id": "k", "text": "完成"}]}},
        {"id": "t2", "title": "下游收尾", "public": "t2", "duration": None,
         "default_next": None, "options": {"b": [{"id": "z", "text": "结束"}]}},
    ],
}
HOST = {"X-Host-Key": "host-1234"}


async def drive(client_state):
    su = await engine.create_session("jup")
    sd = await engine.create_session("jdown")
    spec = {"name": "厂际联动复盘示例", "description": "上下游 AND 汇合",
            "members": [su, sd],
            "gates": [{"id": "g1", "target_session": sd, "target_stage": "t1",
                       "title": "上游预警联动", "public": "上游请求协同关阀",
                       "prerequisites": [{"session_id": su, "stage_id": "s1",
                                          "kind": "choice", "role_id": "a",
                                          "value": "go"}]}]}
    jid = (await joints.create_joint(spec))["id"]
    # target session joins + starts first (t1 open, waiting)
    await engine.join(sd, "BBB", "乙")
    await engine.start(sd)
    # upstream joins + starts + commits the watched choice (s1 -> s2), then
    # reconciliation releases the injection into the open target stage
    await engine.join(su, "AAA", "甲")
    await engine.start(su)
    await engine.submit(su, "a", "s1", "go")
    await joints.reconcile_session(su)
    client_state.update(jid=jid, su=su, sd=sd)


with TestClient(app) as c:
    for dct in (JUP, JDOWN):
        r = c.post("/api/drills", headers=HOST,
                   json={"yaml": yaml.safe_dump(dct, allow_unicode=True, sort_keys=False)})
        assert r.status_code == 200, r.text
        r = c.post(f"/api/drills/{dct['id']}/versions/1/publish", headers=HOST,
                   json={"base_version": None})
        assert r.status_code == 200, r.text

    state: dict = {}
    # anyio BlockingPortal.call awaits the returned coroutine in the app loop
    c.portal.call(drive, state)
    jid, su, sd = state["jid"], state["su"], state["sd"]

    inj = c.get(f"/api/sessions/{sd}/record", params={"fmt": "json"}, headers=HOST).json()
    inj_at = next(i["at"] for s in inj["stages"] for i in s.get("joint_injections", []))

    print("=" * 72)
    print("REQUEST (统一截止时刻模式):")
    print(f"POST /api/joints/{jid}/retrospective")
    print("Header : X-Host-Key: host-1234")
    req = {"cutoff": round(inj_at + 1, 3)}
    print("Body   :", json.dumps(req, ensure_ascii=False))
    r = c.post(f"/api/joints/{jid}/retrospective", headers=HOST, json=req)
    rep = r.json()
    assert r.status_code == 200, rep

    compact = {
        "kind": rep["kind"],
        "joint_id": rep["joint_id"],
        "joint_name": rep["joint_name"],
        "mode": rep["mode"],
        "cutoff": rep["cutoff"],
        "effective_as_of": rep["effective_as_of"],
        "horizon_consistent": rep["horizon_consistent"],
        "counts": rep["counts"],
        "critical_path": rep["critical_path"],
        "bottleneck_gate": rep["bottleneck_gate"],
        "members": [
            {"session_id": m["session_id"], "ord": m["ord"], "drill_id": m["drill_id"],
             "drill_name": m["drill_name"], "drill_version": m["drill_version"],
             "digest_short": m["digest_short"], "status": m["status"],
             "included_version": m["included_version"],
             "prefix_truncated": m["prefix_truncated"]}
            for m in rep["members"]
        ],
        "gates": [
            {"id": g["id"], "title": g["title"], "status": g["status"],
             "status_label": g["status_label"], "wait_seconds": g["wait_seconds"],
             "path_seconds": g["path_seconds"], "critical": g["critical"],
             "block_reason": g["block_reason"],
             "target_session": g["target_session"], "target_stage": g["target_stage"],
             "bottleneck_branch": (g["bottleneck_branch"] or {}).get("label"),
             "last_prerequisite_event": g["last_prerequisite_event"],
             "target_open_event": g["target_open_event"],
             "injection_event": g["injection_event"],
             "prerequisites": [{"label": p["label"], "satisfied": p["satisfied"],
                                "hit_at": p["hit_at"], "hit_event": p["hit_event"]}
                               for p in g["prerequisites"]]}
            for g in rep["gates"]
        ],
        "timeline_marks_preview": [
            {"at": round(e["at"], 3), "session_ord": e["session_ord"],
             "v": f"{e['version']}.{e['seq']}", "title": e["title"],
             "marks": [m["kind"] for m in e["marks"]]}
            for e in rep["timeline"] if e["marks"]
        ],
    }
    print("HTTP   :", r.status_code)
    print("RESPONSE (节选；完整 JSON 由主持台“下载 JSON”获取):")
    print(json.dumps(compact, ensure_ascii=False, indent=2))

    out = Path(TMP) / "retro_response_full.json"
    out.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    print("=" * 72)
    print("完整响应已保存:", out)
    print("JID:", jid, " SU:", su, " SD:", sd)
