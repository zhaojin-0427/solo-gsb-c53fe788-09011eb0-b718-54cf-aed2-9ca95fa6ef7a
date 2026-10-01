# 多人应急演练 Web 应用

主持人用 **YAML** 定义演练的阶段、各角色可见信息、限时选项与条件分支；参与者凭
**角色邀请码** 加入，只能看到本角色的内容并在限时内提交选择。后端基于
**FastAPI + SQLite + WebSocket**，前端为无构建步骤的原生 JavaScript。

- **剧本版本与发布管理**：上传/编辑 YAML 时生成不可变版本号（v1、v2…）与
  **SHA-256 内容摘要**；内容相同则复用同一版本（按摘要幂等去重）。草稿只有
  **发布后**才能用于新会话；发布采用基准版本乐观锁，他人抢先发布会被拒绝且
  **不改变当前发布版**。主持台可查看版本列表、按结构（阶段/角色/视图/选项/
  转移规则）对比任意两版、从任一历史版本复制为新草稿。
- **会话永久绑定创建时版本**：已创建或进行中的会话固化其剧本次序号与摘要，
  后续编辑、再发布都不会改变它的快照、分支与回放；API、WebSocket 快照、
  回放与记录导出均带版本标识。
- 服务端以 **单调递增版本号** 原子结算每一轮（每次状态变更是一个数据库版本）
- 正确处理 **重复提交**（幂等）、**断线重连**（WebSocket 自动重连 + 快照重同步）和
  **截止时刻的并发竞争**（拒绝与结算在同一个 SQLite 立即事务内完成）
- 每次状态变化向客户端推送 **按权限裁剪的快照**（角色视角 / 主持人全量视角）
- 结束后可依据不可变事件日志 **确定性回放任一版本**，并导出
  **个人记录（不含任何他人隐藏信息）** 与 **主持人全量记录**
- **联合演练编排（多会话联动）**：主持人可把多个**独立会话**编入同一场联演，
  为各会话阶段配置**有向依赖**；上游阶段达到指定结局（结算跳转或某角色的确定
  选项）后，系统向目标会话的指定阶段**追加一次性联演注入**并按该会话角色权限
  推送，**多个前置分支须全部满足**（AND 汇合）才释放。保存前校验依赖**无环**且
  会话、阶段、结局、角色均有效；重复/乱序到达不会重复投递或提前解锁；主持台显示
  **分支等待状态与阻塞原因**。注入作为事件写入**目标会话自己的事件日志**，各会话
  仍保留独立的剧本版本与事件记录。
- **联演因果时序复盘**：主持人选择一场**已保存的联合演练**并提交**统一复盘截止
  时刻**（或按各成员会话选择**事件版本前缀**），系统读取各成员会话**冻结剧本
  版本**与**不可变事件日志**，按 *事件时间 → 会话顺序号 → 会话版本号 → 事件
  序号* 稳定合并，标出每个**前置结局 / 分支命中（含结局不符）/ 目标阶段开放 /
  联演注入 / 阶段结算**；对每条编排计算从**最后一个前置满足（AND 汇合点）**到
  注入投递或窗口关闭的等待秒数、AND 汇合耗时与瓶颈分支，并沿**分支链**汇总
  **关键路径与瓶颈编排**。编排状态区分为**尚未满足 / 目标未开放 / 已错过 /
  已投递**，全部带**阻塞原因与可追溯事件引用**（`<会话>#v<版本>.<seq>`）。
  两种截至口径都生成**一致的截至时刻报告**：严格“早于截止”的整版本前缀使
  **同秒与迟到事件不得改写既有前缀**，同一截至时刻重复生成结果逐字节一致。
  主持台支持按状态/成员会话/标注类型/关键字**筛选**并一键**下载 JSON**。

---

## 目录结构

```
.
├── backend/
│   ├── app/
│   │   ├── main.py          # FastAPI：HTTP API、WebSocket、定时结算、静态托管
│   │   ├── engine.py        # 纯事件溯源 reducer + 原子提交（版本号/结算/幂等）
│   │   ├── joints.py        # 联合演练编排：无环校验、AND 汇合仲裁、一次性注入投递
│   │   ├── postmortem.py    # 联演因果时序复盘：稳定合并、等待秒数、关键路径、一致截至报告
│   │   ├── definitions.py   # YAML 剧本解析与校验
│   │   ├── versions.py      # 不可变版本摘要（SHA-256）与结构化差异对比
│   │   ├── db.py            # SQLite（WAL、立即事务、事件日志、版本表与迁移）
│   │   ├── snapshots.py     # 按角色裁剪的快照构造
│   │   ├── records.py       # 确定性回放 + 个人/全量记录导出（Markdown/JSON）
│   │   ├── hub.py           # WebSocket 连接管理（在线状态、背压安全）
│   │   └── config.py        # 路径与密钥（环境变量）
│   ├── tests/
│   │   └── e2e_check.py     # 回归测试：版本/发布/绑定/差异、截止竞争、幂等、回放、迁移、端到端
│   └── requirements.txt
├── frontend/                # 原生 HTML/CSS/JS（由后端直接托管）
│   ├── index.html
│   ├── styles.css
│   └── app.js
├── drills/
│   └── chem-plant.yaml      # 示例剧本：化工厂反应釜泄漏与次生火灾
├── Dockerfile
└── docker-compose.yml
```

---

## 快速开始（Docker Compose）

前置要求：安装 Docker 与 Docker Compose 插件。

```bash
# 1. 启动（首次会构建镜像）
docker compose up -d --build

# 2. 查看日志
docker compose logs -f
```

打开浏览器访问：

| 入口 | 地址 |
| --- | --- |
| 应用首页（主持人/参与者入口） | <http://localhost:8000/> |
| 健康检查 | <http://localhost:8000/api/health> |
| OpenAPI 文档 | <http://localhost:8000/docs> |

停止与清理：

```bash
docker compose down          # 停止
docker compose down -v       # 同时删除会话数据库卷（drill-data）
```

### 验证部署

```bash
# 健康检查
curl http://localhost:8000/api/health        # => {"ok":true}

# 冒烟流程：上传草稿 → 发布 → 创建会话 → 加入 → 主持台开始 → 提交 → 版本/回放/导出
curl -H "X-Host-Key: host-1234" -H "Content-Type: application/json" \
     -X POST http://localhost:8000/api/drills \
     -d '{"yaml": "<YAML 文本>", "base_version": null}'
# => {"id":"...","version":1,"status":"draft","sha256":"…","digest_short":"…"}
curl -H "X-Host-Key: host-1234" -H "Content-Type: application/json" \
     -X POST http://localhost:8000/api/drills/<drill_id>/versions/1/publish \
     -d '{"base_version": null}'          # => {"version":1,"status":"published",…}
curl -H "X-Host-Key: host-1234" -X POST "http://localhost:8000/api/sessions?drill_id=<drill_id>"
# => {"session_id":"xxxxxxxx","drill_version":1,"drill_sha256":"…","digest_short":"…"}
curl -X POST "http://localhost:8000/api/sessions/<session_id>/join" \
     -H "Content-Type: application/json" -d '{"invite":"CMD-2026","name":"张三"}'
# => {"role_id":"cmd",...,"token":"...","version":1,"drill_version":1,"drill_sha256":"…"}
# 之后打开 http://localhost:8000/ 用主持人密钥进入主持台开始演练；
# 版本列表/差异/复制：
curl -H "X-Host-Key: host-1234" http://localhost:8000/api/drills/<drill_id>/versions
curl -H "X-Host-Key: host-1234" \
     "http://localhost:8000/api/drills/<drill_id>/diff?from=1&to=2"
# 回放与导出（响应均含 drill_version / drill_sha256）：
curl -H "X-Host-Key: host-1234" "http://localhost:8000/api/sessions/<session_id>/replay"
curl -H "X-Host-Key: host-1234" "http://localhost:8000/api/sessions/<session_id>/record?fmt=md"
```

### 回归测试

测试不依赖 Docker，直接在本机 Python 运行（覆盖：先加入再启动、末名提交
自动结算、截止竞争、**连续两个限时阶段间旧定时器的身份核对**（末名提交 /
主持人手动结算竞争，旧回调空转不产生事件、正常回调只结算一次）、并发幂等、
任意版本前缀回放、手动终止、定时器登记薄、HTTP+WebSocket 端到端，以及
**联演因果时序复盘**（四态判定、AND 汇合/等待秒数、沿分支链关键路径与瓶颈、
统一截止时刻严格前缀使同秒/迟到事件不改写、成员版本前缀一致性、证据不完整
标记、可追溯事件引用与纯只读性））：

```bash
cd backend
pip install -r requirements.txt httpx
python tests/e2e_check.py        # 全部通过时退出码为 0
```

### 不使用 Docker（本地开发）

```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
# 前端与 drills/ 目录通过相对路径自动定位；数据库默认写入仓库根 /data
```

---

## 配置

所有配置通过环境变量提供（见 `docker-compose.yml`）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HOST_KEY` | `host-1234` | 主持人密钥。前端首页输入；HTTP 用请求头 `X-Host-Key`，WebSocket 用查询参数 `host=`。**生产环境必须修改** |
| `DATA_DIR` | `/data` | SQLite 数据库目录（`drills.db`）。Compose 中为持久化卷 |
| `DRILLS_DIR` | `/drills` | YAML 剧本种子目录，启动时自动加载：新文件发布为 v1、变更文件导入为草稿。Compose 中为只读挂载，版本数据以数据库为准 |
| `STATIC_DIR` | `/app/frontend` | 前端静态文件目录 |

端口映射在 `docker-compose.yml` 的 `ports` 中修改（如 `"18000:8000"`）。
时间均以服务器 UTC 时间戳（POSIX 秒）传输，前端按浏览器本地时区显示。

> 部署模型为 **单副本**：写串行锁与 WebSocket 连接表在进程内维护。
> SQLite 本身已是单文件、零外部依赖；需要横向扩展时应改用粘性会话或外移状态。

---

## 使用指南

### 主持人

1. 打开 <http://localhost:8000/>，选择“主持人入口”，输入密钥（默认 `host-1234`）。
2. 剧本来源两种：
   - 预先把 YAML 放到宿主机 `./drills/` 目录（容器内 `/drills`，只读挂载）：
     服务**首次见到**该文件时自动发布为 v1；之后文件相对当前发布版发生变化时，
     会导入为一条**待发布草稿**（不影响线上会话）。
   - 在“主持人控制台”粘贴 YAML 上传/编辑：保存即生成一条不可变**草稿版本**与
     SHA-256 摘要（数据库为准；只读目录写不进去也不影响保存，响应里的
     `yaml_written` 表示是否同时镜像到了 `drills/`）。
3. 点击剧本的 **“版本管理”**：
   - 查看版本列表（`已发布 / 草稿 / 历史版本 / 已废弃草稿`、摘要、基准版本、时间）；
   - 在“结构化差异对比”里选择任意两版，按 **剧本属性 / 角色 / 阶段 /
     各角色视图 / 选项 / 转移规则** 查看差异；
   - 从任意历史版本 **“复制为新草稿”**，或载入发布版/草稿 YAML 继续编辑；
   - 校对无误后 **“发布此草稿”**。发布带乐观锁：若该草稿所基于的版本已被他人
     更新，发布返回 409 且**当前发布版完全不变**，需重新复制/编辑后再发布。
4. **草稿不能创建会话**；只有当前发布版可以。点击“创建会话（vN）”得到一个
   **8 位会话编号**，进入主持台。会话在创建瞬间固化 **剧本次序号 vN + 内容摘要**，
   此后即使剧本再发布新版，本会话的快照、分支、回放与导出都保持创建时版本。
5. 把 **邀请链接/邀请码** 发给对应角色的参与者（主持台可一键复制）。
6. 人员就绪后点击“▶ 开始演练”。主持台可看到：
   - 每个阶段的公开信息、**所有角色的隐藏信息**、选项指向的分支；
   - 各角色是否已提交、在线/离线状态、倒计时；
   - “⏭ 立即结算当前阶段”（未提交角色按超时处理）、“■ 结束整场演练”；
   - 顶部显示“会话单调版本号”与“绑定剧本版本/摘要”。
7. 结束后在主持台导出 **全量记录（Markdown/JSON）** 或打开“确定性回放”，
   两者都标注该会话固化的剧本版本与摘要。

### 参与者

1. 打开邀请链接（形如 `http://主机:8000/#/join/<会话编号>?invite=<邀请码>`），
   或在首页手动输入会话编号与邀请码、姓名。
2. 加入后浏览器本地保存访问令牌（用于断线重连、个人回放与导出）。
3. 页面只显示 **公开信息 + 本角色可见信息 + 本角色的选项**；
   在倒计时内点选并确认提交（**提交不可更改**）。
4. 网络中断会自动重连并重新获取快照；若重发了已提交的选择，服务器幂等返回原结果。
5. 演练结束后可导出自己的 **个人记录**，并查看自己视角的版本回放。

### 示例剧本的邀请码（chem-plant）

| 角色 | 邀请码 |
| --- | --- |
| 值班班长 | `CMD-2026` |
| 外操员 | `EVA-2026` |
| 安全员 | `SAF-2026` |

---

## 联合演练编排（多会话联动）

主持人控制台首页提供 **“🔗 联合演练编排”** 入口（`#/host/joints`），
用于把**多个各自独立的会话**（可来自不同剧本、各自固化不同剧本版本）
编入同一场联演。

### 编排模型

一条“注入编排（gate）”描述一个跨会话的有向依赖：

```text
前置分支 1：会话 A / 阶段 s1 结算后跳转 = s2
前置分支 2：会话 B / 阶段 t1 中角色 cmd 的选择 = evacuate
        └── 两个分支全部满足（AND），且会话 C 正处在阶段 u1 时
             ──▶ 向会话 C 的阶段 u1 一次性追加“联演注入”
                  · 公开文本：会话 C 全员可见
                  · 角色私有文本：仅该会话中对应角色可见（按角色权限裁剪）
```

- **前置结局有两类**：
  - `next_stage`（结算跳转）：上游阶段结算后解析出的下一阶段等于指定值；
    值为空表示该阶段以“演练结束”收尾；
  - `choice`（角色选择）：上游阶段结算记录中某角色**确定提交**了指定选项
    （超时记为 `null`，不会命中任何选项）。
- 一条编排可有**多个前置分支，必须全部满足**才释放（AND 汇合）；分支不可重复。
- 注入是**一次性**的：只在目标会话**正处在目标阶段、且阶段开放**时追加一次。

### 主持人操作

1. 先为已发布剧本创建需要的多个会话（各自绑定剧本版本），进入
   **主持人控制台 → 🔗 联合演练编排 → ＋ 编排新的联合演练**。
2. 勾选**参演会话**；为每条编排选择目标会话/目标阶段、填写公开注入与
   各角色私有注入，再添加一个或多个前置分支（编辑器按各会话**绑定剧本版本**
   提供阶段、角色、选项下拉）。
3. **保存**：服务端校验会话/阶段/结局/角色均有效、分支不重复、依赖图**无环**；
   任一不满足返回 400 且不写入。
4. 保存后进入**实时监控台**（页面每 2 秒刷新）：显示每个成员会话的当前/已结算
   阶段、每条编排的分支满足情况（✅/⏳）、整体状态
   （等待中 / **前置已齐·等待目标阶段开放** / 已投递 / 已错过）以及
   **阻塞原因**。各会话自己的主持台也内嵌“🔗 联合演练状态”面板。
5. 各会话照常开始、提交、结算；上游达成结局后系统自动仲裁并向目标会话推送。
6. 演练后（或演练中任一刻）点 **“⏱ 因果时序复盘”**：选择**统一截止时刻**或
   **各成员事件版本前缀**生成一致的截至时刻报告，查看四态阻塞原因、等待秒数、
   关键路径/瓶颈与带事件引用的合并时间轴，可在主持台筛选并**下载 JSON**
   （见下文“联演因果时序复盘”一节）。

### 一致性与边界

- **不重复投递**：每个分支命中只落一次（`joint_branch_hits` 主键去重）；
  注入事件 `joint_injection` 以确定性 id 写入目标会话事件日志，重复仲裁/乱序
  到达均幂等。
- **不提前解锁**：注入投递在目标会话自己的 `BEGIN IMMEDIATE` 事务里复核
  “阶段仍开放”；与结算竞争时若阶段已先封，则该编排记为 **`missed`（已错过）**，
  不会补投，也不会解锁已经结算的阶段。
- **各会话保持独立**：注入占用目标会话的一个**单调版本号**，写在它自己的事件
  日志中；上游会话的事件日志、剧本版本完全不受影响。快照、回放、个人/全量
  记录都包含本会话收到的注入，并继续按角色权限裁剪私有文本。
- **可追溯**：已 `released` / `missed` 的编排不可修改或删除（409）；只有
  全部处于“等待中”的联演可以删除。等待中的编排可自由编辑后重新保存。
- 服务重启后会对所有联演做一次全量重扫：分支已满足的补记命中、窗口已关的
  标记错过、目标阶段开放的补投递，且不会重复投递。

### 联演相关接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/joints` | 联演列表（含等待/已投递/错过计数，主持人） |
| POST | `/api/joints` | 创建联演（保存前无环+有效性校验，主持人） |
| GET | `/api/joints/{id}` | 联演详情：成员会话状态、各分支满足与阻塞原因 |
| PUT | `/api/joints/{id}` | 修改联演（已投递/错过的编排锁定，主持人） |
| DELETE | `/api/joints/{id}` | 删除全等待中的联演（主持人） |
| GET | `/api/sessions/{sid}/joints` | 该会话参与的联演状态（主持台面板） |
| GET | `/api/sessions/{sid}/script` | 该会话**绑定版本**的阶段/角色/选项（编辑器用） |
| POST | `/api/joints/{id}/postmortem` | **联演因果时序复盘**：一致截至时刻报告（主持人，纯只读） |

---

## 联演因果时序复盘（多会话时间线）

联合演练保存后，联演列表与监控台提供 **“⏱ 因果时序复盘”** 入口
（`#/host/joint-review/<联演id>`）。复盘是**纯只读**分析：唯一输入是各成员会话
创建时**冻结的剧本版本**与其**不可变事件日志**，不写入任何数据，因此对进行中或
已结束的演练都安全，且绝不触发补投递或状态修改。

### 两种一致的截至口径

1. **统一复盘截止时刻（`as_of`）**：主持人提交一个 POSIX 截止时刻 `T`。一个会话
   的某次原子提交（一个版本内的全部事件）只有在**整版事件时间都严格早于 `T`**
   （`< T`）时才计入前缀——同一版本内事件共享同一逻辑时刻。于是：
   - **同秒事件不计入**：在 `T` 这一秒发生的提交不会被提前纳入；
   - **迟到事件不改写**：晚于早前某次复盘才落账的事件（例如服务重启补仲裁）不会
     改变 `T` 时刻的前缀；
   - 同一 `T` 重复生成的报告**逐字节一致**（纯函数，无服务端时钟参与）。
2. **各成员会话事件版本前缀（`prefix`）**：为每个成员会话指定
   `events.version ≤ n` 的冻结前缀，回答“从这些冻结版本各自能知道什么”。当目标
   会话前缀已含注入、而授权它的某个上游结算位于另一会话前缀之外时，该编排仍判
   **已投递**（注入在目标前缀内是事实），同时给出 `evidence_partial`（证据不完整）
   标记与说明，不掩盖跨成员前缀的不一致。

### 报告内容

- **稳定合并时间轴**：所有成员事件按 `(at, 会话顺序号, version, seq)` 归并，标注
  `prereq_outcome`（前置结局）、`branch_hit` / `branch_miss`（命中/结局不符）、
  `target_open`（目标阶段开放）、`injection`（联演注入）、`target_close` 与
  `stage_settlement`（阶段结算）、`session_end`；每行带会话与 `sid#v版本.序号`
  事件引用。
- **每条编排**：四态 `unsatisfied`（尚未满足）/ `awaiting_target`（目标未开放）/
  `missed`（已错过）/ `delivered`（已投递），含每个前置分支的命中时间与结算事件
  引用、结局不符时的实际 `next`/`choice`、阻塞原因、注入或窗口关闭事件引用。
- **等待秒数**：`wait_seconds` = 最后一个前置满足（AND 汇合点）→ 注入投递
  （已投递）或目标阶段结算/会话结束（已错过）的秒数；`assembly_seconds` = 首个
  分支命中 → 末个分支命中的 AND 汇合耗时；进行中的编排给出相对截止时刻的
  `age_seconds`；`bottleneck_branch` 标出拖慢汇合的瓶颈分支及其事件引用。
- **关键路径 / 瓶颈**：以“注入目标节点被另一编排的前置监听”构成编排依赖边
  （保存时已保证无环），逐门与整体给出累计等待最长的关键链与按权重排序的
  瓶颈编排。
- **主持台筛选**：按编排状态、成员会话、标注类型、关键字（编排/会话/事件引用）
  联动筛选编排卡与时间轴；**⬇ 下载报告 JSON** 保存的就是服务器返回的同一份报告。

### 真实复盘请求 / 响应示例

以下用一场“上游值班长请求联动 → 向下游处置阶段一次性追加注入”的两会话联演
（编排 id `linkvalve`，目标会话处于阶段 `t1`）：

```bash
# 统一截止时刻：取一个晚于全部已发生事件的 POSIX 秒
curl -sS -H "X-Host-Key: host-1234" -H "Content-Type: application/json" \
     -X POST http://localhost:8000/api/joints/614e7c45/postmortem \
     -d '{"mode":"as_of","as_of":2000000000}'
```

```jsonc
{
  "kind": "joint_postmortem",
  "joint_id": "614e7c45",
  "joint_name": "厂区联合处置",
  "mode": "as_of",
  "as_of": 2000000000,
  "horizon_label": "统一截止时刻 2000000000（POSIX 秒，严格小于：同秒事件不计入前缀）",
  "members": [
    {"ord": 0, "session_id": "f16729fe", "drill_name": "上游演练",
     "drill_version": 1, "digest_short": "…", "status": "ended",
     "include_version": 3, "latest_version": 3, "events_included": 6}
  ],
  "cutoff": {"mode": "as_of", "as_of": 2000000000,
             "members": {"f16729fe": {"include_version": 3, "latest_version": 3,
                                      "truncated": false}}},
  "summary": {"total": 1, "delivered": 1, "missed": 0, "awaiting_target": 0,
              "unsatisfied": 0, "total_delivered_wait_seconds": 0.015,
              "total_assembly_seconds": 0.0, "max_wait_seconds": 0.015,
              "timeline_events": 10, "evidence_partial": 0},
  "gates": [
    {
      "id": "linkvalve",
      "title": "联动关阀",
      "target_session": "9685e6de",
      "target_stage": "t1",
      "target_stage_title": "联动处置",
      "status": "delivered",
      "status_label": "已投递",
      "prerequisites": [
        {"branch_no": 0, "session_id": "f16729fe", "stage_id": "s1",
         "kind": "choice", "role_id": "a", "value": "go",
         "label": "[上游演练] 阶段「报警确认」值班长 选择＝请求联动",
         "satisfied": true, "satisfied_at": 1790882313.128,
         "actual": {"next": null, "role_id": "a", "choice": "go"},
         "event_ref": {"session_id": "f16729fe", "version": 3, "seq": 1,
                       "type": "stage_sealed", "ref": "f16729fe#v3.1"}}
      ],
      "all_prerequisites_met": true,
      "last_prerequisite_at": 1790882313.128,
      "delivered_at": 1790882313.143,
      "injection_ref": {"session_id": "9685e6de", "version": 3, "seq": 0,
                        "type": "joint_injection", "ref": "9685e6de#v3.0"},
      "close_ref": null,
      "wait_seconds": 0.015,
      "assembly_seconds": null,
      "age_seconds": null,
      "bottleneck_branch": {"branch_no": 0,
                            "label": "[上游演练] 阶段「报警确认」值班长 选择＝请求联动",
                            "event_ref": {"ref": "f16729fe#v3.1"},
                            "earlier_branch_lead_seconds": []},
      "active_blockers": [],
      "evidence_partial": false,
      "block_reason": "已在 9685e6de#v3.0 投递（最后前置汇合后等待 0.015 秒）",
      "critical_path": {"gate_ids": ["linkvalve"], "cumulative_wait_seconds": 0.015}
    }
  ],
  "critical_path": {"gate_ids": ["linkvalve"], "total_wait_seconds": 0.015},
  "bottleneck_gates": [],
  "timeline": [
    {"at": 1790882313.143, "member_ord": 1, "session_id": "9685e6de",
     "version": 3, "seq": 0, "type": "joint_injection",
     "label": "联演注入追加到阶段「t1」：联动关阀",
     "event_ref": "9685e6de#v3.0",
     "marks": [{"kind": "injection", "gate_id": "linkvalve",
                "label": "联演注入：联动关阀"}]}
  ]
}
```

按成员会话事件版本前缀的请求示例（把上游冻结在注入授权结算之前，下游取最新）：

```bash
curl -sS -H "X-Host-Key: host-1234" -H "Content-Type: application/json" \
     -X POST http://localhost:8000/api/joints/614e7c45/postmortem \
     -d '{"mode":"prefix","prefixes":{"f16729fe":2,"9685e6de":3}}'
# 该前缀下上游 v3 结算尚不存在 -> gates[0].status = "unsatisfied"，
# cutoff.members.f16729fe.truncated = true；同一前缀重复请求结果完全一致。
```

> 页面里的“取最新事件之后”会把截止时刻自动填为成员会话最后事件时间的下一秒；
> 把截止时刻直接填成某个事件所在秒，即可观察“同秒事件不计入前缀”。

---

## 剧本版本与发布模型（不可变版本 / 草稿 / 发布）

### 版本号与内容摘要

- 每次“保存”都在 `drill_versions` 表新增一行，版本号 **`MAX(version)+1` 单调递增、
  永不修改**；状态为 `draft`（草稿）、`published`（当前发布）、`history`（被替代的
  旧发布版）或 `superseded`（被再次保存顶掉、未发布的旧草稿）。
- 每行保存规范化后的剧本 JSON 与 **SHA-256 摘要**（对解析后的结构取哈希，
  因此注释/缩进/键序不同但语义相同的 YAML 摘要一致）。保存与发布都按摘要去重：
  内容没变就复用既有版本，不会平白产生新版本。
- 一个剧本同时最多只有一条开放草稿；再次保存会把旧草稿置为 `superseded`
  （行仍保留、可追溯）并生成新草稿。

### 草稿 → 发布（乐观并发）

```text
上传/编辑 YAML ──▶ 不可变草稿 vN（base=当前发布版）
                     │ 主持台查看差异、从历史版本复制
                     ▼
        发布：校验 base == 当前发布版？
           ├─ 是：旧发布版转 history，vN 成为 published（单一当前发布版）
           └─ 否（他人已抢先发布）：409，当前发布版不变，草稿保留，重新校对后再发
```

- **草稿只能在发布后用于新会话**：未发布时 `POST /api/sessions` 返回 409。
- 发布是单个 `BEGIN IMMEDIATE` 事务：冲突时整体回滚，失败绝不会改动当前发布版。

### 会话与版本的绑定（后续编辑不得改变快照/分支/回放）

- 创建会话时同时写入 `sessions.drill_version` 与 `sessions.drill_sha256`。
- 会话运行期的定义读取**只按这对标识**从 `drill_versions` 取对应不可变版本，
  从不读“最新发布版”。因此：
  - 创建于 v1 的会话，在剧本发布 v2、v3 后，阶段文本、角色视图、选项、
    `default_next`、条件转移全部仍是 v1；
  - 只有**新建**的会话才会使用当时的最新发布版；
  - 旧数据库升级时迁移程序自动把既有发布版补为不可变 v1，并把存量会话绑定到 v1。
- 版本标识贯穿各出口：
  - **HTTP**：创建会话响应、会话列表、加入响应都带 `drill_version`/`drill_sha256`；
  - **WebSocket 快照**：`snapshot.drill.version` 与 `snapshot.drill.sha256`；
  - **回放**：顶层 `drill_version`/`drill_sha256`，帧帧一致；
  - **记录导出**：JSON 顶层与 `drill` 块、Markdown 抬头“剧本版本：vN（摘要）”。

---

## YAML 剧本格式

最小示例：

```yaml
id: demo                 # 必须与文件名一致（demo.yaml）
name: 示例演练
description: 一句话说明

roles:
  - id: cmd
    name: 值班班长
    invite: CMD-2026     # 每个角色唯一的加入邀请码

stages:
  - id: s1
    title: 第一阶段
    public: 所有角色都能看到的公开信息
    announcement: 阶段公告（默认同 public）
    duration: 45         # 限时秒数；null 表示只能由主持人手动结算
    default_next: s2     # 无匹配分支时的去向；省略或 null 表示演练结束

    views:               # 仅对应角色可见的隐藏信息
      cmd: 只有班长知道的情况

    options:             # 需要做决定的角色与其限时选项
      cmd:
        - id: evacuate
          text: 组织撤离
        - id: hold
          text: 原地等待
          next: s3       # 单决策角色可直接用选项指定去向（优先级最高）

    transitions:         # 多角色条件分支：按顺序匹配，首个全部相等的生效
      - when: {cmd: evacuate}
        next: s2
      - when: {cmd: hold}
        next: s3
```

### 结算与分支规则

1. 每个阶段的“决策角色”是 `options` 下出现的角色；其余角色该阶段只需等待。
2. 满足任一条件即结算该阶段：
   - **全部决策角色都已提交** → 自动结算（`complete`）；
   - **到达 `duration` 截止时刻** → 定时器结算，未提交者选择记为 `null`（超时，`deadline`）；
   - **主持人手动结算** → 未提交者同样记为超时（`host`）。
3. 下一阶段按优先级确定：
   1. 已提交选项上的 `next`（适合单决策者直接跳转）；
   2. `transitions` 中首个 `when` 条件全部满足的规则
      （`{role: choice_id}` 全等；超时角色不等于任何选项 id，因此不会命中）；
   3. `default_next`；为 `null`/省略则演练结束。
4. `duration: null` 的阶段永不自动超时，由主持人手动结算（如复盘阶段）。
5. 校验规则：id 唯一、邀请码唯一、所有 `next`/`default_next` 必须指向已存在阶段、
   选项/视图/分支引用的角色必须存在、`duration` 为正整数。

---

## HTTP / WebSocket 接口摘要

### HTTP（`X-Host-Key` 为主持人密钥；参与者接口用 `?token=` 或加入返回的令牌）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查 |
| GET | `/api/drills` | 剧本列表（含 `published_version`/`draft_version`，主持人） |
| POST | `/api/drills` | 上传/编辑 YAML，body `{"yaml": "...", "base_version": n|null}`：生成不可变草稿版本+SHA-256（主持人） |
| GET | `/api/drills/{id}/versions` | 版本列表（含每版状态、摘要、基准、时间与 YAML） |
| GET | `/api/drills/{id}/versions/{v}` | 取某一版本（含原始 YAML） |
| POST | `/api/drills/{id}/versions/{v}/publish` | body `{"base_version": n|null}`：发布草稿；基准被他人更新时 409 且不改变当前发布版 |
| POST | `/api/drills/{id}/copy-draft` | body `{"source_version": v, "base_version": n}`：从任一历史版本复制为新草稿 |
| GET | `/api/drills/{id}/diff?from=a&to=b` | 按结构返回两版差异（属性/角色/阶段/视图/选项/转移规则） |
| POST | `/api/sessions?drill_id=...` | 仅可基于当前发布版创建；返回 `session_id` 及绑定的 `drill_version`/`drill_sha256`（主持人） |
| GET | `/api/sessions` | 会话列表（含每会话绑定的剧本次序号与摘要） |
| POST | `/api/sessions/{sid}/join` | body `{"invite": "...", "name": "..."}`，返回角色、令牌与剧本版本标识 |
| GET | `/api/sessions/{sid}/replay?token=&version=` | 逐版本确定性回放（主持人或参与者），顶层带 `drill_version`/`drill_sha256` |
| GET | `/api/sessions/{sid}/record?fmt=md\|json&token=` | 全量记录（主持人）/ 个人记录（参与者），均标注剧本版本（含联演注入） |
| GET | `/api/joints` · `POST /api/joints` · `GET/PUT/DELETE /api/joints/{id}` | 联合演练编排（主持人，见“联合演练编排”一节） |
| POST | `/api/joints/{id}/postmortem` | **联演因果时序复盘**：统一截止时刻/成员版本前缀的一致报告（主持人） |
| GET | `/api/sessions/{sid}/joints` · `/api/sessions/{sid}/script` | 会话的联演状态 / 绑定剧本引用（主持人） |

### WebSocket

```
ws://<host>:8000/ws/{sessionId}?host=<HOST_KEY>      # 主持人
ws://<host>:8000/ws/{sessionId}?token=<participantToken>  # 参与者
```

- 服务端推送：`{"type":"snapshot","snapshot":{...}}`（裁剪后的完整快照）。
  快照的 `drill.version` / `drill.sha256` / `drill.digest_short` 即本会话
  创建时固化的剧本版本与摘要，整个生命周期恒定不变。
- 参与者发送：`{"type":"submit","choice_id":"...","client_stage":"..."}`，
  服务端回 `submit_result`（`accepted` / `duplicate` 幂等重放 / 截止拒绝并已结算）。
- 主持人发送：`{"type":"start"}`、`{"type":"settle"}`、`{"type":"end"}`。
- 心跳：客户端 `{"type":"ping","t":...}`，服务端回 `{"type":"pong",...}`（同时用于时钟校准）。
- 认证失败返回 `error` 后以 4401 关闭；队列积压时服务端丢弃旧消息并触发快照重同步。

---

## 一致性设计（重复提交 / 断线重连 / 截止竞争）

- **事件溯源 + 单调版本号**：所有状态由 `events(session_id, version, seq)` 折叠得到；
  一次原子提交占用且仅占用一个新版本，版本号恒为 `last_version + 1`（join、start、
  submit、结算、终止无一例外，绝不写死）；同一次提交内的多个事件按固定因果顺序
  分配 `seq = 0,1,2…`，因此主键 `(session_id, version, seq)` 不会冲突，
  任意版本前缀都能确定性回放。各路径的稳定顺序：

  | 操作 | 版本 | 事件（按 seq 顺序） |
  | --- | --- | --- |
  | join | v+1 | `participant_joined` |
  | start | v+1 | `session_started` → `stage_opened` |
  | submit（未触发结算） | v+1 | `stage_choice` |
  | submit（末名提交，自动结算） | v+1 | `stage_choice` → `stage_sealed` → `stage_opened`/`session_ended` |
  | submit（超过截止，被拒） | v+1 | `stage_sealed` → `stage_opened`/`session_ended` |
  | 截止/手动结算 | v+1 | `stage_sealed` → `stage_opened`/`session_ended` |
  | 终止（未开始） | v+1 | `session_ended` |
  | 终止（进行中） | v+1、v+2 | `stage_sealed` →（`stage_opened`）；`session_ended` |
  | 联演注入 | v+1 | `joint_injection`（写入**目标会话**日志，含公开文本+按角色私有视图） |

- **原子结算**：写操作在会话级 `asyncio.Lock` 内执行
  `BEGIN IMMEDIATE`，提交选择与可能触发的结算在同一事务内完成。
  由于所有写共用**单一 SQLite 连接**，另有一把连接级写锁把每个
  `BEGIN IMMEDIATE … COMMIT` 整体串行化：不同会话（如联演里的上游/下游）
  即使持有各自的会话锁，其 SQL 事务也不会在 `await` 处交错。
  连接以 `isolation_level=None`（autocommit）打开，事务仅由显式
  `BEGIN IMMEDIATE` 开启，杜绝隐式 DML 事务跨等待点泄漏。
- **截止竞争**：到达 deadline 后到达的提交会被拒绝，且“拒绝 + 超时结算”在同一事务；
  每个定时器在创建时**捕获自身的 `expected_deadline`** 并随回调一路传入
  `Engine.settle`，在**同一个 SQLite 事务内**与当前 `state.deadline` 精确比对：
  阶段一的截止任务已经唤醒，但末名提交或主持人结算先把会话推进到同样有限时的
  阶段二时，若旧回调受会话锁或事件循环延迟、直到阶段二也到期才真正执行，
  身份不符（捕获值 ≠ 当前值）的旧回调只会**空转且不新增任何事件**，
  绝不会被当成阶段二的超时写入结算；阶段二自己的正常回调不受影响，
  且同一阶段的重复回调最多结算一次。`duration: null` 的阶段永不超时。
  主持人手动结算与定时器结算由同一把锁串行化，同一阶段只会被结算一次。
- **重复提交**：`submissions(session, stage, role)` 主键去重；
  重复请求（含阶段已结束后的重发）幂等返回首次结果，不产生第二条事件；
  重复的开始/加入/终止请求被拒绝或空转，同样不新增事件。
- **断线重连**：WebSocket 指数退避自动重连，连上即收到当前版本快照；
  在线状态随连接建立/关闭广播；服务重启后为所有运行中会话重新武装截止定时器。
- **权限裁剪**：参与者快照只含已开放阶段、公开文本、本人视图/选项/本人选择；
  其他角色的私有视图、选项分支与决定不会出现在任何参与者响应、回放或导出中。
- **确定性回放**：回放只依赖事件日志与会话**绑定的固定剧本版本**，逐版本折叠，
  同一版本永远得到同一快照；主持人可见全量事件，参与者仅得到本人视角。
  剧本后来如何编辑/发布都不改变既有会话的回放结果。
