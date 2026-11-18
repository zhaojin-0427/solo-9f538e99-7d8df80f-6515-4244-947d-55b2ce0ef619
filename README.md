# 地震波形片段拼接服务（Waveform Assembly API）

台网接入员收到上游交接清单后，先按清单核对某台站分钟的波形片段，再把完整且
**与清单完全吻合** 的分钟交给震相识别程序。服务把乱序到达的波形片段按
**台站 + UTC 分钟** 收集、对账、拼装，仅对合格分钟生成不可变的发布记录。

- 框架：FastAPI（自带 `/docs` Swagger UI、`/redoc`）
- 存储：SQLite（WAL 模式，单文件，挂在 `/data` 卷上，重启数据不变）
- 并发：所有写操作走 `BEGIN IMMEDIATE` 事务，清单写入、片段上传与发布交错时
  在同一已提交状态上串行执行，失败整体回滚，不留半份清单或发布

## 一、启动

### Docker Compose（推荐）

```bash
docker compose up --build
# API:    http://localhost:8000
# 文档:   http://localhost:8000/docs
# 健康:   http://localhost:8000/health
```

数据库保存在命名卷 `waveform-data`（容器内 `/data/waveform.db`）。
旧版本写出的 SQLite 卷可直接挂载启动：表结构按需增量迁移，原查询接口照常使用，
旧发布记录的 `manifest_revision` 为 `null`。

### 本地直接运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
WAVEFORM_DB_PATH=./data/waveform.db uvicorn app.main:app --reload
```

## 二、数据模型与规则

### 2.1 交接清单（manifest）

上游交接清单按 **台站 + UTC 分钟** 写入，每行声明该分钟应有的一个片段：

| 字段 | 说明 |
| --- | --- |
| `manifest_revision` | 清单修订号，正整数；同分钟只接受 **更高** 的修订 |
| `entries[].fragment_no` | 应有的片段号，同清单内不得重复 |
| `entries[].revision` | 该片段应有的修订号 |
| `entries[].content_hash` | 该片段应有的内容摘要，即片段 `start_sample`+`samples` 的 SHA-256（64 位小写十六进制），与 `GET /fragments` 返回的 `content_hash` 同一算法 |

清单修订规则：

- 修订号 **更高** → 接受并成为当前清单，旧清单保留在修订历史中（可溯源）
- 修订号 **更低** → 拒绝（409），当前清单不动
- 同修订号、**同内容**（清单指纹 `manifest_hash` 一致）→ 幂等重传，
  沿用原清单（`status: "unchanged"`），不产生新行
- 同修订号、**异文** → 拒绝（409），当前清单不动
- 新清单修订一旦提交，该分钟此前仍为最新的发布自动标记 `stale=true`
  （波形与来源仍可按 ID 读取）；同版同文重传不影响发布状态

### 2.2 片段（fragment）

| 字段 | 约束 |
| --- | --- |
| `station` | 非空字符串，台站代码 |
| `minute_utc` | 整分钟 UTC 时间（秒必须为 0），如 `2026-11-17T05:06:00Z` |
| `fragment_no` | 片段号，非负整数 |
| `revision` | 修订号，**正整数** |
| `start_sample` | 起始采样位，0–59 |
| `samples` | 非空整数数组；`start_sample + len(samples) ≤ 60`，越界拒绝（422） |

修订与重传规则（同一 `station + minute_utc + fragment_no`）：

- 修订号 **更高** → 接受并覆盖旧版（`status: "superseded"`）
- 修订号 **更低** → 拒绝（409），旧数据不动
- 同修订号、**同内容**（SHA-256 一致）→ 幂等重传，沿用原结果（`status: "unchanged"`），不产生新行
- 同修订号、**异文** → 拒绝（409），旧数据不动

### 2.3 对账（reconciliation）

`GET /reconciliation` 在同一个只读快照上比较当前清单与当前片段集合，逐行归入：

- `missing`：清单列出但尚未交来的片段
- `revision_mismatches`：片段已交，但修订号与清单不符（给出期望/实际修订号）
- `content_mismatches`：修订号相同但内容摘要不符（异文）
- `matched`：片段号、修订号、内容摘要全部吻合
- `extra_fragments`：已上传但清单之外的片段

`reconciled = 无缺交、无修订/内容不符、无清单外片段`。
片段或清单更正后再次查询，对账结果立即按新状态变化。

### 2.4 拼装（assembly）

- 每分钟固定 60 个 1 Hz 采样位
- 缺位在 `gaps` 中逐个列出；覆盖数见 `coverage`
- 重叠位数值不同即为冲突，`conflicts` 给出 **位置、涉及片段号、各自数值**；
  重叠但数值相同不算冲突
- `complete = 60 位全覆盖且无冲突`

### 2.5 发布（publication）

只有同时满足以下条件的分钟才可新发布，否则返回 409 并在 `detail` 中说明：

1. 该分钟存在已提交的交接清单；
2. 对账完全吻合（`reconciled`，无缺交/修订不符/内容不符/清单外片段）；
3. 拼装完整且无冲突（`complete`）。

清单、片段、发布在事务中交错时，发布资格始终对应 **同一个已提交状态**；
失败整体回滚，不留下半份清单或发布记录。

发布记录冻结：

- 当时的 60 点波形 `wave`、每个片段的修订号与内容（`items`）
- 片段集合指纹 `fragment_set_hash`
- 来源清单修订号 `manifest_revision`（清单溯源；旧版本数据为 `null`）

同一片段集合 + 同一清单修订重复发布是幂等的，返回同一条记录。
之后补传/更正片段或换新清单只改变当前对账与拼装；一旦状态变化，旧记录自动标
`stale=true`，仍可按 ID 读取，波形、修订与来源清单保持冻结。

## 三、HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/manifests` | 写入/修订交接清单（JSON body） |
| GET | `/manifests?station=&minute_utc=` | 查看该分钟当前清单（无则 404） |
| GET | `/manifests/revisions?station=&minute_utc=` | 清单修订历史（新到旧） |
| GET | `/reconciliation?station=&minute_utc=` | 对账结果 + 拼装状态 + `publishable` |
| POST | `/fragments` | 上传片段（JSON body） |
| GET | `/fragments?station=&minute_utc=` | 查看某分钟当前片段 |
| GET | `/assembly?station=&minute_utc=` | 查看拼装状态：覆盖率、缺口、冲突 |
| POST | `/publications?station=&minute_utc=` | 发布对账吻合且完整无冲突的分钟 |
| GET | `/publications?station=&minute_utc=` | 发布记录列表（可按台站/分钟过滤） |
| GET | `/publications/latest?station=&minute_utc=` | 该分钟最新一条记录 |
| GET | `/publications/{id}` | 按 ID 读取（过期记录同样可读） |

### 典型流程

```bash
MIN=2026-11-17T05:06:00Z
# 1) 上游交接清单：声明该分钟应有的片段号、修订号与内容摘要
curl -s -X POST localhost:8000/manifests -H 'content-type: application/json' -d '{
  "station":"STA1","minute_utc":"'"$MIN"'","manifest_revision":1,
  "entries":[
    {"fragment_no":1,"revision":1,"content_hash":"<sha256 of fragment 1>"},
    {"fragment_no":2,"revision":1,"content_hash":"<sha256 of fragment 2>"}
  ]}'

# 2) 乱序上传两个片段，恰好铺满 60 位
curl -s -X POST localhost:8000/fragments -H 'content-type: application/json' -d '{
  "station":"STA1","minute_utc":"'"$MIN"'","fragment_no":2,"revision":1,
  "start_sample":30,"samples":'"$(python3 -c 'print(list(range(30,60)))')"'}'
curl -s -X POST localhost:8000/fragments -H 'content-type: application/json' -d '{
  "station":"STA1","minute_utc":"'"$MIN"'","fragment_no":1,"revision":1,
  "start_sample":0,"samples":'"$(python3 -c 'print(list(range(0,30)))')"'}'

# 3) 对账：reconciled=true 且 publishable=true 才能发布
curl -s "localhost:8000/reconciliation?station=STA1&minute_utc=$MIN"
# 4) 发布（记录冻结波形、片段修订与来源清单修订号）
curl -s -X POST "localhost:8000/publications?station=STA1&minute_utc=$MIN"
```

## 四、测试与自测

```bash
pip install pytest httpx
python -m pytest tests/ -v          # 45 个用例：校验/清单/对账/修订/拼装/发布/过期/迁移/重启/并发
python scripts/smoke.py             # 对运行中的服务做端到端检查
```

并发用例会启动真实 uvicorn 进程：多个线程同时上传片段、推进清单修订并尝试发布，
断言中间态发布只可能干净失败（409），每份落库发布都来自同一个已提交的
清单 + 片段状态，无半份数据。

迁移用例会手工构造旧版 SQLite（无清单表、无 `manifest_revision` 列），
验证启动时自动迁移后旧数据与原查询接口照常可用，并可纳入新的清单对账流程。

## 五、目录结构

```
app/
  config.py      # 环境变量配置（WAVEFORM_DB_PATH）
  database.py    # SQLite 连接、建表与旧卷增量迁移
  models.py      # Pydantic 请求/响应模型（清单、对账、片段、发布）
  service.py     # 事务、清单规则、对账算法、拼装与发布逻辑
  main.py        # FastAPI 路由
tests/test_api.py
scripts/smoke.py
Dockerfile
docker-compose.yml
requirements.txt
```
