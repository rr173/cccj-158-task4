# 多站点变更汇合服务（Change Convergence Service）

供去标识流水线使用的多站点变更汇合服务。各站只上报**合成描述符**——替代对象键、
前驱摘要、覆盖区段（整数半开区间）、变更摘要、站点单调序号与请求键；服务端据此
持久化重建**有向版本图**，并保证：

- **挂起续办**：前驱晚到的信封先 `waiting`，祖先补齐后在同一写事务中自动续办；
  绝不越过缺失祖先。
- **确定性合成**：覆盖区段互斥的兄弟分支按 `(site_id, site_seq, node_digest)`
  确定顺序合成，结果与到达顺序无关。
- **同区段人工判定**：同一原子区段内容不同即冻结批次、打开冲突案；两名**不同**
  复核员依次批准同一选择后才出最终决定（可选择某个节点，或"保留现任"）。
- **唯一当前决定**：每个对象只有一行 `object_current`；每次决定出具且仅出具
  一条哈希链收据（HMAC 风格的 sha256 前向链 + 跨表交叉校验）。
- **幂等**：相同请求键 + 相同正文重放返回原结果（`replay=true`）；同键不同正文
  一律 409 `request_key_body_conflict`。
- **崩溃续办**：落盘分两步（节点/对象/请求标记与决定/收据），进程中途退出后，
  重启 `boot_recover` 从持久状态继续挂起、合成或判定；不重复出决定、不暴露半成品。
- **脱敏**：输入采用严格白名单模式（多一个字段即拒绝），系统从不接受或存储原始
  身份/像素；响应、页面、收据只含伪名键、整数区段与摘要。监察员视图连站点标识都
  以 `site-01…` 别名呈现，收据内不含任何站点字段。
- **篡改即确定错误**：收据链或审计哈希链被改动后，`/verify` 与下载前校验返回
  409 `receipt_chain_broken` / `journal_chain_broken`；该对象的后续写入与判定
  也被同一错误拒绝，绝不基于断链续出决定。

## 目录

```
converge/
  app/
    canonical.py        确定性规范化 JSON 与哈希
    config.py           CONVERGE_HOME 数据目录
    store.py            SQLite schema/迁移、连接、进程内写锁
    engine.py           版本图、挂起续办、确定性合成、冲突双判、收据链、崩溃恢复
    web/
      auth.py           Basic 认证与三角色门
      api.py            HTTP 接口（stdlib，零第三方依赖）
      static/           控制台 SPA（index.html/app.js/styles.css）
  scripts/
    init_db.py          初始化数据库与演示账户
    run_server.py       启动服务
  tests/                33 个 unittest 用例（引擎级 + 真实 HTTP 端到端，含崩溃/并发）
```

## 快速开始

仅需 Python 3.11+ 标准库，无需安装任何依赖。

```bash
cd converge
python3 scripts/init_db.py
CONVERGE_HOME=./data python3 scripts/run_server.py --port 8090
# 浏览器打开 http://127.0.0.1:8090/ui
```

演示账户（Basic Auth）：

| 用户 | 密码 | 角色 | 能力 |
|---|---|---|---|
| operator | operator-pw | 操作员 | 上报信封、查看图谱与挂起原因 |
| reviewer-a | review-a-pw | 复核员 | 冲突第 1 步批准/异议 |
| reviewer-b | review-b-pw | 复核员 | 冲突第 2 步确认（必须与第 1 步不同人） |
| inspector | inspector-pw | 监察员 | 只读查询图谱、校验并下载脱敏收据 |

## HTTP 接口

```
GET  /healthz
GET  /api/me
POST /api/envelopes                      上报信封（操作员；请求键放 X-Request-Key 或 body.request_key）
GET  /api/objects                        对象列表（节点计数/当前版本/挂起或待判）
GET  /api/objects/{key}                  版本图明细（节点、决定、打开中的冲突案）
GET  /api/objects/{key}/receipts         脱敏收据链（可下载）
GET  /api/objects/{key}/verify           收据链校验（篡改→409 receipt_chain_broken）
GET  /api/cases?status=open              冲突案（操作员/复核员）
POST /api/cases/{id}/reviews             双人判定（action=approve|reject + choice_node_digest）
GET  /api/journal/verify                 审计哈希链校验
```

信封格式（唯一被接受的字段集合）：

```json
{
  "object_key": "study-A",
  "site_id": "site-1",
  "site_seq": 3,
  "prev_digest": "64-hex 或 null",
  "regions": [{"start": 0, "end": 100}],
  "change_digest": "64-hex 摘要"
}
```

## 一致性与故障模型

- **并发**：所有多语句写转换都在进程写锁内以 `BEGIN IMMEDIATE` 串行化；
  `requests.request_key`、`receipts(object,seq)`、`object_current(object)`
  唯一约束兜底。乱序、重复、并发上报最终都只留下一个当前决定和一条收据链。
- **冲突期间的新分支**：冲突打开时，互斥新分支保持 `active` 排队；判定完成的
  同一事务内先合成冻结批次，再立刻续泵，若新分支与结果再次冲突则开新案。
- **崩溃**：`CONVERGE_FAULT=crash_before_commit` 可在决定/收据写入后、提交前
  强制 `os._exit(2)`，用于实际演练重启恢复（测试 `CrashRecoveryCase` 使用）。
  重启时：审计链先验真 → 修复崩溃边界上的 `seen` 请求标记 → 逐对象验链并续泵；
  断链对象被隔离，不参与合成。

## 测试

```bash
cd converge
python3 -m unittest discover -s tests -v
# 33 passed —— 覆盖：
# 正常合成/顺序无关、迟到祖先与级联挂起恢复、请求键重放与同键异文冲突、
# 同区段冲突+双人不同复核员+异议+保留现任+判定期间新分支、
# 并发唯一决定、并发同请求键单记录、崩溃后续办不重复、
# 收据链/审计链篡改确定错误且阻断续写、严格 schema 拒绝身份/像素字段、
# 越权 401/403、监察员脱敏、UI 静态壳无业务数据
```

## 边界说明（演示实现）

- 单进程 + SQLite 适合演示；进程写锁语义与唯一约束可平移到 Postgres + 短事务。
- Basic Auth 为演示用途，生产应接 OIDC/SAML + MFA；两名复核员的账户应绑定独立身份。
- "像素"在本服务中从入口就不存在：流水线只应在本地完成遮盖后上报区段与摘要。
