# Sharded Quorum Store (Go prototype)

一个**完全本地运行**、无外部数据库/外部服务依赖的分片键值存储原型。多个独立节点（各自拥有
独立的持久化 WAL、KV 状态与故障域）在同一进程中通过可注入故障的内存网络模拟真实集群，
用于验证：

- 按键范围分片、成员变更与读写仲裁；
- 副本一致性、故障切换与并发读写；
- 持久化恢复、副本追赶与同步位置校验；
- 分片迁移期间的并发访问边界。

## 目录结构

```
internal/store/
  errors.go          可区分的哨兵错误与一致性级别
  types.go           核心类型：Entry / SyncPosition / ShardConfig / 结果
  wal.go             单分片单节点持久化日志（CRC + 哈希链 + 恢复/隔离）
  hash.go            摘要助手
  node.go            节点核心：状态机、客户端读写、提交与应用
  node_persist.go    节点启动恢复（WAL + commit index + fence）
  node_repl.go       RPC 分发：AppendEntries / Vote / FetchLog / Probe / Admin
  node_leader.go     心跳推送、提交推进、选举
  transport.go       可分区/离线/注入延迟的内存网络，错误码跨网络映射
  cluster.go         控制平面：路由、节点生命周期、仲裁读写
  cluster_migrate.go 迁移 / 成员变更状态机（prepare→cutover→committed）
  client.go          客户端门面（幂等键、默认一致性）
cmd/sqdemo/main.go   本地可运行的完整链路演示
*_test.go            覆盖下述全部链路的测试
```

## 一致性模型

### 分片与代际（epoch）

- 分片由**左闭右开**的键区间 `[Start, End)` 定义；空 Start 表示 -inf，空 End 表示 +inf。
- 每个分片配置 `ShardConfig{Shard, Epoch, Start, End, Replicas, Leader}` 都有单调递增的
  **epoch（代际）**。成员变更、迁移都会生成 epoch+1。
- 每条日志条目 `Entry{Index, Epoch, Op, Key, Value, TxnID, Version}`：
  - `Index` 在分片日志内从 1 开始连续；
  - `Epoch` 记录条目被追加时的代际；
  - `Version` 是键级单调版本，**在追加时（而非提交后）**由 leader 在锁内保留，
    因此并发写永远不会保留到同一个版本号（防止 lost update）；
  - `TxnID` 是客户端幂等键。

### 仲裁

| 级别 | 写 | 读 |
|---|---|---|
| `Quorum` | R/2+1 个副本持久化确认 | 向所有副本读，R/2+1 个成功且对 `(found,version,value)` 达成多数一致 |
| `One` | 1 个副本（本地）持久化 | 1 个副本成功 |
| `RequireAll` | 全部副本确认 | 全部副本一致 |

- 写路径：leader 先**本地 fsync**，再并行推送；在超时前收齐所需 ack 则提交并返回成功，
  否则返回 **`ErrTimeout`**（条目可能留在本地日志但未提交——它会在 fence / 截断时被丢弃，
  或在日志冲突时被截断重放，绝不会造成静默不一致）。
- 读路径：多数派读 + 值对账。读不到足够节点返回 **`ErrTimeout`**；
  没有可路由的分片返回 **`ErrUnavailable`**。

### 幂等：重复请求不重复生效

- 客户端可传入稳定的 `TxnID`；同一 `TxnID`：
  - 已提交完成 → 返回首次结果并置 `Duplicate=true`，不再追加任何条目；
  - 仍在飞行中（首个请求未达仲裁）→ 等待**同一日志索引**达到仲裁，不再追加第二条；
  - follower 应用阶段也按 `(TxnID,index,version)` 去重。
- 因此网络重试、客户端超时后重试、并发重发都只会生效一次。

### 可区分结果

所有客户端失败都映射到稳定哨兵错误（跨模拟网络用字符串错误码传输，见 `transport.go`）：

| 错误 | 含义 |
|---|---|
| `ErrTimeout` | 超时 / 仲裁不足 / 分区导致无法在期限内达成 |
| `ErrUnavailable` | 节点离线、分片未安装、无路由 |
| `ErrConflict` | 迁移/成员变更并发冲突等条件失败 |
| `ErrKeyMigrating` | 迁移 cutover 窗口内键被显式拒绝（而非新旧分片都接受） |
| `ErrStaleFollower` | 旧代际副本（含被 fence 的旧 leader）尝试写入/服务 |
| `ErrNoLeader` | 本机不是 leader |
| `ErrChecksumMismatch` | 日志 CRC 不符（损坏） |
| `ErrLogGap` | 追赶时前驱索引缺失 / 日志不连续 |
| `ErrPositionRegression` | 试图安装比当前更旧的 epoch（同步位置回退） |
| `ErrNotFound` / `ErrInvalidConfig` | 键不存在 / 配置非法 |

## 持久化、恢复与副本追赶

每个节点每个分片落盘在 `<DataRoot>/<nodeID>/shard-<shard>/`：

- `shard.wal`：仅追加日志。每条记录 `index|epoch|version|op|key|value|txn|CRC32`。
- `commit.bin`：独立持久化的已提交下标（原子 rename）。
- `meta.json`：安装的 `ShardConfig` 与已 fence 的最高 epoch。

恢复规则（见 `node_persist.go` / `wal.go`）：

1. **尾部撕裂**（文件最后的半截或 CRC 错尾记录，模拟崩溃写）→ 截断到上一条干净边界后继续启动。
2. **中间损坏**（非尾部 CRC 错、index 不连续）→ **拒绝启动**，把坏文件复制为
   `shard.wal.corrupt-<unixnano>` 保留诊断，并返回包装 `ErrChecksumMismatch` / `ErrLogGap`，
   错误信息含字节偏移与期望索引。
3. 已持久化但未提交的日志尾部在启动时显式截断（崩溃租约的残留不会被当成已同步）。
4. 启动后从提交前缀重建内存 KV、每键版本计数器与幂等缓存；
   **恢复到的位置可通过 LastIndex/CommitIdx/哈希链/LogDigest 验证与停机前逐字节相同**。
5. 追赶（`MsgFetchLog`）必须存在连续前驱索引；前驱缺失返回 `ErrLogGap`，不会流式返回误导性后缀。
6. 安装更旧 epoch 一律返回 `ErrPositionRegression`。

迁移时新副本的历史日志是**逐字节拷贝**（历史条目保留旧 epoch、index、version），
随后追加一条新代 `OpNoop` 纪元标记；协调者比对**历史前缀摘要**与源节点完全一致才允许 cutover。

## 故障恢复、故障切换与迁移规则

- 节点离线用 `Transport.SetOffline`，双向链路用 `Transport.Partition(a,b,true)` 模拟。
- 少数派分区：被隔离的 leader 本地可以追加，但收不到 R/2+1 ack → 客户端得到
  `ErrTimeout`；heal 后冲突尾部按连续日志协议截断重放，不会形成脑裂提交。
- **故障切换 = 成员变更**：幸存多数派通过 `MigrateShard`/`ChangeReplicas` 声明新副本集与
  新 leader（epoch+1）。旧 leader 在分区期间无法参与；heal 后其持久化 fence 生效，
  旧 epoch 写一律 `ErrStaleFollower`，“僵尸写”永不可见。
- 迁移状态机（`migrationMu` 全局串行，迁移与成员变更互斥）：
  1. **prepare**：从数据移动**开始前**就阻塞该分片写（客户端得到 `ErrKeyMigrating`），
     在目标副本安装 epoch+1 空日志；
  2. 从旧 leader 流式拷贝完整历史日志到新副本，逐条连续追加，校验历史前缀摘要；
  3. 追加新代 `Noop`，推进新副本提交点；
  4. **cutover**：fence 旧代所有副本（持久化），保持 `CutoverWindow`；
  5. **committed**：发布新路由元数据，解除写阻塞。
- 旧副本 fence 持久化到 `meta.json`：即使重启也不会再以旧 epoch 接受写。
- 同一键在迁移期间不可能被新旧分片同时接受：prepare 起即拒绝旧代写，
  cutover 后旧代全部被 fence。
- 迁移任一步校验失败 → 状态置 `MigrationAborted`、解除阻塞、元数据保留旧 epoch，
  并返回明确原因（摘要不符即 `ErrChecksumMismatch`）。
- 并发的迁移/成员变更：一个成功，另一个得到 `ErrConflict` 或基于陈旧 epoch 的
  `ErrStaleFollower`（均明确可区分），绝不会两个都生效。

## 可配置项

| 项 | 位置 | 说明 |
|---|---|---|
| `ClusterOptions.CutoverWindow` | cluster | 迁移原子切换窗口时长（默认 50ms） |
| `ClusterOptions.RPCDeadline` | cluster | 单次副本 RPC 期限（默认 500ms）；写客户端期限为其 3 倍，也可用 context 覆盖 |
| `ClusterOptions.DataRoot` | cluster | 所有节点持久化根目录 |
| `NodeOptions.ElectionTimeout` | node | 选举超时（默认 250ms） |
| `NodeOptions.HeartbeatInterval` | node | 心跳/推送周期（默认 40ms；每次 RPC 期限为其 4 倍） |
| `WriteOptions.Consistency` / `ReadOptions.Consistency` | 每次请求 | `Quorum` / `One` / `RequireAll` |
| `Transport.SetLatency` | 网络 | 在任一链路上注入固定单向延迟 |
| `Transport.SetOffline` / `Partition` | 网络 | 模拟节点崩溃/重启与双向网络分区 |

## 本地验证方法

要求 Go 1.25+。

```bash
# 构建
go build ./...

# 运行全部单元测试（-race 强烈建议）
go test ./...
go test -race ./...

# 只看某条链路（测试名即链路）
go test ./internal/store/ -run TestNodeRestartRecoversToCommittedPosition -v
go test ./internal/store/ -run TestPartitionMinorityCannotCommit -v
go test ./internal/store/ -run TestFailoverAcrossMembershipChange -v
go test ./internal/store/ -run TestMigrationCutoverRejectsWrites -v
go test ./internal/store/ -run TestWALMiddleCorruptionRejected -v

# 端到端演示（写入真实磁盘临时目录）
go run ./cmd/sqdemo
```

### 测试覆盖的链路

| 测试 | 验证点 |
|---|---|
| `TestWALAppendAndReopen` | 落盘重开后位置/哈希链/摘要逐字节一致 |
| `TestWALTornTailRecovered` | 崩溃撕裂尾记录被安全截断恢复 |
| `TestWALMiddleCorruptionRejected` | 非尾部损坏被拒绝并隔离出 `.corrupt-*` 诊断文件 |
| `TestWALTruncateSuffixRebuildsChain` | 截断后哈希链正确重建 |
| `TestNodeQuorumWriteSmoke` | 3 副本仲裁写、全部提交、重复 txn 不二次生效 |
| `TestClusterReadWriteQuorum` | 范围路由、仲裁读写、重复请求 |
| `TestClusterQuorumUnavailable` | 2/3 离线时写不可用/超时、仲裁读超时，原因可区分 |
| `TestClusterConcurrentWritesNoLostUpdate` | 20 并发写版本号 1..20 无丢失更新 |
| `TestNodeRestartRecoversToCommittedPosition` | 重启恢复到同一可验证位置，后续版本单调 |
| `TestWALCorruptionOnRestartRejected` | 带损坏日志重启被明确拒绝 |
| `TestReplicaLogCorruptionDetectedOnReplay` | 损坏副本拒绝上线，健康多数派继续服务且不把它当已同步 |
| `TestPartitionMinorityCannotCommit` | 分区少数派不能提交，heal 后摘要一致 |
| `TestFollowerOfflineThenCatchup` | 落后副本不被计入同步，恢复后追赶到同一位置 |
| `TestFailoverAcrossMembershipChange` | 成员变更故障切换，旧 leader fence，僵尸写不可见 |
| `TestMigrationCutoverRejectsWrites` | 迁移与 200 个并发写叠加：成功数==最终版本数，无双重生效 |
| `TestClusterMigration` | 迁移后历史前缀摘要一致、新 epoch 服务、旧副本 fence |
| `TestDuplicateRequestAcrossRetries` | 10 路同 txnID 并发仅一次生效 |
| `TestConcurrentMigrationsSerialized` | 迁移/成员变更并发时恰好一个生效、另一个被明确拒绝 |
| `TestPositionRegressionRejected` | 同步位置回退（旧 epoch 安装）被拒绝 |
| `TestLogGapOnCatchupRejected` | 日志不连续的追赶被 `ErrLogGap` 拒绝 |

测试日志会打印关键同步位置（epoch/last/commit/哈希链/摘要）、仲裁结果（acks=N/M）、
选举结果与迁移状态机状态（prepare/cutover/committed）。

## 范围与简化说明

这是用于**行为验证的原型**，不是生产 Raft：leader 在配置中静态声明，故障切换通过显式
成员变更（epoch+1 + 持久化 fence）表达而非自治选举；网络与磁盘均为进程内模拟。
重点在于让“成功/超时/不可用/冲突/陈旧”可区分、让重复不双生效、让落后与损坏无法冒充
已同步，以及让迁移期间不存在新旧分片双写窗口。
