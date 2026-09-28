# Sharded Quorum Store（本地分片仲裁存储原型）

一个**不依赖任何外部数据库或外部服务**、用多个独立内存/可落盘节点模拟真实集群的
分片键值存储原型。用于在单机上可重复地验证：

- 按键范围分片、节点成员变更、读写仲裁；
- 网络分区、节点离线、重复请求下**数据不静默丢失、同一请求不重复生效**；
- 持久化恢复、落后副本追赶、同步位置回退/日志不连续/校验不一致被明确拒绝；
- 分片迁移及迁移期间的并发访问：同一键不被新旧分片同时接受写，旧副本迁移后不再生效。

## 目录结构

```
internal/store/
  errors.go      可区分结果码（OK/TIMEOUT/UNAVAILABLE/CONFLICT/REJECTED/CORRUPT）
  log.go         分片追加日志：SHA-256 哈希链 + CRC32 WAL 帧 + 元数据持久化
  hash.go        哈希抽象
  node.go        节点：多分片副本、fencing、幂等去重、安装/追赶/截断/重置
  network.go     可注入离线/分区/延迟的模拟传输层 + 每分片写定序锁
  cluster.go     成员管理、范围分片路由表、fence 单调校验、变更下发
  client.go      仲裁读写客户端（ONE/QUORUM/ALL）、读修复、部分写恢复
  migration.go   分片迁移状态机
  *_test.go      故障链路测试
cmd/demo/        本地一键演示
```

## 一致性模型

### 副本日志与同步位置

- 每个分片在每个副本上是一条**追加日志**，序号 `Seq` 从 1 开始、连续、单调递增。
- 每条日志的 `Hash = SHA256(分片, Seq, PrevHash, Op, Key, Value, RequestID, Fence)`，
  形成**哈希链**。同步位置 `LogPosition{ShardID, LastSeq, LastHash}` 由“序号 + 链尾哈希”
  共同确定，因此相同 `Seq` 但不同内容的两条链会得到不同位置、不会被混淆。
- WAL 以 `[CRC32][Len][gob 条目]` 帧落盘，每帧 `fsync`；元数据（fence/serving/frozen）
  以临时文件 + 原子 rename 落盘。

### 定序与仲裁

- **写**：对同一分片的所有客户端写，在一个共享的**分片定序锁**内完成
  “探测位置 → 分配 `LastSeq+1` 与 `PrevHash` → 仲裁扇出”。该定序器是进程内控制面，
  不是某个节点，因此节点宕机只影响可凑齐的票数，不会让定序停摆。串行定序从根本上
  消除了同一槽位出现两个竞争条目的分叉。
- **仲裁级别**：
  - `ONE`：任一副本应答即成功（读可能读到旧值）；
  - `QUORUM`：多数派 `N/2+1`；
  - `ALL`：全部成员。
- 写达到所声明级别即成功；成功后会**尽力把条目补复制**给其余健康副本（best-effort），
  落后副本再由后续写/读修复追赶。
- **读**：并发向所有副本读取，按 `LastSeq` 取最高；同 `Seq` 出现不同哈希时按
  哈希多数派裁决，并记录 `lagging` 数。读到的落后位置**不会被当作已同步**，
  随后通过严格连续的 `InstallEntries` 做**读修复**。

### 幂等：重复请求不重复生效

- 每个写携带 `RequestID`。节点维护去重表：已执行过的 `RequestID` 再次到达时返回
  `Duplicate=true`，**不会**再追加日志、不再应用状态。
- 去重记录既在内存，也由日志条目重建；迁移/追赶随日志复制去重记录，
  因此**切换后用旧 RequestID 重试也不会二次生效**。
- 若一次写只在部分副本生效后超时，客户端用**同一个 RequestID** 重试：
  已执行副本返回 Duplicate，未执行副本先从持有该条目的副本**连续补齐**再应用，
  保证请求“至多生效一次”，且最终能达到仲裁。

### 可区分的结果码

| 场景 | 结果码 |
|------|--------|
| 成功 | `OK` |
| 目标离线 / 网络分区（请求挂起到 RPC 截止） | `TIMEOUT` |
| 无分片拥有该键、分片无成员、冻结窗口写入、非 serving 副本 | `UNAVAILABLE` |
| 同槽位竞争、fence 过期、安装基线分叉、截断点超前 | `CONFLICT` |
| 旧代际/非 serving 副本收到正常写 | `REJECTED` |
| CRC 不符、哈希链断裂、序号不连续、元数据 fence 回退 | `CORRUPT` |

用 `store.CodeOf(err)` 即可区分，`*OpError` 还携带操作名与诊断原因。

## 故障恢复

- **节点重启**：启动时读取各分片 WAL，逐帧校验 CRC，再校验 `Seq` 连续、`PrevHash`
  相接、`Hash` 正确，并重放重建键值状态、fence 与去重表。任何不一致返回
  `CORRUPT` 并**拒绝启动**，损坏的 WAL 文件原样保留在磁盘上以便诊断。
- **短暂失联后追赶**：落后副本的同步位置严格小于多数派。读路径会显式标记 `lagging`
  并通过 `HandleFetch` + `HandleInstallEntries` 从连续位置补齐；安装要求
  `(LastSeq, LastHash)` 与本地基线严格匹配，**位置回退、日志不连续一律拒绝**。
- **网络分区**：分区在传输层表现为“请求挂起直到 RPC 截止”，因此少数派一侧的
  `QUORUM` 写得到 `TIMEOUT` 而不是错误地成功；多数派一侧仍可读写。分区恢复后，
  少数派通过读修复/写补复制收敛，不会脑裂。
- **节点离线（故障切换）**：剩余副本凑得齐多数派时写持续可用；离线节点恢复后
  作为落后副本被追赶。

### 关于未提交尾部

写路径上的追赶只做**连续追加**，绝不静默覆盖。控制面在极少数需要丢弃
“未提交竞争尾部”的场景，使用带严格 `(Seq,Hash)` 前缀校验的 `HandleTruncate`，
它只会截断到多数派提交点之后，并同步移除尾部条目的去重记录；任何前缀不匹配都被拒绝。

## 分片迁移规则

迁移由 `Migrator.MigrateShard(ctx, shardID, to)` 驱动，每个分片有独立迁移锁，
**整个迁移持有该分片的写定序锁**，与客户端写、与其它针对同一分片的迁移/成员变更互斥。
阶段如下：

1. `PREPARING`：新成员以 `serving=false` 安装空副本（fence 提升到 `oldFence+1`），
   并从旧仲裁的最新位置多轮连续追赶；要求旧仲裁多数派可达。
2. `FROZEN`：冻结旧属主写入。冻结窗口内对旧属主的写返回 `UNAVAILABLE`，
   新副本尚未 serving 返回 `REJECTED`——因此同一键不会被新旧分片同时接受写。
   必须能冻结旧仲裁多数派，否则迁移中止。
3. `SWITCHING`：冻结点做最终追赶并严格校验位置后，提交新路由（fence 严格递增），
   再把新副本 `SetServing(true)` 提升。
4. `CLEANUP`：不在新成员集合中的旧副本 `DropShard` 删除数据；旧副本 fence 更低，
   即使清理消息延迟/丢失，旧代际的写也会被 fencing 拒绝，**旧副本迁移后不再生效**。
5. `DONE`。

中止保证：若在路由提交前任一步骤失败（如旧仲裁不可达），迁移 `ABORTED`，
不提交任何路由变更，并尽力把旧属主解冻、回滚到旧 fence，集群恢复可写。
迁移与并发客户端写叠加时，写要么在迁移前于旧配置完成、要么排队到切换后在新配置完成；
测试 `TestConcurrentMigrationAndWrites` 验证所有成功写一个不丢、不重复，新属主收敛。

## 可配置项

| 项 | 位置 | 说明 |
|----|------|------|
| `NodeConfig.ID` | 节点 | 节点唯一标识，重复注册被拒 |
| `NodeConfig.DataDir` | 节点 | 持久化目录；留空为纯内存模式 |
| `WithTimeout(d)` | 客户端 | 操作总超时；0 时使用内置有界 RPC 截止 |
| `WithIdentity(id)` | 客户端 | 客户端在模拟网络中的视角，用于被分区规则覆盖 |
| `WithLogger(fn)` | 客户端/迁移器 | 注入日志，打印同步位置/仲裁结果/迁移阶段 |
| `Network.SetOffline` | 网络 | 模拟节点宕机/恢复 |
| `Network.SetPartition(a,b,cut)` | 网络 | 注入/解除 a↔b 双向分区 |
| `Network.SetBaseDelay` | 网络 | 每次调用的模拟传播延迟 |
| 客户端 RPC 阶段截止 | `client.phaseTimeout` | 80ms 下限、300ms 上限，离线节点快速失败 |
| 迁移器单次 RPC 截止 | `Migrator.rpcTimeout` | 默认 300ms |
| 分片范围/成员/Fence | `Cluster.UpdateShard` | `[StartKey, EndKey)` 范围，Fence 必须严格递增 |

## 本地验证

需要 Go 1.25+，无任何外部依赖。

```bash
# 一键演示：仲裁读写、幂等重复、节点离线+读修复、网络分区、分片迁移
go run ./cmd/demo

# 全量单测（含关键同步位置、仲裁结果、迁移状态日志）
go test ./... -v

# 竞态检测
go test ./internal/store/ -race

# 稳定性（并发写/迁移）
go test ./internal/store/ -run 'TestConcurrent|TestMigration' -count=10
```

测试与场景对应：

| 需求链路 | 测试 |
|----------|------|
| 重复请求不重复生效 | `TestIdempotentDuplicateRequest` |
| 成功/超时/不可用等结果可区分 | `TestDistinctFailureCodes` |
| 并发写不丢不重 | `TestConcurrentWritesNoLossNoDuplicate` |
| 节点重启 + 持久化恢复 | `TestRestartPersistence` |
| 重启后落后副本追赶 | `TestRestartSingleNodeCatchup` |
| 网络分区少数派失败、多数派可用、恢复收敛 | `TestNetworkPartition` |
| 节点离线故障切换 | `TestOfflineFailover` |
| WAL CRC 损坏被拒绝并保留现场 | `TestCorruptWALDetected` |
| 位置回退/哈希不一致被拒绝 | `TestTruncateCannotRollback`、`TestLaggingReplicaNotSynced` |
| 迁移数据可读、旧副本失效 | `TestMigrationBasic` |
| 迁移失败无部分提交 | `TestMigrationAbortNoPartialCommit` |
| 冻结窗口写被可区分拒绝、不双写 | `TestMigrationFrozenWritesRejected` |
| 并发迁移 + 客户端写叠加 | `TestConcurrentMigrationAndWrites` |

## 原型边界（明确不做的事）

- 定序锁是进程内共享控制面，旨在单机模拟“单领导定序”，不模拟真实跨进程领导选举；
  这让原型可以专注验证仲裁、恢复与迁移语义，而不引入外部协调服务。
- 不实现跨进程真实网络、Gossip、快照压缩与垃圾回收；日志规模按测试数据量保持很小。
- 时钟用本地 `time`，未模拟物理时钟漂移与拜占庭节点。
