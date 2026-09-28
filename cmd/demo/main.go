// Command demo 在本地用多个独立内存节点演示分片仲裁存储：
// 仲裁读写、幂等重复请求、故障切换、读修复（副本追赶）、分片迁移。
//
// 运行：go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/lisama569/sharded-quorum-store-go/internal/store"
)

func main() {
	logger := log.New(os.Stdout, "", 0)
	logf := func(format string, args ...any) { logger.Printf(format, args...) }

	net := store.NewNetwork()
	clu := store.NewCluster()
	clu.OnChange(func(_ uint64, shards []store.ShardSpec) {
		if err := net.ApplyShards(shards); err != nil {
			logger.Fatalf("apply shards: %v", err)
		}
	})

	const nodeN = 5
	for _, id := range []string{"n1", "n2", "n3", "n4", "n5"} {
		n := store.NewNode(store.NodeConfig{ID: id})
		must(n.Start())
		must(net.Register(n))
		must(clu.AddMember(id))
	}
	// 两个范围分片，各 3 副本（n1..n3 / n3..n5，重叠 n3）。
	must(clu.UpdateShard(store.ShardSpec{ID: "sA", StartKey: "", EndKey: "m", Members: []string{"n1", "n2", "n3"}, Fence: 1}))
	must(clu.UpdateShard(store.ShardSpec{ID: "sB", StartKey: "m", EndKey: "", Members: []string{"n3", "n4", "n5"}, Fence: 1}))

	cl := store.NewClient(clu, net, store.WithLogger(logf), store.WithTimeout(0))
	ctx := context.Background()
	section("1) 仲裁读写 + 幂等重复请求")
	must(cl.Put(ctx, "apple", []byte("red"), store.QUORUM, "demo-1"))
	must(cl.Put(ctx, "apple", []byte("red"), store.QUORUM, "demo-1")) // 相同 requestID：重复提交
	g, err := cl.Get(ctx, "apple", store.QUORUM)
	must(err)
	fmt.Printf("apple=%q responses=%d positions=%v\n", g.Value, g.Responses, g.Positions)

	section("2) 节点离线：写入仍满足多数派；恢复后读修复追赶")
	net.SetOffline("n1", true)
	must(cl.Put(ctx, "apricot", []byte("orange"), store.QUORUM, "demo-2"))
	net.SetOffline("n1", false)
	g2, err := cl.Get(ctx, "apricot", store.QUORUM)
	must(err)
	fmt.Printf("apricot=%q (after n1 recovered + read-repair)\n", g2.Value)

	section("3) 网络分区：少数派侧 QUORUM 写返回 TIMEOUT，数据不丢")
	// 以 n3 视角接入的客户端：n3 被与 n4/n5 隔离开，sB（n3,n4,n5）只剩自己一票。
	cl3 := store.NewClient(clu, net, store.WithLogger(logf), store.WithTimeout(0), store.WithIdentity("n3"))
	net.SetPartition("n3", "n4", true)
	net.SetPartition("n3", "n5", true)
	err = cl3.Put(ctx, "melon", []byte("green"), store.QUORUM, "demo-3")
	fmt.Printf("minority-side write result: %v (code=%s)\n", err, store.CodeOf(err))
	// 多数派一侧（n4/n5 视角）写入成功，分区恢复后不丢。
	cl4 := store.NewClient(clu, net, store.WithLogger(logf), store.WithTimeout(0), store.WithIdentity("n4"))
	must(cl4.Put(ctx, "mango", []byte("yellow"), store.QUORUM, "demo-3b"))
	net.HealAll()
	g3, err := cl.Get(ctx, "mango", store.QUORUM)
	fmt.Printf("mango after heal: found=%v value=%q err=%v\n", g3.Found, g3.Value, err)

	section("4) 分片迁移 sA: n1,n2,n3 -> n3,n4,n5（迁移期间并发读）")
	mig := store.NewMigrator(clu, net, logf)
	must(mig.MigrateShard(ctx, "sA", []string{"n3", "n4", "n5"}))
	if ph, ok := mig.Status("sA"); ok {
		fmt.Printf("migration sA final phase=%s\n", ph)
	}
	// 迁移后旧属主 n1 不应再接受该分片写；新路由下写成功
	must(cl.Put(ctx, "avocado", []byte("green"), store.QUORUM, "demo-4"))
	g4, err := cl.Get(ctx, "avocado", store.QUORUM)
	must(err)
	fmt.Printf("avocado=%q via new shard members n3,n4,n5\n", g4.Value)
	err = net.Call(ctx, "check", "n1", func(n *store.Node) error {
		_, e := n.HandleStatus("sA")
		return e
	})
	fmt.Printf("old replica n1/sA status after migration: %v (old generation no longer authoritative)\n", err)

	logger.Println("demo finished")
}

func section(title string) {
	fmt.Printf("\n===== %s =====\n", title)
}

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
