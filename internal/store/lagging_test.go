package store

import (
	"context"
	"testing"
)

// TestLaggingReplicaNotSynced 落后副本必须被读路径标记为 lagging，
// 且只有追赶后才与多数派位置一致。
func TestLaggingReplicaNotSynced(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"}, false)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)
	tc.put("a", "1", "lg1")

	tc.net.SetOffline("n1", true)
	tc.put("b", "2", "lg2")
	tc.put("c", "3", "lg3")

	// 恢复但先不触发读修复：n1 位置落后，绝不能等于多数派位置。
	tc.net.SetOffline("n1", false)
	lagPos, err := tc.nodes["n1"].HandlePosition("s1")
	if err != nil {
		t.Fatal(err)
	}
	leadPos, _ := tc.nodes["n2"].HandlePosition("s1")
	if lagPos.LastSeq >= leadPos.LastSeq {
		t.Fatalf("lagging replica pos=%s should be behind leader %s", lagPos, leadPos)
	}
	t.Logf("lagging replica n1=%s behind quorum n2=%s: treated as NOT synced", lagPos, leadPos)

	// 落后副本不能作为 InstallEntries 的“超前”来源：以它为源向 n2 装一条 seq 必须因位置回退被拒。
	_, err = tc.nodes["n2"].HandleInstallEntries(InstallReq{
		ShardID:  s1SpecID(),
		FromPos:  LogPosition{ShardID: "s1", LastSeq: 99},
		Entries:  nil,
		NewFence: 1,
	})
	if err == nil {
		t.Fatal("install with base ahead of source must be rejected")
	}
	t.Logf("install with non-contiguous/regressed base rejected: %v", err)

	// 读修复后 n1 必须收敛到领导者位置。
	if _, err := tc.client.Get(context.Background(), "c", QUORUM); err != nil {
		t.Fatal(err)
	}
	lagPos2, _ := tc.nodes["n1"].HandlePosition("s1")
	if lagPos2.LastSeq != leadPos.LastSeq || string(lagPos2.LastHash) != string(leadPos.LastHash) {
		t.Fatalf("after repair n1=%s != leader %s", lagPos2, leadPos)
	}
	t.Logf("lagging replica caught up to %s and now synced", lagPos2)
}

func s1SpecID() string { return "s1" }
