package tcpcall

import (
	"fmt"
	"testing"
)

func TestShard(t *testing.T) {
	itemCount := 1000
	// generate IDs
	ids := make([]string, itemCount)
	for i := 0; i < len(ids); i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}
	// calculate shards
	shards := map[string]int{}
	nodes := 5
	for _, id := range ids {
		shards[id] = shard([]byte(id), nodes)
	}
	// check shards are persistent
	for _, id := range ids {
		v := shard([]byte(id), nodes)
		expect, _ := shards[id]
		if v != expect {
			t.Fatalf("ID: %v; first shard is %d but "+
				"second is %d", id, expect, v)
		}
	}
	// check distribution
	counts := make([]int, nodes)
	for _, id := range ids {
		counts[shards[id]]++
	}
	maxDiff := itemCount / 20 // 5%
	for i := 0; i < len(counts); i++ {
		for j := 0; j < len(counts); j++ {
			diff := counts[i] - counts[j]
			if diff < -maxDiff || maxDiff < diff {
				t.Fatalf("too big diff between shard"+
					" %d and %d: %d", i, j, diff)
			}
		}
	}
}
