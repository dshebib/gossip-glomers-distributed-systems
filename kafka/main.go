package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NodeContext struct {
	n         *maelstrom.Node
	logs      LocalLog
	nodeReady chan struct{}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	c := NodeContext{
		n:         n,
		logs:      LocalLog{LinKV: kv},
		nodeReady: make(chan struct{}),
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		close(c.nodeReady)
		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("send", func(msg maelstrom.Message) error {
		return c.HandleSend(&msg)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		return c.HandlePoll(&msg)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		return c.HandleCommitOffsets(&msg)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		return c.HandleListCommittedOffsets(&msg)
	})

	n.Handle("replicate_msg", func(msg maelstrom.Message) error {
		return c.HandleReplicateMsg(&msg)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
