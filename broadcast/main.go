package main

import (
	"log"
	"math/rand"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Context struct {
	n                *maelstrom.Node
	neighbors        []string
	broadcastContext BroadcastContext
}

func main() {
	n := *maelstrom.NewNode()
	c := Context{&n, make([]string, 0), BroadcastContext{receivedMsgs: make(map[int]bool)}}
	c.n = &n

	n.Handle("init", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return c.HandleTopology(&msg)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		return c.HandleBroadcast(&msg)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return c.HandleRead(&msg)
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	// Start sync broadcast goroutine
	go func() {
		for {
			// Random offset between 0 and 1 second
			offset := time.Duration(rand.Intn(1000)) * time.Millisecond
			interval := 2*time.Second + offset
			time.Sleep(interval)
			c.SyncBroadcast()
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
