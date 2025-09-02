package main

import (
	"context"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NodeContext struct {
	n         *maelstrom.Node
	kv        *maelstrom.KV
	val       int
	updates   chan int
	nodeReady chan struct{}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	c := NodeContext{n, kv, 0, make(chan int), make(chan struct{})}

	n.Handle("init", func(msg maelstrom.Message) error {
		close(c.nodeReady)
		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return c.HandleRead(&msg)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		return c.HandleAdd(&msg)
	})

	go func() {
		<-c.nodeReady
		if err := c.kvUpdater(); err != nil {
			log.Fatal(err)
			return
		}
	}()

	go func() {
		<-c.nodeReady
		for {
			time.Sleep(700 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			if err := c.readKV(ctx); err != nil {
				log.Fatal(err)
				cancel()
				return
			}
			cancel()
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
