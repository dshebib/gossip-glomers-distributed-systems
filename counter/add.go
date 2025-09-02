package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const KV_VAL_KEY = "value"

type EmptyResponse struct {
	MsgType string `json:"type"`
}

type ReadResponse struct {
	MsgType string `json:"type"`
	Value   int    `json:"value"`
}

type AddRequest struct {
	MsgType string `json:"type"`
	Delta   int    `json:"delta"`
}

func (c *NodeContext) HandleRead(msg *maelstrom.Message) error {
	return c.n.Reply(*msg, ReadResponse{"read_ok", c.val})
}

func (c *NodeContext) HandleAdd(msg *maelstrom.Message) error {
	var req AddRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	c.updates <- req.Delta
	return c.n.Reply(*msg, EmptyResponse{"add_ok"})
}

func (c *NodeContext) kvUpdater() error {
	delta := 0
	for {
		select {
		case newDelta := <-c.updates:
			delta += newDelta
		default:
			if delta > 0 {
				updateSucceeded, err := c.updateKV(delta)
				if err != nil {
					return err
				}
				if !updateSucceeded {
					time.Sleep(time.Duration(25+rand.Intn(51)) *
						time.Millisecond) // short retry
					continue
				}
			}
			delta = 0
			time.Sleep(200 * time.Millisecond) // long wait
		}
	}
}

func (c *NodeContext) updateKV(delta int) (bool, error) {
	// Update KV, keeping local state in sync
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := c.readKV(ctx)
	if err != nil {
		return false, err
	}

	err = c.kv.CompareAndSwap(ctx, KV_VAL_KEY, c.val, c.val+delta, false)
	if err != nil {
		switch v := err.(type) {
		case *maelstrom.RPCError:
			switch v.Code {
			case maelstrom.PreconditionFailed:
				log.Print(err)
				return false, nil
			default:
				log.Print(err)
				return false, err
			}
		default:
			log.Print(err)
			return false, err
		}
	}
	c.val += delta
	return true, nil
}

func (c *NodeContext) readKV(ctx context.Context) error {
	// Read current value from KV, updating if not exists
	externalVal, err := c.kv.ReadInt(ctx, KV_VAL_KEY)
	if err != nil {
		switch v := err.(type) {
		case *maelstrom.RPCError:
			switch v.Code {
			case maelstrom.KeyDoesNotExist:
				c.kv.CompareAndSwap(ctx, KV_VAL_KEY, c.val, c.val, true)
				return nil
			default:
				log.Print(err)
				return err
			}
		default:
			log.Print(err)
			return err
		}
	}
	c.val = externalVal
	return err
}
