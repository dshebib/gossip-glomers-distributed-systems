package main

import (
	"encoding/json"
	"slices"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const CLEANUP_SIZE = 512

type BroadcastContext struct {
	receivedMsgs map[int]bool
	mutex        sync.RWMutex
}

type TopologyMsgBody struct {
	Topology map[string][]string `json:"topology"`
}

type BroadcastMsgBody struct {
	MsgType string `json:"type"`
	Message int    `json:"message"`
}

type EmptyResponse struct {
	MsgType string `json:"type"`
}

type ReadResponse struct {
	MsgType  string `json:"type"`
	Messages []int  `json:"messages"`
}

func (c *Context) HandleTopology(msg *maelstrom.Message) error {
	var topology TopologyMsgBody
	json.Unmarshal(msg.Body, &topology)

	neighbors, ok := topology.Topology[c.n.ID()]
	if !ok {
		c.neighbors = make([]string, 0)
	} else {
		c.neighbors = neighbors
	}

	return c.n.Reply(*msg, EmptyResponse{"topology_ok"})
}

func (c *Context) rebroadcastAllExcept(excluded string, msg int) {
	for _, n := range c.neighbors {
		if n == excluded {
			continue
		}
		c.n.Send(n, BroadcastMsgBody{"broadcast", msg})
	}
}

func (c *Context) HandleBroadcast(msg *maelstrom.Message) error {
	var req BroadcastMsgBody
	json.Unmarshal(msg.Body, &req)
	nodeFrom := msg.Src

	// Don't rebroadcast if already received
	c.broadcastContext.mutex.RLock()
	_, ok := c.broadcastContext.receivedMsgs[req.Message]
	c.broadcastContext.mutex.RUnlock()
	if ok {
		return c.n.Reply(*msg, EmptyResponse{"broadcast_ok"})
	}

	c.broadcastContext.mutex.Lock()
	c.broadcastContext.receivedMsgs[req.Message] = true
	c.broadcastContext.mutex.Unlock()

	c.rebroadcastAllExcept(nodeFrom, req.Message)

	return c.n.Reply(*msg, EmptyResponse{"broadcast_ok"})
}

func (c *Context) SyncBroadcast() {
	syncMsgs := func(fullMsg maelstrom.Message) error {
		var resp ReadResponse
		json.Unmarshal(fullMsg.Body, &resp)

		c.broadcastContext.mutex.RLock()
		receivedMsgsSnapshot := make(map[int]bool)
		for m, exists := range c.broadcastContext.receivedMsgs {
			if exists {
				receivedMsgsSnapshot[m] = true
			}
		}
		c.broadcastContext.mutex.RUnlock()

		missingMessages := make([]int, 0)

		for _, msg := range resp.Messages {
			if !receivedMsgsSnapshot[msg] {
				c.rebroadcastAllExcept(fullMsg.Src, msg)
			}
			missingMessages = append(missingMessages, msg)
		}

		for msg := range receivedMsgsSnapshot {
			if !(slices.Contains(resp.Messages, msg)) {
				c.n.Send(fullMsg.Src, BroadcastMsgBody{"broadcast", msg})
			}
		}

		c.broadcastContext.mutex.Lock()
		for _, msg := range missingMessages {
			c.broadcastContext.receivedMsgs[msg] = true
		}
		c.broadcastContext.mutex.Unlock()

		return nil
	}

	for _, n := range c.neighbors {
		c.n.RPC(n, EmptyResponse{"read"}, syncMsgs)
	}
}

func (c *Context) HandleRead(msg *maelstrom.Message) error {
	var messages []int
	c.broadcastContext.mutex.RLock()
	for k := range c.broadcastContext.receivedMsgs {
		messages = append(messages, k)
	}
	c.broadcastContext.mutex.RUnlock()
	return c.n.Reply(*msg, ReadResponse{"read_ok", messages})
}
