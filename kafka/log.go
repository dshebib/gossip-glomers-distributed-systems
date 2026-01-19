package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type EmptyResponse struct {
	MsgType string `json:"type"`
}

type SendRequest struct {
	EmptyResponse
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

type SendResponse struct {
	EmptyResponse
	Offset int `json:"offset"`
}

type PollRequest struct {
	EmptyResponse
	Offsets map[string]int `json:"offsets"`
}

type PollResponse struct {
	EmptyResponse
	Msgs map[string][][2]int `json:"msgs"`
}

type CommitOffsetsRequest struct {
	PollRequest
}

type CommitOffsetsResponse struct {
	EmptyResponse
}

type ListCommittedOffsets struct {
	EmptyResponse
	Keys []string `json:"keys"`
}

type ListCommittedOffsetsResponse struct {
	EmptyResponse
	Offsets map[string]int `json:"offsets"`
}

type ReplicateMsgBody struct {
	MsgType string `json:"type"`
	Key     string `json:"key"`
	Msg     int    `json:"msg"`
	Offset  int    `json:"offset"`
}

func (c *NodeContext) HandleSend(msg *maelstrom.Message) error {
	var req SendRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	offset, err := c.logs.AppendMsg(req.Key, req.Msg)
	if err != nil {
		return err
	}

	c.sendReplicateMsg(req.Key, req.Msg, offset)

	res := SendResponse{
		EmptyResponse: EmptyResponse{MsgType: "send_ok"},
		Offset:        int(offset),
	}
	return c.n.Reply(*msg, res)
}

func (c *NodeContext) HandlePoll(msg *maelstrom.Message) error {
	var req PollRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	res := PollResponse{
		EmptyResponse: EmptyResponse{MsgType: "poll_ok"},
		Msgs:          make(map[string][][2]int),
	}

	for key, offset := range req.Offsets {
		msgs, err := c.logs.GetMsgsFromOffset(key, offset)
		if err != nil {
			return err
		}

		if len(msgs) == 0 {
			res.Msgs[key] = make([][2]int, 0)
			continue
		}

		pairs := make([][2]int, len(msgs))
		for i, msg := range msgs {
			pairs[i] = [2]int{msg.offset, msg.msg}
		}

		res.Msgs[key] = pairs
	}

	return c.n.Reply(*msg, res)
}

func (c *NodeContext) HandleCommitOffsets(msg *maelstrom.Message) error {
	var req CommitOffsetsRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	for key, offset := range req.Offsets {
		err := c.logs.CommitOffset(key, offset)
		if err != nil {
			return err
		}
	}

	res := CommitOffsetsResponse{
		EmptyResponse: EmptyResponse{MsgType: "commit_offsets_ok"},
	}
	return c.n.Reply(*msg, res)
}

func (c *NodeContext) HandleListCommittedOffsets(msg *maelstrom.Message) error {
	var req ListCommittedOffsets
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	res := ListCommittedOffsetsResponse{
		EmptyResponse: EmptyResponse{MsgType: "list_committed_offsets_ok"},
		Offsets:       make(map[string]int),
	}

	for _, key := range req.Keys {
		// NOTE does this have to be sync?
		// offset, err := c.logs.GetCurrentOffsetSync(key)
		// if err != nil {
		// 	continue
		// }
		offset, exists := c.logs.getLocalOffset(key)
		if !exists {
			continue
		}
		res.Offsets[key] = int(offset)
	}

	return c.n.Reply(*msg, res)
}

// sendReplicateMsg sends replicate_msg RPCs to all other available nodes (fire and forget).
func (c *NodeContext) sendReplicateMsg(k string, msg int, offset int) error {
	body := ReplicateMsgBody{
		MsgType: "replicate_msg",
		Key:     k,
		Msg:     msg,
		Offset:  offset,
	}

	// Send to all other nodes (fire and forget)
	for _, nodeID := range c.n.NodeIDs() {
		if nodeID != c.n.ID() {
			go c.n.Send(nodeID, body)
		}
	}

	return nil
}

func (c *NodeContext) HandleReplicateMsg(msg *maelstrom.Message) error {
	var req ReplicateMsgBody
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	err := c.logs.AppendMsgLocal(req.Key, req.Msg, req.Offset)
	if err != nil {
		return err
	}

	return nil

	// res := EmptyResponse{MsgType: "replicate_msg_ok"}
	// return c.n.Reply(*msg, res)
}
