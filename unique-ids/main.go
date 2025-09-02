package main

import (
	"crypto/rand"
	"encoding/json"
	"log"
	"slices"

	uuid "github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type GenerateMsgBody struct {
	MsgType string `json:"type"`
}

type GenerateMsgResponseBody struct {
	MsgType string `json:"type"`
	Id      string `json:"id"`
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("init", func(msg maelstrom.Message) error {
		nodeId := []byte(n.ID())
		if len(nodeId) < 6 {
			r := make([]byte, 6)
			rand.Read(r)
			nodeId = slices.Concat(nodeId, r)
		}
		uuid.SetNodeID(nodeId)
		return nil
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var msgIn GenerateMsgBody
		if err := json.Unmarshal(msg.Body, &msgIn); err != nil {
			return err
		}

		uuidOut, err := uuid.NewUUID()
		if err != nil {
			return err
		}
		response := GenerateMsgResponseBody{
			MsgType: "generate_ok",
			Id:      uuidOut.String(),
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
