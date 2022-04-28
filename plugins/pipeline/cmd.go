package main

import (
	"encoding/json"
)

type Message struct {
	Cmd        string      `json:"cmd"`
	Plugin     string      `json:"plugin"`
	InstanceID string      `json:"instanceid"`
	Data       interface{} `json:"data"`
	Mode       string      `json:"mode"`
}

func RecvMsg(queue chan []byte) (*Message, error) {
	var msg Message
	data := <-queue
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func SendMsg(msg *Message, queue chan []byte) (err error) {
	content, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	queue <- content
	return nil
}
