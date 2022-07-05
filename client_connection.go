package net

import (
	"bufio"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"time"
)

type ClientMetadata map[string]interface{}

type ClientConnection struct {
	id        string
	sessionId string

	responseWriter *bufio.Writer
	msg            chan []byte
	doneChan       chan interface{}
}

// Users should not create instances of client. This should be handled by the SSE broker.
func newClientConnection(id string, w *bufio.Writer) (*ClientConnection, error) {
	return &ClientConnection{
		id:             id,
		sessionId:      uuid.New().String(),
		responseWriter: w,
		msg:            make(chan []byte),
		doneChan:       make(chan interface{}, 1),
	}, nil
}

func (c *ClientConnection) Id() string {
	return c.id
}

func (c *ClientConnection) SessionId() string {
	return c.sessionId
}

func (c *ClientConnection) Send(event Event) {
	bytes := event.Prepare()
	c.msg <- bytes
}

func (c *ClientConnection) serve(interval time.Duration, onClose func()) {
	heartBeat := time.NewTicker(interval)

writeLoop:
	for {
		select {
		case <-heartBeat.C:
			go c.Send(HeartbeatEvent{})
		case msg, open := <-c.msg:
			if !open {
				break writeLoop
			}
			_, err := c.responseWriter.Write(msg)
			if err != nil {
				logrus.Errorf("unable to write to client %v: %v", c.id, err.Error())
				break writeLoop
			}
			_ = c.responseWriter.Flush()
		}
	}

	heartBeat.Stop()
	c.doneChan <- true
	onClose()
}

func (c *ClientConnection) Done() <-chan interface{} {
	return c.doneChan
}
