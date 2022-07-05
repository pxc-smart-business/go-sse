package net

import (
	"bufio"
	"sync"
	"time"
)

type Broker struct {
	mtx sync.Mutex

	clientSessions map[string]map[string]*ClientConnection
	clientMetadata map[string]ClientMetadata

	disconnectCallback func(clientId string, sessionId string)
}

func NewBroker() *Broker {
	return &Broker{
		clientSessions: make(map[string]map[string]*ClientConnection),
		clientMetadata: map[string]ClientMetadata{},
	}
}

func (b *Broker) Connect(clientId string, w *bufio.Writer) (*ClientConnection, error) {
	return b.ConnectWithHeartBeatInterval(clientId, w, 15*time.Second)
}

func (b *Broker) ConnectWithHeartBeatInterval(clientId string, w *bufio.Writer, interval time.Duration) (*ClientConnection, error) {
	client, err := newClientConnection(clientId, w)
	if err != nil {
		// this should not happen with fiber
		return nil, NewStreamingUnsupportedError(err.Error())
	}

	b.addClient(clientId, client)

	go client.serve(
		interval,
		func() {
			b.removeClient(clientId, client.sessionId) //onClose callback
		},
	)

	return client, nil
}

func (b *Broker) IsClientPresent(clientId string) bool {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	_, ok := b.clientSessions[clientId]
	return ok
}

func (b *Broker) SetClientMetadata(clientId string, metadata map[string]interface{}) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	_, ok := b.clientSessions[clientId]
	if !ok {
		return NewUnknownClientError(clientId)
	}

	b.clientMetadata[clientId] = metadata

	return nil
}

func (b *Broker) GetClientMetadata(clientId string) (map[string]interface{}, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	_, ok := b.clientSessions[clientId]
	md, ok2 := b.clientMetadata[clientId]
	if !ok || !ok2 {
		return nil, NewUnknownClientError(clientId)
	}

	return md, nil
}

func (b *Broker) addClient(clientId string, connection *ClientConnection) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	_, ok := b.clientSessions[clientId]
	if !ok {
		b.clientSessions[clientId] = make(map[string]*ClientConnection)
	}

	b.clientSessions[clientId][connection.sessionId] = connection
}

func (b *Broker) removeClient(clientId string, sessionId string) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	sessions, ok := b.clientSessions[clientId]
	if !ok {
		return
	}

	delete(sessions, sessionId)

	if len(b.clientSessions[clientId]) == 0 {
		delete(b.clientSessions, clientId)
		delete(b.clientMetadata, clientId)
	}

	if b.disconnectCallback != nil {
		go b.disconnectCallback(clientId, sessionId)
	}
}

func (b *Broker) Broadcast(event Event) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	for _, sessions := range b.clientSessions {
		for _, c := range sessions {
			c.Send(event)
		}
	}
}

func (b *Broker) Send(clientId string, event Event) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	sessions, ok := b.clientSessions[clientId]
	if !ok {
		return NewUnknownClientError(clientId)
	}
	for _, c := range sessions {
		c.Send(event)
	}
	return nil
}

func (b *Broker) SetDisconnectCallback(cb func(clientId string, sessionId string)) {
	b.disconnectCallback = cb
}

func (b *Broker) Close() error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	for _, v := range b.clientSessions {
		// Mark all client sessions as done
		for _, session := range v {
			session.doneChan <- true
		}
	}

	// Clear client sessions
	b.clientSessions = map[string]map[string]*ClientConnection{}

	return nil
}
