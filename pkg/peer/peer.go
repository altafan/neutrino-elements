package peer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/vulpemventures/neutrino-elements/pkg/binary"
	"github.com/vulpemventures/neutrino-elements/pkg/protocol"
)

// PeerID is peer IP address.
type PeerID string

// Peer describes a network's node.
type Peer interface {
	// ID returns peer ID. Must be unique in the network.
	ID() PeerID
	// Connection returns peer connection, using to send and receive Elements messages.
	Connection() io.ReadWriteCloser
	// RecvMsg returns message header received from peer
	RecvMsg() (*protocol.MessageHeader, error)
	// SendMsg sends a msg to peer
	SendMsg(msg *protocol.Message) error
	// Addr returns the Network Address of the peer
	Addr() *protocol.Addr
	// PeersTip returns current tip block height
	PeersTip() uint32
	// SetPeersTip sets the block height tip of the peer
	SetPeersTip(startBlockHeight uint32)
}

type elementsPeer struct {
	peerAddr         string
	networkAddress   *protocol.Addr
	tcpConnection    net.Conn
	startBlockHeight uint32

	//used to synchronize access to the peers startBlockHeight
	m *sync.RWMutex
}

func NewElementsPeer(peerAddr string) (Peer, error) {
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return nil, err
	}

	netAddress, err := protocol.ParseNodeAddr(peerAddr)
	if err != nil {
		return nil, err
	}

	return &elementsPeer{
		peerAddr:       peerAddr,
		networkAddress: netAddress,
		tcpConnection:  conn,
		m:              new(sync.RWMutex),
	}, nil
}

func (e *elementsPeer) ID() PeerID {
	return PeerID(e.tcpConnection.LocalAddr().String())
}

func (e *elementsPeer) RecvMsg() (*protocol.MessageHeader, error) {
	tmp := make([]byte, protocol.MsgHeaderLength)
	nn, err := e.tcpConnection.Read(tmp)
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			logrus.Debugf("connection to peer %s closed, reconnecting...")
			conn, err := net.Dial("tcp", e.peerAddr)
			if err != nil {
				return nil, err
			}
			e.tcpConnection = conn
			return e.RecvMsg()
		}
		return nil, err
	}

	var msgHeader protocol.MessageHeader
	if err := binary.NewDecoder(bytes.NewReader(tmp[:nn])).Decode(&msgHeader); err != nil {
		return nil, fmt.Errorf("failed to decode header: %+v", err)
	}

	if err := msgHeader.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate header: %+v", err)
	}

	return &msgHeader, nil
}

func (e *elementsPeer) SendMsg(msg *protocol.Message) error {
	msgSerialized, err := binary.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = e.tcpConnection.Write(msgSerialized)
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			logrus.Debugf("connection to peer %s closed, reconnecting...")
			conn, err := net.Dial("tcp", e.peerAddr)
			if err != nil {
				return err
			}
			e.tcpConnection = conn
			return e.SendMsg(msg)
		}
	}
	return err
}

func (e *elementsPeer) Connection() io.ReadWriteCloser {
	return e.tcpConnection
}

func (e *elementsPeer) Addr() *protocol.Addr {
	return e.networkAddress
}

func (e *elementsPeer) PeersTip() uint32 {
	e.m.RLock()
	defer e.m.RUnlock()

	return e.startBlockHeight
}

func (e *elementsPeer) SetPeersTip(startBlockHeight uint32) {
	e.m.Lock()
	defer e.m.Unlock()

	e.startBlockHeight = startBlockHeight
}

func (e *elementsPeer) String() string {
	return e.tcpConnection.RemoteAddr().String()
}
