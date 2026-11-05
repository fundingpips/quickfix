// Copyright (c) quickfixengine.org  All rights reserved.
//
// This file may be distributed under the terms of the quickfixengine.org
// license as defined by quickfixengine.org and appearing in the file
// LICENSE included in the packaging of this file.
//
// This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING
// THE WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A
// PARTICULAR PURPOSE.
//
// See http://www.quickfixengine.org/LICENSE for licensing information.
//
// Contact ask@quickfixengine.org if any conditions of this licensing
// are not clear to you.

package quickfix

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stallingConn stands in for a connection to a counterparty that stops
// reading: once stalled, every write blocks until the connection is closed.
type stallingConn struct {
	net.Conn
	stalled   atomic.Bool
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *stallingConn) Write(b []byte) (int, error) {
	if c.stalled.Load() {
		<-c.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Write(b)
}

func (c *stallingConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func (c *stallingConn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

type stallingDialer struct {
	conns chan *stallingConn
}

func (d stallingDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	stalling := &stallingConn{Conn: conn, closed: make(chan struct{})}
	d.conns <- stalling
	return stalling, nil
}

type logonRecorder struct {
	loggedOn chan SessionID
}

func (a logonRecorder) OnCreate(SessionID) {}
func (a logonRecorder) OnLogon(sessionID SessionID) {
	select {
	case a.loggedOn <- sessionID:
	default:
	}
}
func (a logonRecorder) OnLogout(SessionID)                               {}
func (a logonRecorder) ToAdmin(*Message, SessionID)                      {}
func (a logonRecorder) ToApp(*Message, SessionID) error                  { return nil }
func (a logonRecorder) FromAdmin(*Message, SessionID) MessageRejectError { return nil }
func (a logonRecorder) FromApp(*Message, SessionID) MessageRejectError   { return nil }

// A counterparty that stops reading leaves the writer blocked on the Logout
// with no deadline, while the session loop times the Logout out and exits.
// Stop must not return with that connection still open: the caller takes a
// returned Stop to mean the session is gone, and may log the same session on
// again.
func TestInitiatorStopClosesAConnectionBlockedOnLogout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())

	acceptorSettings, err := ParseSettings(strings.NewReader(fmt.Sprintf(`[DEFAULT]
SocketAcceptPort=%d
[SESSION]
BeginString=FIX.4.4
SenderCompID=STALL_ACCEPTOR
TargetCompID=STALL_INITIATOR
HeartBtInt=30
`, port)))
	require.NoError(t, err)
	acceptor, err := NewAcceptor(logonRecorder{loggedOn: make(chan SessionID, 1)}, NewMemoryStoreFactory(), acceptorSettings, NewNullLogFactory())
	require.NoError(t, err)
	require.NoError(t, acceptor.Start())
	t.Cleanup(acceptor.Stop)

	initiatorSettings, err := ParseSettings(strings.NewReader(fmt.Sprintf(`[DEFAULT]
ReconnectInterval=1
LogoutTimeout=1
[SESSION]
BeginString=FIX.4.4
SenderCompID=STALL_INITIATOR
TargetCompID=STALL_ACCEPTOR
SocketConnectHost=127.0.0.1
SocketConnectPort=%d
HeartBtInt=30
`, port)))
	require.NoError(t, err)
	app := logonRecorder{loggedOn: make(chan SessionID, 1)}
	initiator, err := NewInitiator(app, NewMemoryStoreFactory(), initiatorSettings, NewNullLogFactory())
	require.NoError(t, err)

	// Start, with a dialer whose connection the test can stall.
	dialer := stallingDialer{conns: make(chan *stallingConn, 1)}
	initiator.stopChan = make(chan interface{})
	for sessionID := range initiator.sessionSettings {
		initiator.wg.Add(1)
		go func(sessionID SessionID) {
			defer initiator.wg.Done()
			initiator.handleConnection(initiator.sessions[sessionID], nil, dialer)
		}(sessionID)
	}

	conn := <-dialer.conns
	select {
	case <-app.loggedOn:
	case <-time.After(10 * time.Second):
		t.Fatal("the initiator did not log on")
	}
	conn.stalled.Store(true)

	stopped := make(chan struct{})
	go func() {
		initiator.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		conn.Close() // let the stuck Stop end with the test
		t.Fatal("Stop did not return")
	}
	require.True(t, conn.isClosed(), "Stop returned with the connection open")
}
