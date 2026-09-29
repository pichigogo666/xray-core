//go:build linux

package proxy

import (
	"context"
	"io"
	stdnet "net"
	"testing"
	"time"
)

func tcpPair(t *testing.T) (*stdnet.TCPConn, *stdnet.TCPConn) {
	t.Helper()
	listener, err := stdnet.ListenTCP("tcp4", &stdnet.TCPAddr{IP: stdnet.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan *stdnet.TCPConn, 1)
	errs := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptTCP()
		if err != nil {
			errs <- err
			return
		}
		accepted <- conn
	}()

	client, err := stdnet.DialTCP("tcp4", nil, listener.Addr().(*stdnet.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case server := <-accepted:
		t.Cleanup(func() { _ = client.Close() })
		t.Cleanup(func() { _ = server.Close() })
		return client, server
	case err := <-errs:
		_ = client.Close()
		t.Fatal(err)
	case <-time.After(time.Second):
		_ = client.Close()
		t.Fatal("timed out accepting TCP connection")
	}
	return nil, nil
}

func TestWatchSpliceIdleClosesIdlePair(t *testing.T) {
	client, server := tcpPair(t)
	stop := watchSpliceIdle(context.Background(), client, server, 80*time.Millisecond)
	defer stop()

	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle connection remained open")
	}
}

func TestWatchSpliceIdleKeepsActivePair(t *testing.T) {
	client, server := tcpPair(t)
	stop := watchSpliceIdle(context.Background(), client, server, 120*time.Millisecond)
	defer stop()

	deadline := time.Now().Add(350 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := client.Write([]byte{1}); err != nil {
			t.Fatalf("active connection was closed: %v", err)
		}
		_ = server.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if _, err := io.ReadFull(server, make([]byte, 1)); err != nil {
			t.Fatalf("failed to read active connection: %v", err)
		}
		time.Sleep(30 * time.Millisecond)
	}
}
