package freedom

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/session"
)

func TestCancelIdleConnectionClosesInbound(t *testing.T) {
	inboundConn, peerConn := net.Pipe()
	defer peerConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ctx = session.ContextWithInbound(ctx, &session.Inbound{Conn: inboundConn})
	extraCancelled := make(chan struct{})

	cancelIdleConnection(ctx, cancel, func() { close(extraCancelled) })

	select {
	case <-ctx.Done():
	default:
		t.Fatal("primary context was not cancelled")
	}
	select {
	case <-extraCancelled:
	default:
		t.Fatal("secondary context was not cancelled")
	}

	_ = peerConn.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := peerConn.Write([]byte{1}); err == nil {
		t.Fatal("peer write succeeded after idle connection was closed")
	}
}
