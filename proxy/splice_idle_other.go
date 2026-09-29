//go:build !linux

package proxy

import (
	"context"
	stdnet "net"
	"time"
)

func watchSpliceIdle(ctx context.Context, readerConn stdnet.Conn, writerConn stdnet.Conn, timeout time.Duration) func() {
	return func() {}
}

func WatchTCPIdle(ctx context.Context, readerConn stdnet.Conn, writerConn stdnet.Conn, timeout time.Duration) func() {
	return watchSpliceIdle(ctx, readerConn, writerConn, timeout)
}
