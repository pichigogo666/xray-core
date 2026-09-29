//go:build linux

package proxy

import (
	"context"
	"fmt"
	stdnet "net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/xtls/xray-core/common/errors"
)

type spliceTCPActivity struct {
	readerAcked    uint64
	readerReceived uint64
	writerAcked    uint64
	writerReceived uint64
}

func tcpActivity(conn stdnet.Conn) (acked uint64, received uint64, err error) {
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return 0, 0, fmt.Errorf("connection does not expose syscall.Conn")
	}
	rawConn, err := syscallConn.SyscallConn()
	if err != nil {
		return 0, 0, err
	}

	var info *unix.TCPInfo
	var socketErr error
	if err := rawConn.Control(func(fd uintptr) {
		info, socketErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	}); err != nil {
		return 0, 0, err
	}
	if socketErr != nil {
		return 0, 0, socketErr
	}
	return info.Bytes_acked, info.Bytes_received, nil
}

func spliceActivity(readerConn stdnet.Conn, writerConn stdnet.Conn) (spliceTCPActivity, error) {
	readerAcked, readerReceived, err := tcpActivity(readerConn)
	if err != nil {
		return spliceTCPActivity{}, err
	}
	writerAcked, writerReceived, err := tcpActivity(writerConn)
	if err != nil {
		return spliceTCPActivity{}, err
	}
	return spliceTCPActivity{
		readerAcked:    readerAcked,
		readerReceived: readerReceived,
		writerAcked:    writerAcked,
		writerReceived: writerReceived,
	}, nil
}

func spliceIdlePollInterval(timeout time.Duration) time.Duration {
	interval := timeout / 10
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	if interval > time.Minute {
		interval = time.Minute
	}
	return interval
}

// watchSpliceIdle preserves kernel zero-copy while enforcing a real idle
// timeout. The normal ActivityTimer cannot observe bytes transferred inside
// net.TCPConn.ReadFrom, so this watcher reads Linux TCP_INFO byte counters and
// closes both sockets only when neither side has moved data for the full
// timeout. Closing the sockets also guarantees that a canceled context wakes a
// blocking splice call.
func watchSpliceIdle(ctx context.Context, readerConn stdnet.Conn, writerConn stdnet.Conn, timeout time.Duration) func() {
	watchCtx, stop := context.WithCancel(context.Background())
	initial, err := spliceActivity(readerConn, writerConn)
	if err != nil {
		errors.LogWarningInner(ctx, err, "unable to start splice idle watcher")
		return stop
	}

	go func() {
		ticker := time.NewTicker(spliceIdlePollInterval(timeout))
		defer ticker.Stop()
		lastActivity := time.Now()
		previous := initial

		closeBoth := func() {
			_ = readerConn.Close()
			_ = writerConn.Close()
		}

		for {
			select {
			case <-ctx.Done():
				closeBoth()
				return
			case <-watchCtx.Done():
				return
			case now := <-ticker.C:
				current, err := spliceActivity(readerConn, writerConn)
				if err != nil {
					return
				}
				if current != previous {
					previous = current
					lastActivity = now
					continue
				}
				if now.Sub(lastActivity) >= timeout {
					errors.LogInfo(ctx, "closing idle splice connection after ", timeout)
					closeBoth()
					return
				}
			}
		}
	}()
	return stop
}
