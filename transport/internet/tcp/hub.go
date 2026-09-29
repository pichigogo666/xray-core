package tcp

import (
	"context"
	gotls "crypto/tls"
	"strings"
	"time"

	goreality "github.com/xtls/reality"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

// Listener is an internet.Listener that listens for TCP connections.
type Listener struct {
	listener      net.Listener
	tlsConfig     *gotls.Config
	realityConfig *goreality.Config
	authConfig    internet.ConnectionAuthenticator
	config        *Config
	addConn       internet.ConnHandler
}

// ListenTCP creates a new Listener based on configurations.
func ListenTCP(ctx context.Context, address net.Address, port net.Port, streamSettings *internet.MemoryStreamConfig, handler internet.ConnHandler) (internet.Listener, error) {
	l := &Listener{
		addConn: handler,
	}
	tcpSettings := streamSettings.ProtocolSettings.(*Config)
	l.config = tcpSettings
	if l.config != nil {
		if streamSettings.SocketSettings == nil {
			streamSettings.SocketSettings = &internet.SocketConfig{}
		}
		streamSettings.SocketSettings.AcceptProxyProtocol = l.config.AcceptProxyProtocol || streamSettings.SocketSettings.AcceptProxyProtocol
	}
	var listener net.Listener
	var err error
	if port == net.Port(0) { // unix
		if !address.Family().IsDomain() {
			return nil, errors.New("invalid unix listen: ", address).AtError()
		}
		listener, err = internet.ListenSystem(ctx, &net.UnixAddr{
			Name: address.Domain(),
			Net:  "unix",
		}, streamSettings.SocketSettings)
		if err != nil {
			return nil, errors.New("failed to listen Unix Domain Socket on ", address).Base(err)
		}
		errors.LogInfo(ctx, "listening Unix Domain Socket on ", address)
	} else {
		listener, err = internet.ListenSystem(ctx, &net.TCPAddr{
			IP:   address.IP(),
			Port: int(port),
		}, streamSettings.SocketSettings)
		if err != nil {
			return nil, errors.New("failed to listen TCP on ", address, ":", port).Base(err)
		}
		errors.LogInfo(ctx, "listening TCP on ", address, ":", port)
	}

	if streamSettings.TcpmaskManager != nil {
		listener, _ = streamSettings.TcpmaskManager.WrapListener(listener)
	}

	if streamSettings.SocketSettings != nil && streamSettings.SocketSettings.AcceptProxyProtocol {
		errors.LogWarning(ctx, "accepting PROXY protocol")
	}

	l.listener = listener

	if config := tls.ConfigFromStreamSettings(streamSettings); config != nil {
		l.tlsConfig = config.GetTLSConfig()
	}
	if config := reality.ConfigFromStreamSettings(streamSettings); config != nil {
		l.realityConfig = config.GetREALITYConfig()
		go goreality.DetectPostHandshakeRecordsLens(l.realityConfig)
	}

	if tcpSettings.HeaderSettings != nil {
		headerConfig, err := tcpSettings.HeaderSettings.GetInstance()
		if err != nil {
			return nil, errors.New("invalid header settings").Base(err).AtError()
		}
		auth, err := internet.CreateConnectionAuthenticator(headerConfig)
		if err != nil {
			return nil, errors.New("invalid header settings.").Base(err).AtError()
		}
		l.authConfig = auth
	}

	go l.keepAccepting()
	return l, nil
}

func (v *Listener) keepAccepting() {
	for {
		conn, err := v.listener.Accept()
		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "closed") {
				break
			}
			errors.LogWarningInner(context.Background(), err, "failed to accepted raw connections")
			if strings.Contains(errStr, "too many") {
				time.Sleep(time.Millisecond * 500)
			}
			continue
		}

		go func() {
			if v.tlsConfig != nil {
				conn = tls.Server(conn, v.tlsConfig)
			} else if v.realityConfig != nil {
				// REALITY.Server performs the authenticated handshake and also
				// forwards unauthenticated traffic to the camouflage target. A peer
				// that keeps either path silent can otherwise hold both connections
				// forever because it has not reached the normal inbound policy yet.
				rawConn := conn
				handshakeTimeout := time.AfterFunc(10*time.Minute, func() {
					_ = rawConn.Close()
				})
				if err = rawConn.SetDeadline(time.Now().Add(10 * time.Minute)); err != nil {
					handshakeTimeout.Stop()
					_ = rawConn.Close()
					errors.LogInfo(context.Background(), "failed to set REALITY handshake deadline: ", err)
					return
				}
				if conn, err = reality.Server(conn, v.realityConfig); err != nil {
					handshakeTimeout.Stop()
					_ = rawConn.Close()
					errors.LogInfo(context.Background(), err.Error())
					return
				}
				if !handshakeTimeout.Stop() {
					_ = conn.Close()
					return
				}
				if err = conn.SetDeadline(time.Time{}); err != nil {
					_ = conn.Close()
					errors.LogInfo(context.Background(), "failed to clear REALITY handshake deadline: ", err)
					return
				}
			}
			if v.authConfig != nil {
				conn = v.authConfig.Server(conn)
			}
			v.addConn(stat.Connection(conn))
		}()
	}
}

// Addr implements internet.Listener.Addr.
func (v *Listener) Addr() net.Addr {
	return v.listener.Addr()
}

// Close implements internet.Listener.Close.
func (v *Listener) Close() error {
	return v.listener.Close()
}

func init() {
	common.Must(internet.RegisterTransportListener(protocolName, ListenTCP))
}
