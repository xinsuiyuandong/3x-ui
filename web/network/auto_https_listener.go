package network

import (
	"net"
)

// AutoHttpsListener 包装 net.Listener，支持每个新连接调用回调
type AutoHttpsListener struct {
	net.Listener
	onAccept func(conn net.Conn) error // 新连接回调，用于设备限制检查
}

// NewAutoHttpsListener 构造函数，传入原始 listener 和回调
func NewAutoHttpsListener(listener net.Listener, onAccept func(conn net.Conn) error) net.Listener {
	return &AutoHttpsListener{
		Listener: listener,
		onAccept: onAccept,
	}
}

// Accept 每当有新连接进来时调用
func (l *AutoHttpsListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	// 调用回调进行设备限制检查
	if l.onAccept != nil {
		if err := l.onAccept(conn); err != nil {
			// 超过限制直接关闭连接
			conn.Close()
			return nil, err
		}
	}

	return conn, nil
}
