package utils

import (
	"net"
	"time"
)

func TCPConnectWithTimeout(addr string) (net.Conn, error) {
	remote, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, err
	}
	return remote, nil
}

// TCPConnectWithTimeoutAndCustomTimeout connects to a TCP address with a custom timeout
func TCPConnectWithTimeoutAndCustomTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	remote, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return remote, nil
}
