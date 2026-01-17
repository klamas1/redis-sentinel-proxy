package proxy

import (
	"context"
	"io"
	"log"
	"net"
	"strings"

	"github.com/klamas1/redis-sentinel-proxy/pkg/utils"
	"golang.org/x/sync/errgroup"
)

type resolver interface {
	MasterAddress() string
	ReplicaAddress() string
	DecrementConn(addr string)
}

type RedisSentinelProxy struct {
	localAddr *net.TCPAddr
	resolver  resolver
	mode      string // "master" or "replica"
	debug     bool
}

func NewRedisSentinelProxy(localAddr *net.TCPAddr, r resolver, mode string, debug bool) *RedisSentinelProxy {
	return &RedisSentinelProxy{
		localAddr: localAddr,
		resolver:  r,
		mode:      mode,
		debug:     debug,
	}
}

func (r *RedisSentinelProxy) Run(bigCtx context.Context) error {
	listener, err := net.ListenTCP("tcp", r.localAddr)
	if err != nil {
		return err
	}

	errGr, ctx := errgroup.WithContext(bigCtx)
	errGr.Go(func() error { return r.runListenLoop(ctx, listener) })
	errGr.Go(func() error { return closeListenerByContext(ctx, listener) })

	return errGr.Wait()
}

func (r *RedisSentinelProxy) runListenLoop(ctx context.Context, listener *net.TCPListener) error {
	log.Println("Waiting for connections for proxy", strings.ToUpper(r.mode), "on", listener.Addr().String())
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Println(err)
			continue
		}

		if r.debug {
			log.Printf("[DEBUG] New client connection from %s to %s proxy", conn.RemoteAddr().String(), r.mode)
		}

		go r.proxy(conn)
	}
}

func (r *RedisSentinelProxy) proxy(incoming io.ReadWriteCloser) {
	defer incoming.Close()
	var remoteAddr string
	if r.mode == "master" {
		remoteAddr = r.resolver.MasterAddress()
	} else {
		remoteAddr = r.resolver.ReplicaAddress()
	}
	if remoteAddr == "" {
		log.Printf("[ERROR] Proxy request to %s: no upstream address available", r.mode)
		return
	}
	if r.debug {
		log.Printf("[DEBUG] Proxy request: connecting to %s", remoteAddr)
	}
	remote, err := utils.TCPConnectWithTimeout(remoteAddr)
	if err != nil {
		log.Printf("Proxy error: failed to connect to %s: %s", remoteAddr, err)
		return
	}
	defer r.resolver.DecrementConn(remoteAddr)

	sigChan := make(chan struct{})
	defer close(sigChan)

	go pipe(incoming, remote, sigChan)
	go pipe(remote, incoming, sigChan)

	<-sigChan
	<-sigChan
}

func closeListenerByContext(ctx context.Context, listener *net.TCPListener) error {
	defer listener.Close()
	<-ctx.Done()
	return nil
}

func pipe(w io.WriteCloser, r io.Reader, sigChan chan<- struct{}) {
	defer func() { sigChan <- struct{}{} }()
	defer w.Close()

	if _, err := io.Copy(w, r); err != nil && err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("Error writing content: %s", err)
	}
}
