package main

import (
	"net/http"
	"net/url"
	"sync"
)

type streamConn struct {
	resp   *http.Response
	url    *url.URL
	stale  bool
	closed bool
	mu     sync.Mutex
	// wait time before trying to reconnect, this will be
	// exponentially moved up until reaching maxWait, when
	// it will exit
	wait    int
	maxWait int
	connect func() (*http.Response, error)
}

func (conn *streamConn) Close() {
	// Just mark the connection as stale, and let the connect() handler close after a read
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.stale = true
	conn.closed = true
	if conn.resp != nil {
		conn.resp.Body.Close()
	}
}

func (conn *streamConn) isStale() bool {
	conn.mu.Lock()
	r := conn.stale
	conn.mu.Unlock()
	return r
}

func newStreamConn(max int) streamConn {
	return streamConn{wait: 1, maxWait: max}
}
