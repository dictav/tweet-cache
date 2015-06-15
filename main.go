package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/kurrik/json"
	"github.com/kurrik/twittergo"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func connect(client *twittergo.Client, path string, query url.Values) (resp *twittergo.APIResponse, err error) {
	url := "https://stream.twitter.com" + path + "?" + query.Encode()
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		err = fmt.Errorf("Could not parse request: %v\n", err)
		return
	}

	resp, err = client.SendRequest(req)
	if err != nil {
		err = fmt.Errorf("Could not send request: %v\n", err)
		return
	}
	return
}

func readStream(
	client *twittergo.Client,
	sc streamConn,
	path string,
	query url.Values,
	resp *twittergo.APIResponse,
	handler func([]byte),
	done chan bool) {

	var reader *bufio.Reader
	reader = bufio.NewReader(resp.Body)

	for {
		//we've been closed
		if sc.isStale() {
			sc.Close()
			fmt.Println("Connection closed, shutting down ")
			break
		}

		line, err := reader.ReadBytes('\n')

		if err != nil {
			if sc.isStale() {
				fmt.Println("conn stale, continue")
				continue
			}

			time.Sleep(time.Second * time.Duration(sc.wait))
			//try reconnecting, but exponentially back off until MaxWait is reached then exit?
			resp, err := connect(client, path, query)
			if err != nil || resp == nil {
				fmt.Println(" Could not reconnect to source? sleeping and will retry ")
				if sc.wait < sc.maxWait {
					sc.wait = sc.wait * 2
				} else {
					fmt.Println("exiting, max wait reached")
					done <- true
					return
				}
				continue
			}
			if resp.StatusCode != 200 {
				fmt.Printf("resp.StatusCode = %d", resp.StatusCode)
				if sc.wait < sc.maxWait {
					sc.wait = sc.wait * 2
				}
				continue
			}

			reader = bufio.NewReader(resp.Body)
			continue
		} else if sc.wait != 1 {
			sc.wait = 1
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		handler(line)
	}
}

const maxTweetsCount = 10000

var index = make([]uint64, 0, maxTweetsCount)
var tweets = make([]string, 0, maxTweetsCount)

func indexUInt64(s []uint64, c uint64) int {
	for i, n := range s {
		if n == c {
			return i
		}
	}

	return -1
}

func filterStream(client *twittergo.Client, path string, query url.Values) (err error) {
	var (
		resp *twittergo.APIResponse
	)

	sc := newStreamConn(300)
	resp, err = connect(client, path, query)

	done := make(chan bool)
	stream := make(chan []byte, 1000)
	go func() {
		for data := range stream {
			tweet := &twittergo.Tweet{}
			err := json.Unmarshal(data, tweet)
			if err == nil {
				var n = len(index)
				if n == maxTweetsCount {
					copy(index, index[1:n])
					index = index[:n-1]
					copy(tweets, tweets[1:n])
					tweets = tweets[:n-1]
				}

				index = append(index, tweet.Id())
				tweets = append(tweets, string(data))
			}
		}
	}()

	readStream(client, sc, path, query, resp, func(line []byte) {
		stream <- line
	}, done)

	return
}

func handler(w http.ResponseWriter, r *http.Request) {
	var (
		start int
		end   int
		count int = 100
	)

	q := r.URL.Query()

	if c, err := strconv.Atoi(q.Get("count")); err == nil {
		if c > 0 && c <= 200 {
			count = c
		}
	}

	if sinceId, err := strconv.ParseUint("since_id", 10, 64); err == nil {
		if idx := indexUInt64(index, sinceId); idx >= 0 {
			start = idx
			if start+count > maxTweetsCount {
				end = maxTweetsCount
			} else {
				end = start + count
			}
		}
	} else if maxId, err := strconv.ParseUint("max_id", 10, 64); err == nil {
		fmt.Println("max_id", maxId)
		if idx := indexUInt64(index, maxId); idx >= 0 {
			fmt.Println("idx", idx)
			end = idx
			if idx > count {
				start = idx - count
			} else {
				start = 0
			}
		}
	} else {
		end = len(index)
		if end-count > 0 {
			start = end - count
		} else {
			start = 0
		}
	}

	if start >= maxTweetsCount || start >= len(tweets) {
		http.Error(w, "{error: \"Invalid parameter\"}", http.StatusBadRequest)
		return
	}

	fmt.Fprint(w, "["+strings.Join(tweets[start:end], ",")+"]")
}

type args struct {
	host string
	port int
}

func parseArgs() *args {
	a := &args{}
	flag.StringVar(&a.host, "host", "localhost", "etcd host")
	flag.IntVar(&a.port, "port", 2379, "etcd port")
	flag.Parse()
	return a
}

func main() {
	var (
		err    error
		client *twittergo.Client
	)

	args := parseArgs()
	if client, err = loadCredentials(
		"http://"+args.host+":"+strconv.Itoa(args.port),
		"/twi-at-idle/credentials"); err != nil {
		fmt.Printf("Could not parse CREDENTIALS file: %v\n", err)
		os.Exit(1)
	}

	query := url.Values{}
	query.Set("track", "AKB")

	go func() {
		fmt.Printf("=========================================================\n")
		if err = filterStream(client, "/1.1/statuses/filter.json", query); err != nil {
			fmt.Println("Error: %v\n", err)
		}
	}()

	http.HandleFunc("/", handler)
	http.ListenAndServe(":8080", nil)
}
