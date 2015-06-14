package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/kurrik/json"
	"github.com/kurrik/twittergo"
	"net/http"
	"net/url"
	"os"
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
				for i := 0; i < 100; i++ {
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
				fmt.Println("index:", len(index), cap(index))
				fmt.Println("tweets:", len(tweets), cap(tweets))
			}
		}
	}()

	readStream(client, sc, path, query, resp, func(line []byte) {
		stream <- line
	}, done)

	return
}

func handler(w http.ResponseWriter, r *http.Request) {
	var start = 100
	var length = 100
	var end = start + length
	if start >= maxTweetsCount || start >= len(tweets) {
		http.Error(w, "{error: \"Invalid parameter\"}", http.StatusBadRequest)
		return
	}

	if end > maxTweetsCount {
		end = maxTweetsCount
	}

	fmt.Fprint(w, tweets[start:end])
}

func main() {
	var (
		err    error
		client *twittergo.Client
	)
	if client, err = loadCredentials(
		"http://192.168.64.3:2379",
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
