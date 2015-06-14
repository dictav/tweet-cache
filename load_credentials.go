package main

import (
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"github.com/kurrik/oauth1a"
	"github.com/kurrik/twittergo"
	pathUtil "path"
)

func loadCredentials(host, path string) (client *twittergo.Client, err error) {
	machines := []string{host}
	etcdClient := etcd.NewClient(machines)

	res, err := etcdClient.Get(path, false, false)
	if err != nil {
		fmt.Println("Cannot load credentials")
		return
	}

	var consumerKey, consumerSecret, accessKey, accessSecret string
	for _, node := range res.Node.Nodes {
		switch pathUtil.Base(node.Key) {
		case "consumer_key":
			consumerKey = node.Value
		case "consumer_secret":
			consumerSecret = node.Value
		case "access_key":
			accessKey = node.Value
		case "access_secret":
			accessSecret = node.Value
		}
	}

	config := &oauth1a.ClientConfig{
		ConsumerKey:    consumerKey,
		ConsumerSecret: consumerSecret,
	}
	user := oauth1a.NewAuthorizedConfig(accessKey, accessSecret)
	client = twittergo.NewClient(config, user)
	return
}
