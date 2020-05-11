// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
)

type Config struct {
	ClientID     string
	ClientSecret string
	APIAddress     string
	ShardID string `config:"shard_id"`
}

// Client returns the cloudfoundry client.
func main() {
	uuid, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}

	cfg := &Config{
		APIAddress: "api_address",
		ClientID: "client_id",
		ClientSecret: "client_secret",
		ShardID: uuid.String(),
	}

	listener, err := cfg.RlpListener(RlpListenerCallbacks{
		Log: func(envelope *events.Envelope) {
			log := envelope.GetLogMessage()
			if *log.AppId == "fe1745f8-5e11-4cf1-af12-e40ccadb2267" {
				fmt.Printf("%s\n", string(log.Message))
			}
		},
	})
	if err != nil {
		panic(err)
	}

	listener.Start(context.Background())
	defer listener.Stop()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func (h *Config) Client() (*cfclient.Client, error) {
	httpClient, insecure, err := h.httpClient()
	if err != nil {
		return nil, err
	}

	cf, err := cfclient.NewClient(&cfclient.Config{
		ClientID:          h.ClientID,
		ClientSecret:      h.ClientSecret,
		ApiAddress:        h.APIAddress,
		HttpClient:        httpClient,
		SkipSslValidation: insecure,
		UserAgent:         "go/test",
	})
	if err != nil {
		return nil, errors.Wrap(err, "error creating cloudfoundry client")
	}
	return cf, nil
}

// RlpListener returns a listener client that calls the passed callback when the provided events are streamed through
// the loggregator to this client.
func (h *Config) RlpListener(callbacks RlpListenerCallbacks) (*RlpListener, error) {
	client, err := h.Client()
	if err != nil {
		return nil, err
	}
	return h.RlpListenerFromClient(client, callbacks)
}

// RlpListener returns a listener client that calls the passed callback when the provided events are streamed through
// the loggregator to this client.
//
// In the case that the cloudfoundry client was already needed by the code path, call this method
// as not to create a intermediate client that will not be used.
func (h *Config) RlpListenerFromClient(client *cfclient.Client, callbacks RlpListenerCallbacks) (*RlpListener, error) {
	rlpAddress := strings.Replace(h.APIAddress, "api", "log-stream", 1)
	doer, err := h.doerFromClient(client)
	if err != nil {
		return nil, err
	}
	return newRlpListener(rlpAddress, doer, h.ShardID, callbacks), nil
}

// doerFromClient returns an auth token doer using uaa.
func (h *Config) doerFromClient(client *cfclient.Client) (*authTokenDoer, error) {
	httpClient, _, err := h.httpClient()
	if err != nil {
		return nil, err
	}
	url := client.Endpoint.AuthEndpoint
	return newAuthTokenDoer(url, h.ClientID, h.ClientSecret, httpClient), nil
}

// httpClient returns an HTTP client configured with the configuration TLS.
func (h *Config) httpClient() (*http.Client, bool, error) {
	httpClient := cfclient.DefaultConfig().HttpClient
	tp := defaultTransport()
	httpClient.Transport = tp
	return httpClient, true, nil
}

// defaultTransport returns a new http.Transport for http.Client
func defaultTransport() *http.Transport {
	defaultTransport := http.DefaultTransport.(*http.Transport)
	return &http.Transport{
		Proxy:                 defaultTransport.Proxy,
		TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
	}
}
