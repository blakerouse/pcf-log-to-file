// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package main

import (
	"context"
	"github.com/cloudfoundry/sonde-go/events"
	"sync"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/conversion"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

type RlpListenerCallbacks struct {
	HttpAccess      func(*events.Envelope)
	Log             func(*events.Envelope)
	Counter         func(*events.Envelope)
	ValueMetric     func(*events.Envelope)
	ContainerMetric func(*events.Envelope)
	Error           func(*events.Envelope)
}

// RlpListener is a listener client that connects to the cloudfoundry loggregator.
type RlpListener struct {
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	rlpAddress string
	doer       *authTokenDoer
	shardID    string
	callbacks  RlpListenerCallbacks
}

// newRlpListener returns default implementation for RLPClient
func newRlpListener(
	rlpAddress string,
	doer *authTokenDoer,
	shardID string,
	callbacks RlpListenerCallbacks) *RlpListener {
	return &RlpListener{
		rlpAddress: rlpAddress,
		doer:       doer,
		shardID:    shardID,
		callbacks:  callbacks,
	}
}

// Start receiving events through from loggregator.
func (c *RlpListener) Start(ctx context.Context) {
	ops := []loggregator.RLPGatewayClientOption{loggregator.WithRLPGatewayHTTPClient(c.doer)}
	rlpClient := loggregator.NewRLPGatewayClient(c.rlpAddress, ops...)

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	l := &loggregator_v2.EgressBatchRequest{
		ShardId:   c.shardID,
		Selectors: c.getSelectors(),
	}
	es := rlpClient.Stream(ctx, l)

	go func() {
		c.wg.Add(1)
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				envelopes := es()
				for i := range envelopes {
					v1s := conversion.ToV1(envelopes[i])
					for _, v := range v1s {
						switch *v.EventType {
						case events.Envelope_HttpStartStop:
							c.callbacks.HttpAccess(v)
						case events.Envelope_LogMessage:
							c.callbacks.Log(v)
						case events.Envelope_CounterEvent:
							c.callbacks.Counter(v)
						case events.Envelope_ValueMetric:
							c.callbacks.ValueMetric(v)
						case events.Envelope_ContainerMetric:
							c.callbacks.ContainerMetric(v)
						case events.Envelope_Error:
							c.callbacks.Error(v)
						}
					}
				}
			}
		}
	}()
}

// Stop receiving events
func (c *RlpListener) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

// getSelectors returns the server side selectors based on the callbacks defined on the listener.
func (c *RlpListener) getSelectors() []*loggregator_v2.Selector {
	selectors := make([]*loggregator_v2.Selector, 0)
	if c.callbacks.HttpAccess != nil {
		selectors = append(selectors, &loggregator_v2.Selector{
			Message: &loggregator_v2.Selector_Timer{
				Timer: &loggregator_v2.TimerSelector{},
			},
		})
	}
	if c.callbacks.Log != nil {
		selectors = append(selectors, &loggregator_v2.Selector{
			Message: &loggregator_v2.Selector_Log{
				Log: &loggregator_v2.LogSelector{},
			},
		})
	}
	if c.callbacks.Counter != nil {
		selectors = append(selectors, &loggregator_v2.Selector{
			Message: &loggregator_v2.Selector_Counter{
				Counter: &loggregator_v2.CounterSelector{},
			},
		})
	}
	if c.callbacks.ValueMetric != nil || c.callbacks.ContainerMetric != nil {
		selectors = append(selectors, &loggregator_v2.Selector{
			Message: &loggregator_v2.Selector_Gauge{
				Gauge: &loggregator_v2.GaugeSelector{},
			},
		})
	}
	if c.callbacks.Error != nil {
		selectors = append(selectors, &loggregator_v2.Selector{
			Message: &loggregator_v2.Selector_Event{
				Event: &loggregator_v2.EventSelector{},
			},
		})
	}
	return selectors
}
