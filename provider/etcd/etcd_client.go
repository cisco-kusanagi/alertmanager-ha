// Copyright 2019 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ErrorEtcdNotInitialized     = errors.New("Etcd not initialized")
	ErrorEtcdGetNoResult        = errors.New("etcdGet did not receive a result for fingerprint")
	ErrorEtcdGetMultipleResults = errors.New("etcdGet received multiple results for fingerprint")

	// Prometheus Counters
	etcdCheckAndPutTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "alertmanager_etcd_checkandput_total",
			Help: "The total number of CheckAndPut calls received",
		},
		[]string{"status"},
	) // "status":"filtered|accepted|error"
	etcdOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "alertmanager_etcd_operations_total",
			Help: "The total number of operations initiated to etcd",
		},
		[]string{"operation", "result", "attempt"},
	) // "operation": "get|put|delete", "result":"success|error"
	etcdWatchOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "alertmanager_etcd_watch_operations_total",
			Help: "The total number of operations received from etcd watch",
		},
		[]string{"operation"},
	) // "operation":"put|delete"
	etcdQueueLength = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "alertmanager_etcd_queue_length",
			Help: "The total number of operations pending to be processed by etcd watch",
		},
		[]string{"name"},
	)
)

type EtcdClient struct {
	alerts           *Alerts
	endpoints        []string
	prefix           string
	logger           log.Logger
	client           *clientv3.Client
	mtx              sync.Mutex
	timeoutGet       time.Duration
	timeoutPut       time.Duration
	timeoutDel       time.Duration
	operationRetries int
	retryFailureLoad time.Duration
}

func NewEtcdClient(ctx context.Context, a *Alerts, endpoints []string, prefix string, timeoutGet time.Duration, timeoutPut time.Duration, timeoutDel time.Duration, operationRetries int, retryFailureLoad time.Duration) (*EtcdClient, error) {

	ec := &EtcdClient{
		alerts:           a,
		endpoints:        endpoints,
		prefix:           prefix,
		logger:           log.With(a.logger, "component", "provider.etcd"),
		timeoutGet:       timeoutGet,
		timeoutPut:       timeoutPut,
		timeoutDel:       timeoutDel,
		operationRetries: operationRetries,
		retryFailureLoad: retryFailureLoad,
	}

	// create the configuration
	etcdConfig := clientv3.Config{
		Endpoints:        endpoints,
		AutoSyncInterval: 60 * time.Second,
		DialTimeout:      10 * time.Second,
		DialOptions:      []grpc.DialOption{grpc.WithBlock()}, // block until connect
	}

	// create the client
	client, err := clientv3.New(etcdConfig)
	if err != nil {
		// On startup, if we cannot connect to the etcd cluster, then fail hard so that the
		// user may address a potential configuration issue.  Once the clientv3 connects
		// successfully, clientv3 will reconnect to the etcd cluster as it goes down or up,
		// or into or out of network connectivity.
		level.Error(ec.logger).Log("msg", "Etcd connection failed", "err", err)
		os.Exit(1)
	} else {
		level.Info(ec.logger).Log("msg", "Etcd connection successful")
	}
	ec.mtx.Lock()
	ec.client = client
	ec.mtx.Unlock()

	// start a goroutine to ensure the client will be cleaned up when the context is done
	go func() {
		defer func() {
			ec.mtx.Lock()
			if ec.client != nil {
				ec.client.Close()
				ec.client = nil
			}
			ec.mtx.Unlock()
			level.Info(ec.logger).Log("msg", "Etcd connection shut down")
		}()

		for range ctx.Done() {
		}
	}()
	return ec, nil
}

func (ec *EtcdClient) CheckAndPut(oldAlert *types.Alert, alert *types.Alert) error {
	// Reduce writes to Etcd.  Only put to Etcd if the current alert is
	// "different" enough than the same alert in memory, as denoted by the
	// alertsShouldWriteToEtcd function.
	if !ec.alertsShouldWriteToEtcd(oldAlert, alert) {
		etcdCheckAndPutTotal.With(prometheus.Labels{"status": "filtered"}).Inc()
		return nil // skip write to etcd
	}

	etcdCheckAndPutTotal.With(prometheus.Labels{"status": "accepted"}).Inc()
	return ec.Put(alert)
}

func (ec *EtcdClient) Get(fp model.Fingerprint) (*types.Alert, error) {
	// We do a best effort.  If etcd is not initialized yet, then skip
	if ec.client == nil {
		level.Error(ec.logger).Log("msg", "Not getting alert from etcd, etcd not initialized yet")
		return nil, ErrorEtcdNotInitialized
	}

	key := ec.prefix + fp.String()
	level.Debug(ec.logger).Log("msgs", "Attempting to get alert from etcd", "key", key)

	// ensure the operation does not take too long
	ctx, cancel := context.WithTimeout(context.Background(), ec.timeoutGet)
	defer cancel()

	var err error
	var i int
	var resp *clientv3.GetResponse
	for i = 0; i < ec.operationRetries+1; i++ {
		ec.mtx.Lock()
		resp, err = ec.client.Get(ctx, key)
		ec.mtx.Unlock()
		if err != nil {
			etcdOperationsTotal.With(prometheus.Labels{"operation": "get", "result": "error", "attempt": strconv.Itoa(i)}).Inc()
			continue
		} else {
			etcdOperationsTotal.With(prometheus.Labels{"operation": "get", "result": "success", "attempt": strconv.Itoa(i)}).Inc()
			break
		}
	}
	if err != nil {
		level.Error(ec.logger).Log("msg", "Etcd get alert error", "key", key, "attempt", strconv.Itoa(i), "err", err)
		return nil, err
	} else {
		level.Debug(ec.logger).Log("msgs", "Etcd get alert success", "key", key, "attempt", strconv.Itoa(i))
	}

	if len(resp.Kvs) == 0 {
		return nil, ErrorEtcdGetNoResult
	} else if len(resp.Kvs) != 1 {
		return nil, ErrorEtcdGetMultipleResults
	}

	alert, err := UnmarshalAlert(string(resp.Kvs[0].Value))
	if err != nil {
		level.Error(ec.logger).Log("msg", "Error unmarshaling JSON Alert", "err", err)
		return nil, err
	}

	level.Debug(ec.logger).Log("msgs", "Retrieved alert from etcd", "key", key, "alert", fmt.Sprintf("%+v", alert))
	return alert, nil
}

func (ec *EtcdClient) Put(alert *types.Alert) error {
	// We do a best effort.  If etcd is not initialized yet, then skip
	if ec.client == nil {
		level.Error(ec.logger).Log("msg", "Not putting alert to etcd, etcd not initialized yet")
		return ErrorEtcdNotInitialized
	}

	key := ec.prefix + alert.Fingerprint().String()
	level.Debug(ec.logger).Log("msgs", "Attempting to put alert into etcd", "key", "key", "alert", fmt.Sprintf("%+v", alert))

	alertStr, err := MarshalAlert(alert)
	if err != nil {
		level.Error(ec.logger).Log("msg", "Error marshaling JSON Alert", "err", err)
		return err
	}

	// ensure the operation does not take too long
	ctx, cancel := context.WithTimeout(context.Background(), ec.timeoutPut)
	defer cancel()

	var i int
	for i = 0; i < ec.operationRetries+1; i++ {
		ec.mtx.Lock()
		_, err = ec.client.Put(ctx, key, alertStr)
		ec.mtx.Unlock()
		if err != nil {
			etcdOperationsTotal.With(prometheus.Labels{"operation": "put", "result": "error", "attempt": strconv.Itoa(i)}).Inc()
			continue
		} else {
			etcdOperationsTotal.With(prometheus.Labels{"operation": "put", "result": "success", "attempt": strconv.Itoa(i)}).Inc()
			break
		}
	}
	if err != nil {
		level.Error(ec.logger).Log("msg", "Etcd put alert error", "key", key, "attempt", strconv.Itoa(i), "alert", fmt.Sprintf("%+v", alert), "err", err)
		return err
	} else {
		level.Debug(ec.logger).Log("msgs", "Etcd put alert success", "key", key, "attempt", strconv.Itoa(i), "alert", fmt.Sprintf("%+v", alert))
	}
	return nil
}

func (ec *EtcdClient) Del(fp model.Fingerprint) error {
	// We do a best effort.  If etcd is not initialized yet, then skip
	if ec.client == nil {
		level.Error(ec.logger).Log("msg", "Not deleting alert from etcd, etcd not initialized yet")
		return ErrorEtcdNotInitialized
	}

	key := ec.prefix + fp.String()
	level.Debug(ec.logger).Log("msgs", "Attempting to delete alert from etcd", "key", "key")

	// ensure the operation does not take too long
	ctx, cancel := context.WithTimeout(context.Background(), ec.timeoutDel)
	defer cancel()

	var err error
	var i int
	for i = 0; i < ec.operationRetries+1; i++ {
		ec.mtx.Lock()
		_, err = ec.client.Delete(ctx, ec.prefix+fp.String())
		ec.mtx.Unlock()
		if err != nil {
			etcdOperationsTotal.With(prometheus.Labels{"operation": "del", "result": "error", "attempt": strconv.Itoa(i)}).Inc()
			continue
		} else {
			etcdOperationsTotal.With(prometheus.Labels{"operation": "del", "result": "success", "attempt": strconv.Itoa(i)}).Inc()
			break
		}
	}
	if err != nil {
		level.Error(ec.logger).Log("msg", "Etcd del alert error", "key", key, "attempt", strconv.Itoa(i), "err", err)
		return err
	} else {
		level.Debug(ec.logger).Log("msgs", "Etcd del alert success", "key", key, "attempt", strconv.Itoa(i))
	}
	return nil
}

func (ec *EtcdClient) RunWatch(ctx context.Context) {
	// watch for alert changes in etcd and writes them back to our
	// local alert state
	ctx = clientv3.WithRequireLeader(ctx)

	go func() {
		ec.mtx.Lock()
		rch := ec.client.Watch(ctx, ec.prefix, clientv3.WithPrefix())
		ec.mtx.Unlock()

		level.Info(ec.logger).Log("msg", "Etcd Watch Started")
		for wresp := range rch {
			etcdQueueLength.With(prometheus.Labels{"name": "watch"}).Set(float64(len(rch)))

			for _, ev := range wresp.Events {
				level.Debug(ec.logger).Log("msg", "Watch received",
					"type", ev.Type, "key", fmt.Sprintf("%q", ev.Kv.Key), "value", fmt.Sprintf("%q", ev.Kv.Value))
				if ev.Type.String() == "PUT" {
					etcdWatchOperationsTotal.With(prometheus.Labels{"operation": "put"}).Inc()
					alert, err := UnmarshalAlert(string(ev.Kv.Value))
					if err != nil {
						continue
					}
					if len(alert.Labels) == 0 {
						// TODO: Saw this case happen.  Unsure if it was due to someone curling against AM.
						//   For now, skip loading of this alert
						level.Warn(ec.logger).Log("msg", "Watch received Unmarshalled alert with empty LabelSet")
						continue
					}
					_ = ec.alerts.PutFromEtcd(alert) // best effort only
				} else if ev.Type.String() == "DELETE" { // ignore DELETE operations
					etcdWatchOperationsTotal.With(prometheus.Labels{"operation": "del"}).Inc()
				} // else, ignore all other etcd operations, especially DELETE
			}
		}
	}()
}

func (ec *EtcdClient) RunLoadAllAlerts(ctx context.Context) {
	go func() {
		level.Info(ec.logger).Log("msg", "Etcd Load All Alerts Started")
		count := 0
		for {
			ec.mtx.Lock()
			resp, err := ec.client.Get(ctx, ec.prefix, clientv3.WithPrefix())
			ec.mtx.Unlock()
			if err != nil {
				level.Error(ec.logger).Log("msg", "Error fetching all alerts etcd", "err", err)
				time.Sleep(ec.retryFailureLoad)
				continue // retry
			}

			for _, ev := range resp.Kvs {
				level.Debug(ec.logger).Log("msg", "Get received",
					"key", fmt.Sprintf("%q", ev.Key), "value", fmt.Sprintf("%q", ev.Value))
				alert, err := UnmarshalAlert(string(ev.Value))
				if err != nil {
					continue // retry
				}
				count += 1
				_ = ec.alerts.PutFromEtcd(alert) // best effort only
			}
			level.Info(ec.logger).Log("msg", "Etcd Load All Alerts Finished", "count", count)
			return // we only need to load all of the alerts once
		}
	}()
}

func (ec *EtcdClient) alertsShouldWriteToEtcd(a *types.Alert, o *types.Alert) bool {
	// Check if the alerts are different
	// If alerts ARE "different" then return 'true' in order to write to Etcd
	// If alerts are NOT "different" then return 'false' to skip writing to etcd

	if a == nil || o == nil {
		return true
	}
	if !reflect.DeepEqual(a.Labels, o.Labels) {
		return true
	}
	if !reflect.DeepEqual(a.Annotations, o.Annotations) {
		return true
	}
	if a.GeneratorURL != o.GeneratorURL {
		return true
	}
	if !a.StartsAt.Equal(o.StartsAt) {
		return true
	}
	// we explicitly ignore UpdatedAt
	// if !a.UpdatedAt.Equal(o.UpdatedAt) {
	// 	return true
	// }
	return a.Timeout != o.Timeout
}

func MarshalAlert(alert *types.Alert) (string, error) {
	b, err := json.Marshal(alert)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func UnmarshalAlert(alertStr string) (*types.Alert, error) {
	var alert types.Alert
	err := json.Unmarshal([]byte(alertStr), &alert)
	if err != nil {
		return nil, err
	}
	return &alert, nil
}
