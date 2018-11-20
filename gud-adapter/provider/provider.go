/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package provider

import (
	"net"
	"sync"
	"time"

	//"github.com/golang/glog"

	//apierr "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	//"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/metrics/pkg/apis/custom_metrics"
	//"k8s.io/metrics/pkg/apis/external_metrics"

	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
	//"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider/helpers"
)

const serviceExpiry = 30 * time.Second

// CustomMetricResource wraps provider.CustomMetricInfo in a struct which stores the Name and Namespace of the resource
// So that we can accurately store and retrieve the metric as if this were an actual metrics server.
type CustomMetricResource struct {
	provider.CustomMetricInfo
	types.NamespacedName
}

// testingProvider is a sample implementation of provider.MetricsProvider which stores a map of fake metrics
type StatsdProvider struct {
	client dynamic.Interface
	mapper apimeta.RESTMapper

	listener *net.UDPConn

	servicesLock sync.RWMutex
	// Mapping between service name -> service queue data.
	services map[string]*ServiceData
}

func NewStatsdProvider(client dynamic.Interface, mapper apimeta.RESTMapper, address string) (*StatsdProvider, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	provider := &StatsdProvider{
		client:   client,
		mapper:   mapper,
		listener: udpConn,
		services: make(map[string]*ServiceData),
	}

	return provider, nil
}

func (statsd *StatsdProvider) Count(name string) int64 {
	data, ok := p.services[name]
	if ok {
		return data.Count()
	}

	return 0
}

func (statsd *StatsdProvider) AddServiceDepth(depth ServiceDepth) {
	statsd.servicesLock.Lock()
	defer statsd.servicesLock.Unlock()

	data, ok := statsd.services[depth.Name]
	if !ok {
		data = NewServiceData()
		statsd.services[depth.Name] = data
	}

	data.AddServiceDepth(depth)
}

type ServiceData struct {
	// Mapping between service ident -> service instance.
	Services map[string]ServiceInstance
}

func NewServiceData() *ServiceData {
	return &ServiceData{
		Services: make(map[string]ServiceInstance),
	}
}

func (data *ServiceData) Count() int64 {
	data.Clean()

	var count int64
	for _, instance := range data.Services {
		if time.Now().Sub(instance.Timestamp) < serviceExpiry {
			count += instance.Data.Depth
		}
	}

	return count
}

// Clean removes any instances that stopped responding.
func (data *ServiceData) Clean() {
	for ident, instance := range data.Services {
		if time.Now().Sub(instance.Timestamp) > serviceExpiry {
			delete(data.Services, ident)
		}
	}
}

func (data *ServiceData) AddServiceDepth(depth ServiceDepth) {
	data.Services[depth.Ident] = NewServiceInstance(depth)
}

type ServiceInstance struct {
	Timestamp time.Time
	Data      ServiceDepth
}

func NewServiceInstance(data ServiceDepth) ServiceInstance {
	return ServiceInstance{
		Timestamp: time.Now(),
		Data:      data,
	}
}

func (p *StatsdProvider) GetMetricByName(name types.NamespacedName, info provider.CustomMetricInfo) (*custom_metrics.MetricValue, error) {
	p.servicesLock.RLock()
	defer p.servicesLock.RUnlock()

	count := p.Count(info.Metric)

	return &custom_metrics.MetricValue{
		MetricName: info.Metric,
		Timestamp:  metav1.Time{time.Now()},
		Value:      *resource.NewQuantity(count, resource.DecimalSI),
	}, nil
}

func (p *StatsdProvider) GetMetricBySelector(namespace string, selector labels.Selector, info provider.CustomMetricInfo) (*custom_metrics.MetricValueList, error) {
	p.servicesLock.RLock()
	defer p.servicesLock.RUnlock()

	count := p.Count(info.Metric)

	metric := custom_metrics.MetricValue{
		MetricName: info.Metric,
		Timestamp:  metav1.Time{time.Now()},
		Value:      *resource.NewQuantity(count, resource.DecimalSI),
	}, nil

	return &custom_metrics.MetricValueList{
		Items: []custom_metrics.MetricValue{metric},
	}, nil
}

func (p *StatsdProvider) ListAllMetrics() []provider.CustomMetricInfo {
	p.servicesLock.RLock()
	defer p.servicesLock.RUnlock()

	metrics := make([]provider.CustomMetricInfo, 0, len(p.services))
	for metricName := range p.services {
		metrics = append(metrics, provider.CustomMetricInfo{
			Metric: metricName,
		})
	}

	return metrics
}
