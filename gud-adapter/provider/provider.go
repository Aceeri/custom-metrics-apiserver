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

	apierr "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	//"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/metrics/pkg/apis/custom_metrics"
	"k8s.io/metrics/pkg/apis/external_metrics"

	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider/helpers"
)

const serviceExpiry = 30 * time.Second

// CustomMetricResource wraps provider.CustomMetricInfo in a struct which stores the Name and Namespace of the resource
// So that we can accurately store and retrieve the metric as if this were an actual metrics server.
type CustomMetricResource struct {
	provider.CustomMetricInfo
	types.NamespacedName
}

// externalMetric provides examples for metrics which would otherwise be reported from an external source
// TODO (damemi): add dynamic external metrics instead of just hardcoded examples
type externalMetric struct {
	info   provider.ExternalMetricInfo
	labels map[string]string
	value  external_metrics.ExternalMetricValue
}

// testingProvider is a sample implementation of provider.MetricsProvider which stores a map of fake metrics
type StatsdProvider struct {
	client dynamic.Interface
	mapper apimeta.RESTMapper

	listener *net.UDPConn

	valuesLock sync.RWMutex
	values     map[string]ServiceData

	externalMetrics []externalMetric
}

type ServiceData struct {
	// Mapping between service name -> service instance.
	Services map[string]ServiceInstance
}

func NewServiceData() ServiceData {
	return ServiceData{
		Services: make(map[string]ServiceInstance),
	}
}

func (data *ServiceData) Count(name string) int {
	data.Clean()

	count := 0
	for ident, instance := range data.Services {
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
		values:   make(map[string]ServiceData),
	}

	return provider, nil
}

// updateMetric writes the metric provided by a restful request and stores it in memory
//func (p *testingProvider) updateMetric(request *restful.Request, response *restful.Response) {
//p.valuesLock.Lock()
//defer p.valuesLock.Unlock()

//namespace := request.PathParameter("namespace")
//resourceType := request.PathParameter("resourceType")
//namespaced := false
//if len(namespace) > 0 || resourceType == "namespaces" {
//namespaced = true
//}
//name := request.PathParameter("name")
//metricName := request.PathParameter("metric")

//value := new(resource.Quantity)
//err := request.ReadEntity(value)
//if err != nil {
//response.WriteErrorString(http.StatusBadRequest, err.Error())
//return
//}

//groupResource := schema.ParseGroupResource(resourceType)

//info := provider.CustomMetricInfo{
//GroupResource: groupResource,
//Metric:        metricName,
//Namespaced:    namespaced,
//}

//info, _, err = info.Normalized(p.mapper)
//if err != nil {
//glog.Errorf("Error normalizing info: %s", err)
//}
//namespacedName := types.NamespacedName{
//Name:      name,
//Namespace: namespace,
//}

//metricInfo := CustomMetricResource{
//CustomMetricInfo: info,
//NamespacedName:   namespacedName,
//}
//p.values[metricInfo] = *value
//}

// valueFor is a helper function to get just the value of a specific metric
//func (p *StatsdProvider) valueFor(info provider.CustomMetricInfo, name types.NamespacedName) (resource.Quantity, error) {
//info, _, err := info.Normalized(p.mapper)
//if err != nil {
//return resource.Quantity{}, err
//}
//metricInfo := CustomMetricResource{
//CustomMetricInfo: info,
//NamespacedName:   name,
//}

//value, found := p.values[metricInfo]
//if !found {
//return resource.Quantity{}, provider.NewMetricNotFoundForError(info.GroupResource, info.Metric, name.Name)
//}

//return value, nil
//}

// metricFor is a helper function which formats a value, metric, and object info into a MetricValue which can be returned by the metrics API
//func (p *StatsdProvider) metricFor(value resource.Quantity, name types.NamespacedName, info provider.CustomMetricInfo) (*custom_metrics.MetricValue, error) {
//objRef, err := helpers.ReferenceFor(p.mapper, name, info)
//if err != nil {
//return nil, err
//}

//return &custom_metrics.MetricValue{
//DescribedObject: objRef,
//MetricName:      info.Metric,
//Timestamp:       metav1.Time{time.Now()},
//Value:           value,
//}, nil
//}

// metricsFor is a wrapper used by GetMetricBySelector to format several metrics which match a resource selector
//func (p *StatsdProvider) metricsFor(namespace string, selector labels.Selector, info provider.CustomMetricInfo) (*custom_metrics.MetricValueList, error) {
//names, err := helpers.ListObjectNames(p.mapper, p.client, namespace, selector, info)
//if err != nil {
//return nil, err
//}

//res := make([]custom_metrics.MetricValue, 0, len(names))
//for _, name := range names {
//namespacedName := types.NamespacedName{Name: name, Namespace: namespace}
//value, err := p.valueFor(info, namespacedName)
//if err != nil {
//if apierr.IsNotFound(err) {
//continue
//}
//return nil, err
//}

//metric, err := p.metricFor(value, namespacedName, info)
//if err != nil {
//return nil, err
//}
//res = append(res, *metric)
//}

//return &custom_metrics.MetricValueList{
//Items: res,
//}, nil
//}

func (p *StatsdProvider) GetMetricByName(name types.NamespacedName, info provider.CustomMetricInfo) (*custom_metrics.MetricValue, error) {
	p.valuesLock.RLock()
	defer p.valuesLock.RUnlock()

	return nil, nil
}

func (p *StatsdProvider) GetMetricBySelector(namespace string, selector labels.Selector, info provider.CustomMetricInfo) (*custom_metrics.MetricValueList, error) {
	p.valuesLock.RLock()
	defer p.valuesLock.RUnlock()

	return nil, nil
}

func (p *StatsdProvider) ListAllMetrics() []provider.CustomMetricInfo {
	p.valuesLock.RLock()
	defer p.valuesLock.RUnlock()

	// Get unique CustomMetricInfos from wrapper CustomMetricResources
	infos := make(map[provider.CustomMetricInfo]struct{})
	for resource := range p.values {
		infos[resource.CustomMetricInfo] = struct{}{}
	}

	// Build slice of CustomMetricInfos to be returns
	metrics := make([]provider.CustomMetricInfo, 0, len(infos))
	for info := range infos {
		metrics = append(metrics, info)
	}

	return metrics
}

func (p *StatsdProvider) GetExternalMetric(namespace string, metricSelector labels.Selector, info provider.ExternalMetricInfo) (*external_metrics.ExternalMetricValueList, error) {
	p.valuesLock.RLock()
	defer p.valuesLock.RUnlock()

	matchingMetrics := []external_metrics.ExternalMetricValue{}
	for _, metric := range p.externalMetrics {
		if metric.info.Metric == info.Metric &&
			metricSelector.Matches(labels.Set(metric.labels)) {
			metricValue := metric.value
			metricValue.Timestamp = metav1.Now()
			matchingMetrics = append(matchingMetrics, metricValue)
		}
	}
	return &external_metrics.ExternalMetricValueList{
		Items: matchingMetrics,
	}, nil
}

func (p *StatsdProvider) ListAllExternalMetrics() []provider.ExternalMetricInfo {
	p.valuesLock.RLock()
	defer p.valuesLock.RUnlock()

	externalMetricsInfo := []provider.ExternalMetricInfo{}
	for _, metric := range p.externalMetrics {
		externalMetricsInfo = append(externalMetricsInfo, metric.info)
	}
	return externalMetricsInfo
}
