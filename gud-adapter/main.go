/*
Copyright 2018 The Kubernetes Authors.

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

package main

import (
	"context"
	"flag"
	//"fmt"
	"os"

	//"github.com/emicklei/go-restful"
	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/util/logs"

	statsdProvider "github.com/kubernetes-incubator/custom-metrics-apiserver/gud-adapter/provider"
	basecmd "github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/cmd"
	_ "github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
)

type Adapter struct {
	basecmd.AdapterBase

	// Message is printed on succesful startup
	Message string
}

func (a *Adapter) makeProvider() (*statsdProvider.StatsdProvider, error) {
	client := nil
	mapper := nil
	//client, err := a.DynamicClient()
	//if err != nil {
	//return nil, fmt.Errorf("unable to construct dynamic client: %v", err)
	//}

	//mapper, err := a.RESTMapper()
	//if err != nil {
	//return nil, fmt.Errorf("unable to construct discovery REST mapper: %v", err)
	//}

	provider, err := statsdProvider.NewStatsdProvider(client, mapper, ":8125")
	return provider, err
}

func main() {
	logs.InitLogs()
	defer logs.FlushLogs()

	adapter := &Adapter{}
	adapter.Flags().StringVar(&adapter.Message, "msg", "starting adapter...", "startup message")
	adapter.Flags().AddGoFlagSet(flag.CommandLine) // make sure we get the glog flags
	adapter.Flags().Parse(os.Args)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	statsd, err := adapter.makeProvider()
	if err != nil {
		glog.Fatalf("%s", err)
		return
	}

	go statsd.ListenForStatsd(ctx)

	adapter.WithCustomMetrics(statsd)
	adapter.WithExternalMetrics(statsd)

	glog.Infof(adapter.Message)

	if err := adapter.Run(wait.NeverStop); err != nil {
		glog.Fatalf("unable to run custom metrics adapter: %v", err)
	}
}
