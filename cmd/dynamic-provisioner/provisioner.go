/*
(c) Copyright 2017 Hewlett Packard Enterprise Development LP

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
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	log "github.com/hpe-storage/common-host-libs/logger"
	"github.com/hpe-storage/k8s-dynamic-provisioner/pkg/provisioner"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"k8s.io/klog"
)

var (
	logFilePath = "/var/log/hpe-dynamic-provisioner.log"
)

// nolint: gocyclo
func main() {

	// glog configuration control is a bit lacking (which is to say it doesn't exist),
	// so we simply hack the the value to true.
	f := flag.Lookup("logtostderr")
	if f != nil {
		f.Value.Set("true")
	}

	// needed to support grpc logging
	klog.InitFlags(nil)

	flag.Parse()
	if len(os.Args) < 1 {
		fmt.Println("Please specify the full path (including filename) to admin config file.")
		return
	}

	kubeConfig := os.Args[1]
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		fmt.Printf("Error getting config from file %s - %s\n", kubeConfig, err.Error())
		config, err = rest.InClusterConfig()
		if err != nil {
			fmt.Printf("Error getting config cluster - %s\n", err.Error())
			os.Exit(1)
		}
	}

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error getting client - %s\n", err.Error())
		os.Exit(1)
	}

	log.InitLogging(logFilePath, &log.LogParams{Level: "debug"}, true)

	stop := make(chan struct{})

	p := provisioner.NewProvisioner(
		kubeClient,
		true,
		true,
	)

	// grab the uid from the kube-system namespace and save that as the cluster ID
	kubeSystemNamespace, err := kubeClient.CoreV1().Namespaces().Get("kube-system", meta_v1.GetOptions{})
	if err != nil {
		fmt.Printf("error getting kube-system namespace - %s\n", err.Error())
		os.Exit(1)
	}
	uid := kubeSystemNamespace.GetObjectMeta().GetUID()
	p.ClusterID = string(uid)

	p.Start(stop)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGSEGV)
	go func() {
		s := <-sigc
		log.Fatalf("Exiting due to signal notification.  Signal was %v.", s.String())
		return
	}()
	select {
	case msg := <-stop:
		log.Errorf("error in provisioner: %#v", msg)
	}
	<-stop
}
