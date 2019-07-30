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

	csi_spec "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/hpe-storage/common-host-libs/util"
	crd_client "github.com/hpe-storage/k8s-custom-resources/pkg/client/clientset/versioned"
	"github.com/hpe-storage/k8s-dynamic-provisioner/pkg/provisioner"
	snap_client "github.com/kubernetes-csi/external-snapshotter/pkg/client/clientset/versioned"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	csi_client "k8s.io/csi-api/pkg/client/clientset/versioned"
	"k8s.io/klog"
)

var (
	// init to empty as we always want to get this from env variable
	csiEndpoint = flag.String("endpoint", "", "Address of the CSI driver socket.")
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

	// csiClient needed for volumeattachment workflows
	csiClient, err := csi_client.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error getting csi client - %s\n", err.Error())
		os.Exit(1)
	}

	crdClient, err := crd_client.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error getting crd client - %s\n", err.Error())
		os.Exit(1)
	}

	snapshotClient, err := snap_client.NewForConfig(config)
	if err != nil {
		fmt.Printf("Failed to create snapshot client: %v", err)
		os.Exit(1)
	}

	// set the csiEndpoint
	provisioner.CsiEndpoint = *csiEndpoint
	var csiDriverClient csi_spec.ControllerClient

	// instantiate a csiDriverClient only if csiEndpoint is not empty
	if provisioner.CsiEndpoint != "" {
		csiDriverClient, err = provisioner.GetCsiDriverClient()
		if err != nil {
			util.LogError.Printf("Error getting csi driver client for %s - %s", provisioner.CsiProvisioner, err.Error())
			os.Exit(1)
		}
	}

	util.OpenLog(true)

	stop := make(chan struct{})

	p := provisioner.NewProvisioner(
		kubeClient,
		csiClient,
		csiDriverClient,
		crdClient,
		snapshotClient,
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
		util.LogError.Fatalf("Exiting due to signal notification.  Signal was %v.", s.String())
		return
	}()
	select {
	case msg := <-stop:
		util.LogError.Printf("error in provisioner: %#v", msg)
	}
	<-stop
}
