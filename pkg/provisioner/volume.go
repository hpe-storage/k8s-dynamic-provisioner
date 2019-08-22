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

package provisioner

import (
	"fmt"
	"strings"

	"github.com/hpe-storage/common-host-libs/chain"
	log "github.com/hpe-storage/common-host-libs/logger"
	api_v1 "k8s.io/api/core/v1"
	storage_v1 "k8s.io/api/storage/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/reference"
)

type volumeCreateOptions struct {
	volName        string
	classParams    map[string]string
	claim          *api_v1.PersistentVolumeClaim
	class          *storage_v1.StorageClass
	provisionChain *chain.Chain
	secretRef      *api_v1.SecretReference
	nameSpace      string
	claimID        string
}

func (p *Provisioner) listAllVolumes(options meta_v1.ListOptions) (runtime.Object, error) {
	return p.kubeClient.CoreV1().PersistentVolumes().List(options)
}

func (p *Provisioner) watchAllVolumes(options meta_v1.ListOptions) (watch.Interface, error) {
	return p.kubeClient.CoreV1().PersistentVolumes().Watch(options)
}

//NewVolumeController provides a controller that watches for PersistentVolumes and takes action on them
//nolint: dupl
func (p *Provisioner) newVolumeController() (cache.Store, cache.Controller) {
	volListWatch := &cache.ListWatch{
		ListFunc:  p.listAllVolumes,
		WatchFunc: p.watchAllVolumes,
	}

	return cache.NewInformer(
		volListWatch,
		&api_v1.PersistentVolume{},
		resyncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    p.addedVolume,
			UpdateFunc: p.updatedVolume,
			DeleteFunc: p.deletedVolume,
		},
	)

}

// nolint: dupl
func (p *Provisioner) addedVolume(t interface{}) {
	vol, err := getPersistentVolume(t)
	if err != nil {
		log.Errorf("unable to process pv add - %v,  %s", t, err.Error())
	}
	go p.processVolEvent("added", vol, true)
}

// nolint: dupl
func (p *Provisioner) updatedVolume(oldT interface{}, newT interface{}) {
	log.Debug(">>>>>> updatedVolume called")
	defer log.Debug("<<<<<< updatedVolume")
	vol, err := getPersistentVolume(newT)
	if err != nil {
		log.Errorf("unable to process pv update - %v,  %s", newT, err.Error())
	}

	go p.processVolEvent("updatedVol", vol, true)
}

// nolint: dupl
func (p *Provisioner) deletedVolume(t interface{}) {
	log.Debug(">>>> deletedVolume called")
	defer log.Debug("<<<<<< deletedVolume")
	vol, err := getPersistentVolume(t)
	if err != nil {
		log.Errorf("unable to process pv delete - %v,  %s", t, err.Error())
	}
	go p.processVolEvent("deletedVol", vol, false)
}

// We map updated and deleted events here incase we were not running when the pv state changed to Released.  If rmPV is true, we try to remove the pv object from the cluster.  If its false, we don't.
func (p *Provisioner) processVolEvent(event string, vol *api_v1.PersistentVolume, rmPV bool) {
	log.Infof(">>>>> processVolEvent called for event %s vol %s", event, vol.Name)
	defer log.Info("<<<<< processVolEvent")
	//notify the monitor
	go p.sendUpdate(vol)

	if vol.Status.Phase != api_v1.VolumeReleased || vol.Spec.PersistentVolumeReclaimPolicy != api_v1.PersistentVolumeReclaimDelete {
		log.Infof("%s event: pv:%s phase:%v (reclaim policy:%v) - skipping", event, vol.Name, vol.Status.Phase, vol.Spec.PersistentVolumeReclaimPolicy)
		return
	}
	if _, found := vol.Annotations[k8sProvisionedBy]; !found {
		log.Infof("%s event: pv:%s phase:%v (reclaim policy:%v) - missing annotation skipping", event, vol.Name, vol.Status.Phase, vol.Spec.PersistentVolumeReclaimPolicy)
		return
	}

	if !strings.HasPrefix(vol.Annotations[k8sProvisionedBy], CsiProvisioner) && !strings.HasPrefix(vol.Annotations[k8sProvisionedBy], FlexVolumeProvisioner) {
		log.Infof("%s event: pv:%s phase:%v (reclaim policy:%v) provisioner:%v - unknown provisioner skipping", event, vol.Name, vol.Status.Phase, vol.Spec.PersistentVolumeReclaimPolicy, vol.Annotations[k8sProvisionedBy])
		return
	}

	log.Debugf("%s event: cleaning up pv:%s phase:%v", event, vol.Name, vol.Status.Phase)
	p.deleteVolume(vol, rmPV)
}

func getPersistentVolume(t interface{}) (*api_v1.PersistentVolume, error) {
	switch t := t.(type) {
	default:
		return nil, fmt.Errorf("unexpected type %T for %v", t, t)
	case *api_v1.PersistentVolume:
		return t, nil
	case api_v1.PersistentVolume:
		return &t, nil
	}
}

// get the pv corresponding to this pvc and substitute with pv (docker/csi volume name)
func (p *Provisioner) getVolumeNameFromClaimName(nameSpace, claimName string) (string, error) {
	log.Debugf(">>>>> getVolumeNameFromClaimName called %s with PVC Name %s", cloneOfPVC, claimName)
	defer log.Debug("<<<< getVolumeNameFromClaimName")
	claim, err := p.getClaimFromPVCName(nameSpace, claimName)
	if err != nil {
		return "", err
	}
	if claim == nil || claim.Spec.VolumeName == "" {
		return "", fmt.Errorf("no volume found for claim %s", claimName)
	}
	return claim.Spec.VolumeName, nil
}

func (p *Provisioner) parseStorageClassParams(params map[string]string, class *storage_v1.StorageClass, claimSizeinGiB int, listOfOptions []string, nameSpace string) (map[string]interface{}, error) {
	log.Debug(">>>> parseStorageClassParams called")
	defer log.Debug("<<<< parseStorageClassParams")
	volCreateOpts := make(map[string]interface{}, len(params))
	foundSizeKey := false
	for key, value := range params {
		if key == cloneOfPVC {
			pvName, err := p.getVolumeNameFromClaimName(nameSpace, value)
			if err != nil {
				log.Errorf("Error to retrieve pvc %s/%s : %s return existing options", nameSpace, value, err.Error())
				p.eventRecorder.Event(class, api_v1.EventTypeWarning, "ProvisionStorage",
					fmt.Sprintf("Error to retrieve pvc %s/%s : %s", nameSpace, value, err.Error()))
				return nil, err
			}
			log.Debugf("setting key : cloneOf value : %v", pvName)
			volCreateOpts["cloneOf"] = pvName
			continue
		}
		volCreateOpts[key] = value
		if claimSizeinGiB > 0 && contains(listOfOptions, key) {
			foundSizeKey = true
			for _, option := range listOfOptions {
				if key == option {
					log.Infof("storageclass option matched storage resource option:%s ,overriding the value to %d", key, claimSizeinGiB)
					volCreateOpts[key] = claimSizeinGiB
					break
				}
			}
		}
	}
	if claimSizeinGiB > 0 && !foundSizeKey {
		log.Debug("storage class does not contain size key, overriding to claim size")
		volCreateOpts["size"] = claimSizeinGiB
	}
	return volCreateOpts, nil
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (p *Provisioner) newPersistentVolume(pvName string, params map[string]string, claim *api_v1.PersistentVolumeClaim, class *storage_v1.StorageClass) (*api_v1.PersistentVolume, error) {
	claimRef, err := reference.GetReference(scheme.Scheme, claim)
	if err != nil {
		log.Errorf("unable to get reference for claim %v. %s", claim, err)
		return nil, err
	}

	claimName := getClaimClassName(claim)
	if class.Parameters == nil {
		class.Parameters = make(map[string]string)
	}
	class.Parameters["name"] = pvName

	if class.ReclaimPolicy == nil {
		class.ReclaimPolicy = new(api_v1.PersistentVolumeReclaimPolicy)
		*class.ReclaimPolicy = api_v1.PersistentVolumeReclaimDelete
	}

	pv := &api_v1.PersistentVolume{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      pvName,
			Namespace: claim.Namespace,
			Labels:    getClaimMatchLabels(claim),
			Annotations: map[string]string{
				"volume.beta.kubernetes.io/storage-class": claimName,
				k8sProvisionedBy: class.Provisioner,
			},
		},
		Spec: api_v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: *class.ReclaimPolicy,
			AccessModes:                   claim.Spec.AccessModes,
			VolumeMode:                    claim.Spec.VolumeMode,
			ClaimRef:                      claimRef,
			StorageClassName:              claimName,
			MountOptions:                  class.MountOptions,
			Capacity: api_v1.ResourceList{
				api_v1.ResourceName(api_v1.ResourceStorage): claim.Spec.Resources.Requests[api_v1.ResourceName(api_v1.ResourceStorage)],
			},
		},
	}
	return pv, nil
}

func (p *Provisioner) newFlexVolPersistentVolume(pvName string, params map[string]string, claim *api_v1.PersistentVolumeClaim, class *storage_v1.StorageClass) (*api_v1.PersistentVolume, error) {
	pv, err := p.newPersistentVolume(pvName, params, claim, class)
	if err != nil {
		return nil, err
	}
	pv.ObjectMeta.Annotations[p.dockerVolNameAnnotation] = pvName
	pv.Spec.PersistentVolumeSource = api_v1.PersistentVolumeSource{
		FlexVolume: &api_v1.FlexPersistentVolumeSource{
			Driver:  class.Provisioner,
			Options: params,
		},
	}
	return pv, nil

}

func (p *Provisioner) newCsiPersistentVolume(options *volumeCreateOptions) (*api_v1.PersistentVolume, error) {
	pv, err := p.newPersistentVolume(options.volName, options.classParams, options.claim, options.class)
	if err != nil {
		return nil, err
	}
	pv.Spec.PersistentVolumeSource = api_v1.PersistentVolumeSource{
		CSI: &api_v1.CSIPersistentVolumeSource{
			Driver:                     options.class.Provisioner,
			VolumeAttributes:           options.classParams,
			ControllerPublishSecretRef: options.secretRef,
			NodeStageSecretRef:         options.secretRef,
			NodePublishSecretRef:       options.secretRef,
		},
	}
	// TODO: does fstype need to be added here. If added to csi plugin create request?
	return pv, nil
}
