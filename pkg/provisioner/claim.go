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

	log "github.com/hpe-storage/common-host-libs/logger"
	api_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

func (p *Provisioner) listAllClaims(options meta_v1.ListOptions) (runtime.Object, error) {
	return p.kubeClient.CoreV1().PersistentVolumeClaims(meta_v1.NamespaceAll).List(options)
}

func (p *Provisioner) watchAllClaims(options meta_v1.ListOptions) (watch.Interface, error) {
	return p.kubeClient.CoreV1().PersistentVolumeClaims(meta_v1.NamespaceAll).Watch(options)
}

//NewClaimController provides a controller that watches for PersistentVolumeClaims and takes action on them
//nolint: dupl
func (p *Provisioner) newClaimController() (cache.Store, cache.Controller) {
	claimListWatch := &cache.ListWatch{
		ListFunc:  p.listAllClaims,
		WatchFunc: p.watchAllClaims,
	}

	return cache.NewInformer(
		claimListWatch,
		&api_v1.PersistentVolumeClaim{},
		resyncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    p.addedClaim,
			UpdateFunc: p.updatedClaim,
		},
	)
}

func (p *Provisioner) addedClaim(t interface{}) {
	claim, err := getPersistentVolumeClaim(t)
	if err != nil {
		log.Errorf("Failed to get persistent volume claim from %v, %s", t, err.Error())
		return
	}
	go p.processAddedClaim(claim)
}

func (p *Provisioner) processAddedClaim(claim *api_v1.PersistentVolumeClaim) {
	// is this a state we can do anything about
	if claim.Status.Phase != api_v1.ClaimPending {
		log.Infof("pvc %s was not in pending phase.  current phase=%s - skipping", claim.Name, claim.Status.Phase)
		return
	}

	// is this a class we support
	className := getClaimClassName(claim)
	class, err := p.getClass(className)
	if err != nil {
		log.Errorf("error getting class named %s for pvc %s. err=%v", className, claim.Name, err)
		return
	}
	if !strings.HasPrefix(class.Provisioner, FlexVolumeProvisioner) {
		log.Infof("class named %s in pvc %s did not refer to a supported provisioner (name must begin with %s).  current provisioner=%s - skipping", className, claim.Name, FlexVolumeProvisioner, class.Provisioner)
		return
	}

	log.Infof("processAddedClaim: provisioner:%s pvc:%s  class:%s", class.Provisioner, claim.Name, className)
	p.addMessageChan(fmt.Sprintf("%s", claim.UID), nil)
	p.provisionVolume(claim, class)
}

func (p *Provisioner) updatedClaim(oldT interface{}, newT interface{}) {
	claim, err := getPersistentVolumeClaim(newT)
	if err != nil {
		log.Errorf("Oops - %s\n", err.Error())
		return
	}
	log.Debugf("updatedClaim: pvc %s current phase=%s", claim.Name, claim.Status.Phase)
	go p.sendUpdate(claim)
}

func getClaimClassName(claim *api_v1.PersistentVolumeClaim) (name string) {
	name, beta := claim.Annotations[api_v1.BetaStorageClassAnnotation]

	//if no longer in beta
	if !beta && claim.Spec.StorageClassName != nil {
		name = *claim.Spec.StorageClassName
	}
	return name
}

func getClaimMatchLabels(claim *api_v1.PersistentVolumeClaim) map[string]string {
	if claim.Spec.Selector == nil || claim.Spec.Selector.MatchLabels == nil {
		return map[string]string{}
	}
	return claim.Spec.Selector.MatchLabels
}

func getPersistentVolumeClaim(t interface{}) (*api_v1.PersistentVolumeClaim, error) {
	switch t := t.(type) {
	default:
		return nil, fmt.Errorf("unexpected type %T for %v", t, t)
	case *api_v1.PersistentVolumeClaim:
		return t, nil
	case api_v1.PersistentVolumeClaim:
		return &t, nil
	}
}

func (p *Provisioner) getClaimFromPVCName(nameSpace, claimName string) (*api_v1.PersistentVolumeClaim, error) {
	log.Debugf(">>>>> getClaimFromPVCNames called with %s/%s", nameSpace, claimName)
	defer log.Debug("<<<<< getClaimFromPVCName")
	if p.claimsStore == nil {
		return nil, fmt.Errorf("requested pvc %s/%s was not found because claimStore was nil", nameSpace, claimName)
	}
	if len(p.claimsStore.List()) < 1 {
		return nil, fmt.Errorf("requested pvc %s/%s was not found", nameSpace, claimName)
	}
	claimInterface, found, err := p.claimsStore.GetByKey(nameSpace + "/" + claimName)
	if err != nil {
		log.Errorf("Error to retrieve pvc %s/%s : %s", nameSpace, claimName, err.Error())
		return nil, fmt.Errorf("Error to retrieve pvc %s/%s : %s", nameSpace, claimName, err.Error())
	}
	if !found {
		log.Errorf("requested pvc %s/%s was not found", nameSpace, claimName)
		return nil, fmt.Errorf("requested pvc %s/%s was not found", nameSpace, claimName)
	}
	var claim *api_v1.PersistentVolumeClaim
	claim, err = getPersistentVolumeClaim(claimInterface)
	if err != nil {
		log.Errorf("requested pvc %s/%s was not found : %s", nameSpace, claimName, err.Error())
		return nil, fmt.Errorf("requested pvc %s/%s was not found : %s", nameSpace, claimName, err.Error())
	}
	log.Debugf("claim found namespace :%s name: %s", claim.Namespace, claim.Name)

	return claim, nil
}

func (p *Provisioner) getClaimOverrideOptions(claim *api_v1.PersistentVolumeClaim, overrides []string, optionsMap map[string]interface{}, provisioner string) (map[string]interface{}, error) {
	log.Debugf(">>>> getClaimOverrideOptions for %s", provisioner)
	defer log.Debug("<<<<< getClaimOverrideOptions")
	provisionerName := provisioner
	for _, override := range overrides {
		for key, annotation := range claim.Annotations {
			if strings.HasPrefix(strings.ToLower(key), provisionerName+"/"+strings.ToLower(override)) {
				if valOpt, ok := optionsMap[override]; ok {
					if override == "size" || override == "sizeInGiB" {
						// do not allow  override of size and sizeInGiB
						log.Debugf("override of size and sizeInGiB is not permitted, default to claim capacity of %v", valOpt)
						p.eventRecorder.Event(claim, api_v1.EventTypeNormal, "ProvisionStorage", fmt.Errorf("override of size and sizeInGiB is not permitted, default to claim capacity of %v", valOpt).Error())
						continue
					}
				}
				log.Debugf("adding key: %v with override value: %v", override, annotation)
				optionsMap[override] = annotation
			}
		}
	}

	// check to see if there is cloneOfPVC annotation present
	volumeName, err := p.getPVFromPVCAnnotation(claim, provisioner)
	if err != nil {
		return nil, err
	}

	// update the options map with the pv name if there is one
	if volumeName != "" {
		optionsMap[cloneOf] = volumeName
	}

	// make sure cloneOfPVC is removed from the options (all cases)
	if _, found := optionsMap[cloneOfPVC]; found {
		delete(optionsMap, cloneOfPVC)
	}
	return optionsMap, nil
}

func (p *Provisioner) getClaimNameSpace(claim *api_v1.PersistentVolumeClaim) string {
	nameSpace := "default"
	if claim != nil && claim.Namespace != "" {
		nameSpace = claim.Namespace
	}
	log.Debugf("namespace of the claim %s is : %s", claim.Name, nameSpace)
	return nameSpace
}

func (p *Provisioner) getPVFromPVCAnnotation(claim *api_v1.PersistentVolumeClaim, provisioner string) (string, error) {
	// check to see if we have the cloneOfPVC annotation
	pvcToClone, foundClonePVC := claim.Annotations[fmt.Sprintf("%s%s", provisioner, cloneOfPVC)]
	if !foundClonePVC {
		return "", nil
	}

	namespace := p.getClaimNameSpace(claim)
	log.Debugf("%s%s: %s was found in claim annotations", provisioner, cloneOfPVC, pvcToClone)

	pvName, err := p.getVolumeNameFromClaimName(namespace, pvcToClone)
	log.Debugf("pvc %s/%s maps to pv %s", namespace, pvcToClone, pvName)
	if err != nil {
		return "", fmt.Errorf("unable to retrieve pvc %s/%s : %s", namespace, pvcToClone, err.Error())
	}
	if pvName == "" {
		return "", fmt.Errorf("unable to retrieve pvc %s/%s : not found", namespace, pvcToClone)
	}
	return pvName, nil
}

func (p *Provisioner) checkClaimDataSorce(claim *api_v1.PersistentVolumeClaim) (bool, error) {
	log.Debugf(">>>>> checkClaimDataSorce called for PVC %s", claim.Name)
	defer log.Debug("<<<<<< checkClaimDataSorce")
	if claim.Spec.DataSource == nil {
		return false, nil
	}
	// PVC.Spec.DataSource.Name is the name of the VolumeSnapshot API object
	if claim.Spec.DataSource.Name == "" {
		return false, fmt.Errorf("the PVC source not found for PVC %s", claim.Name)
	}
	if claim.Spec.DataSource.Kind != snapshotKind {
		return false, fmt.Errorf("the PVC source is not the right type. Expected %s, Got %s", snapshotKind, claim.Spec.DataSource.Kind)
	}
	if *(claim.Spec.DataSource.APIGroup) != snapshotAPIGroup {
		return false, fmt.Errorf("the PVC source does not belong to the right APIGroup. Expected %s, Got %s", snapshotAPIGroup, *(claim.Spec.DataSource.APIGroup))
	}
	return true, nil
}
