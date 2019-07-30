// Copyright 2019 Hewlett Packard Enterprise Development LP

package provisioner

import (
	"context"
	"encoding/json"
	"fmt"

	csi_spec "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/hpe-storage/common-host-libs/model"
	"github.com/hpe-storage/common-host-libs/util"
	api_v1 "k8s.io/api/core/v1"
	storage_v1 "k8s.io/api/storage/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	csi_v1_alpha1 "k8s.io/csi-api/pkg/apis/csi/v1alpha1"
)

const (
	defaultFSType      = "xfs"
	nodeIDAnnotation   = "csi.volume.kubernetes.io/nodeid"
	vaNodeIDAnnotation = "csi.alpha.kubernetes.io/node-id"
)

func (p *Provisioner) listAllVolumesAttachments(options meta_v1.ListOptions) (runtime.Object, error) {
	return p.kubeClient.StorageV1().VolumeAttachments().List(options)
}
func (p *Provisioner) watchAllVolumeAttachments(options meta_v1.ListOptions) (watch.Interface, error) {
	return p.kubeClient.StorageV1().VolumeAttachments().Watch(options)
}

//NewVolumeAttachmentController provides a controller that watches for VolumeAttachment and takes action on them
// nolint: dupl
func (p *Provisioner) newVolumeAttachmentController() (cache.Store, cache.Controller) {
	VAListWatch := &cache.ListWatch{
		ListFunc:  p.listAllVolumesAttachments,
		WatchFunc: p.watchAllVolumeAttachments,
	}

	return cache.NewInformer(
		VAListWatch,
		&storage_v1.VolumeAttachment{},
		resyncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    p.addedVolumeAttachments,
			UpdateFunc: p.updatedVolumeAttachments,
			DeleteFunc: p.deletedVolumeAttachments,
		},
	)
}

func (p *Provisioner) addedVolumeAttachments(t interface{}) {
	va, err := getVolumeAttachment(t)
	if err != nil {
		util.LogError.Printf("unable to process va add - %v,  %s", t, err.Error())
	}
	go p.processVAAddEvent(va)
}

// nolint: dupl
func (p *Provisioner) updatedVolumeAttachments(oldT interface{}, newT interface{}) {
	va, err := getVolumeAttachment(newT)
	if err != nil {
		util.LogError.Printf("unable to process va update - %v,  %s", newT, err.Error())
	}
	go p.processVAUpdateEvent(va)
}

// nolint: dupl
func (p *Provisioner) deletedVolumeAttachments(t interface{}) {
	va, err := getVolumeAttachment(t)
	if err != nil {
		util.LogError.Printf("unable to process va delete - %v,  %s", t, err.Error())
	}
	go p.processVADeleteEvent(va)
}

// nolint: dupl
func getVolumeAttachment(t interface{}) (*storage_v1.VolumeAttachment, error) {
	switch t := t.(type) {
	default:
		return nil, fmt.Errorf("unexpected type %T for %v", t, t)
	case *storage_v1.VolumeAttachment:
		return t, nil
	case storage_v1.VolumeAttachment:
		return &t, nil
	}
}

func (p *Provisioner) processVAUpdateEvent(va *storage_v1.VolumeAttachment) error {
	util.LogInfo.Printf(">>>>>> processVAUpdateEvent called")
	defer util.LogInfo.Printf("<<<<<< processVAUpdateEvent")
	//TODO : check for updates
	return nil
}

func (p *Provisioner) processVADeleteEvent(va *storage_v1.VolumeAttachment) error {
	util.LogInfo.Printf(">>>>>> processVADeleteEvent called")
	defer util.LogInfo.Printf("<<<<<< processVADeleteEvent")
	err := p.processDetach(va)
	if err != nil {
		util.LogDebug.Printf("err=%s", err.Error())
		return nil
	}
	util.LogDebug.Printf("deleting va %s", va.Name)
	err = p.kubeClient.StorageV1().VolumeAttachments().Delete(va.Name, &meta_v1.DeleteOptions{})
	if err != nil {
		util.LogError.Printf(err.Error())
		return nil
	}
	return nil
}

func (p *Provisioner) processVAAddEvent(va *storage_v1.VolumeAttachment) error {
	util.LogInfo.Printf(">>>>>> processVAAddEvent called")
	defer util.LogInfo.Printf("<<<<<< processVAAddEvent")
	// if the VA is already in deletion state, we can't process it for addition
	if va.DeletionTimestamp != nil {
		return fmt.Errorf("va %s DeletionTimestamp is set to %v, ignoring Attach", va.Name, va.DeletionTimestamp)
	}
	util.LogInfo.Printf("va %s has been created for attach.  current phase=%v", va.Name, va.Status)
	err := p.processAttach(va)
	if err != nil {
		util.LogDebug.Printf("err=%s", err.Error())
		return err
	}
	return nil
}

func (p *Provisioner) processDetach(va *storage_v1.VolumeAttachment) error {
	util.LogInfo.Printf(">>>>>> processDetach called")
	defer util.LogInfo.Printf("<<<<<< processDetach")
	va, err := p.executeDetach(va)
	if err != nil {
		return err
	}
	util.LogDebug.Printf("va %s after detach is %#v", va.Name, va)
	return nil
}

func (p *Provisioner) processAttach(va *storage_v1.VolumeAttachment) error {
	util.LogInfo.Printf(">>>>>> processAttach called")
	defer util.LogInfo.Printf("<<<<<< processAttach")
	if va.Status.Attached {
		// volume is attached nothing to do
		return nil
	}
	// executeAttach  volumeattachment attach workflow
	va, metadata, err := p.executeAttach(va)
	if err != nil {
		util.LogDebug.Printf("err=%s", err.Error())
		return err
	}
	util.LogInfo.Printf("Attached %q", va.Name)
	// update VA Status
	if _, err := p.markAsAttached(va, metadata); err != nil {
		return fmt.Errorf("failed to mark va %s as attached: %s", va.Name, err.Error())
	}
	util.LogInfo.Printf("Fully attached %q", va.Name)
	return nil
}

func (p *Provisioner) getPVFromVA(va *storage_v1.VolumeAttachment) (*api_v1.PersistentVolume, error) {
	util.LogDebug.Printf(">>>>>> getPVFromVA called")
	defer util.LogDebug.Printf("<<<<<< getPVFromVA")
	pvName := *va.Spec.Source.PersistentVolumeName
	pvInterface, found, err := p.pvStore.GetByKey(pvName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("unable to find pv %s", pvName)
	}
	pv, err := getPersistentVolume(pvInterface)
	if err != nil {
		return nil, err
	}
	return pv, nil
}

func (p *Provisioner) executeDetach(va *storage_v1.VolumeAttachment) (*storage_v1.VolumeAttachment, error) {
	util.LogInfo.Printf(">>>>>> executeDetach called")
	defer util.LogInfo.Printf("<<<<<< executeDetach")
	if va.Spec.Source.PersistentVolumeName == nil {
		return va, fmt.Errorf("VolumeAttachment.spec.persistentVolumeName is empty")
	}

	pv, err := p.getPVFromVA(va)
	if err != nil {
		return nil, err
	}

	if pv.Spec.CSI == nil {
		return va, fmt.Errorf("could not retrieve CSI Spec from PersistentVolume %s", pv.Name)
	}

	secrets, err := p.getCredentials(pv.Spec.CSI.ControllerPublishSecretRef)
	if err != nil {
		return va, err
	}

	nodeID, err := p.getNodeID(va)
	if err != nil {
		return va, err
	}

	nodeInfo, err := p.getNodeInfoFromHPENodeInfo(nodeID)
	if err != nil {
		return va, err
	}

	controllerUnpublishReq := &csi_spec.ControllerUnpublishVolumeRequest{
		VolumeId: pv.Spec.CSI.VolumeHandle,
		NodeId:   nodeInfo,
		Secrets:  secrets,
	}

	util.LogDebug.Printf("ControllerUnpublishVolumeRequest for volume %s = %#v", pv.Name, controllerUnpublishReq)

	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()
	_, err = p.csiDriverClient.ControllerUnpublishVolume(ctx, controllerUnpublishReq)
	if err != nil {
		return va, err
	}

	// update to remove attached status
	if va, err = p.markAsDetached(va); err != nil {
		return va, fmt.Errorf("could not mark as detached: %s", err)
	}

	util.LogInfo.Printf("va %s is Detached", va.Name)
	return va, nil
}

// nolint : gocyclo
func (p *Provisioner) executeAttach(va *storage_v1.VolumeAttachment) (*storage_v1.VolumeAttachment, map[string]string, error) {
	util.LogDebug.Printf(">>>>>> executeAttach called")
	defer util.LogDebug.Printf("<<<<<< executeAttach")
	if va.Spec.Source.PersistentVolumeName == nil {
		return va, nil, fmt.Errorf("VolumeAttachment.Spec.Source.PersistentVolumeName is empty")
	}
	pv, err := p.getPVFromVA(va)
	if err != nil {
		return va, nil, err
	}
	// Refuse to attach volumes that are marked for deletion.
	if pv.DeletionTimestamp != nil {
		return va, nil, fmt.Errorf("PersistentVolume %q is marked for deletion", pv.Name)
	}
	//TODO: do we need to check/add finalizers?
	if pv.Spec.CSI == nil {
		return va, nil, fmt.Errorf("could not retrieve CSI Spec from PersistentVolume %s", pv.Name)
	}

	secrets, err := p.getCredentials(pv.Spec.CSI.ControllerPublishSecretRef)
	if err != nil {
		return va, nil, err
	}

	nodeID, err := p.getNodeID(va)
	if err != nil {
		return va, nil, err
	}

	nodeInfo, err := p.getNodeInfoFromHPENodeInfo(nodeID)
	if err != nil {
		return va, nil, err
	}

	// Validate PV access modes specified in the request
	if err := p.validatePvAccessModes(pv.Spec.AccessModes); err != nil {
		return va, nil, fmt.Errorf("Failed to validate PV access modes, err: %v", err.Error())
	}

	// Get volume capability from PV
	volCap, err := p.getVolumeCapabililty(pv)
	if err != nil {
		return va, nil, err
	}
	util.LogDebug.Printf("getVolumeCapabililty = %#v", volCap)

	controllerPublishReq := &csi_spec.ControllerPublishVolumeRequest{
		VolumeId:         pv.Spec.CSI.VolumeHandle,
		NodeId:           nodeInfo,
		VolumeCapability: volCap,
		Readonly:         pv.Spec.CSI.ReadOnly,
		Secrets:          secrets,
		VolumeContext:    pv.Spec.CSI.VolumeAttributes,
	}

	util.LogDebug.Printf("ControllerPublishVolumeRequest for volume %s = %#v", pv.Name, controllerPublishReq)

	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()
	publishInfoRsp, err := p.csiDriverClient.ControllerPublishVolume(ctx, controllerPublishReq)

	if err != nil {
		return va, nil, err
	}
	util.LogInfo.Printf("ControllerPublishVolumeResponse for volume %s is %#v", pv.Name, publishInfoRsp)
	if publishInfoRsp.PublishContext == nil {
		return va, nil, fmt.Errorf("unable to retrieve PublishContext from ControllePublishVolume for %s", pv.Name)
	}
	return va, publishInfoRsp.PublishContext, nil
}

// If the mode is Block, return true otherwise return false.
func isPvModeBlock(pv *api_v1.PersistentVolume) bool {
	return pv.Spec.VolumeMode != nil && *pv.Spec.VolumeMode == api_v1.PersistentVolumeBlock
}

func (p *Provisioner) getVolumeCapabililty(pv *api_v1.PersistentVolume) (*csi_spec.VolumeCapability, error) {
	util.LogDebug.Printf(">>>>>> getVolumeCapabililty called")
	defer util.LogDebug.Printf("<<<<<< getVolumeCapabililty")

	// If block access type, then return block volume capability
	if isPvModeBlock(pv) {
		return &csi_spec.VolumeCapability{
			AccessType: p.getAccessTypeBlock(),
			AccessMode: &csi_spec.VolumeCapability_AccessMode{
				Mode: csi_spec.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, // ReadWriteOnce
			},
		}, nil
	}
	// Else mount access type, then return mount access capability
	fsType := pv.Spec.CSI.FSType
	if fsType == "" {
		util.LogDebug.Print("Using default filesystem: ", defaultFSType)
		fsType = defaultFSType
	}
	return &csi_spec.VolumeCapability{
		AccessType: p.getAccessTypeMount(fsType, pv.Spec.MountOptions),
		AccessMode: &csi_spec.VolumeCapability_AccessMode{
			Mode: csi_spec.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, // ReadWriteOnce
		},
	}, nil
}

// getNodeIDFromNode returns nodeID string from node annotations.
func (p *Provisioner) getNodeIDFromNode(node *api_v1.Node) (string, error) {
	util.LogDebug.Printf(">>>>>> getNodeIDFromNode")
	defer util.LogDebug.Printf("<<<<<< getNodeIDFromNode")
	nodeIDJSON, ok := node.Annotations[nodeIDAnnotation]
	if !ok {
		return "", fmt.Errorf("node %q has no NodeID annotation", node.Name)
	}

	var nodeIDs map[string]string
	if err := json.Unmarshal([]byte(nodeIDJSON), &nodeIDs); err != nil {
		return "", fmt.Errorf("cannot parse NodeID annotation on node %q: %s", node.Name, err)
	}
	nodeID, ok := nodeIDs[CsiProvisioner]
	if !ok {
		return "", fmt.Errorf("cannot find NodeID for driver %q for node %q", CsiProvisioner, node.Name)
	}

	return nodeID, nil
}

func (p *Provisioner) getNodeID(va *storage_v1.VolumeAttachment) (string, error) {
	util.LogDebug.Printf(">>>>>> getNodeID")
	defer util.LogDebug.Printf("<<<<<< getNodeID")
	if va.Spec.NodeName == "" {
		return "", fmt.Errorf("no NodeName present in VA Spec for %s", va.Name)
	}
	//TODO: check if a non alpha version is out
	nodeInfo, err := p.csiClient.CsiV1alpha1().CSINodeInfos().Get(va.Spec.NodeName, meta_v1.GetOptions{})
	if err == nil {
		if nodeID, found := p.getNodeIDFromNodeInfo(nodeInfo); found {
			util.LogInfo.Printf("Found NodeID %s in CSINodeInfo %s", nodeID, va.Spec.NodeName)
			return nodeID, nil
		}
		// Fall through to Node annotation.
	} else {
		// Can't get CSINodeInfo, Maybe because of feature gate disabled for CSINodeInfos, fall through to Node annotation.
		util.LogInfo.Printf("Can't get CSINodeInfo %s: %s", va.Spec.NodeName, err.Error())
	}

	// Check Node annotation if it contains the node-id which matches the VA Node name
	node, err := p.kubeClient.CoreV1().Nodes().Get(va.Spec.NodeName, meta_v1.GetOptions{})
	if err == nil {
		util.LogDebug.Printf("retrieved Node %#v for VA %s", node, va.Name)
		return p.getNodeIDFromNode(node)
	}

	// last resort is to check if we added vaNodeIDAnnotation to the VA
	if va == nil {
		return "", err
	}
	if nodeID, found := va.Annotations[vaNodeIDAnnotation]; found {
		return nodeID, nil
	}
	// exhausted ways to retrieve nodeID , return error
	return "", err
}

// GetNodeIDFromNodeInfo returns nodeID from CSIDriverInfoSpec
func (p *Provisioner) getNodeIDFromNodeInfo(nodeInfo *csi_v1_alpha1.CSINodeInfo) (string, bool) {
	for _, d := range nodeInfo.Spec.Drivers {
		if d.Name == CsiProvisioner {
			return d.NodeID, true
		}
	}
	return "", false
}

func (p *Provisioner) getNodeInfoFromHPENodeInfo(nodeID string) (string, error) {
	util.LogDebug.Printf(">>>>>> getNodeInfoFromHPENodeInfo from node ID %s", nodeID)
	defer util.LogDebug.Printf("<<<<<< getNodeInfoFromHPENodeInfo")

	nodeInfoList, err := p.crdClient.StorageV1().HPENodeInfos().List(meta_v1.ListOptions{})
	if err != nil {
		return "", err
	}
	util.LogDebug.Printf("Found the following HPE Node Info objects: %v", nodeInfoList)

	for _, nodeInfo := range nodeInfoList.Items {
		util.LogDebug.Printf("Processing node info %v", nodeInfo)

		if nodeInfo.Spec.UUID == nodeID {
			iqns := make([]*string, len(nodeInfo.Spec.IQNs))
			for i := range iqns {
				iqns[i] = &nodeInfo.Spec.IQNs[i]
			}
			networks := make([]*string, len(nodeInfo.Spec.Networks))
			for i := range networks {
				networks[i] = &nodeInfo.Spec.Networks[i]
			}
			wwpns := make([]*string, len(nodeInfo.Spec.WWPNs))
			for i := range wwpns {
				wwpns[i] = &nodeInfo.Spec.WWPNs[i]
			}
			node := &model.Node{
				Name:     nodeInfo.ObjectMeta.Name,
				UUID:     nodeInfo.Spec.UUID,
				Iqns:     iqns,
				Networks: networks,
				Wwpns:    wwpns,
			}

			nodeBytes, err := json.Marshal(node)
			if err != nil {
				return "", err
			}
			return string(nodeBytes), nil
		}
	}

	return "", fmt.Errorf("failed to get node with id %s", nodeID)
}

func (p *Provisioner) markAsAttached(va *storage_v1.VolumeAttachment, metadata map[string]string) (*storage_v1.VolumeAttachment, error) {
	util.LogInfo.Printf(">>>>>> markAsAttached called")
	defer util.LogDebug.Printf("<<<<<<< markAsAttached")
	clone := va.DeepCopy()
	clone.Status.Attached = true
	clone.Status.AttachmentMetadata = metadata
	clone.Status.AttachError = nil
	// volumeattachment/status create/update capability is needed
	newVA, err := p.kubeClient.StorageV1().VolumeAttachments().UpdateStatus(clone)
	if err != nil {
		util.LogError.Printf("err = %s", err.Error())
		return va, err
	}
	util.LogInfo.Printf("Marked as attached %q Status :%v", newVA.Name, newVA.Status)
	return newVA, nil
}

func (p *Provisioner) markAsDetached(va *storage_v1.VolumeAttachment) (*storage_v1.VolumeAttachment, error) {
	util.LogInfo.Printf(">>>>>> markAsDetached called")
	defer util.LogDebug.Printf("<<<<<<< markAsDetached")
	// get the latest state of the va before updating. It could be Detached by the time it reaches here
	updatedVA, _ := p.kubeClient.StorageV1().VolumeAttachments().Get(va.Name, meta_v1.GetOptions{})
	if updatedVA == nil {
		// already detached
		return va, nil
	}
	if !updatedVA.Status.Attached {
		util.LogInfo.Printf("Already fully detached %q", va.Name)
		return va, nil
	}
	util.LogDebug.Printf("Marking %q as detached", va.Name)
	clone := updatedVA.DeepCopy()
	clone.Status.Attached = false
	clone.Status.AttachmentMetadata = nil
	clone.Status.AttachError = nil
	newVA, err := p.kubeClient.StorageV1().VolumeAttachments().UpdateStatus(clone)
	if err != nil {
		return newVA, err
	}
	return newVA, nil
}
