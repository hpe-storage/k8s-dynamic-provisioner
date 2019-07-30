// Copyright 2019 Hewlett Packard Enterprise Development LP

package provisioner

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"time"

	csi_spec "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/connection"
	"github.com/kubernetes-csi/csi-lib-utils/rpc"

	"github.com/hpe-storage/common-host-libs/util"
	api_v1 "k8s.io/api/core/v1"
	storage_v1 "k8s.io/api/storage/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	csiParameterPrefix   = "csi.storage.k8s.io/"
	csiFsTypeParameter   = csiParameterPrefix + "fstype"
	tokenPVNameKey       = "pv.name"
	tokenPVCNameKey      = "pvc.name"
	tokenPVCNameSpaceKey = "pvc.namespace"
	secretNameKey        = csiParameterPrefix + "secret-name"
	secretNamespaceKey   = csiParameterPrefix + "secret-namespace"
	clusterIDKey         = "clusterId"
	csiTimeout           = 10 * time.Second
)

var (
	// We support only 'ReadWriteOnce' in DoryD and CSI driver for now.
	defaultVolAccessModes = []*csi_spec.VolumeCapability_AccessMode{
		&csi_spec.VolumeCapability_AccessMode{
			Mode: csi_spec.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
		},
		&csi_spec.VolumeCapability_AccessMode{
			Mode: csi_spec.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}

	// CsiEndpoint is the endpoint
	CsiEndpoint string
)

type createCsiVol struct {
	requestedName        string
	createVolumeResponse *csi_spec.CreateVolumeResponse
	deleteVolumeResponse *csi_spec.DeleteVolumeResponse
	pvc                  *api_v1.PersistentVolumeClaim
	pv                   *api_v1.PersistentVolume
	createRequest        *csi_spec.CreateVolumeRequest
	deleteRequest        *csi_spec.DeleteVolumeRequest
	client               csi_spec.ControllerClient
}

type deleteCsiVol struct {
	requestedName        string
	deleteVolumeResponse *csi_spec.DeleteVolumeResponse
	deleteRequest        *csi_spec.DeleteVolumeRequest
	client               csi_spec.ControllerClient
}

type secretParamsMap struct {
	name               string
	secretNameKey      string
	secretNamespaceKey string
}

var (
	csiSecretParams = secretParamsMap{
		name:               "Provisioner",
		secretNameKey:      secretNameKey,
		secretNamespaceKey: secretNamespaceKey,
	}
)

func (c createCsiVol) Name() string {
	return reflect.TypeOf(c).Name()
}

func (c *createCsiVol) Run() (name interface{}, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()
	c.createVolumeResponse, err = c.client.CreateVolume(ctx, c.createRequest)
	if err != nil {
		util.LogError.Printf("failed to create csi volume %s, error=%s", c.requestedName, err.Error())
		return nil, err
	}
	util.LogInfo.Printf("created csi volume %v", c.createVolumeResponse)
	name, ok := c.createVolumeResponse.Volume.VolumeContext["name"]
	if !ok {
		return nil, fmt.Errorf("unable to retrieve the name from csi volume %v", c.createVolumeResponse)
	}
	if c.pv.Spec.PersistentVolumeSource.CSI.VolumeAttributes != nil {
		//copy the volumeContext into VolumeAttributes of CSIPersistentVolumeSource
		c.pv.Spec.PersistentVolumeSource.CSI.VolumeAttributes = c.createVolumeResponse.Volume.VolumeContext
	}
	// set the CSI.VolumeHandle which is needed for PV deletes
	c.pv.Spec.PersistentVolumeSource.CSI.VolumeHandle = c.createVolumeResponse.Volume.VolumeId

	// If Mount volume, set the filesystem details
	if !isPvcModeBlock(c.pvc) {
		fstype, ok := c.createRequest.Parameters[csiFsTypeParameter]
		if !ok {
			// the fstype parameter is not present, default to xfs
			util.LogDebug.Printf("%s is not set. Setting the fstype to %s", csiFsTypeParameter, defaultFSType)
			fstype = defaultFSType
		}
		c.pv.Spec.PersistentVolumeSource.CSI.FSType = fstype
	}

	// check if the response size is greater or equal to the requested size.
	// If smaller size, then delete the volume
	volSizeBytes := c.createRequest.CapacityRange.RequiredBytes
	respCap := c.createVolumeResponse.GetVolume().GetCapacityBytes()
	if respCap < volSizeBytes {
		capErr := fmt.Errorf("created volume capacity %v less than requested capacity %v", respCap, volSizeBytes)
		// Log error and invoke volume delete
		util.LogError.Printf("Volume %s with id %s created with insufficient size. So, deleting it",
			c.createRequest.GetName(), c.createVolumeResponse.GetVolume().GetVolumeId())
		delReq := &csi_spec.DeleteVolumeRequest{
			VolumeId: c.createVolumeResponse.GetVolume().GetVolumeId(),
			Secrets:  c.createRequest.Secrets,
		}
		_, err = c.client.DeleteVolume(ctx, delReq)
		if err != nil {
			capErr = fmt.Errorf("%v. Cleanup of volume %s failed, volume is orphaned: %v", capErr, name, err)
		}
		return nil, capErr
	}

	return name, err
}

func (c *createCsiVol) Rollback() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()
	if c.createVolumeResponse != nil && c.createVolumeResponse.Volume.VolumeId != "" {
		c.deleteRequest = &csi_spec.DeleteVolumeRequest{VolumeId: c.createVolumeResponse.Volume.VolumeId}
		c.deleteVolumeResponse, err = c.client.DeleteVolume(ctx, c.deleteRequest)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *deleteCsiVol) Name() string {
	return reflect.TypeOf(d).Name()
}
func (d *deleteCsiVol) Run() (name interface{}, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()
	d.deleteVolumeResponse, err = d.client.DeleteVolume(ctx, d.deleteRequest)
	if err != nil {
		util.LogError.Printf("failed to delete csi volume %s, error=%s", d.requestedName, err.Error())
	}
	return nil, err
}
func (d *deleteCsiVol) Rollback() (err error) {
	// noop
	return nil
}

// GetCsiDriverClient : get the csiDriverClient
func GetCsiDriverClient() (csi_spec.ControllerClient, error) {
	util.LogDebug.Print(">>>> GetCsiDriverClient called")
	defer util.LogDebug.Print("<<<<<< GetCsiDriverClient")
	csiConn, err := connection.Connect(CsiEndpoint)
	if err != nil {
		util.LogError.Printf("unable to connect to CsiEndpoint %s err=%s", CsiEndpoint, err.Error())
		return nil, err
	}

	err = rpc.ProbeForever(csiConn, csiTimeout)
	if err != nil {
		util.LogError.Printf(err.Error())
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()
	// Find driver name and validate it is csiProvisioner
	csiDriver, err := rpc.GetDriverName(ctx, csiConn)
	if err != nil {
		util.LogError.Printf(err.Error())
		return nil, err
	}
	if csiDriver != CsiProvisioner {
		return nil, fmt.Errorf("got CSI driver name: %s, expecting: %s. Failing request", csiDriver, CsiProvisioner)
	}
	csiClient := csi_spec.NewControllerClient(csiConn)
	return csiClient, nil
}

// ported from https://github.com/kubernetes-csi/external-provisioner/blob/master/pkg/controller/controller.go
//nolint: gocyclo
func getSecretReference(secretParams secretParamsMap, storageClassParams map[string]string, pvName string, pvc *api_v1.PersistentVolumeClaim) (*api_v1.SecretReference, error) {
	nameTemplate, namespaceTemplate, err := verifyAndGetSecretNameAndNamespaceTemplate(secretParams, storageClassParams)
	if err != nil {
		return nil, fmt.Errorf("failed to get name and namespace template from params: %v", err)
	}

	ref := &api_v1.SecretReference{}
	{
		// Secret namespace template can make use of the PV name or the PVC namespace.
		// Note that neither of those things are under the control of the PVC user.
		namespaceParams := map[string]string{tokenPVNameKey: pvName}
		if pvc != nil {
			namespaceParams[tokenPVCNameSpaceKey] = pvc.Namespace
		}

		resolvedNamespace, err := resolveTemplate(namespaceTemplate, namespaceParams)
		if err != nil {
			return nil, fmt.Errorf("error resolving value %q: %v", namespaceTemplate, err)
		}
		if len(validation.IsDNS1123Label(resolvedNamespace)) > 0 {
			if namespaceTemplate != resolvedNamespace {
				return nil, fmt.Errorf("%q resolved to %q which is not a valid namespace name", namespaceTemplate, resolvedNamespace)
			}
			return nil, fmt.Errorf("%q is not a valid namespace name", namespaceTemplate)
		}
		ref.Namespace = resolvedNamespace
	}

	{
		// Secret name template can make use of the PV name, PVC name or namespace, or a PVC annotation.
		// Note that PVC name and annotations are under the PVC user's control.
		nameParams := map[string]string{tokenPVNameKey: pvName}
		if pvc != nil {
			nameParams[tokenPVCNameKey] = pvc.Name
			nameParams[tokenPVCNameSpaceKey] = pvc.Namespace
			for k, v := range pvc.Annotations {
				nameParams["pvc.annotations['"+k+"']"] = v
			}
		}
		resolvedName, err := resolveTemplate(nameTemplate, nameParams)
		if err != nil {
			return nil, fmt.Errorf("error resolving value %q: %v", nameTemplate, err)
		}
		if len(validation.IsDNS1123Subdomain(resolvedName)) > 0 {
			if nameTemplate != resolvedName {
				return nil, fmt.Errorf("%q resolved to %q which is not a valid secret name", nameTemplate, resolvedName)
			}
			return nil, fmt.Errorf("%q is not a valid secret name", nameTemplate)
		}
		ref.Name = resolvedName
	}

	return ref, nil
}

// ported from https://github.com/kubernetes-csi/external-provisioner/blob/master/pkg/controller/controller.go
func resolveTemplate(template string, params map[string]string) (string, error) {
	missingParams := sets.NewString()
	resolved := os.Expand(template, func(k string) string {
		v, ok := params[k]
		if !ok {
			missingParams.Insert(k)
		}
		return v
	})
	if missingParams.Len() > 0 {
		return "", fmt.Errorf("invalid tokens: %q", missingParams.List())
	}
	return resolved, nil
}

// ported from https://github.com/kubernetes-csi/external-provisioner/blob/master/pkg/controller/controller.go
func verifyAndGetSecretNameAndNamespaceTemplate(secret secretParamsMap, storageClassParams map[string]string) (nameTemplate, namespaceTemplate string, err error) {
	numName := 0
	numNamespace := 0

	if t, ok := storageClassParams[secret.secretNameKey]; ok {
		nameTemplate = t
		numName++
	}
	if t, ok := storageClassParams[secret.secretNamespaceKey]; ok {
		namespaceTemplate = t
		numNamespace++
	}

	if numName > 1 || numNamespace > 1 {
		// Double specified error
		return "", "", fmt.Errorf("%s secrets specified in parameters with both \"csi\" and \"%s\" keys", secret.name, csiParameterPrefix)
	} else if numName != numNamespace {
		// Not both 0 or both 1
		return "", "", fmt.Errorf("either name and namespace for %s secrets specified, Both must be specified", secret.name)
	} else if numName == 1 {
		// Case where we've found a name and a namespace template
		if nameTemplate == "" || namespaceTemplate == "" {
			return "", "", fmt.Errorf("%s secrets specified in parameters but value of either namespace or name is empty", secret.name)
		}
		return nameTemplate, namespaceTemplate, nil
	}
	// No secrets specified
	return "", "", fmt.Errorf("no secret and secret namespace specified in storage class parameters")
}

// If the mode is Block, return true otherwise return false.
func isPvcModeBlock(claim *api_v1.PersistentVolumeClaim) bool {
	return claim.Spec.VolumeMode != nil && *claim.Spec.VolumeMode == api_v1.PersistentVolumeBlock
}

func (p *Provisioner) getCredentials(ref *api_v1.SecretReference) (map[string]string, error) {
	util.LogDebug.Printf(">>>>>>>> getCredentials called")
	defer util.LogDebug.Printf("<<<<<<< getCredentials")
	if ref == nil {
		return nil, nil
	}
	secret, err := p.kubeClient.CoreV1().Secrets(ref.Namespace).Get(ref.Name, meta_v1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting secret %s in namespace %s: %v", ref.Name, ref.Namespace, err)
	}
	credentials := map[string]string{}
	for key, value := range secret.Data {
		credentials[key] = string(value)
	}
	return credentials, nil
}

func (p *Provisioner) getAccessTypeBlock() *csi_spec.VolumeCapability_Block {
	return &csi_spec.VolumeCapability_Block{
		Block: &csi_spec.VolumeCapability_BlockVolume{},
	}
}

func (p *Provisioner) getAccessTypeMount(fsType string, mountFlags []string) *csi_spec.VolumeCapability_Mount {
	return &csi_spec.VolumeCapability_Mount{
		Mount: &csi_spec.VolumeCapability_MountVolume{
			FsType:     fsType,
			MountFlags: mountFlags,
		},
	}
}

// getVolumeCapabilities constructs volume capabilities with appropriate volume access type
func (p *Provisioner) getVolumeCapabilities(class *storage_v1.StorageClass, claim *api_v1.PersistentVolumeClaim) ([]*csi_spec.VolumeCapability, error) {
	util.LogDebug.Printf(">>>>> getVolumeCapabilities, claim: %+v", claim)
	defer util.LogDebug.Printf("<<<<<<< getVolumeCapabilities")

	volumeCaps := make([]*csi_spec.VolumeCapability, 0)

	// Check if block access claim
	if isPvcModeBlock(claim) {
		util.LogDebug.Print("Requested for Block volume access")

		// Set access type as 'block' for each supported access mode
		for _, accessMode := range defaultVolAccessModes {
			volumeCaps = append(volumeCaps, &csi_spec.VolumeCapability{
				AccessType: p.getAccessTypeBlock(), // set block type
				AccessMode: accessMode,
			})
		}
		return volumeCaps, nil
	}

	// Else, Filesystem/Mount access claim
	util.LogDebug.Print("Requested for Mount volume access")

	// Get the filesystem type if specified, else use default
	fsType := ""
	if val, ok := class.Parameters[csiFsTypeParameter]; ok {
		fsType = val
	}
	if fsType == "" {
		util.LogDebug.Print("Using default filesystem: ", defaultFSType)
		fsType = defaultFSType
	}

	// Set access type as 'mount' for each supported access mode
	for _, accessMode := range defaultVolAccessModes {
		volumeCaps = append(volumeCaps, &csi_spec.VolumeCapability{
			AccessType: p.getAccessTypeMount(fsType, class.MountOptions), // set mount type
			AccessMode: accessMode,
		})
	}
	return volumeCaps, nil
}

// Check if all PV access modes specified in the PVC are supported.
func (p *Provisioner) validatePvAccessModes(pvAccessModes []api_v1.PersistentVolumeAccessMode) error {
	util.LogDebug.Printf(">>>>> validatePvAccessModes, pvAccessModes: %v", pvAccessModes)
	defer util.LogDebug.Printf("<<<<< validatePvAccessModes")

	// Only 'ReadWriteOnly' is supported by the CSI driver for now
	for _, pvAccessMode := range pvAccessModes {
		if pvAccessMode != api_v1.ReadWriteOnce {
			/* TODO: With multi-writer feature, we may enable other access modes like:
			   - api_v1.ReadWriteMany
			   - api_v1.ReadOnlyMany
			*/
			util.LogError.Printf("Found unsupported PV AccessMode: %v", pvAccessMode)
			return fmt.Errorf("Found unsupported PV AccessMode '%v'", pvAccessMode)
		}
	}
	return nil
}
func (p *Provisioner) buildCsiVolumeCreateRequest(volName string, class *storage_v1.StorageClass, claim *api_v1.PersistentVolumeClaim, csiCredentials map[string]string) (*csi_spec.CreateVolumeRequest, error) {
	util.LogDebug.Printf(">>>>> buildCsiVolumeCreateRequest for %s", volName)
	defer util.LogDebug.Print("<<<<< buildCsiVolumeCreateRequest")

	request := &csi_spec.CreateVolumeRequest{}
	request.Name = volName

	// Secrets
	if csiCredentials == nil {
		util.LogError.Printf("Missing secrets in the request for %s", volName)
		return nil, fmt.Errorf("empty credentials retrieved. Failing create request for %s", volName)
	}
	request.Secrets = csiCredentials

	// Parameters
	if class.Parameters == nil {
		util.LogError.Printf("Missing class arameters in the request for %s", volName)
		return nil, fmt.Errorf("class parameters not present. Failing create request for %s", volName)
	}
	request.Parameters = class.Parameters

	// Add cluster ID to the volume request
	request.Parameters[clusterIDKey] = p.ClusterID

	// Configure the volume size
	capacity := claim.Spec.Resources.Requests[api_v1.ResourceName(api_v1.ResourceStorage)]
	volSizeBytes := capacity.Value()
	request.CapacityRange = &csi_spec.CapacityRange{
		RequiredBytes: int64(volSizeBytes),
	}

	// Validate PV access modes specified in the request
	if err := p.validatePvAccessModes(claim.Spec.AccessModes); err != nil {
		return nil, fmt.Errorf("Failed to validate PV access modes, err: %v", err.Error())
	}

	// Add volume capabilities
	volumeCaps, err := p.getVolumeCapabilities(class, claim)
	if err != nil {
		util.LogError.Printf("Failed while building the volume capabilities for %s, err: %v", volName, err.Error())
		return nil, fmt.Errorf("Unable to build volume capabilities. Failing create request for %s", volName)
	}
	request.VolumeCapabilities = volumeCaps
	util.LogDebug.Printf("CSI volume create request: %+v", request)

	return request, nil
}

func (p *Provisioner) buildCsiVolumeDeleteRequest(pv *api_v1.PersistentVolume, csiCredentials map[string]string, className string) (*csi_spec.DeleteVolumeRequest, error) {
	util.LogDebug.Printf(">>>>> getCsiVolumeDeleteRequest for pv %s", pv.Name)
	defer util.LogDebug.Print("<<<<< getCsiVolumeDeleteRequest")
	request := &csi_spec.DeleteVolumeRequest{}

	if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle == "" {
		return nil, fmt.Errorf("unable to retrieve VolumeID from pv %s", pv.Name)
	}
	if className == "" {
		return nil, fmt.Errorf("no class found to retrieve the volume")
	}
	util.LogDebug.Printf("volumeID=%s", pv.Spec.CSI.VolumeHandle)
	request.VolumeId = pv.Spec.CSI.VolumeHandle

	request.Secrets = csiCredentials
	return request, nil
}

func (p *Provisioner) getVolumeContentSource(claim *api_v1.PersistentVolumeClaim) (*csi_spec.VolumeContentSource, error) {
	util.LogDebug.Printf(">>>>> getVolumeContentSource called for %s", claim.Name)
	defer util.LogDebug.Printf("<<<<<< getVolumeContentSource")
	snapshotObj, err := p.snapshotClient.VolumesnapshotV1alpha1().VolumeSnapshots(claim.Namespace).Get(claim.Spec.DataSource.Name, meta_v1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting snapshot %s from api server: %v", claim.Spec.DataSource.Name, err)
	}
	if snapshotObj.Status.ReadyToUse == false {
		return nil, fmt.Errorf("snapshot %s is not Ready", claim.Spec.DataSource.Name)
	}
	if snapshotObj.ObjectMeta.DeletionTimestamp != nil {
		return nil, fmt.Errorf("snapshot %s is currently being deleted", claim.Spec.DataSource.Name)
	}
	util.LogInfo.Printf("VolumeSnapshot %+v", snapshotObj)
	snapContentObj, err := p.snapshotClient.VolumesnapshotV1alpha1().VolumeSnapshotContents().Get(snapshotObj.Spec.SnapshotContentName, meta_v1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting snapshot:snapshotcontent %s:%s from api server: %v", snapshotObj.Name, snapshotObj.Spec.SnapshotContentName, err)
	}
	util.LogInfo.Printf("VolumeSnapshotContent %+v", snapContentObj)
	if snapContentObj.Spec.VolumeSnapshotSource.CSI == nil {
		return nil, fmt.Errorf("error getting snapshot source from snapshot:snapshotcontent %s:%s", snapshotObj.Name, snapshotObj.Spec.SnapshotContentName)
	}
	snapshotSource := csi_spec.VolumeContentSource_Snapshot{
		Snapshot: &csi_spec.VolumeContentSource_SnapshotSource{
			SnapshotId: snapContentObj.Spec.VolumeSnapshotSource.CSI.SnapshotHandle,
		},
	}
	util.LogInfo.Printf("VolumeContentSource_Snapshot %+v", snapshotSource)
	if snapshotObj.Status.RestoreSize != nil {
		capacity, exists := claim.Spec.Resources.Requests[api_v1.ResourceName(api_v1.ResourceStorage)]
		if !exists {
			return nil, fmt.Errorf("error getting capacity for PVC %s when creating snapshot %s", claim.Name, snapshotObj.Name)
		}
		volSizeBytes := capacity.Value()
		util.LogInfo.Printf("Requested volume size is %d and snapshot size is %d for the source snapshot %s", int64(volSizeBytes), int64(snapshotObj.Status.RestoreSize.Value()), snapshotObj.Name)
		// When restoring volume from a snapshot, the volume size should
		// be equal to or larger than its snapshot size.
		if int64(volSizeBytes) < int64(snapshotObj.Status.RestoreSize.Value()) {
			return nil, fmt.Errorf("requested volume size %d is less than the size %d for the source snapshot %s", int64(volSizeBytes), int64(snapshotObj.Status.RestoreSize.Value()), snapshotObj.Name)
		}
		if int64(volSizeBytes) > int64(snapshotObj.Status.RestoreSize.Value()) {
			util.LogInfo.Printf("requested volume size %d is greater than the size %d for the source snapshot %s.", int64(volSizeBytes), int64(snapshotObj.Status.RestoreSize.Value()), snapshotObj.Name)
		}
	}
	volumeContentSource := &csi_spec.VolumeContentSource{
		Type: &snapshotSource,
	}
	return volumeContentSource, nil
}

func (p *Provisioner) isCapabilitySupported(capType csi_spec.ControllerServiceCapability_RPC_Type) (bool, error) {
	util.LogDebug.Printf(">>>>> isCapabilitySupported called with %s", capType)
	defer util.LogDebug.Printf("<<<<<< isCapabilitySupported")
	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()
	response, err := p.csiDriverClient.ControllerGetCapabilities(ctx, nil)

	if err != nil {
		return false, err
	}
	for _, cap := range response.GetCapabilities() {
		util.LogDebug.Printf("handling capability %s", cap.GetRpc().Type)
		if cap.GetRpc().Type == capType {
			return true, nil
		}
	}
	return false, nil
}
