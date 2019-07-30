// Copyright 2019 Hewlett Packard Enterprise Development LP

package provisioner

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	csi_spec "github.com/container-storage-interface/spec/lib/go/csi"
	api_v1 "k8s.io/api/core/v1"
	storage_v1 "k8s.io/api/storage/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testSecretCredentials = "https://admin:admin@array-name.domain.com:443/csp"
)

var (
	testMountCap = append(defaultVolCapabilites,
		&csi_spec.VolumeCapability{
			AccessType: &csi_spec.VolumeCapability_Mount{
				Mount: &csi_spec.VolumeCapability_MountVolume{
					FsType:     "ext3",
					MountFlags: []string{"rw"},
				},
			},
			AccessMode: &csi_spec.VolumeCapability_AccessMode{
				Mode: csi_spec.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		})

	requestedVolumeSize = resource.MustParse("15Gi")
)

func TestGetSecretReference(t *testing.T) {
	testcases := map[string]struct {
		secretParams secretParamsMap
		params       map[string]string
		pvName       string
		pvc          *api_v1.PersistentVolumeClaim
		expectRef    *api_v1.SecretReference
		expectErr    bool
	}{
		"scParams, no pvc": {
			secretParams: csiSecretParams,
			params:       map[string]string{secretNameKey: "name", secretNamespaceKey: "ns"},
			pvc:          nil,
			expectRef:    &api_v1.SecretReference{Name: "name", Namespace: "ns"},
		},
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			ref, err := getSecretReference(tc.secretParams, tc.params, tc.pvName, tc.pvc)
			if err != nil {
				if tc.expectErr {
					return
				}
				t.Fatalf("Did not expect error but got: %v", err)
			} else {
				if tc.expectErr {
					t.Fatalf("Expected error but got none")
				}
			}
			if !reflect.DeepEqual(ref, tc.expectRef) {
				t.Errorf("Expected %v, got %v", tc.expectRef, ref)
			}
		})
	}
}

//nolint : dupl
func TestVolumeCreateRequest(t *testing.T) {
	testcasesCreate := map[string]struct {
		pvName      string
		class       *storage_v1.StorageClass
		credentials map[string]string
		pvc         *api_v1.PersistentVolumeClaim
		expectReq   *csi_spec.CreateVolumeRequest
		expectErr   bool
	}{
		"valid": {
			// requests := api_v1.ResourceList{api_v1.ResourceName(api_v1.ResourceStorage): tenGBs},

			pvName: "name",
			class: &storage_v1.StorageClass{
				Parameters:   map[string]string{secretNameKey: "name", secretNamespaceKey: "ns", csiParameterPrefix + "fstype": "ext3"},
				MountOptions: []string{"rw"}},
			pvc: &api_v1.PersistentVolumeClaim{
				Spec: api_v1.PersistentVolumeClaimSpec{
					Resources: api_v1.ResourceRequirements{
						Requests: api_v1.ResourceList{
							api_v1.ResourceName(api_v1.ResourceStorage): requestedVolumeSize,
						},
					},
				},
			},
			credentials: map[string]string{"secret": testSecretCredentials},
			expectErr:   false,
			expectReq: &csi_spec.CreateVolumeRequest{
				Name: "name",
				CapacityRange: &csi_spec.CapacityRange{
					RequiredBytes: requestedVolumeSize.Value(),
				},
				VolumeCapabilities: testMountCap,
				Parameters: map[string]string{
					secretNameKey:                 "name",
					secretNamespaceKey:            "ns",
					csiParameterPrefix + "fstype": "ext3",
					clusterIDKey:                  "",
				},
				Secrets: map[string]string{"secret": testSecretCredentials},
			},
		},
		"invalid credentials": {
			pvName: "name",
			class:  &storage_v1.StorageClass{Parameters: map[string]string{secretNameKey: "name", secretNamespaceKey: "ns"}},
			pvc: &api_v1.PersistentVolumeClaim{
				Spec: api_v1.PersistentVolumeClaimSpec{
					Resources: api_v1.ResourceRequirements{
						Requests: api_v1.ResourceList{
							api_v1.ResourceName(api_v1.ResourceStorage): resource.MustParse("15Gi"),
						},
					},
				},
			},
			credentials: nil,
			expectErr:   true,
			expectReq: &csi_spec.CreateVolumeRequest{
				Name:               "name",
				VolumeCapabilities: defaultVolCapabilites,
				Parameters: map[string]string{
					secretNameKey:      "name",
					secretNamespaceKey: "ns",
				},
				Secrets: nil,
			},
		},
		"invalid classParams": {
			pvName: "name",
			class:  nil,
			pvc: &api_v1.PersistentVolumeClaim{
				Spec: api_v1.PersistentVolumeClaimSpec{
					Resources: api_v1.ResourceRequirements{
						Requests: api_v1.ResourceList{
							api_v1.ResourceName(api_v1.ResourceStorage): resource.MustParse("15Gi"),
						},
					},
				},
			},
			credentials: nil,
			expectErr:   true,
			expectReq: &csi_spec.CreateVolumeRequest{
				Name:               "name",
				VolumeCapabilities: defaultVolCapabilites,
				Secrets:            map[string]string{"secret": testSecretCredentials},
			},
		},
	}
	for k, tc := range testcasesCreate {
		t.Run(k, func(t *testing.T) {
			request, err := getTestProvisioner().buildCsiVolumeCreateRequest(tc.pvName, tc.class, tc.pvc, tc.credentials)
			if err != nil {
				if tc.expectErr {
					return
				}
				t.Fatalf("Did not expect error but got: %v", err)
			} else {
				if tc.expectErr {
					t.Fatalf("Expected error but got none")
				}
			}
			if !reflect.DeepEqual(request, tc.expectReq) {
				t.Errorf("Expected %v, got %v", tc.expectReq, request)
			}
		})
	}
}

//nolint : dupl
func TestVolumeDeleteRequest(t *testing.T) {
	testcasesDelete := map[string]struct {
		pv          *api_v1.PersistentVolume
		credentials map[string]string
		className   string
		expectReq   *csi_spec.DeleteVolumeRequest
		expectErr   bool
	}{
		"valid": {
			pv: &api_v1.PersistentVolume{
				ObjectMeta: meta_v1.ObjectMeta{
					Name: "testPV",
				},
				Spec: api_v1.PersistentVolumeSpec{
					PersistentVolumeSource: api_v1.PersistentVolumeSource{
						CSI: &api_v1.CSIPersistentVolumeSource{
							Driver:       CsiProvisioner,
							VolumeHandle: "1",
						},
					},
				},
			},
			credentials: map[string]string{"secret": testSecretCredentials},
			className:   "testClass",
			expectErr:   false,
			expectReq: &csi_spec.DeleteVolumeRequest{
				VolumeId: "1",
				Secrets:  map[string]string{"secret": testSecretCredentials},
			},
		},
		"invalid volumeHandle": {
			pv: &api_v1.PersistentVolume{
				ObjectMeta: meta_v1.ObjectMeta{
					Name: "testPV",
				},
				Spec: api_v1.PersistentVolumeSpec{
					PersistentVolumeSource: api_v1.PersistentVolumeSource{
						CSI: &api_v1.CSIPersistentVolumeSource{
							Driver:       CsiProvisioner,
							VolumeHandle: "",
						},
					},
				},
			},
			credentials: map[string]string{"secret": testSecretCredentials},
			className:   "testClass",
			expectErr:   true,
			expectReq: &csi_spec.DeleteVolumeRequest{
				Secrets: map[string]string{"secret": testSecretCredentials},
			},
		},
		"invalid className": {
			pv: &api_v1.PersistentVolume{
				ObjectMeta: meta_v1.ObjectMeta{
					Name: "testPV",
				},
				Spec: api_v1.PersistentVolumeSpec{
					PersistentVolumeSource: api_v1.PersistentVolumeSource{
						CSI: &api_v1.CSIPersistentVolumeSource{
							Driver:       CsiProvisioner,
							VolumeHandle: "1",
						},
					},
				},
			},
			credentials: map[string]string{"secret": testSecretCredentials},
			expectErr:   true,
			expectReq: &csi_spec.DeleteVolumeRequest{
				VolumeId: "1",
				Secrets:  map[string]string{"secret": testSecretCredentials},
			},
		},
	}
	for k, tc := range testcasesDelete {
		t.Run(k, func(t *testing.T) {
			request, err := getTestProvisioner().buildCsiVolumeDeleteRequest(tc.pv, tc.credentials, tc.className)
			if err != nil {
				if tc.expectErr {
					return
				}
				t.Fatalf("Did not expect error but got: %v", err)
			} else {
				if tc.expectErr {
					t.Fatalf("Expected error but got none")
				}
			}
			if !reflect.DeepEqual(request, tc.expectReq) {
				t.Errorf("Expected %v, got %v", tc.expectReq, request)
			}
		})
	}
}
