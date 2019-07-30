// Copyright 2019 Hewlett Packard Enterprise Development LP

package provisioner

import (
	csi_v1_alpha1 "k8s.io/csi-api/pkg/apis/csi/v1alpha1"
	"reflect"
	"testing"
)

const (
	testNodeID = "testNodeID"
)

func TestGetCSINodeIDFromNodeInfo(t *testing.T) {
	testcases := map[string]struct {
		pvName      string
		csiNodeInfo *csi_v1_alpha1.CSINodeInfo
		expectRef   string
		expectErr   bool
	}{
		"Valid CSI NodeInfo": {
			csiNodeInfo: &csi_v1_alpha1.CSINodeInfo{
				Spec: csi_v1_alpha1.CSINodeInfoSpec{
					Drivers: []csi_v1_alpha1.CSIDriverInfoSpec{
						csi_v1_alpha1.CSIDriverInfoSpec{
							Name:   CsiProvisioner,
							NodeID: testNodeID,
						},
						csi_v1_alpha1.CSIDriverInfoSpec{
							Name:   FlexVolumeProvisioner,
							NodeID: "",
						},
					},
				},
			},
			expectRef: testNodeID,
			expectErr: false,
		},
		"Invalid CSI NodeInfo": {
			csiNodeInfo: &csi_v1_alpha1.CSINodeInfo{
				Spec: csi_v1_alpha1.CSINodeInfoSpec{
					Drivers: []csi_v1_alpha1.CSIDriverInfoSpec{
						csi_v1_alpha1.CSIDriverInfoSpec{
							Name:   FlexVolumeProvisioner,
							NodeID: "",
						},
					},
				},
			},
			expectRef: testNodeID,
			expectErr: true,
		},
	}
	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			ref, _ := getTestProvisioner().getNodeIDFromNodeInfo(tc.csiNodeInfo)
			if ref == "" {
				if tc.expectErr {
					return
				}
				t.Fatalf("Did not expect error but got empty nodeID")
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
