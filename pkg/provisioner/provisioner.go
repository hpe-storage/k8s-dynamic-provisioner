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
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hpe-storage/common-host-libs/chain"
	"github.com/hpe-storage/common-host-libs/docker/dockervol"
	"github.com/hpe-storage/common-host-libs/jconfig"
	log "github.com/hpe-storage/common-host-libs/logger"
	uuid "github.com/satori/go.uuid"
	api_v1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	storage_v1 "k8s.io/api/storage/v1"
	resource_v1 "k8s.io/apimachinery/pkg/api/resource"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	core_v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

const (
	dockerVolumeName   = "docker-volume-name"
	flexVolumeBasePath = "/usr/libexec/kubernetes/kubelet-plugins/volume/exec/"
	k8sProvisionedBy   = "pv.kubernetes.io/provisioned-by"
	chainTimeout       = 2 * time.Minute
	chainRetries       = 2
	//TODO allow this to be set per docker volume driver
	maxCreates = 4
	//TODO allow this to be set per docker volume driver
	maxDeletes                 = 10
	defaultSocketFile          = "/etc/hpe-storage/nimble.sock"
	defaultfactorForConversion = 1073741824
	defaultStripValue          = true
	maxWaitForClaims           = 60
	allowOverrides             = "allowOverrides"
	cloneOf                    = "cloneOf"
	cloneOfPVC                 = "cloneOfPVC"
	manager                    = "manager"
	defaultManagerName         = "k8s"
	id2chanMapSize             = 1024
	deleteRetrySleep           = 5 * time.Second
	// FlexVolumeProvisioner name prefix
	FlexVolumeProvisioner = "hpe.com"
	snapshotKind          = "VolumeSnapshot"
	snapshotAPIGroup      = "snapshot.storage.k8s.io"
)

var (
	// resyncPeriod describes how often to get a full resync (0=never)
	resyncPeriod = 5 * time.Minute
	// maxWaitForBind refers to a single execution of the retry loop
	maxWaitForBind = 30 * time.Second
	// statusLoggingWait is only used when debug is true
	statusLoggingWait                   = 5 * time.Second
	defaultListOfStorageResourceOptions = []string{"size", "sizeInGiB"}
	defaultDockerOptions                = map[string]interface{}{manager: defaultManagerName}
)

// Provisioner provides dynamic pvs based on pvcs and storage classes.
type Provisioner struct {
	kubeClient *kubernetes.Clientset
	// serverVersion is the k8s server version
	serverVersion *version.Info
	// classStore provides access to StorageClasses on the cluster
	classStore              cache.Store
	claimsStore             cache.Store
	vaStore                 cache.Store
	pvStore                 cache.Store
	id2chan                 map[string]chan *updateMessage
	id2chanLock             *sync.Mutex
	affectDockerVols        bool
	dockerVolNameAnnotation string
	eventRecorder           record.EventRecorder
	provisionCommandChains  uint32
	deleteCommandChains     uint32
	parkedCommands          uint32
	debug                   bool
	// ClusterID stores the ID of the cluster creating a volume
	ClusterID string
}

type updateMessage struct {
	pv  *api_v1.PersistentVolume
	pvc *api_v1.PersistentVolumeClaim
}

// addMessageChan adds a chan to the map index by id.  If channel is nil, a new chan is allocated and added
func (p *Provisioner) addMessageChan(id string, channel chan *updateMessage) {
	p.id2chanLock.Lock()
	defer p.id2chanLock.Unlock()

	if _, found := p.id2chan[id]; found {
		return
	}
	if channel != nil {
		log.Debugf("addMessageChan: adding %s", id)
		p.id2chan[id] = channel
	} else {
		log.Debugf("addMessageChan: creating %s", id)
		p.id2chan[id] = make(chan *updateMessage, 1024)
	}
}

// getMessageChan gets a chan from the map index by claim or vol id to be passed to the consumer.
// Do not use this pointer to send data as the channel might be closed right after the
// pointer is returned.  Instead use sendUpdate(...).
func (p *Provisioner) getMessageChan(id string) chan *updateMessage {
	p.id2chanLock.Lock()
	defer p.id2chanLock.Unlock()

	return p.id2chan[id]
}

// sendUpdate sends an claim or volume update to the consumer.  A big lock (entire map)
// is used for now.
func (p *Provisioner) sendUpdate(t interface{}) {
	var id string
	var mess *updateMessage

	claim, _ := getPersistentVolumeClaim(t)
	if claim != nil {
		log.Debugf("sendUpdate: pvc:%s (%s) phase:%s", claim.Name, claim.UID, claim.Status.Phase)
		id = fmt.Sprintf("%s", claim.UID)
		mess = &updateMessage{pvc: claim}
	} else {
		vol, _ := getPersistentVolume(t)
		if vol != nil {
			log.Debugf("sendUpdate: pv:%s (%s) phase:%s", vol.Name, vol.UID, vol.Status.Phase)
			id = fmt.Sprintf("%s", vol.UID)
			mess = &updateMessage{pv: vol}
		}
	}

	// hold the big lock just to send
	p.id2chanLock.Lock()
	defer p.id2chanLock.Unlock()

	messChan := p.id2chan[id]
	if messChan == nil {
		log.Debugf("send: skipping %s, not in map", id)
		return
	}
	messChan <- mess
}

// removeMessageChan closes (if open) chan and removes it from the map
func (p *Provisioner) removeMessageChan(claimID string, volID string) {
	log.Debugf("removeMessageChan called with claimID %s volID %s", claimID, volID)
	p.id2chanLock.Lock()
	defer p.id2chanLock.Unlock()

	messChan := p.id2chan[claimID]
	if messChan != nil {
		delete(p.id2chan, claimID)
	}
	if byVolID, found := p.id2chan[volID]; found {
		delete(p.id2chan, volID)
		if messChan == nil {
			messChan = byVolID
		}
	}
	if messChan == nil {
		return
	}

	select {
	case <-messChan:
	default:
		close(messChan)
	}
}

//NewProvisioner provides a Provisioner for a k8s cluster
func NewProvisioner(clientSet *kubernetes.Clientset, affectDockerVols bool, debug bool) *Provisioner {
	id := uuid.NewV4()
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&core_v1.EventSinkImpl{Interface: clientSet.CoreV1().Events(v1.NamespaceAll)})
	log.Debugf("provisioner (prefix=*.hpe.com) is being created with instance id %s and id2chan capacity %d.", id.String(), id2chanMapSize)

	return &Provisioner{
		kubeClient:       clientSet,
		id2chan:          make(map[string]chan *updateMessage, id2chanMapSize), //make a id to chan (updatemessage) map with a capacity of 10k entries
		id2chanLock:      &sync.Mutex{},
		affectDockerVols: affectDockerVols,
		eventRecorder:    broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: fmt.Sprintf("*.hpe.com-%s", id.String())}),
		debug:            debug,
	}
}

// update the existing volume's metadata for the claims
func (p *Provisioner) updateDockerVolumeMetadata(store cache.Store) {
	log.Debug("updateDockerVolumeMetadata started")
	optionsMap := map[string]interface{}{manager: defaultManagerName}

	i := 0
	for len(store.List()) < 1 {
		if i > maxWaitForClaims {
			log.Infof("No Claims found after waiting for %d seconds. Ignoring update", maxWaitForClaims)
			return
		}
		time.Sleep(time.Second)
		i++
	}

	for _, pvc := range store.List() {
		claim, err := getPersistentVolumeClaim(pvc)
		if err != nil {
			log.Debugf("unable to retrieve the claim from %v", pvc)
			continue
		}

		if claim.Status.Phase != api_v1.ClaimBound {
			log.Debugf("claim %s was not bound - skipping", claim.Name)
			continue
		}

		className := getClaimClassName(claim)
		log.Debugf("found classname %s for claim %s.", className, claim.Name)
		class, err := p.getClass(className)
		if err != nil {
			log.Errorf("unable to retrieve the class object for claim %v", claim)
			continue
		}

		if !strings.HasPrefix(class.Provisioner, FlexVolumeProvisioner) {
			log.Infof("updateDockerVolumeMetadata: class named %s in pvc %s did not refer to a supported provisioner (name must begin with %s).  current provisioner=%s - skipping", className, claim.Name, FlexVolumeProvisioner, class.Provisioner)
			continue
		}

		err = p.updateVolume(claim, class.Provisioner, optionsMap)
		if err != nil {
			// we don't want to beat on the docker plugin if it doesn't support update
			// so we simply move on to the next volume if we hit an error
			log.Errorf("unable to update volume %v Err: %v", claim.Spec.VolumeName, err.Error())
			continue
		}
	}

	log.Debug("updateDockerVolumeMetadata ended")
}

// Start the provision workflow.  Note that Start will block until there are storage classes found.
func (p *Provisioner) Start(stop chan struct{}) {
	var err error
	// get the server version
	p.serverVersion, err = p.kubeClient.Discovery().ServerVersion()
	if err != nil {
		log.Errorf("Unable to get server version.  %s", err.Error())
	}

	// Get the StorageClass store and start it's reflector
	var classReflector *cache.Reflector
	p.classStore, classReflector = p.newClassReflector(p.kubeClient)
	go classReflector.Run(stop)

	// Get and start the Persistent Volume Claim Controller
	var claimInformer cache.Controller
	p.claimsStore, claimInformer = p.newClaimController()
	go claimInformer.Run(stop)

	go p.updateDockerVolumeMetadata(p.claimsStore)

	var volInformer cache.Controller
	p.pvStore, volInformer = p.newVolumeController()
	go volInformer.Run(stop)

	if p.debug {
		go p.statusLogger()
	}

	// Wait for our reflector to load (or for someone to add a Storage Class)
	p.waitForClasses()

	log.Debugf("provisioner has been started and is watching a server with version %s.", p.serverVersion)

}

func (p *Provisioner) statusLogger() {
	for {
		time.Sleep(statusLoggingWait)
		_, err := p.kubeClient.Discovery().ServerVersion()
		if err != nil {
			log.Errorf("statusLogger: provision chains=%d, delete chains=%d, parked chains=%d, ids tracked=%d, connection error=%s", atomic.LoadUint32(&p.provisionCommandChains), atomic.LoadUint32(&p.deleteCommandChains), atomic.LoadUint32(&p.parkedCommands), len(p.id2chan), err.Error())
			return
		}
		log.Infof("statusLogger: provision chains=%d, delete chains=%d, parked chains=%d, ids tracked=%d, connection=valid", atomic.LoadUint32(&p.provisionCommandChains), atomic.LoadUint32(&p.deleteCommandChains), atomic.LoadUint32(&p.parkedCommands), len(p.id2chan))
	}
}

func (p *Provisioner) deleteVolume(pv *api_v1.PersistentVolume, rmPV bool) {
	provisioner := pv.Annotations[k8sProvisionedBy]
	log.Debugf("provisioner is %s", provisioner)

	// slow down a delete storm
	limit(&p.deleteCommandChains, &p.parkedCommands, maxDeletes)

	atomic.AddUint32(&p.deleteCommandChains, 1)
	defer atomic.AddUint32(&p.deleteCommandChains, ^uint32(0))
	deleteChain := chain.NewChain(chainRetries, deleteRetrySleep)

	log.Debugf("in deleteVolume: cleaning up pv:%s Status:%v with deleteChain %d parkedCommands %d with affectDockerVols %v", pv.Name, pv.Status, atomic.LoadUint32(&p.deleteCommandChains), atomic.LoadUint32(&p.parkedCommands), p.affectDockerVols)
	p.deleteFlexVolume(pv, deleteChain, provisioner)

	if rmPV {
		deleteChain.AppendRunner(&deletePersistentVolume{
			p:   p,
			vol: pv,
		})
	}

	err := deleteChain.Execute()

	if err != nil {
		p.eventRecorder.Event(pv, api_v1.EventTypeWarning, "DeleteVolume",
			fmt.Sprintf("Failed to delete volume for pv %s: %v", pv.Name, err))
	}
}
func (p *Provisioner) deleteFlexVolume(pv *api_v1.PersistentVolume, deleteChain *chain.Chain, provisioner string) {
	log.Debug(">>>>> deleteFlexVolume called")
	defer log.Debug("<<<<< deleteFlexVolume")
	if p.affectDockerVols {
		dockerClient, _, err := p.newDockerVolumePluginClient(provisioner)
		if err != nil {
			info := fmt.Sprintf("failed to get docker client for %s while trying to delete pv %s: %v", FlexVolumeProvisioner, pv.Name, err)
			log.Error(info)
			p.eventRecorder.Event(pv, api_v1.EventTypeWarning, "DeleteVolumeGetClient", info)
			return
		}
		vol := p.getDockerVolume(dockerClient, pv.Name)
		if vol != nil && vol.Name == pv.Name {
			p.eventRecorder.Event(pv, api_v1.EventTypeNormal, "DeleteVolume", fmt.Sprintf("cleaning up volume named %s", pv.Name))
			log.Debugf("Docker volume with name %s found.  Delete using %s.", pv.Name, FlexVolumeProvisioner)
			deleteChain.AppendRunner(&deleteDockerVol{
				name:   pv.Name,
				client: dockerClient,
			})
		}
	}
	return
}

func (p *Provisioner) updateVolume(claim *api_v1.PersistentVolumeClaim, provisioner string, updateMap map[string]interface{}) error {
	log.Debugf("updateVolume called with claim:%s, provisioner:%s and options:%v", claim.Name, provisioner, updateMap)

	// get the volume name for update
	volName := claim.Spec.VolumeName

	var dockerClient *dockervol.DockerVolumePlugin
	dockerClient, _, err := p.newDockerVolumePluginClient(provisioner)
	if err != nil {
		return err
	}

	vol := p.getDockerVolume(dockerClient, volName)
	if (vol == nil) || (volName != vol.Name) {
		return fmt.Errorf("error updating pv from claim: %v and provisioner :%s. err=Docker volume %v with name %s was not found ", claim, provisioner, vol, volName)
	}

	if val, ok := vol.Status[manager]; ok && val != "" {
		log.Debugf("claim:%s has manager set to value %v - skipping", claim.Name, val)
		return nil
	}

	log.Debugf("invoking VolumeDriver.Update with name :%s updateMap :%v", vol.Name, updateMap)
	_, err = dockerClient.Update(vol.Name, updateMap)
	if err != nil {
		return err
	}
	return nil
}

func (p *Provisioner) provisionVolume(claim *api_v1.PersistentVolumeClaim, class *storage_v1.StorageClass) {
	log.Debugf(">>>>> provisionVolume for %s", claim.UID)
	defer log.Debug("<<<<< provisionVolume")
	// this can fire multiple times without issue, so we defer this even though we don't have a volume yet
	id := fmt.Sprintf("%s", claim.UID)
	defer p.removeMessageChan(id, "")
	// find a name...
	volName := p.getBestVolName(claim, class)
	//namespace of the claim
	nameSpace := p.getClaimNameSpace(claim)

	// create a copy of the storage class options for NLT-1172
	params := make(map[string]string)
	for key, value := range class.Parameters {
		params[key] = value
	}
	// add name to options
	params["name"] = volName

	// slow down a create storm
	limit(&p.provisionCommandChains, &p.parkedCommands, maxCreates)

	provisionChain := chain.NewChain(chainRetries, chainTimeout)
	atomic.AddUint32(&p.provisionCommandChains, 1)
	defer atomic.AddUint32(&p.provisionCommandChains, ^uint32(0))

	volumeCreateOptions := &volumeCreateOptions{
		volName:        volName,
		classParams:    params,
		claim:          claim,
		class:          class,
		provisionChain: provisionChain,
		nameSpace:      nameSpace,
	}

	volumeCreateOptions.claimID = id
	p.provisionFlexVolume(volumeCreateOptions)

	// slow down if there is a create storm of pv/pvc. for regular scenario just introduce a delay
	time.Sleep(time.Duration(time.Second))

	p.eventRecorder.Event(class, api_v1.EventTypeNormal, "ProvisionStorage", fmt.Sprintf("%s provisioning storage for pvc %s (%s) using class %s", class.Provisioner, claim.Name, id, class.Name))
	err := provisionChain.Execute()
	if err != nil {
		log.Errorf("failed to create volume for claim %s with class %s: %s", claim.Name, class.Name, err)
		p.eventRecorder.Event(class, api_v1.EventTypeWarning, "ProvisionStorage",
			fmt.Sprintf("failed to create volume for claim %s with class %s: %s", claim.Name, class.Name, err))
	}

	// if we created a volume, remove its uuid from the message map
	pvol, _ := getPersistentVolume(provisionChain.GetRunnerOutput("createPersistentVolume"))
	if pvol != nil {
		p.removeMessageChan(fmt.Sprintf("%s", claim.UID), fmt.Sprintf("%s", pvol.UID))
	}
}

func (p *Provisioner) provisionFlexVolume(options *volumeCreateOptions) {
	log.Debug(">>>>> provisionFlexVolume")
	defer log.Debug("<<<<< provisionFlexVolume")
	p.dockerVolNameAnnotation = FlexVolumeProvisioner + "/" + dockerVolumeName
	pv, err := p.newFlexVolPersistentVolume(options.volName, options.classParams, options.claim, options.class)
	if err != nil {
		log.Errorf("error building pv from %v %v and %v. err=%v", options.claim, options.classParams, options.class, err)
		return
	}
	var dockerClient *dockervol.DockerVolumePlugin
	var dockerOptions map[string]interface{}
	dockerClient, dockerOptions, err = p.newDockerVolumePluginClient(options.class.Provisioner)
	if err != nil {
		log.Errorf("unable to get docker client for class %v while trying to provision pvc named %s (%s): %s", options.class, options.claim.Name, options.claimID, err)
		p.eventRecorder.Event(options.class, api_v1.EventTypeWarning, "ProvisionVolumeGetClient",
			fmt.Sprintf("failed to get docker volume client for class %s while trying to provision claim %s (%s): %s", options.class.Name, options.claim.Name, options.claimID, err))
		return
	}
	vol := p.getDockerVolume(dockerClient, options.volName)
	if vol != nil && options.volName == vol.Name {
		log.Errorf("error provisioning pv from %v and %v. err=Docker volume with this name was found %v.", options.claim, options.class, vol)
		return
	}

	sizeForDockerVolumeinGiB := getClaimSizeForFactor(options.claim, dockerClient, 0)

	// handling storage class overrides
	overrideKeys := p.getClassOverrideOptions(options.classParams)
	var optionsMap map[string]interface{}
	optionsMap, err = p.parseStorageClassParams(options.classParams, options.class, sizeForDockerVolumeinGiB, dockerClient.ListOfStorageResourceOptions, options.nameSpace)
	if err != nil {
		log.Errorf("error parsing storage class parameters from %v %v and %v. err=%v", options.claim, options.classParams, options.class, err)
		return
	}

	// get updated options map for docker after handling overrides and annotations
	optionsMap, err = p.getClaimOverrideOptions(options.claim, overrideKeys, optionsMap, FlexVolumeProvisioner)
	if err != nil {
		p.eventRecorder.Event(options.class, api_v1.EventTypeWarning, "ProvisionStorage", err.Error())
		log.Errorf("error handling annotations. err=%v", err)
		return
	}

	log.Debugf("updated optionsMap with overrideKeys %#v", optionsMap)

	baseVolume := ""
	if val, ok := optionsMap[cloneOf]; ok {
		baseVolume = val.(string)
		log.Debugf("found base volume %s for create request of pv %s ", baseVolume, options.volName)
	}

	// verify if we are creating from existing volume and size matches with requested PVC
	if baseVolume != "" {
		vol := p.getDockerVolume(dockerClient, baseVolume)
		if vol != nil && baseVolume == vol.Name {
			log.Debugf("got base volume from docker %s", vol.Name)
			if value, ok := vol.Status["VolSizeMiB"]; ok {
				baseVolSizeInMiB := value.(float64)
				baseVolSizeInGiB := int(baseVolSizeInMiB) / 1024
				if sizeForDockerVolumeinGiB != baseVolSizeInGiB {
					log.Errorf("failed to create volume %s using base volume %s due to size mismatch of %d vs original size %d", options.volName, baseVolume, sizeForDockerVolumeinGiB, baseVolSizeInGiB)
					p.eventRecorder.Event(options.class, api_v1.EventTypeWarning, "ProvisionStorage",
						fmt.Sprintf("failed to create volume %s using base volume %s due to size mismatch", options.volName, baseVolume))
					return
				}
			}
		}
	}

	// set default docker options if not already set
	p.setDefaultDockerOptions(optionsMap, options.classParams, dockerOptions, dockerClient)
	if p.affectDockerVols {
		options.provisionChain.AppendRunner(&createDockerVol{
			requestedName: pv.Name,
			options:       optionsMap,
			client:        dockerClient,
		})
	}

	options.provisionChain.AppendRunner(&createPersistentVolume{
		p:   p,
		vol: pv,
	})

	options.provisionChain.AppendRunner(&monitorBind{
		origClaim: options.claim,
		pChain:    options.provisionChain,
		p:         p,
	})
}

func (p *Provisioner) setDefaultDockerOptions(optionsMap map[string]interface{}, params map[string]string, dockerOptions map[string]interface{}, dockerClient *dockervol.DockerVolumePlugin) {
	for k, v := range dockerOptions {
		log.Debugf("processing %s:%v", k, v)
		_, ok := params[k]
		if ok == false {
			log.Infof("setting the docker option %s:%v", k, v)
			val := reflect.ValueOf(v)
			optionsMap[k] = val.Interface()
		}
	}
	log.Debugf("optionsMap %v", optionsMap)
}

func limit(watched, parked *uint32, max uint32) {
	if atomic.LoadUint32(watched) >= max {
		atomic.AddUint32(parked, 1)
		for atomic.LoadUint32(watched) >= max {
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		}
		atomic.AddUint32(parked, ^uint32(0))
	}
}

func getClaimSizeForFactor(claim *api_v1.PersistentVolumeClaim, dockerClient *dockervol.DockerVolumePlugin, sizeForDockerVolumeinGiB int) int {
	requestParams := claim.Spec.Resources.Requests
	for key, val := range requestParams {
		if key == "storage" {
			if val.Format == resource_v1.BinarySI || val.Format == resource_v1.DecimalSI {
				sizeInBytes, isInt := val.AsInt64()
				if isInt && sizeInBytes > 0 {
					if dockerClient.ListOfStorageResourceOptions != nil &&
						dockerClient.FactorForConversion != 0 {
						sizeForDockerVolumeinGiB = int(sizeInBytes) / dockerClient.FactorForConversion
						log.Debugf("claimSize=%d for size=%d bytes and factorForConversion=%d", sizeForDockerVolumeinGiB, sizeInBytes, dockerClient.FactorForConversion)
						return sizeForDockerVolumeinGiB
					}
				}
			}
		}
	}
	return sizeForDockerVolumeinGiB
}

func (p *Provisioner) newDockerVolumePluginClient(provisionerName string) (*dockervol.DockerVolumePlugin, map[string]interface{}, error) {
	driverName := strings.Split(provisionerName, "/")
	if len(driverName) < 2 {
		log.Infof("Unable to parse provisioner name %s.", provisionerName)
		return nil, nil, fmt.Errorf("unable to parse provisioner name %s", provisionerName)
	}
	configPathName := fmt.Sprintf("%s%s/%s.json", flexVolumeBasePath, strings.Replace(provisionerName, "/", "~", 1), driverName[1])
	log.Debugf("looking for %s", configPathName)
	var (
		socketFile                   = defaultSocketFile
		strip                        = defaultStripValue
		listOfStorageResourceOptions = defaultListOfStorageResourceOptions
		factorForConversion          = defaultfactorForConversion
		dockerOpts                   = defaultDockerOptions
	)
	c, err := jconfig.NewConfig(configPathName)
	if err != nil {
		log.Infof("Unable to process config at %s, %v.  Using defaults.", configPathName, err)
	} else {
		socketFile, err = c.GetStringWithError("dockerVolumePluginSocketPath")
		if err != nil {
			socketFile = defaultSocketFile
		}
		b, err := c.GetBool("stripK8sFromOptions")
		if err == nil {
			strip = b
		}
		ss, err := c.GetStringSliceWithError("listOfStorageResourceOptions")
		if err == nil {
			listOfStorageResourceOptions = ss
		}
		i := c.GetInt64("factorForConversion")
		if i != 0 {
			factorForConversion = int(i)
		}
		defaultOpts, err := c.GetMapSlice("defaultOptions")
		if err == nil {
			log.Debugf("parsing defaultOptions %v", defaultOpts)
			optMap := make(map[string]interface{})

			for _, values := range defaultOpts {
				for k, v := range values {
					optMap[k] = v
					log.Debugf("key %v value %v", k, optMap[k])
				}
			}
			dockerOpts = optMap
			log.Debugf("dockerOptions %v", dockerOpts)
		}
	}
	options := &dockervol.Options{
		SocketPath:                   socketFile,
		StripK8sFromOptions:          strip,
		ListOfStorageResourceOptions: listOfStorageResourceOptions,
		FactorForConversion:          factorForConversion,
	}
	client, er := dockervol.NewDockerVolumePlugin(options)
	return client, dockerOpts, er
}

// block until there are some classes defined in the cluster
func (p *Provisioner) waitForClasses() {
	i := 0
	for len(p.classStore.List()) < 1 {
		if i > 29 {
			log.Infof("No StorageClass found.  Unable to make progress.")
			i = 0
		}
		time.Sleep(time.Second)
		i++
	}
}

func (p *Provisioner) getBestVolName(claim *api_v1.PersistentVolumeClaim, class *storage_v1.StorageClass) string {
	val, ok := claim.Annotations[p.dockerVolNameAnnotation]
	if ok && val != "" {
		return fmt.Sprintf("%s-%s", claim.Namespace, val)
	}
	if claim.GetGenerateName() != "" {
		return fmt.Sprintf("%s-%s", claim.Namespace, claim.GetGenerateName())
	}
	return fmt.Sprintf("%s-%s", class.Name, claim.UID)
}

func (p *Provisioner) getDockerVolume(dockerClient *dockervol.DockerVolumePlugin, volName string) *dockervol.DockerVolume {
	vol, err := dockerClient.Get(volName)
	if err != nil {
		return nil
	}
	return &vol.Volume
}

type createDockerVol struct {
	requestedName string
	returnedName  string
	options       map[string]interface{}
	client        *dockervol.DockerVolumePlugin
}

func (c createDockerVol) Name() string {
	return reflect.TypeOf(c).Name()
}

func (c *createDockerVol) Run() (name interface{}, err error) {
	log.Debugf(">>>>>> Run createDockerVol with volume %s options %#v", c.requestedName, c.options)
	defer log.Debug("<<<<<< Run createDockerVol")
	c.returnedName, err = c.client.Create(c.requestedName, c.options)
	if err != nil {
		log.Errorf("failed to create docker volume vol=%s, error=%s", c.requestedName, err.Error())
		return nil, err
	}
	log.Infof("created docker volume named %s", c.returnedName)
	name = c.returnedName
	return name, err
}

func (c *createDockerVol) Rollback() (err error) {
	log.Debugf(">>>>>> Rollback createDockerVol called with %s", c.requestedName)
	defer log.Debug("<<<<<< Rollback createDockerVol")
	if c.returnedName != "" {
		err = c.client.Delete(c.returnedName, defaultManagerName)
		if err != nil {
			err = c.client.Delete(c.returnedName, "")
		}
	}
	return err
}

type deleteDockerVol struct {
	name   string
	client *dockervol.DockerVolumePlugin
}

func (c deleteDockerVol) Name() string {
	return reflect.TypeOf(c).Name()
}

func (c *deleteDockerVol) Run() (name interface{}, err error) {
	log.Debugf(">>>>>> Run deleteDockerVol called with %s", c.name)
	defer log.Debug("<<<<<< Run deleteDockerVol")
	// slow down if there is a volume delete storm
	time.Sleep(time.Duration(time.Second))
	err = c.client.Delete(c.name, defaultManagerName)
	if err != nil {
		err = c.client.Delete(c.name, "")
	}
	return nil, err
}

func (c *deleteDockerVol) Rollback() (err error) {
	//no op
	return nil
}

type createPersistentVolume struct {
	p   *Provisioner
	vol *api_v1.PersistentVolume
}

func (c createPersistentVolume) Name() string {
	return reflect.TypeOf(c).Name()
}

func (c *createPersistentVolume) Run() (name interface{}, err error) {
	log.Debugf(">>>>>> Run createPersistentVolume called with %s", c.vol)
	defer log.Debug("<<<<<< Run createPersistentVolume")
	pv, err := c.p.kubeClient.CoreV1().PersistentVolumes().Create(c.vol)
	if err != nil {
		c.p.eventRecorder.Event(pv, api_v1.EventTypeWarning, "CreatePersistentVolume", fmt.Sprintf("Failed to create pv %#v: %v", c.vol.Name, err))
		return nil, err
	}
	if pv == nil {
		c.p.eventRecorder.Event(pv, api_v1.EventTypeWarning, "CreatePersistentVolume", fmt.Sprintf("Unable to create pv %#v", c.vol.Name))
		return nil, err
	}
	return pv, nil
}

func (c *createPersistentVolume) Rollback() (err error) {
	log.Debugf(">>>>>> Rollback createPersistentVolume called with %s", c.vol.Name)
	defer log.Debug("<<<<<< Rollback createPersistentVolume")
	return c.p.kubeClient.CoreV1().PersistentVolumes().Delete(c.vol.Name, &meta_v1.DeleteOptions{})
}

type deletePersistentVolume struct {
	p   *Provisioner
	vol *api_v1.PersistentVolume
}

func (d deletePersistentVolume) Name() string {
	return reflect.TypeOf(d).Name()
}

func (d *deletePersistentVolume) Run() (name interface{}, err error) {
	log.Debugf(">>>>>> Run deletePersistentVolume called with %s", d.vol.Name)
	defer log.Debug("<<<<<< Run deletePersistentVolume")
	err = d.p.kubeClient.CoreV1().PersistentVolumes().Delete(d.vol.Name, &meta_v1.DeleteOptions{})
	if err != nil {
		d.p.eventRecorder.Event(d.vol, api_v1.EventTypeWarning, "DeletePersistentVolume", fmt.Sprintf("Error Deleting pv %v %s", d.vol.Name, err.Error()))
	}
	d.p.eventRecorder.Event(d.vol, api_v1.EventTypeNormal, "DeletePersistentVolume", fmt.Sprintf("Deleted pv %v", d.vol.Name))
	return nil, err
}

func (d *deletePersistentVolume) Rollback() (err error) {
	//no op
	return nil
}
