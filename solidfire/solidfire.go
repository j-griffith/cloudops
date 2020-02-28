package solidfire

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/j-griffith/solidfire-go/sdk"
	"github.com/libopenstorage/cloudops"
	"github.com/libopenstorage/cloudops/unsupported"
	"github.com/sirupsen/logrus"
)

const (
	GiB       = 1073741824
	SolidFire = "solidfire"
)

type solidfireOps struct {
	cloudops.Compute
	cfg    *SFConfig
	client *Client
}

type CloudResourceInfo struct {
	Name   string
	ID     string
	Labels map[string]string
	Region string
}

type Client struct {
	SFClient        *sdk.SFClient
	Endpoint        string
	URL             string
	Login           string
	Password        string
	Version         string
	SVIP            string
	InitiatorIface  string
	TargetSecret    string
	InitiatorSecret string
	TenantName      string
	AccountID       int64
}

type SFConfig struct {
	Endpoint           string
	AccountName        string
	SVIP               string
	InitiatorInterface string
}

func parseEndpointString(ep string, c *Client) error {
	items := strings.Split(ep, "/")
	creds := strings.Split(items[2], "@")
	login := strings.Split(creds[0], ":")

	c.URL = creds[1]
	c.Login = login[0]
	c.Password = login[1]
	c.Version = items[4]
	return nil

}

func (ops *solidfireOps) getVolumeByID(id int64) (*sdk.Volume, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &sdk.ListVolumesRequest{}
	req.StartVolumeID = id

	volumes, err := ops.client.SFClient.ListVolumes(ctx, req)
	if err != nil {
		logrus.Errorf("error encountered trying to list active solidfire volumes: %v", err)
		return &sdk.Volume{}, err
	}

	if volumes == nil || len(volumes.Volumes) == 0 || volumes.Volumes[0].VolumeID != id {
		return &sdk.Volume{}, fmt.Errorf("volume %d not found", id)
	}

	return &volumes.Volumes[0], nil
}

// NewClient creates a new solidfire cloudops instance
func NewClient(cfg *SFConfig) (cloudops.Ops, error) {

	client := Client{
		Endpoint:       cfg.Endpoint,
		TenantName:     cfg.AccountName,
		SVIP:           cfg.SVIP,
		InitiatorIface: cfg.InitiatorInterface,
	}

	err := parseEndpointString(client.Endpoint, &client)
	if err != nil {
		logrus.Errorf("failure parsing sfconfig endpoint string: %v", err)
		return nil, err
	}

	var sf sdk.SFClient
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sf.Connect(ctx, client.URL, client.Version, client.Login, client.Password)

	// We want to persist the connection info we created above, otherwise ever call is prefaced with
	// this connect routine (blek)
	client.SFClient = &sf

	if err != nil {
		logrus.Errorf("failure verifying endpoint config while conducting initial client connection: %v", err)
		return nil, err
	}
	// Verify specified user tenant/account and ge it's ID, if it doesn't exist
	// create it
	req := sdk.GetAccountByNameRequest{}
	req.Username = client.TenantName
	result, sdkErr := client.SFClient.GetAccountByName(ctx, &req)
	if sdkErr != nil {
		if sdkErr.Detail == "500:xUnknownAccount" {
			req := sdk.AddAccountRequest{}
			req.Username = client.TenantName
			result, sdkErr := client.SFClient.AddAccount(ctx, &req)
			if sdkErr != nil {
				logrus.Errorf("failed to create default account: %+v", sdkErr)
				return nil, err
			}
			client.AccountID = result.Account.AccountID

		}
	}
	client.AccountID = result.Account.AccountID
	client.InitiatorSecret = result.Account.InitiatorSecret
	client.TargetSecret = result.Account.TargetSecret

	if client.InitiatorIface == "" {
		client.InitiatorIface = "default"
	}

	sfops := &solidfireOps{
		Compute: unsupported.NewUnsupportedCompute(),
		client:  &client,
		cfg:     cfg,
	}
	return sfops, nil
}

func (ops *solidfireOps) Name() string {
	return SolidFire
}

// Create volume based on input template volume and also apply given labels.
func (ops *solidfireOps) Create(options interface{}, labels map[string]string) (interface{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vOptions, ok := options.(*sdk.CreateVolumeRequest)
	if !ok {
	}
	vOptions.AccountID = ops.client.AccountID
	v, err := ops.client.SFClient.CreateVolume(ctx, vOptions)
	if err != nil {
		logrus.Errorf("failed to create requested solidfire volume: %v", err)
		return nil, err
	}

	return v, nil
}

// GetDeviceID returns ID/Name of the given device/disk or snapshot
func (ops *solidfireOps) GetDeviceID(template interface{}) (string, error) {
	return "", nil
}

// Attach volumeID.
// Return attach path.
func (ops *solidfireOps) Attach(volumeID string) (string, error) {
	return "", nil
}

// Expand expands the provided device from the existing size to the new size
// It returns the new size of the device. It is a blocking API where it will
// only return once the requested size is validated with the cloud provider or
// the number of retries prescribed by the cloud provider are exhausted.
func (ops *solidfireOps) Expand(volumeID string, newSizeInGiB uint64) (uint64, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sfVolumeID, err := strconv.ParseInt(volumeID, 10, 64)
	if err != nil {
		logrus.Errorf("failed to convert supplied volumeID to int64 SF Volume ID: %v", err)
		return 0, err
	}

	// get the sf volume object, error if it doesn't exist
	sfVolume, err := ops.getVolumeByID(sfVolumeID)
	if err != nil {
		logrus.Errorf("failed to find specified volume id %s for expand request: %v", volumeID, err)
	}

	if sfVolume.TotalSize >= int64(newSizeInGiB*GiB) {
		actualSize := sfVolume.TotalSize / GiB
		logrus.Errorf("request to expand volume, but volume %s is already greater than or equal to the requested size (requested: %d, currrent: %d)", newSizeInGiB, actualSize)
		return uint64(actualSize), fmt.Errorf("invalid expand size requested")
	}

	req := &sdk.ModifyVolumeRequest{}
	req.VolumeID = sfVolumeID
	req.TotalSize = int64(newSizeInGiB)

	modifyResult, err := ops.client.SFClient.ModifyVolume(ctx, req)
	if err != nil {
		logrus.Errorf("failed resize request for volume %s", volumeID)
		return 0, err
	}
	return uint64(modifyResult.Volume.TotalSize / GiB), nil
}

// Detach volumeID.
func (ops *solidfireOps) Detach(volumeID string) error {
	return nil
}

// DetachFrom detaches the disk/volume with given ID from the given instance ID
func (ops *solidfireOps) DetachFrom(volumeID, instanceID string) error {
	return nil
}

// Delete volumeID.
func (ops *solidfireOps) Delete(volumeID string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sfVolumeID, err := strconv.ParseInt(volumeID, 10, 64)
	if err != nil {
		logrus.Errorf("failed to convert supplied volumeID to int64 SF Volume ID: %v", err)
		return err
	}

	req := &sdk.DeleteVolumeRequest{}
	req.VolumeID = sfVolumeID

	_, err = ops.client.SFClient.DeleteVolume(ctx, req)
	if err != nil {
		logrus.Errorf("failed delete request for volume %s", volumeID)
		return err
	}
	return nil

	return nil
}

// DeleteFrom deletes the given volume/disk from the given instanceID
func (ops *solidfireOps) DeleteFrom(volumeID, instanceID string) error {
	return nil
}

// Desribe an instance
func (ops *solidfireOps) Describe() (interface{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return nil, nil
}

// FreeDevices returns free block devices on the instance.
// blockDeviceMappings is a data structure that contains all block devices on
// the instance and where they are mapped to
func (ops *solidfireOps) FreeDevices(blockDeviceMappings []interface{}, rootDeviceName string) ([]string, error) {
	return nil, nil
}

// Inspect volumes specified by volumeID
func (ops *solidfireOps) Inspect(volumeIds []*string) ([]interface{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return nil, nil
}

// DeviceMappings returns map[local_attached_volume_path]->volume ID/NAME
func (ops *solidfireOps) DeviceMappings() (map[string]string, error) {
	return nil, nil
}

// Enumerate volumes that match given filters. Organize them into
// sets identified by setIdentifier.
// labels can be nil, setIdentifier can be empty string.
func (ops *solidfireOps) Enumerate(volumeIds []*string,
	labels map[string]string,
	setIdentifier string,
) (map[string][]interface{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return nil, nil
}

// DevicePath for the given volume i.e path where it's attached
func (ops *solidfireOps) DevicePath(volumeID string) (string, error) {
	return "", nil
}

// Snapshot the volume with given volumeID
func (ops *solidfireOps) Snapshot(volumeID string, readonly bool) (interface{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return nil, nil
}

// SnapshotDelete deletes the snapshot with given ID
func (ops *solidfireOps) SnapshotDelete(snapID string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return nil
}

// ApplyTags will apply given labels/tags on the given volume
func (ops *solidfireOps) ApplyTags(volumeID string, labels map[string]string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return nil
}

// RemoveTags removes labels/tags from the given volume
func (ops *solidfireOps) RemoveTags(volumeID string, labels map[string]string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return nil
}

// Tags will list the existing labels/tags on the given volume
func (ops *solidfireOps) Tags(volumeID string) (map[string]string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return nil, nil
}
