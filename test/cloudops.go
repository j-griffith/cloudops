package test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/libopenstorage/cloudops"
	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

const (
	// clusterNodeCount node count per availability zone to use during tests
	clusterNodeCount = 4
	// retrySeconds interval in secs between consicutive retries
	retrySeconds = 15
	// timeoutMinutes timeout in minutes for cloud operation to complete
	timeoutMinutes = 5
)

var diskLabels = map[string]string{
	"source": "openstorage-test",
	"foo":    "bar",
	"Test":   "UPPER_CASE",
}

// RunTest runs all tests
func RunTest(
	drivers map[string]cloudops.Ops,
	diskTemplates map[string]map[string]interface{},
	t *testing.T) {
	for _, d := range drivers {
		name(t, d)
		compute(t, d)

		for _, template := range diskTemplates[d.Name()] {
			disk := create(t, d, template)
			fmt.Printf("Created disk: %v\n", disk)
			diskID := id(t, d, disk)
			snapshot(t, d, diskID)
			tags(t, d, diskID)
			enumerate(t, d, diskID)
			inspect(t, d, diskID)
			attach(t, d, diskID)
			devicePath(t, d, diskID)
			teardown(t, d, diskID)
			fmt.Printf("Tore down disk: %v\n", disk)
		}
	}
}

func name(t *testing.T, driver cloudops.Ops) {
	name := driver.Name()
	require.NotEmpty(t, name, "driver returned empty name")
}

func compute(t *testing.T, driver cloudops.Ops) {
	instanceID := driver.InstanceID()
	require.NotEmpty(t, instanceID, "failed to get instance ID")

	info, err := driver.InspectInstance(instanceID)
	if _, ok := err.(*cloudops.ErrNotSupported); ok {
		return
	}
	require.NoError(t, err, "failed to inspect instance")
	require.NotNil(t, info, "got nil instance info from inspect")

	groupInfo, err := driver.InspectInstanceGroupForInstance(instanceID)
	if _, ok := err.(*cloudops.ErrNotSupported); ok {
		return
	}
	require.NoError(t, err, "failed to inspect instance group")
	require.NotNil(t, groupInfo, "got nil instance group info from inspect")

	instanceToDelete := os.Getenv("INSTANCE_TO_DELETE")
	zoneOfInstanceToDelete := os.Getenv("INSTANCE_TO_DELETE_ZONE")
	if instanceToDelete != "" {
		err := driver.DeleteInstance(instanceToDelete, zoneOfInstanceToDelete, 5*time.Minute)
		require.NoError(t, err, fmt.Sprintf("failed to delete instance [%v]. Error:[%v]", instanceToDelete, err))
	} else {
		logrus.Fatalf("Set INSTANCE_TO_DELETE environment variable")
	}

	err = driver.SetInstanceGroupSize(groupInfo.Name, clusterNodeCount, 5*time.Minute)
	if err != nil {
		_, ok := err.(*cloudops.ErrNotSupported)
		if !ok {
			t.Errorf("failed to set node count. Error:[%v]", err)
		}
	}

	currentCount, err := driver.GetInstanceGroupSize(groupInfo.Name)
	if err != nil {
		_, ok := err.(*cloudops.ErrNotSupported)
		if !ok {
			t.Errorf("failed to get node count. Error:[%v]", err)
		}
	} else {
		// clusterNodeCount is per availability zone.
		// So total cluster-wide node count is clusterNodeCount*num. of az
		require.Equal(t, int64(clusterNodeCount*len(groupInfo.Zones)), currentCount,
			"expected cluster node count does not match with actual node count")
	}

	// Validate when timeout is given as 0, API does not error out.
	err = driver.SetInstanceGroupSize(groupInfo.Name, clusterNodeCount+1, 0)
	if err != nil {
		_, ok := err.(*cloudops.ErrNotSupported)
		if !ok {
			t.Errorf("failed to set node count. Error:[%v]", err)
		}
	} else {
		// Validate GetInstanceGroupSize() only if set operation is successful
		// Wait for count to get updated for an instance group
		expectedNodeCount := (clusterNodeCount + 1) * int64(len(groupInfo.Zones))
		f := func() (interface{}, bool, error) {
			currentCount, err := driver.GetInstanceGroupSize(groupInfo.Name)
			if err != nil {
				_, ok := err.(*cloudops.ErrNotSupported)
				if !ok {
					// Some err occured, retry
					return nil, true, err
				}
				// If operation not supported by cloud-driver
				// Ignore the error and don't retry
				return nil, false, nil
			}

			if currentCount == expectedNodeCount {
				return nil, false, nil
			}

			return nil,
				true,
				fmt.Errorf("cluster node count of [%s] does not match. Expected: [%v], Actual:[%v]. Waiting",
					groupInfo.Name,
					expectedNodeCount,
					currentCount)
		}

		_, err = task.DoRetryWithTimeout(f, timeoutMinutes*time.Minute, retrySeconds*time.Second)
		require.NoErrorf(t, err, fmt.Sprintf("error occured while getting cluster size after being set with 0 timeout. Error:[%v]", err))
	}

}

func create(t *testing.T, driver cloudops.Ops, template interface{}) interface{} {
	d, err := driver.Create(template, nil)
	require.NoError(t, err, "failed to create disk")
	require.NotNil(t, d, "got nil disk from create api")

	return d
}

func id(t *testing.T, driver cloudops.Ops, disk interface{}) string {
	id, err := driver.GetDeviceID(disk)
	require.NoError(t, err, "failed to get disk ID")
	require.NotEmpty(t, id, "got empty disk name/ID")
	return id
}

func snapshot(t *testing.T, driver cloudops.Ops, diskName string) {
	snap, err := driver.Snapshot(diskName, true)
	if _, typeOk := err.(*cloudops.ErrNotSupported); typeOk {
		return
	}

	require.NoError(t, err, "failed to create snapshot")
	require.NotEmpty(t, snap, "got empty snapshot from create API")

	snapID, err := driver.GetDeviceID(snap)
	require.NoError(t, err, "failed to get snapshot ID")
	require.NotEmpty(t, snapID, "got empty snapshot name/ID")

	err = driver.SnapshotDelete(snapID)
	require.NoError(t, err, "failed to delete snapshot")
}

func tags(t *testing.T, driver cloudops.Ops, diskName string) {
	err := driver.ApplyTags(diskName, diskLabels)
	if _, typeOk := err.(*cloudops.ErrNotSupported); typeOk {
		return
	}

	require.NoError(t, err, "failed to apply tags to disk")

	tags, err := driver.Tags(diskName)
	require.NoError(t, err, "failed to get tags for disk")
	require.Len(t, tags, 3, "invalid number of labels found on disk")

	err = driver.RemoveTags(diskName, diskLabels)
	require.NoError(t, err, "failed to remove tags from disk")

	tags, err = driver.Tags(diskName)
	require.NoError(t, err, "failed to get tags for disk")
	require.Len(t, tags, 0, "invalid number of labels found on disk")

	err = driver.ApplyTags(diskName, diskLabels)
	require.NoError(t, err, "failed to apply tags to disk")
}

func enumerate(t *testing.T, driver cloudops.Ops, diskName string) {
	disks, err := driver.Enumerate([]*string{&diskName}, diskLabels, cloudops.SetIdentifierNone)
	if _, typeOk := err.(*cloudops.ErrNotSupported); typeOk {
		return
	}

	require.NoError(t, err, "failed to enumerate disk")
	require.Len(t, disks, 1, "enumerate returned invalid length")
	require.Len(t, disks[cloudops.SetIdentifierNone], 1, "enumerate returned invalid length")

	// enumerate with invalid labels
	randomStr := uuid.New()
	randomStr = strings.Replace(randomStr, "-", "", -1)
	invalidLabels := map[string]string{
		fmt.Sprintf("key%s", randomStr): fmt.Sprintf("val%s", randomStr),
	}
	disks, err = driver.Enumerate([]*string{&diskName}, invalidLabels, cloudops.SetIdentifierNone)
	require.NoError(t, err, "failed to enumerate disk")
	require.Len(t, disks, 0, "enumerate returned invalid length")
}

func inspect(t *testing.T, driver cloudops.Ops, diskName string) {
	disks, err := driver.Inspect([]*string{&diskName})
	if _, typeOk := err.(*cloudops.ErrNotSupported); typeOk {
		return
	}

	require.NoError(t, err, "failed to inspect disk")
	require.Len(t, disks, 1, fmt.Sprintf("inspect returned invalid length: %d", len(disks)))
}

func attach(t *testing.T, driver cloudops.Ops, diskName string) {
	devPath, err := driver.Attach(diskName)
	if err != nil && canErrBeIgnored(err) {
		// don't check devPath
	} else {
		require.NoError(t, err, "failed to attach disk")
		require.NotEmpty(t, devPath, "disk attach returned empty devicePath")
	}

	mappings, err := driver.DeviceMappings()
	if err != nil && canErrBeIgnored(err) {
		// don't check mappings
	} else {
		require.NoError(t, err, "failed to get device mappings")
		require.NotEmpty(t, mappings, "received empty device mappings")
	}

	err = driver.DetachFrom(diskName, driver.InstanceID())
	require.NoError(t, err, "disk DetachFrom returned error")

	time.Sleep(3 * time.Second)

	devPath, err = driver.Attach(diskName)
	if err != nil && canErrBeIgnored(err) {
		// don't check devPath
	} else {
		require.NoError(t, err, "failed to attach disk")
		require.NotEmpty(t, devPath, "disk attach returned empty devicePath")
	}

	mappings, err = driver.DeviceMappings()
	if err != nil && canErrBeIgnored(err) {
		// don't check mappings
	} else {
		require.NoError(t, err, "failed to get device mappings")
		require.NotEmpty(t, mappings, "received empty device mappings")
	}
}

func devicePath(t *testing.T, driver cloudops.Ops, diskName string) {
	devPath, err := driver.DevicePath(diskName)
	if err != nil && canErrBeIgnored(err) {
		// don't check devPath
	} else {
		require.NoError(t, err, "get device path returned error")
		require.NotEmpty(t, devPath, "received empty devicePath")
	}
}

func teardown(t *testing.T, driver cloudops.Ops, diskID string) {
	err := driver.Detach(diskID)
	require.NoError(t, err, "disk detach returned error")

	time.Sleep(3 * time.Second)

	err = driver.Delete(diskID)
	require.NoError(t, err, "failed to delete disk")
}

func canErrBeIgnored(err error) bool {
	if err == nil {
		return false
	}

	if strings.Contains(err.Error(), "no such file or directory") ||
		strings.Contains(err.Error(), "unable to map volume") {
		logrus.Infof("ignoring err: %v as it's expected when test is not running on the actual instance", err)
		return true
	}

	return false

}