/*
Copyright 2021 The Kubernetes Authors.

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

// Package state manages the internal state of the driver which needs to be maintained
// across driver restarts.
package state

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type AccessType int

const (
	MountAccess AccessType = iota
	BlockAccess

	// BlockSizeBytes represents the default block size.
	BlockSizeBytes = 4096
)

type LocalVolume struct {
	ReadOnlyAttach bool
	Attached       bool
	// Staged contains the staging target path at which the volume
	// was staged. A set of paths is used for consistency
	// with Published.
	Staged Strings
	// Published contains the target paths where the volume
	// was published.
	Published Strings
}

type Volume struct {
	VolName       string
	VolID         string
	VolSize       int64
	VolPath       string
	VolAccessType AccessType
	ParentVolID   string
	ParentSnapID  string
	Ephemeral     bool
	NodeID        string
	Kind          string
	LocalVolume
}

type Snapshot struct {
	Name            string
	Id              string
	VolID           string
	Path            string
	CreationTime    *timestamppb.Timestamp
	SizeBytes       int64
	ReadyToUse      bool
	GroupSnapshotID string
}

type GroupSnapshot struct {
	Name            string
	Id              string
	SnapshotIDs     []string
	SourceVolumeIDs []string
	CreationTime    *timestamppb.Timestamp
	ReadyToUse      bool
}

// State is the interface that the rest of the code has to use to
// access and change state. All error messages contain gRPC
// status codes and can be returned without wrapping.
type State interface {
	// GetVolumeByID retrieves a volume by its unique ID or returns
	// an error including that ID when not found.
	GetVolumeByID(volID string) (Volume, error)

	// GetVolumeByName retrieves a volume by its name or returns
	// an error including that name when not found.
	GetVolumeByName(volName string) (Volume, error)

	// GetVolumes returns all currently existing volumes.
	GetVolumes() []Volume

	// UpdateVolume updates the existing hostpath volume,
	// identified by its volume ID, or adds it if it does
	// not exist yet.
	UpdateVolume(volume Volume) error

	// DeleteVolume deletes the volume with the given
	// volume ID. It is not an error when such a volume
	// does not exist.
	DeleteVolume(volID string) error

	// GetSnapshotByID retrieves a snapshot by its unique ID or returns
	// an error including that ID when not found.
	GetSnapshotByID(snapshotID string) (Snapshot, error)

	// GetSnapshotByName retrieves a snapshot by its name or returns
	// an error including that name when not found.
	GetSnapshotByName(volName string) (Snapshot, error)

	// GetSnapshots returns all currently existing snapshots.
	GetSnapshots() []Snapshot

	// UpdateSnapshot updates the existing hostpath snapshot,
	// identified by its snapshot ID, or adds it if it does
	// not exist yet.
	UpdateSnapshot(snapshot Snapshot) error

	// DeleteSnapshot deletes the snapshot with the given
	// snapshot ID. It is not an error when such a snapshot
	// does not exist.
	DeleteSnapshot(snapshotID string) error

	// GetGroupSnapshotByID retrieves a groupsnapshot by its unique ID or
	// returns an error including that ID when not found.
	GetGroupSnapshotByID(vgsID string) (GroupSnapshot, error)

	// GetGroupSnapshotByName retrieves a groupsnapshot by its name or
	// returns an error including that name when not found.
	GetGroupSnapshotByName(volName string) (GroupSnapshot, error)

	// GetGroupSnapshots returns all currently existing groupsnapshots.
	GetGroupSnapshots() []GroupSnapshot

	// UpdateGroupSnapshot updates the existing hostpath groupsnapshot,
	// identified by its snapshot ID, or adds it if it does not exist yet.
	UpdateGroupSnapshot(snapshot GroupSnapshot) error

	// DeleteGroupSnapshot deletes the groupsnapshot with the given
	// groupsnapshot ID. It is not an error when such a groupsnapshot does
	// not exist.
	DeleteGroupSnapshot(groupSnapshotID string) error
}

type resources struct {
	Volumes        []Volume
	Snapshots      []Snapshot
	GroupSnapshots []GroupSnapshot
}

type localResources map[string]LocalVolume

type state struct {
	resources
	localResources

	statefilePath      string
	modTime            time.Time
	localStatefilePath string
	nodeID             string
}

var _ State = &state{}

// New retrieves the complete state of the driver from the file if given
// and then ensures that all changes are mirrored immediately in the
// given file. If not given, the initial state is empty and changes
// are not saved.
func New(statefilePath string, localStatefilePath string, nodeID string) (State, error) {
	s := &state{
		statefilePath:      statefilePath,
		localStatefilePath: localStatefilePath,
		nodeID:             nodeID,
		localResources:     make(localResources),
	}

	return s, s.restore()
}

func (s *state) dump() (errFn error) {
	state, err := os.OpenFile(s.statefilePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return status.Errorf(codes.Internal, "error opening state file: %v", err)
	}
	defer func() {
		if errFn == nil {
			errFn = state.Close()
		} else {
			_ = state.Close()
		}
	}()

	localState, err := os.OpenFile(s.localStatefilePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return status.Errorf(codes.Internal, "error opening local state file: %v", err)
	}
	defer func() {
		if errFn == nil {
			errFn = localState.Close()
		} else {
			_ = localState.Close()
		}
	}()

	err = unix.Flock(int(state.Fd()), unix.LOCK_EX)
	if err != nil {
		return status.Errorf(codes.Internal, "error locking state file: %v", err)
	}
	defer func() {
		_ = unix.Flock(int(state.Fd()), unix.LOCK_UN)
	}()

	// only truncate the file while holding the lock
	if err := state.Truncate(0); err != nil {
		return status.Errorf(codes.Internal, "error truncating state file: %v", err)
	}

	if err := json.NewEncoder(state).Encode(&s.resources); err != nil {
		return status.Errorf(codes.Internal, "error encoding volumes and snapshots: %v", err)
	}

	if err := localState.Truncate(0); err != nil {
		return status.Errorf(codes.Internal, "error truncating local state file: %v", err)
	}

	if err := json.NewEncoder(localState).Encode(&s.localResources); err != nil {
		return status.Errorf(codes.Internal, "error encoding local resources: %v", err)
	}

	return nil
}

func (s *state) restore() error {
	s.Volumes = nil
	s.Snapshots = nil
	s.GroupSnapshots = nil

	state, err := os.OpenFile(s.statefilePath, os.O_RDONLY, 0600)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Nothing to do.
			return nil
		}
		return status.Errorf(codes.Internal, "error reading state file: %v", err)
	}
	defer state.Close()

	localState, err := os.OpenFile(s.localStatefilePath, os.O_RDONLY, 0600)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return status.Errorf(codes.Internal, "error reading local state file: %v", err)
		}
	} else {
		defer localState.Close()
	}

	if err := unix.Flock(int(state.Fd()), unix.LOCK_SH); err != nil {
		return status.Errorf(codes.Internal, "error locking state file: %v", err)
	}
	defer unix.Flock(int(state.Fd()), unix.LOCK_UN)

	if err := json.NewDecoder(state).Decode(&s.resources); err != nil {
		return status.Errorf(codes.Internal, "error decoding volumes and snapshots from state file %q: %v", s.statefilePath, err)
	}

	if localState != nil {
		if err := json.NewDecoder(localState).Decode(&s.localResources); err != nil {
			return status.Errorf(codes.Internal, "error decoding local resources from state file %q: %v", s.localStatefilePath, err)
		}
	}

	for i, vol := range s.Volumes {
		if localVolume, ok := s.localResources[vol.VolID]; ok {
			s.Volumes[i].LocalVolume = localVolume
		}
		s.Volumes[i].NodeID = s.nodeID
	}

	return nil
}

func (s *state) reload() error {
	st, err := os.Stat(s.statefilePath) // ensure the file exists
	if os.IsNotExist(err) {
		// Nothing to reload.
		return nil
	} else if err != nil {
		return status.Errorf(codes.Internal, "error getting state file status: %v", err)
	}

	if s.modTime.IsZero() || !s.modTime.Equal(st.ModTime()) {
		s.modTime = st.ModTime()

		for range 10 {
			if err := s.restore(); err == nil {
				return nil
			}
			// If we fail to restore, wait a bit and try again.
			time.Sleep(100 * time.Millisecond)
		}
	}

	// No changes, nothing to do.
	return nil
}

func (s *state) GetVolumeByID(volID string) (Volume, error) {
	if err := s.reload(); err != nil {
		return Volume{}, err
	}

	for _, volume := range s.Volumes {
		if volume.VolID == volID {
			return volume, nil
		}
	}
	return Volume{}, status.Errorf(codes.NotFound, "volume id %s does not exist in the volumes list", volID)
}

func (s *state) GetVolumeByName(volName string) (Volume, error) {
	if err := s.reload(); err != nil {
		return Volume{}, err
	}

	for _, volume := range s.Volumes {
		if volume.VolName == volName {
			return volume, nil
		}
	}
	return Volume{}, status.Errorf(codes.NotFound, "volume name %s does not exist in the volumes list", volName)
}

func (s *state) GetVolumes() []Volume {
	_ = s.reload()

	volumes := make([]Volume, len(s.Volumes))
	copy(volumes, s.Volumes)
	return volumes
}

func (s *state) UpdateVolume(update Volume) error {
	for i, volume := range s.Volumes {
		if volume.VolID == update.VolID {
			s.Volumes[i] = update
			s.localResources[update.VolID] = update.LocalVolume
			return s.dump()
		}
	}
	s.Volumes = append(s.Volumes, update)
	s.localResources[update.VolID] = update.LocalVolume
	return s.dump()
}

func (s *state) DeleteVolume(volID string) error {
	for i, volume := range s.Volumes {
		if volume.VolID == volID {
			delete(s.localResources, volID)
			s.Volumes = append(s.Volumes[:i], s.Volumes[i+1:]...)
			return s.dump()
		}
	}
	return nil
}

func (s *state) GetSnapshotByID(snapshotID string) (Snapshot, error) {
	if err := s.reload(); err != nil {
		return Snapshot{}, err
	}

	for _, snapshot := range s.Snapshots {
		if snapshot.Id == snapshotID {
			return snapshot, nil
		}
	}
	return Snapshot{}, status.Errorf(codes.NotFound, "snapshot id %s does not exist in the snapshots list", snapshotID)
}

func (s *state) GetSnapshotByName(name string) (Snapshot, error) {
	if err := s.reload(); err != nil {
		return Snapshot{}, err
	}

	for _, snapshot := range s.Snapshots {
		if snapshot.Name == name {
			return snapshot, nil
		}
	}
	return Snapshot{}, status.Errorf(codes.NotFound, "snapshot name %s does not exist in the snapshots list", name)
}

func (s *state) GetSnapshots() []Snapshot {
	_ = s.reload()

	snapshots := make([]Snapshot, len(s.Snapshots))
	copy(snapshots, s.Snapshots)
	return snapshots
}

func (s *state) UpdateSnapshot(update Snapshot) error {
	for i, snapshot := range s.Snapshots {
		if snapshot.Id == update.Id {
			s.Snapshots[i] = update
			return s.dump()
		}
	}
	s.Snapshots = append(s.Snapshots, update)
	return s.dump()
}

func (s *state) DeleteSnapshot(snapshotID string) error {
	for i, snapshot := range s.Snapshots {
		if snapshot.Id == snapshotID {
			s.Snapshots = append(s.Snapshots[:i], s.Snapshots[i+1:]...)
			return s.dump()
		}
	}
	return nil
}

func (s *state) GetGroupSnapshotByID(groupSnapshotID string) (GroupSnapshot, error) {
	if err := s.reload(); err != nil {
		return GroupSnapshot{}, err
	}

	for _, groupSnapshot := range s.GroupSnapshots {
		if groupSnapshot.Id == groupSnapshotID {
			return groupSnapshot, nil
		}
	}
	return GroupSnapshot{}, status.Errorf(codes.NotFound, "groupsnapshot id %s does not exist in the groupsnapshots list", groupSnapshotID)
}

func (s *state) GetGroupSnapshotByName(name string) (GroupSnapshot, error) {
	if err := s.reload(); err != nil {
		return GroupSnapshot{}, err
	}

	for _, groupSnapshot := range s.GroupSnapshots {
		if groupSnapshot.Name == name {
			return groupSnapshot, nil
		}
	}
	return GroupSnapshot{}, status.Errorf(codes.NotFound, "groupsnapshot name %s does not exist in the groupsnapshots list", name)
}

func (s *state) GetGroupSnapshots() []GroupSnapshot {
	_ = s.reload()

	groupSnapshots := make([]GroupSnapshot, len(s.GroupSnapshots))
	copy(groupSnapshots, s.GroupSnapshots)
	return groupSnapshots
}

func (s *state) UpdateGroupSnapshot(update GroupSnapshot) error {
	for i, groupSnapshot := range s.GroupSnapshots {
		if groupSnapshot.Id == update.Id {
			s.GroupSnapshots[i] = update
			return s.dump()
		}
	}
	s.GroupSnapshots = append(s.GroupSnapshots, update)
	return s.dump()
}

func (s *state) DeleteGroupSnapshot(groupSnapshotID string) error {
	for i, groupSnapshot := range s.GroupSnapshots {
		if groupSnapshot.Id == groupSnapshotID {
			s.GroupSnapshots = append(s.GroupSnapshots[:i], s.GroupSnapshots[i+1:]...)
			return s.dump()
		}
	}
	return nil
}

func (gs *GroupSnapshot) MatchesSourceVolumeIDs(sourceVolumeIDs []string) bool {
	return equalIDs(gs.SourceVolumeIDs, sourceVolumeIDs)
}

func (gs *GroupSnapshot) MatchesSnapshotIDs(snapshotIDs []string) bool {
	return equalIDs(gs.SnapshotIDs, snapshotIDs)
}

func equalIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	// sort slices so that values are at the same location
	sort.Strings(a)
	sort.Strings(b)

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}
