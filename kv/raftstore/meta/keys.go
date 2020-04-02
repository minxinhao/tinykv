package meta

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap/errors"
)

const (
	// local is in (0x01, 0x02)
	LocalPrefix byte = 0x01

	// We save two types region data in DB, for raft and other meta data.
	// When the store starts, we should iterate all region meta data to
	// construct peer, no need to travel large raft data, so we separate them
	// with different prefixes.
	RegionRaftPrefix    byte = 0x02
	RegionMetaPrefix    byte = 0x03
	RegionRaftPrefixLen      = 11 // REGION_RAFT_PREFIX_KEY + region_id + suffix
	RegionRaftLogLen         = 19 // REGION_RAFT_PREFIX_KEY + region_id + suffix + index

	// Following are the suffix after the local prefix.
	// For region id
	RaftLogSuffix    byte = 0x01
	RaftStateSuffix  byte = 0x02
	ApplyStateSuffix byte = 0x03

	// For region meta
	RegionStateSuffix byte = 0x01
)

var (
	MinKey           = []byte{}
	MaxKey           = []byte{255}
	LocalMinKey      = []byte{LocalPrefix}
	LocalMaxKey      = []byte{LocalPrefix + 1}
	RegionMetaMinKey = []byte{LocalPrefix, RegionMetaPrefix}
	RegionMetaMaxKey = []byte{LocalPrefix, RegionMetaPrefix + 1}

	// Following keys are all local keys, so the first byte must be 0x01.
	PrepareBootstrapKey = []byte{LocalPrefix, 0x01}
	StoreIdentKey       = []byte{LocalPrefix, 0x02}
)

func makeRegionPrefix(regionID uint64, suffix byte) []byte {
	key := make([]byte, 11)
	key[0] = LocalPrefix
	key[1] = RegionRaftPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	key[10] = suffix
	return key
}

func makeRegionKey(regionID uint64, suffix byte, subID uint64) []byte {
	key := make([]byte, 19)
	key[0] = LocalPrefix
	key[1] = RegionRaftPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	key[10] = suffix
	binary.BigEndian.PutUint64(key[11:], subID)
	return key
}

func RegionRaftPrefixKey(regionID uint64) []byte {
	key := make([]byte, 10)
	key[0] = LocalPrefix
	key[1] = RegionRaftPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	return key
}

// Returns the key for raftLog at index of regionID
func RaftLogKey(regionID, index uint64) []byte {
	return makeRegionKey(regionID, RaftLogSuffix, index)
}

// Returns the key for RaftState of regionID
func RaftStateKey(regionID uint64) []byte {
	return makeRegionPrefix(regionID, RaftStateSuffix)
}

// Returns the key for ApplyState of regionID
func ApplyStateKey(regionID uint64) []byte {
	return makeRegionPrefix(regionID, ApplyStateSuffix)
}

func IsRaftStateKey(key []byte) bool {
	return len(key) == 11 && key[0] == LocalPrefix && key[1] == RegionRaftPrefix
}

// Read the region ID from a key encoded according agreed format
func DecodeRegionMetaKey(key []byte) (uint64, byte, error) {
	if len(RegionMetaMinKey)+8+1 != len(key) {
		return 0, 0, errors.Errorf("invalid region meta key length for key %v", key)
	}
	if !bytes.HasPrefix(key, RegionMetaMinKey) {
		return 0, 0, errors.Errorf("invalid region meta key prefix for key %v", key)
	}
	regionID := binary.BigEndian.Uint64(key[len(RegionMetaMinKey):])
	return regionID, key[len(key)-1], nil
}

// Return the result of encapsulating the regionID according to the agreed format without specified suffix
// Other help function can add corresponding suffix to get specifed encapsulated key
func RegionMetaPrefixKey(regionID uint64) []byte {
	key := make([]byte, 10)
	key[0] = LocalPrefix
	key[1] = RegionMetaPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	return key
}

// Return RegionStateKey of the regionID according to the agreed format
func RegionStateKey(regionID uint64) []byte {
	key := make([]byte, 11)
	key[0] = LocalPrefix
	key[1] = RegionMetaPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	key[10] = RegionStateSuffix
	return key
}

/// RaftLogIndex gets the log index from raft log key generated by `raft_log_key`.
func RaftLogIndex(key []byte) (uint64, error) {
	if len(key) != RegionRaftLogLen {
		return 0, errors.Errorf("key %v is not a valid raft log key", key)
	}
	return binary.BigEndian.Uint64(key[RegionRaftLogLen-8:]), nil
}
