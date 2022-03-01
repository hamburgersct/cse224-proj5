package surfstore

import (
	context "context"
	"errors"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	Mutex          *sync.RWMutex
	UnimplementedMetaStoreServer
}

// Implement MetaStore Interface's methods
func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	if m.FileMetaMap == nil {
		return nil, ctx.Err()
	}
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

// Updates the FileInfo values associated with a file stored in the cloud.
// This method replaces the hash list for the file with the provided hash list only if the new version number is exactly one greater than the current version number.
// Otherwise, an error is sent to the client telling them that the version they are trying to store is not right (likely too old) as well as the current value of the file’s version on the server.
func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// Get current metadata
	var currMetadata *FileMetaData
	filename := fileMetaData.GetFilename()
	if metadata, ok := m.FileMetaMap[filename]; ok {
		currMetadata = metadata
		// Check version
		if fileMetaData.GetVersion()-currMetadata.GetVersion() == 1 {
			m.FileMetaMap[filename] = fileMetaData
			return &Version{Version: fileMetaData.GetVersion()}, nil
		} else {
			return nil, errors.New("incorrect version")
		}
	} else {
		m.FileMetaMap[filename] = fileMetaData
		return &Version{Version: fileMetaData.GetVersion()}, nil
	}
}

// Returns the BlockStore address.
func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
