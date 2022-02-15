package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	mu             sync.Mutex
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	var fim FileInfoMap
	fim.FileInfoMap = m.FileMetaMap

	return &fim, nil
	// panic("todo")
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.FileMetaMap[fileMetaData.Filename]; exists {
		if m.FileMetaMap[fileMetaData.Filename].GetVersion() >= fileMetaData.GetVersion() {
			return &Version{Version: int32(-1)}, nil
		}
	}
	m.FileMetaMap[fileMetaData.Filename] = fileMetaData

	return &Version{Version: fileMetaData.GetVersion()}, nil
	// panic("todo")
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
	// panic("todo")
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
