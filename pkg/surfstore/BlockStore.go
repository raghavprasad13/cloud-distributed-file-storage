package surfstore

import (
	context "context"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// TODO: Figure out why ctx is needed
	return bs.BlockMap[blockHash.Hash], nil
	// panic("todo")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// TODO: Figure out why ctx is needed
	blockHash := GetBlockHashString(block.BlockData)
	bs.BlockMap[blockHash] = block

	return &Success{Flag: true}, nil
	// panic("todo")
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// TODO: Figure out why ctx is needed
	var blockHashesOut BlockHashes
	var hashesOut = make([]string, 0)
	for _, hash := range blockHashesIn.Hashes {
		if _, blockExists := bs.BlockMap[hash]; blockExists {
			hashesOut = append(hashesOut, hash)
		}
	}

	blockHashesOut.Hashes = hashesOut
	return &blockHashesOut, nil
	// panic("todo")
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
