package surfstore

import (
	context "context"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	Mutex    *sync.RWMutex
	UnimplementedBlockStoreServer
}

// Implement BlockStore Interface's methods
// Retrieves a block indexed by hash value h
func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	if blockVal, ok := bs.BlockMap[blockHash.GetHash()]; ok {
		return blockVal, nil
	}
	return nil, ctx.Err()
}

// Stores block b in the key-value store, indexed by hash value h
// add support for concurrency
func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// First, get hash of the block
	if block == nil {
		return &Success{Flag: false}, ctx.Err()
	}
	hashString := GetBlockHashString(block.GetBlockData())
	// Put the new entry into map
	bs.BlockMap[hashString] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// First, get the hash list
	if blockHashesIn == nil {
		return nil, ctx.Err()
	}
	hashList := blockHashesIn.GetHashes()
	// init output list
	blockExist := make([]string, len(hashList))
	for _, hash := range hashList {
		if _, ok := bs.BlockMap[hash]; ok {
			blockExist = append(blockExist, hash)
		}
	}
	return &BlockHashes{Hashes: blockExist}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
