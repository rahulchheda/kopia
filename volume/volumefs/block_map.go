package volumefs

import (
	"sync"

	"github.com/google/btree"

	"github.com/kopia/kopia/repo/object"
)

// BlockMap provides functionality to look up and traverse block addresses.
// It is thread safe as long as the underlying block map is not modified.
type BlockMap interface {
	Find(blockAddr int64) object.ID
	Iterator() BlockMapIterator
}

// BlockAddressMapping contains the address to OID tuple.
type BlockAddressMapping struct {
	BlockAddr int64
	Oid       object.ID
}

// BlockMapIterator is used to traverse a sequence of block address mappings.
type BlockMapIterator interface {
	// Next returns the next block address mapping.
	// It returns BlockAddressMapping{} when exhausted.
	Next() BlockAddressMapping
	// Close is used to terminate the iterator and release resources.
	Close()
}

// blockMapMutable is a mutable block address map.
// Modification operations are not thread safe.
type blockMapMutable interface {
	BlockMap
	InsertOrReplace(bam BlockAddressMapping)
}

// blockMapIter is an iterator helper
type blockMapIter struct {
	mux      sync.Mutex
	isClosed bool
	stopChan chan struct{}
	mChan    chan BlockAddressMapping
}

func (bmi *blockMapIter) init() {
	bmi.stopChan = make(chan struct{})
	bmi.mChan = make(chan BlockAddressMapping)
}

func (bmi *blockMapIter) Next() BlockAddressMapping {
	return <-bmi.mChan
}

// trySend attempts to send on the channel and returns false when stopped.
// Caller _must_ close the mChan to indicate end of source data.
func (bmi *blockMapIter) trySend(bam BlockAddressMapping) bool {
	select {
	case <-bmi.stopChan:
		return false
	case bmi.mChan <- bam:
		return true
	}
}

func (bmi *blockMapIter) Close() {
	bmi.mux.Lock()

	if !bmi.isClosed {
		bmi.isClosed = true

		close(bmi.stopChan)
	}

	bmi.mux.Unlock()

	for {
		bam := <-bmi.mChan
		if bam.Oid == "" {
			break
		}
	}
}

// BTree map supports sorted traversal
func newBTreeMap(order int) blockMapMutable {
	btm := &bTreeMap{}
	btm.tree = btree.New(order)

	return btm
}

type bTreeMap struct {
	tree *btree.BTree
}

var _ BlockMap = (*bTreeMap)(nil)

type bTreeItem struct {
	BlockAddressMapping
}

// Less satisfies btree.Item
func (item *bTreeItem) Less(than btree.Item) bool {
	thanItem := than.(*bTreeItem)
	return item.BlockAddr < thanItem.BlockAddr
}

func (btm *bTreeMap) InsertOrReplace(bam BlockAddressMapping) {
	item := &bTreeItem{BlockAddressMapping: bam}
	btm.tree.ReplaceOrInsert(item)
}

func (btm *bTreeMap) Find(blockAddr int64) object.ID {
	key := &bTreeItem{}
	key.BlockAddr = blockAddr

	if item := btm.tree.Get(key); item != nil {
		val := item.(*bTreeItem)

		return val.Oid
	}

	return ""
}

func (btm *bTreeMap) Iterator() BlockMapIterator {
	bmi := &bTreeIter{}
	bmi.init()

	go func() {
		btm.tree.Ascend(bmi.dispatch)
		close(bmi.mChan)
	}()

	return bmi
}

type bTreeIter struct {
	blockMapIter
}

func (bmi *bTreeIter) dispatch(i btree.Item) bool {
	item := i.(*bTreeItem)
	return bmi.trySend(item.BlockAddressMapping)
}

// HashMap is simplistic: does not have a sorted iterator
// func newHashMap(order int) blockMapMutable {
// 	hm := &hashMap{}
// 	hm.m = make(map[int64]object.ID)
// 	return hm
// }

// type hashMap struct {
// 	m map[int64]object.ID
// }

// func (hm *hashMap) InsertOrReplace(bam BlockAddressMapping) {
// 	hm.m[bam.BlockAddr] = bam.Oid
// }

// func (hm *hashMap) Find(blockAddr int64) object.ID {
// 	return hm.m[blockAddr]
// }

// func (hm *hashMap) Iterator() BlockMapIterator {
// 	bmi := &blockMapIter{}
// 	bmi.init()
// 	go func() {
// 		for addr, oid := range hm.m { // unsorted
// 			bam := BlockAddressMapping{addr, oid}
// 			select {
// 			case <-bmi.stopChan:
// 				return
// 			case bmi.mChan <- bam:
// 			}
// 		}
// 		close(bmi.mChan)
// 	}()
// 	return bmi
// }

// createTreeFromBlockMap creates an in-memory hierarchy from a block map and returns its root.
// Directories are not written to the repo.
func (f *Filesystem) createTreeFromBlockMap(bm BlockMap) (*dirMeta, error) {
	bi := bm.Iterator()
	defer bi.Close()

	mapRootDm := &dirMeta{
		name: currentSnapshotDirName,
	}
	emptyBam := BlockAddressMapping{}

	for bam := bi.Next(); bam != emptyBam; bam = bi.Next() {
		pp, err := f.addrToPath(bam.BlockAddr)
		if err != nil {
			return nil, err
		}

		fm := f.ensureFileInTree(mapRootDm, pp)
		fm.oid = bam.Oid

		f.logger.Debugf("block [%s] %s", pp.String(), bam.Oid)
	}

	return mapRootDm, nil
}
