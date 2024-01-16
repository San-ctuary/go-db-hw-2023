package godb

import (
	"fmt"
	"sync"
	"time"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

var permMap = map[RWPerm]string{
	ReadPerm:  "ReadPerm",
	WritePerm: "WritePerm",
}

type LockType int

const printTid = 0
const (
	WriteState LockType = iota - 1
	InitState
	ReadState
)

type lockInfo struct {
	mu        sync.RWMutex
	lockState LockType
	mp        map[TransactionID]any
}

func (l *lockInfo) unlockByType(tid TransactionID) {
	// 这里要判断page的lockinfo是否和当前tid有关， 没关系直接跳过， 这里卡了很久
	if _, ok := l.mp[tid]; !ok {
		return
	}
	switch l.lockState {
	case WriteState:
		l.lockState = InitState
		l.mu.Unlock()
	case InitState:
	default:
		fmt.Printf("lockstate is %v", l.lockState)
		l.lockState -= 1
		l.mu.RUnlock()
	}
}

type BufferPool struct {
	// TODO: some code goes here
	mapPage   map[any]*Page
	dbfile    DBFile
	numPages  int
	mu        sync.Mutex
	abortmu   sync.Mutex
	tidMap    map[TransactionID][]int
	lockmap   map[int]*lockInfo
	waitGraph map[TransactionID][]TransactionID
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	// TODO: some code goes here
	return &BufferPool{
		mapPage:   make(map[any]*Page, numPages),
		numPages:  numPages,
		tidMap:    make(map[TransactionID][]int),
		lockmap:   make(map[int]*lockInfo),
		waitGraph: make(map[TransactionID][]TransactionID),
	}
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	// TODO: some code goes here
	// 这里是需要实现的， 文档中未告知
	for _, page := range bp.mapPage {
		if (*page).isDirty() {
			bp.dbfile.flushPage(page)
		}
	}
}

func printMapKeys(mp map[TransactionID]any) {
	for key := range mp {
		fmt.Printf("%v ", *key)
	}
	fmt.Println()
}

func printMap(mp map[TransactionID][]TransactionID) {
	fmt.Println("------wait graph:-------")
	for k, v := range mp {
		fmt.Printf("%v:", *k)
		for _, value := range v {
			fmt.Printf("%v ", *value)
		}
		fmt.Println()
	}
	fmt.Println("------wait end-------")
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here
	//bp.abortmu.Lock()
	bp.mu.Lock()
	fmt.Println("abort transaction: ", *tid)
	printMap(bp.waitGraph)
	pages := bp.tidMap[tid]
	for _, pageId := range pages {
		pageKey := bp.dbfile.pageKey(pageId)
		delete(bp.mapPage, pageKey)
		lInfo := bp.lockmap[pageId]
		lInfo.unlockByType(tid)
		delete(lInfo.mp, tid)
	}
	delete(bp.tidMap, tid)
	bp.delEdges(tid)
	//bp.abortmu.Unlock()
	bp.mu.Unlock()
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
	bp.mu.Lock()
	defer bp.mu.Unlock()
	pages := bp.tidMap[tid]
	for _, pageId := range pages {
		pageKey := bp.dbfile.pageKey(pageId)
		page, ok := bp.mapPage[pageKey]
		if ok && (*page).isDirty() {
			bp.dbfile.flushPage(page)
			(*page).setDirty(false)
		}
		lInfo := bp.lockmap[pageId]
		lInfo.unlockByType(tid)
		delete(lInfo.mp, tid)
	}
	delete(bp.tidMap, tid)
	bp.delEdges(tid)

}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	// TODO: some code goes here
	for !bp.mu.TryLock() {
		time.Sleep(time.Millisecond * 100)
		//fmt.Printf("tid:%v try to get buffer pool mu\n", *tid)
	}
	//fmt.Printf("tid:%v success get buffer pool mu perm is %s\n", *tid, permMap[perm])
	pages, ok := bp.tidMap[tid]
	if !ok {
		pages = []int{pageNo}
	} else {
		pages = append(pages, pageNo)
	}
	bp.tidMap[tid] = pages
	lInfo, ok := bp.lockmap[pageNo]
	if !ok {
		bp.lockmap[pageNo] = &lockInfo{lockState: InitState, mp: make(map[TransactionID]any)}
		lInfo = bp.lockmap[pageNo]
	}

	switch perm {
	case ReadPerm:
		if *tid > printTid {
			fmt.Printf("tid:%v, try to get read lock on pageNum:%v, lockState:%v, linfo map:", *tid, pageNo, lInfo.lockState)
			printMapKeys(lInfo.mp)
		}
		if _, ok := lInfo.mp[tid]; !ok {
			if lInfo.lockState == WriteState {
				cnt := 0
				flag := false
				for !lInfo.mu.TryRLock() {
					if cnt == 0 {
						bp.addEdges(tid, lInfo.mp)
						flag = true
						bp.mu.Unlock()
					}
					cnt += 1
					time.Sleep(time.Millisecond * 100)
					fmt.Printf("tid: %v wait to get read lock on page:%v, run count:%v\n", *tid, pageNo, cnt)
					if bp.deadLockDetection(tid) {
						//bp.AbortTransaction(tid)
						fmt.Printf("find deadlock in tid:%v", *tid)
						return nil, GoDBError{DeadlockError, "dead lock occur"}
					}
				}

				if flag {
					bp.mu.Lock()
				}
				lInfo.mp[tid] = true
				fmt.Printf("tid:%v before lockState on page %v is %v\n", *tid, pageNo, lInfo.lockState)
				if lInfo.lockState < ReadState {
					lInfo.lockState = ReadState
				} else {
					lInfo.lockState += 1
				}
				//if *tid >= printTid {
				//	fmt.Printf("%v success get read lock on page:%v lockstate:%v\n", *tid, pageNo, lInfo.lockState)
				//}
			} else {
				lInfo.mu.RLock()
				lInfo.mp[tid] = true
				fmt.Printf("tid:%v before lockState on page %v is %v\n", *tid, pageNo, lInfo.lockState)
				if lInfo.lockState < ReadState {
					lInfo.lockState = ReadState
				} else {
					lInfo.lockState += 1
				}
			}
		}
		if *tid >= printTid {
			fmt.Printf("tid:%v success get read lock on page:%v lockstate:%v\n", *tid, pageNo, lInfo.lockState)
		}

	case WritePerm:
		if *tid > printTid {
			fmt.Printf("tid:%v, try to get write lock on pageNum:%v, lockState:%v, linfo map:", *tid, pageNo, lInfo.lockState)
			printMapKeys(lInfo.mp)
		}
		if _, ok := lInfo.mp[tid]; ok {
			if lInfo.lockState == ReadState {
				lInfo.mu.RUnlock()
				lInfo.lockState = WriteState
				fmt.Printf("tid:%v success upgrade read lock to write lock on page:%v, lockstate is %v\n", *tid, pageNo, lInfo.lockState)
				lInfo.mu.Lock()
			} else if lInfo.lockState > ReadState {
				cnt := 0
				flag := false
				for !lInfo.mu.TryLock() {
					if cnt == 0 {
						bp.addEdges(tid, lInfo.mp)
						flag = true
						bp.mu.Unlock()
					}
					if lInfo.lockState == ReadState {
						lInfo.mu.RUnlock()
					}
					cnt += 1
					time.Sleep(time.Millisecond * 100)
					fmt.Println(*tid, " try to get write lock")
					if bp.deadLockDetection(tid) {
						//bp.AbortTransaction(tid)
						fmt.Printf("find deadlock in tid:%v", *tid)

						return nil, GoDBError{DeadlockError, "deadlock occur"}
					}
				}

				if flag {
					bp.mu.Lock()
				}
				lInfo.lockState = WriteState
				//lInfo.mu.Lock()
				if *tid >= printTid {
					fmt.Printf("tid:%v success get write lock on page:%v, lockstate is %v\n", *tid, pageNo, lInfo.lockState)
				}
			}
		} else {
			cnt := 0
			flag := false
			for !lInfo.mu.TryLock() {
				if cnt == 0 {
					bp.addEdges(tid, lInfo.mp)
					flag = true
					bp.mu.Unlock()
				}
				cnt += 1
				fmt.Println(*tid, " try to get write lock")
				time.Sleep(time.Millisecond * 100)
				if bp.deadLockDetection(tid) {
					//bp.AbortTransaction(tid)
					fmt.Printf("find deadlock in tid:%v", *tid)

					return nil, GoDBError{DeadlockError, "dead lock occur"}
				}
			}

			if flag {
				bp.mu.Lock()
			}
			lInfo.lockState = WriteState
			lInfo.mp[tid] = true
			if *tid >= printTid {
				fmt.Printf("tid:%v success get write lock on page:%v, lockstate is %v\n", *tid, pageNo, lInfo.lockState)
			}
		}
	}
	bp.mu.Unlock()
	key := file.pageKey(pageNo)
	page, ok := bp.mapPage[key]

	if ok {
		return page, nil
	}
	// page not in cache
	page, err := file.readPage(pageNo)
	if err != nil {
		return nil, err
	}
	cnt := len(bp.mapPage)
	// delete a page if it is not dirty
	if cnt == bp.numPages {
		flag := true
		for key, p := range bp.mapPage {
			if !(*p).isDirty() {
				delete(bp.mapPage, key)
				flag = false
				break
			}
		}
		if flag {
			return nil, GoDBError{BufferPoolFullError, "buffer pool is full"}
		}
	}
	bp.mapPage[key] = page
	return page, err
}

func (bp *BufferPool) deadLockDetection(tid TransactionID) bool {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	f := make(map[int]int)
	for key, value := range bp.waitGraph {
		from := *key
		if _, ok := f[from]; !ok {
			f[from] = from
		}
		pa := find(from, f)
		for _, to := range value {
			next := *to
			if _, ok := f[next]; !ok {
				f[next] = next
			}
			pb := find(next, f)
			if pa == pb {
				fmt.Println("find deadlock in ", *tid)
				return true
			}
			f[pa] = pb
		}
	}
	return false
}

func find(x int, f map[int]int) int {
	if f[x] != x {
		f[x] = find(f[x], f)
	}
	return f[x]
}

// 获取map中所有key
func getValues(mp map[TransactionID]any) (arr []TransactionID) {
	for value := range mp {
		arr = append(arr, value)
	}
	return
}

func (bp *BufferPool) addEdges(from TransactionID, mp map[TransactionID]any) {
	arr := getValues(mp)
	edges, ok := bp.waitGraph[from]
	if !ok {
		edges = []TransactionID{}
	}
	toMap := make(map[TransactionID]any)
	for _, edge := range edges {
		toMap[edge] = true
	}
	for _, to := range arr {
		if _, ok := toMap[to]; ok {
			continue
		}
		toMap[to] = true
		edges = append(edges, to)
	}
	bp.waitGraph[from] = edges
}

func (bp *BufferPool) delEdges(tid TransactionID) {
	delete(bp.waitGraph, tid)
}
