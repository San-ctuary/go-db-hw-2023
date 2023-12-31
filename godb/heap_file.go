package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"sync"
)

// HeapFile is an unordered collection of tuples Internally, it is arranged as a
// set of heapPage objects
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// TODO: some code goes here
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	bufPool *BufferPool
	sync.Mutex
	desc     *TupleDesc
	fileName string
	file     *os.File
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	// TODO: some code goes here
	file, err := os.OpenFile(fromFile, os.O_RDWR|os.O_CREATE, fs.ModePerm)
	if err != nil {
		return nil, err
	}
	heapFile := &HeapFile{
		bufPool:  bp,
		desc:     td,
		fileName: fromFile,
		file:     file,
	}
	bp.dbfile = DBFile(heapFile)
	return heapFile, nil //replace me
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	// TODO: some code goes here
	stat, _ := f.file.Stat()
	fileSize := int(stat.Size())
	return fileSize / PageSize //replace me
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for j := 0; j < f.NumPages(); j++ {
			pg, err := bp.GetPage(f, j, tid, ReadPerm)
			if pg == nil || err != nil {
				fmt.Println("page nil or error", err)
				break
			}
			if (*pg).isDirty() {
				(*f).flushPage(pg)
				(*pg).setDirty(false)
			}

		}

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [heapPage] object, using
// the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (*Page, error) {
	// TODO: some code goes here
	file := f.file
	offset := pageNo * PageSize
	byteArr := make([]byte, PageSize)
	_, err := file.ReadAt(byteArr, int64(offset))
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(byteArr)
	page := newHeapPage(f.desc, pageNo, f)
	page.initFromBuffer(buffer)
	p := Page(page)
	return &p, nil
}

// Add the tuple to the HeapFile.  This method should search through pages in
// the heap file, looking for empty slots and adding the tuple in the first
// empty slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile.  We will
// add support for concurrent modifications in lab 3.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here
	numPages := f.NumPages()
	bf := f.bufPool
	for pageId := 0; pageId < numPages; pageId++ {
		page, err := bf.GetPage(f, pageId, tid, WritePerm)
		if err != nil {
			return err
		}
		heapPage := (*page).(*heapPage)
		if heapPage.usedSlots != heapPage.numSlots {
			_, err := heapPage.insertTuple(t)
			heapPage.setDirty(true)
			if err != nil {
				return err
			} else {
				return nil
			}
		}
	}
	newPage := newHeapPage(&t.Desc, f.NumPages(), f)
	_, err := newPage.insertTuple(t)
	if err != nil {
		return err
	}
	writePage := Page(newPage)
	// need to flush page
	err = f.flushPage(&writePage)
	if err != nil {
		return err
	}
	return nil //replace me
}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here
	rid := t.Rid.(heapFileRID)
	page, err := f.bufPool.GetPage(f, rid.pageNum, tid, ReadPerm)
	if err != nil {
		return err
	}
	heappage := (*page).(*heapPage)
	err = heappage.deleteTuple(rid)
	heappage.setDirty(true)
	if err != nil {
		return err
	}
	return nil //replace me
}

// Method to force the specified page back to the backing file at the appropriate
// location.  This will be called by BufferPool when it wants to evict a page.
// The Page object should store information about its offset on disk (e.g.,
// that it is the ith page in the heap file), so you can determine where to write it
// back.
func (f *HeapFile) flushPage(p *Page) error {
	// TODO: some code goes here
	page, ok := (*p).(*heapPage)
	if !ok {
		return GoDBError{TypeMismatchError, "cannot cast to heappage"}
	}
	file := f.file
	buffer, err := page.toBuffer()
	if err != nil {
		return err
	}
	byteArr := buffer.Bytes()
	writeArr := make([]byte, PageSize)
	copy(writeArr, byteArr)
	offset := page.pageNo * PageSize
	file.WriteAt(writeArr, int64(offset))
	return nil //replace me
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return f.desc //replace me
}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	numPages := f.NumPages()
	pageId := -1
	var iter func() (*Tuple, error)
	initNewPage := true
	return func() (*Tuple, error) {
		for {
			if initNewPage {
				pageId += 1
				if pageId == numPages {
					return nil, nil
				}
				page, err := f.bufPool.GetPage(f, pageId, tid, ReadPerm)
				if err != nil {
					return nil, nil
				}
				heappage := (*page).(*heapPage)
				iter = heappage.tupleIter()
				initNewPage = false
			}
			tuple, err := iter()
			if err != nil {
				return nil, err
			}
			if tuple != nil {
				return tuple, nil
			} else {
				initNewPage = true
			}
		}
		return nil, nil
	}, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	// TODO: some code goes here
	key := heapHash{FileName: f.fileName, PageNo: pgNo}
	return key
}
