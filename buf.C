#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


/*
    Allocates a free frame using the clock algorithm; if necessary,
    writing a dirty page back to disk. Returns BUFFEREXCEEDED
    if all buffer frames are pinned, 
    UNIXERR if the call to the I/O layer returned an error 
    when a dirty page was being written to disk and OK otherwise.  
    This private method will get called by the readPage() and allocPage() 
    methods described below.
*/
const Status BufMgr::allocBuf(int & frame) 
{
    advanceClock();
    unsigned int tempClockHand = clockHand; // save current pos to determine a full clock cycle
    do
    {
        BufDesc* tmpbuf = &bufTable[clockHand];

        //Valid is not set
        if(tmpbuf->valid == false)
        {
            tmpbuf->Set(tmpbuf->file, tmpbuf->pageNo);
            hashTable->remove(tmpbuf->file,tmpbuf->pageNo);
            return OK;
        }
        //Ref bit is set
        else if (tmpbuf->refbit == true)
        {
            tmpbuf->refbit = false;
            advanceClock();
            continue;
        }
        // page is pinned
        else if (tmpbuf->pinCnt != 0)
        {
            advanceClock();
            continue;
        }
        //Dirty bit is set
        else if (tmpbuf->dirty == true) {
            try
            {
                //flush page to disk and invoke Set() on frame
                tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[clockHand]));
                tmpbuf->Set(tmpbuf->file, tmpbuf->pageNo);
                hashTable->remove(tmpbuf->file,tmpbuf->pageNo);
            }
            catch(const std::exception& e)
            {
                return UNIXERR;
            }
            return OK;
        }
        // Invoke Set() on Frame
        else{
            tmpbuf->Set(tmpbuf->file, tmpbuf->pageNo);
            hashTable->remove(tmpbuf->file,tmpbuf->pageNo);
            
            return OK;
        }

    } while (clockHand != tempClockHand);
    //All buffer frames are pinned
    return BUFFEREXCEEDED;
}

const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    // check whether the page is already in the buffer pool
    Status status;
    int frameNo = 0;
    status = hashTable->lookup(file, PageNo, frameNo);

    // the page is already in the buffer pool (Case 2)
    // frameNo is already updated in this case
    if (status == OK){
        // set the appropriate refbit / pinCnt
        bufTable[frameNo].refbit = true
        bufTable[frameNo].pinCnt += 1
        return status
    }

    // the page is NOT in the buffer pool (Case 1)
    status = allocBuf(frameNo);
    if (status != OK){
        return status;
    }
    status = file->readPage(PageNo, page);
    if (status != OK){
        return status;
    }

    status = hashTable->insert(file, pageNo, frameNo);
    if (status != OK){
        return status;
    }
    status = bufTable[frameNo]->Set(file, pageNo);

    return status

// note1: how to return the frameNo? through hashTable?
// note2: create a new function status check & return??

}

/*
    Decrements the pinCnt of the frame containing (file, PageNo)
    and, if dirty == true, sets the dirty bit.  
    Returns OK if no errors occurred,
    HASHNOTFOUND if the page is not in the buffer pool hash table,
    PAGENOTPINNED if the pin count is already 0. 
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{



}
/*
    This call is kind of weird.  The first step is to to allocate an empty page
    in the specified file by invoking the file->allocatePage() method. 
    This method will return the page number of the newly allocated page.
    Then allocBuf() is called to obtain a buffer pool frame. 
    Next, an entry is inserted into the hash table and Set() is invoked 
    on the frame to set it up properly.  The method returns both the page 
    number of the newly allocated page to the caller via the pageNo parameter 
    and a pointer to the buffer frame allocated for the page via the page parameter.
    Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED
    if all buffer frames are pinned and HASHTBLERROR if a hash table error occurred.  
*/

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)
{


Status s = file->allocatePage(pageNo); // page no will be th evalue of the new page
//check if s if ok
int newframe = 0;
Status f = allocBuf(newframe);
// TO DO : Check if f is ok
hashTable->add(file,pageNo,newframe); 
bufTable[newFrame].Set(file,pageNo); // go to the position of the new frame

}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


