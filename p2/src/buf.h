///////////////////////////////////////////////////////////////////////////////
/////////////  The Header File for the Buffer Manager /////////////////////////
///////////////////////////////////////////////////////////////////////////////


#ifndef BUF_H
#define BUF_H

#include "db.h"
#include "page.h"


#define NUMBUF 20   
// Default number of frames, artifically small number for ease of debugging.

#define HTSIZE 7
#define HA 3
#define HB 5
// Hash Table size
//You should define the necessary classes and data structures for the hash table, 
// and the queues for LSR, MRU, etc.


/*******************ALL BELOW are purely local to buffer Manager********/

// You should create enums for internal errors in the buffer manager.
enum bufErrCodes  { 
};

class Replacer;

struct node
{
	node(PageId t1 = -1, int t2 = -1)
	{
		pre = next = this;
		pageNumber = t1;
		position = t2;
	}

	void add(node *a)
	{
		node *tmp = this->pre;
		this->pre->next = a;
		this->pre = a;
		a->next = this;
		a->pre = tmp;
	}

	void del()
	{
		this->pre->next = this->next;
		this->next->pre = this->pre;
		delete this;
	}

	node *pre, *next;
	PageId pageNumber;
	int position;
};

struct descr
{
	descr(PageId t1 = -1, int t2 = 0, bool t3 = false, bool t4 = false, node* t5 = NULL)
	{
		pageNumber = t1;
		pinCount = t2;
		dirtybit = t3;

		loved = t4;
		lhpos = t5;
	}

	PageId pageNumber;
	int pinCount;
	bool dirtybit;

	bool loved;
	node* lhpos;
};


class BufMgr {

	private:

		int N;

		node *hashTable[HTSIZE], *free, *love, *hate;

		descr *bufDescr;

		int hash(PageId pageId)
		{
			return (HA * pageId + HB) % HTSIZE;
		}

		node* find(PageId pageId)
		{
			int val = hash(pageId);
			for (node* i = hashTable[val]->next; i != hashTable[val]; i = i->next)
				if (i->pageNumber == pageId)
					return i;
			return NULL;
		}

		void release()
		{
			for (int j = 0; j < HTSIZE; ++j)
				while (hashTable[j]->next != hashTable[j])
					hashTable[j]->next->del();
	//		while (free->next != free) free->next->del();
			while (love->next != love) love->next->del();
			while (hate->next != hate) hate->next->del();
		}

	public:

		Page* bufPool; // The actual buffer pool

		BufMgr (int numbuf, Replacer *replacer = 0); 
		// Initializes a buffer manager managing "numbuf" buffers.
		// Disregard the "replacer" parameter for now. In the full 
		// implementation of minibase, it is a pointer to an object
		// representing one of several buffer pool replacement schemes.

		~BufMgr();           // Flush all valid dirty pages to disk

		Status pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage=0);
		// Check if this page is in buffer pool, otherwise
		// find a frame for this page, read in and pin it.
		// also write out the old page if it's dirty before reading
		// if emptyPage==TRUE, then actually no read is done to bring
		// the page

		Status unpinPage(PageId globalPageId_in_a_DB, int dirty, int hate);
		// hate should be TRUE if the page is hated and FALSE otherwise
		// if pincount>0, decrement it and if it becomes zero,
		// put it in a group of replacement candidates.
		// if pincount=0 before this call, return error.

		Status newPage(PageId& firstPageId, Page*& firstpage, int howmany=1); 
		// call DB object to allocate a run of new pages and 
		// find a frame in the buffer pool for the first page
		// and pin it. If buffer is full, ask DB to deallocate 
		// all these pages and return error

		Status freePage(PageId globalPageId); 
		// user should call this method if it needs to delete a page
		// this routine will call DB to deallocate the page 

		Status flushPage(PageId pageid);
		// Used to flush a particular page of the buffer pool to disk
		// Should call the write_page method of the DB class

		Status flushAllPages();
		// Flush all pages of the buffer pool to disk, as per flushPage.

		/* DO NOT REMOVE THIS METHOD */    
		Status unpinPage(PageId globalPageId_in_a_DB, int dirty=FALSE)
			//for backward compatibility with the libraries
		{
			return unpinPage(globalPageId_in_a_DB, dirty, FALSE);
		}
};

#endif


