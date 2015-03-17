/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/


#include "buf.h"


// Define buffer manager error messages here
//enum bufErrCodes  {...};

// Define error message here
static const char* bufErrMsgs[] = { 
	// error message strings go here
};

// Create a static "error_string_table" object and register the error messages
// with minibase system 
static error_string_table bufTable(BUFMGR,bufErrMsgs);

#define DEBUG 0

BufMgr::BufMgr (int numbuf, Replacer *replacer) {
	if (DEBUG) cout << "sys init " << numbuf << endl;
	N = numbuf;
	bufPool = new Page[N];
	bufDescr = new descr[N];
	free = new node();
	love = new node();
	hate = new node();
	for (int i = 0; i < N; ++i)
	{
		bufDescr[i] = descr();
		free->add(new node(-1, i));
	}
	for (int i = 0; i < HTSIZE; ++i)
		hashTable[i] = new node();
}

Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage) {
	if (DEBUG) cout << "sys pinPage " << PageId_in_a_DB << endl;
	node *tmp = find(PageId_in_a_DB);
	if (tmp != NULL)
	{
		int i = tmp->position;
		if (bufDescr[i].pinCount++ == 0)
			bufDescr[i].lhpos->del();
		page = bufPool + i;
		return OK;
	}
	for (node *tmp = free->next; tmp != free;)
	{
		int i = tmp->position;
		if (MINIBASE_DB->read_page(PageId_in_a_DB, bufPool + i) != OK) return FAIL;
		bufDescr[i] = descr(PageId_in_a_DB, 1, false, false, NULL);
		hashTable[hash(PageId_in_a_DB)]->add(new node(PageId_in_a_DB, i));
		tmp->del();
		page = bufPool + i;
		return OK;
	}
	for (node *tmp = hate->pre; tmp != hate;)
	{
		int i = tmp->position;
		if (bufDescr[i].dirtybit && MINIBASE_DB->write_page(tmp->pageNumber, bufPool + i) != OK) return FAIL;
		if (MINIBASE_DB->read_page(PageId_in_a_DB, bufPool + i) != OK) return FAIL;
		bufDescr[i] = descr(PageId_in_a_DB, 1, false, false, NULL);
		hashTable[hash(PageId_in_a_DB)]->add(new node(PageId_in_a_DB, i));
		node *tmp2 = find(tmp->pageNumber);
		tmp2->del();
		tmp->del();
		page = bufPool + i;
		return OK;
	}
	for (node *tmp = love->next; tmp != love;)
	{
		int i = tmp->position;
		if (bufDescr[i].dirtybit && MINIBASE_DB->write_page(tmp->pageNumber, bufPool + i) != OK) return FAIL;
		if (MINIBASE_DB->read_page(PageId_in_a_DB, bufPool + i) != OK) return FAIL;
		bufDescr[i] = descr(PageId_in_a_DB, 1, false, false, NULL);
		hashTable[hash(PageId_in_a_DB)]->add(new node(PageId_in_a_DB, i));
		node *tmp2 = find(tmp->pageNumber);
		tmp2->del();
		tmp->del();
		page = bufPool + i;
		return OK;
	}
	return FAIL;
}//end pinPage


Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany) {
	if (DEBUG) cout << "sys newPage" << endl;
	if (MINIBASE_DB->allocate_page(firstPageId, howmany) != OK) return FAIL;
	if (pinPage(firstPageId, firstpage) != OK) return FAIL;
	return OK;
}

Status BufMgr::flushPage(PageId pageid) {
	if (DEBUG) cout << "sys flushPage" << endl;
	node *tmp = find(pageid);
	if (tmp == NULL) return FAIL;
	int i = tmp->position;
	if (bufDescr[i].dirtybit)
	{
		if (MINIBASE_DB->write_page(tmp->pageNumber, bufPool + i) != OK) return FAIL;
		bufDescr[i].dirtybit = false;
	}
	return OK;
}


//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr(){
	if (DEBUG) cout << "~BufMgr" << endl;
	flushAllPages();
	release();
	delete [] bufPool;
	delete [] bufDescr;
	delete love;
	delete hate;
	while (free->next != free) free->next->del();
	delete free;
	for (int i = 0; i < HTSIZE; ++i)
		delete hashTable[i];
}


//*************************************************************
//** This is the implementation of unpinPage
//************************************************************

Status BufMgr::unpinPage(PageId page_num, int dirty=FALSE, int HATE = FALSE){
	if (DEBUG) cout << "sys unpinPage "<< page_num << " " << (HATE ? "hate" : "love") << endl;
	node *tmp = find(page_num);
	if (tmp == NULL) return FAIL;
	int i = tmp->position;
	bufDescr[i].dirtybit = true;
	if (bufDescr[i].pinCount <= 0) return FAIL;
	if (!HATE) bufDescr[i].loved = true;
	if (--bufDescr[i].pinCount == 0)
	{
		if (bufDescr[i].loved)
		{
			love->add(new node(tmp->pageNumber, i));
			bufDescr[i].lhpos = love->pre;
		}
		else
		{
			hate->add(new node(tmp->pageNumber, i));
			bufDescr[i].lhpos = hate->pre;
		}
	}
	return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************

Status BufMgr::freePage(PageId globalPageId){
	if (DEBUG) cout << "sys freePage " << globalPageId << endl;
	node *tmp = find(globalPageId);
	if (tmp != NULL && bufDescr[tmp->position].pinCount > 0) return FAIL;
	if (tmp != NULL)
	{
		int i = tmp->position;
		bufDescr[i].lhpos->del();
		tmp->del();
		bufDescr[i] = descr();
		free->add(new node(-1, i));
	}
	MINIBASE_DB->deallocate_page(globalPageId);
	return OK;
}

Status BufMgr::flushAllPages(){
	if (DEBUG) cout << "sys flushALLPages" << endl;
	for (int i = 0; i < N; ++i)
	{
		if (bufDescr[i].dirtybit)
		{
			if (MINIBASE_DB->write_page(bufDescr[i].pageNumber, bufPool + i) != OK)
				return FAIL;
			bufDescr[i].dirtybit = false;
		}
	}
	return OK;
}
