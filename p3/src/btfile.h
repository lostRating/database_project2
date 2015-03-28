/*
 * btfile.h
 *
 * sample header file
 *
 */

#ifndef _BTREE_H
#define _BTREE_H

#include "btindex_page.h"
#include "btleaf_page.h"
#include "index.h"
#include "btreefilescan.h"
#include "bt.h"

// Define your error code for B+ tree here
// enum btErrCodes  {...}a

struct BTreeHeaderPage
{
	BTreeHeaderPage(PageId a, AttrType b, int c, const char *filename)
	{
		root = a;
		key_type = b;
		keysize = c;
		strcpy(fileName, filename);
	}
	PageId root;
	AttrType key_type;
	int keysize;
	char fileName[MAX_NAME + 5];
};

class BTreeFile: public IndexFile
{
	public:
		BTreeFile(Status& status, const char *filename)
		{
			PageId start_pg;
			status = MINIBASE_DB->get_file_entry(filename, start_pg);
			if (status != OK) return status;
			BTree = new BTreeHeaderPage(start_pg, );
		}
		// an index with given filename should already exist,
		// this opens it.

		BTreeFile(Status& status, const char *filename, const AttrType keytype, const int keysize)
		{
			BTreeFile(status, filename);
			if (status == OK) return OK;
			PageId start_pg;
			status = add
			BTree = new BTreeHeaderPage(, keytype, keysize);
		}
		// if index exists, open it; else create it.

		~BTreeFile()
		{
			delete BTree;
		}
		// closes index

		Status destroyFile()
		{
		}
		// destroy entire index file, including the header page and the file entry

		Status insert(const void *key, const RID rid)
		{
		}
		// insert <key,rid> into appropriate leaf page

		Status Delete(const void *key, const RID rid)
		{
		}
		// delete leaf entry <key,rid> from the appropriate leaf
		// you need not implement merging of pages when occupancy
		// falls below the minimum threshold (unless you want extra credit!)

		IndexFileScan *new_scan(const void *lo_key = NULL, const void *hi_key = NULL)
		{
		}
		// create a scan with given keys
		// Cases:
		//      (1) lo_key = NULL, hi_key = NULL
		//              scan the whole index
		//      (2) lo_key = NULL, hi_key!= NULL
		//              range scan from min to the hi_key
		//      (3) lo_key!= NULL, hi_key = NULL
		//              range scan from the lo_key to max
		//      (4) lo_key!= NULL, hi_key!= NULL, lo_key = hi_key
		//              exact match ( might not unique)
		//      (5) lo_key!= NULL, hi_key!= NULL, lo_key < hi_key
		//              range scan from lo_key to hi_key

		int keysize()
		{
			return Btree->keysize;
		}

	private:
		BTreeHeaderPage *BTree;

};

#endif

