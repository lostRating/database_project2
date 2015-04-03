/*
 * btreefilescan.h
 *
 * sample header file
 *
 */

#ifndef _BTREEFILESCAN_H
#define _BTREEFILESCAN_H

#include "btfile.h"
#include "db.h"
#include "buf.h"
#include "cassert"

// errors from this class should be defined in btfile.h

const bool debug = false;
const bool debug2 = false;

struct BTreeHeaderPage
{
	int specialKey;
	PageId root;
	AttrType key_type;
	int keysize;
	char fileName[MAX_SPACE - (sizeof root + sizeof key_type + sizeof keysize)];
};

class BTreeFileScan : public IndexFileScan {
	public:
		friend class BTreeFile;

		bool find()
		{
			Page *page;
			BTLeafPage *leaf;
			while (1)
			{
				if (scanPageId == INVALID_PAGE) return false;
				old = scanPageId;

				assert((MINIBASE_BM->pinPage(scanPageId, page)) == OK);
				leaf = (BTLeafPage*)page;
				Status status;
				if (first)
					status = (leaf->get_first(scanRid, &scanKey, dataRid));
				else
					status = (leaf->get_next(scanRid, &scanKey, dataRid));
				first = false;
				if (status != OK)
				{
					if (debug)
					{
						cout << "->  sys    FileScan->find()  jump page " << (leaf->getNextPage()) << endl;
					}
					scanPageId = (leaf->getNextPage());
					first = true;
					jump = true;
				}
				else
				{
					if (high != NULL && keyCompare(&scanKey, high, key_type) > 0)
					{
						assert((MINIBASE_BM->unpinPage(old, true)) == OK);
						return false;
					}
					if (low == NULL || keyCompare(&scanKey, low, key_type) >= 0)
					{
						if (debug)
						{
							Keytype *tmp;
							tmp = (Keytype*)low;
							cout << "->  sys  ?????????  " << tmp->intkey << ' ';
							tmp = (Keytype*)&scanKey;
							cout << tmp->intkey << endl;
						}
						assert((MINIBASE_BM->unpinPage(old, true)) == OK);
						return true;
					}
				}
				assert((MINIBASE_BM->unpinPage(old, true)) == OK);
			}
		}

		Status init(PageId scanPageId2, const void *low2, const void *high2, AttrType key_type2, int keysize2)
		{
			if (debug)
			{
				cout << "->  sys    init " << endl;
				Keytype *tmp = (Keytype*)low2;
				cout << "->  sys  low value = " << tmp->intkey << endl;
			}
			scanPageId = scanPageId2;
			low = low2;
			high = high2;
			key_type = key_type2;
			key_size = keysize2;

			first = true;
			del = false;
			return OK;
		}

		// get the next record
		Status get_next(RID & rid, void* keyptr)
		{
			if (debug) cout << "->  sys    fileScan get_next()" << endl;
			tmpPageId = scanPageId;
			tmpKey = scanKey;
			tmpRid1 = scanRid;
			tmpRid2 = dataRid;
			b1 = first;
			b2 = del;

			Status status = OK;

			if (debug2)
			{
				cout << "-> sys " << del << endl;
			}

			if (del)
			{
				del = false;
				if (RetDel == DONE) return DONE;
				if (jump)
				{
					scanPageId = nextPageId;
					scanRid = nextRid;
				}
			}
			else if (!find())
			{
				scanPageId = tmpPageId;
				scanKey = tmpKey;
				scanRid = tmpRid1;
				dataRid = tmpRid2;
				first = b1;
				del = b2;

				status = DONE;
			}

			rid = dataRid;
			Keytype *tmp = (Keytype*)keyptr;
			Keytype *tmp2 = &scanKey;
			if (key_type == attrInteger)
				tmp->intkey = tmp2->intkey;
			else
				strcpy(tmp->charkey, tmp2->charkey);

			if (debug) cout << "!! get_next DONE? " << (status == DONE) << ' ' << (MINIBASE_BM->getNumUnpinnedBuffers()) << endl;

			return status;
		}

		// delete the record currently scanned
		Status delete_current()
		{
			if (debug) cout << "!!!!  sys  itr  delete cur" << endl;
			if (del) return DONE;

			del = true;

			tmpPageId = scanPageId;
			tmpKey = scanKey;
			tmpRid1 = scanRid;
			tmpRid2 = dataRid;
			b1 = first;
			b2 = del;

			jump = false;
			if (find())	RetDel = OK;
			else RetDel = DONE;

			nextPageId = scanPageId;
			nextRid = scanRid;

			scanPageId = tmpPageId;
			scanRid = tmpRid1;

			Page *page;
			assert((MINIBASE_BM->pinPage(scanPageId, page)) == OK);
			BTLeafPage *leaf = (BTLeafPage*)page;
			assert((leaf->deleteRecord(scanRid)) == OK);
			del = true;
			assert((MINIBASE_BM->unpinPage(scanPageId, true)) == OK);

			return OK;
		}

		int keysize() // size of the key
		{
			return key_size;
		}

		// destructor
		~BTreeFileScan()
		{
			if (debug) cout << "->  sys    ~scan" << endl;
		}
	private:

		PageId scanPageId, old, tmpPageId, nextPageId;
		Keytype scanKey, tmpKey;
		RID scanRid, dataRid, tmpRid1, tmpRid2, nextRid;
		AttrType key_type;
		int key_size;
		bool first, del, b1, b2;
		const void *low, *high;
		char extra[MAX_SPACE];
		Status RetDel;
		bool jump;

};

#endif
