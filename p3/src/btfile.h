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
#include "db.h"
#include "buf.h"
#include "cassert"

// Define your error code for B+ tree here
// enum btErrCodes  {...}a

/*
struct BTreeHeaderPage
{
	PageId root;
	AttrType key_type;
	int keysize;
	char fileName[MAX_SPACE - (sizeof root + sizeof key_type + sizeof keysize)];
};
*/

class BTreeFile: public IndexFile
{
	private:
		BTreeHeaderPage *BTree;
		BTreeFileScan *fileScan;

	public:
		BTreeFile(Status& status, const char *filename)
		{
			if (debug) cout << "->  sys    BTreeFile 1 " << filename << endl;
			PageId headPageId;
			if ((status = (MINIBASE_DB->get_file_entry(filename, headPageId))) != OK) return;
			Page *headPage;
			assert((MINIBASE_BM->pinPage(headPageId, headPage)) == OK);
			BTree = (BTreeHeaderPage*)headPage;
		}
		// an index with given filename should already exist,
		// this opens it.

		BTreeFile(Status& status, const char *filename, const AttrType keytype, const int keysize)
		{
			if (debug) cout << "->  sys    BTreeFile 2 " << filename << endl;
			PageId headPageId, rootPageId;
			Page *headPage, *rootPage;

			if ((status = (MINIBASE_DB->get_file_entry(filename, headPageId))) == OK)
			{
				assert((MINIBASE_BM->pinPage(headPageId, headPage)) == OK);
				BTree = (BTreeHeaderPage*)headPage;
				return;
			}

			assert((MINIBASE_BM->newPage(headPageId, headPage)) == OK);
			assert((MINIBASE_BM->newPage(rootPageId, rootPage)) == OK);
			assert((MINIBASE_DB->add_file_entry(filename, headPageId)) == OK);

			BTree = (BTreeHeaderPage*)headPage;
			BTree->root = rootPageId;
			BTree->key_type = keytype;
			BTree->keysize = keysize;
			strcpy(BTree->fileName, filename);

			BTLeafPage* leafPage = (BTLeafPage*)rootPage;
			leafPage->init(rootPageId);
			leafPage->setNextPage(INVALID_PAGE);

			assert((MINIBASE_BM->unpinPage(rootPageId, true)) == OK);

			status = OK;
		}
		// if index exists, open it; else create it.

		~BTreeFile()
		{
			if (debug) cout << "->  sys    ~BTreeFile " << BTree->fileName << endl;
			PageId page;
			if ((MINIBASE_DB->get_file_entry(BTree->fileName, page)) == OK)
				assert((MINIBASE_BM->unpinPage(page, true)) == OK);
		}
		// closes index

		Status destroyFile()
		{
			if (debug) cout << "->  sys    destroyFile" << endl;
			destroy(BTree->root);
			PageId page;
			assert((MINIBASE_DB->get_file_entry(BTree->fileName, page)) == OK);
			assert((MINIBASE_DB->delete_file_entry(BTree->fileName)) == OK);
			assert((MINIBASE_BM->unpinPage(page, true)) == OK);
			assert((MINIBASE_BM->freePage(page)) == OK);
			return OK;
		}

		void destroy(PageId pageNo)
		{
			Page *page;
			assert((MINIBASE_BM->pinPage(pageNo, page)) == OK);
			SortedPage *sortedPage = (SortedPage*)page;
			if (sortedPage->get_type() == INDEX)
			{
				BTIndexPage *indexPage = (BTIndexPage*)page;
				destroy(indexPage->getLeftLink());

				RID itr;
				Keytype itrKey;
				PageId curPage;
				
				assert((indexPage->get_first(itr, &itrKey, curPage)) == OK);
				while (1)
				{
					destroy(curPage);
					if ((indexPage->get_next(itr, &itrKey, curPage)) != OK) break;
				}
			}
			assert((MINIBASE_BM->unpinPage(pageNo, true)) == OK);
			assert((MINIBASE_BM->freePage(pageNo)) == OK);
		}
		// destroy entire index file, including the header page and the file entry

		Status insert(const void *key, const RID rid)
		{
			if (debug)
			{
				Keytype *tmp = (Keytype*)key;
//				cout << "->  sys    insert " << tmp->intkey << endl;
			}
			Keytype newKey;
			PageId newPageId;
			Status status = work(BTree->root, key, rid, 0, newKey, newPageId);
			if (newPageId != INVALID_PAGE)
			{
				Page *tmpPage;
				PageId oldPageId = BTree->root;
				RID tmpRid;

				assert((MINIBASE_BM->newPage(BTree->root, tmpPage)) == OK);
				BTIndexPage *tmpIndexPage = (BTIndexPage*)tmpPage;
				tmpIndexPage->init(BTree->root);
				tmpIndexPage->setLeftLink(oldPageId);
				tmpIndexPage->insertKey(&newKey, BTree->key_type, newPageId, tmpRid);
				assert((MINIBASE_BM->unpinPage(BTree->root, true)) == OK);
			}
			return status;
		}
		// insert <key,rid> into appropriate leaf page

		Status Delete(const void *key, const RID rid)
		{
			if (debug)
			{
				Keytype *tmp = (Keytype*)key;
	//			cout << "->  sys    delete " << tmp->intkey << endl;
			}
			Keytype tmpNewKey;
			PageId tmpNewPageId;
			return work(BTree->root, key, rid, 1, tmpNewKey, tmpNewPageId);
		}

		Status work(PageId pageNo, const void *key, const RID rid, int workType, Keytype &newKey, PageId &newPageId)
		{
			newPageId = INVALID_PAGE;

			Status status;
			Keytype tmpNewKey;
			PageId tmpNewPageId;

			Page *tmpPage;
			assert((MINIBASE_BM->pinPage(pageNo, tmpPage)) == OK); //pinPage
			SortedPage *tmpSortedPage = (SortedPage*)tmpPage;

			if (tmpSortedPage->get_type() == INDEX) //IndexPage
			{
				BTIndexPage *tmpIndexPage = (BTIndexPage*)tmpPage;
				PageId nextPageId;
				assert((tmpIndexPage->get_page_no(key, BTree->key_type, nextPageId)) == OK);
				status = work(nextPageId, key, rid, workType, tmpNewKey, tmpNewPageId);

				if (tmpNewPageId != INVALID_PAGE)
				{
					RID tmptmpRid;
					if ((tmpIndexPage->insertKey(&tmpNewKey, BTree->key_type, tmpNewPageId, tmptmpRid)) == OK)
					{
						assert((MINIBASE_BM->unpinPage(pageNo, true)) == OK);
						return OK;
					}

					//Index split begin
					int cnt = tmpSortedPage->numberOfRecords() + 1;
					assert(cnt >= 2);
					cnt /= 2;

					PageId pageId1, pageId2;
					Page *tmp1, *tmp2;
					assert((MINIBASE_BM->newPage(pageId1, tmp1)) == OK);
					assert((MINIBASE_BM->newPage(pageId2, tmp2)) == OK);
					BTIndexPage *left = (BTIndexPage*)tmp1;
					BTIndexPage *right = (BTIndexPage*)tmp2;
					left->init(pageId1);
					right->init(pageId2);

					RID itr, tmpRid;
					Keytype itrKey;
					PageId curPage;
					assert((tmpIndexPage->get_first(itr, &itrKey, curPage)) == OK);
					left->setLeftLink(tmpIndexPage->getLeftLink()); --cnt;

					bool second = false;
					while (1)
					{
						if (cnt-- > 0)
							assert((left->insertKey(&itrKey, BTree->key_type, curPage, tmpRid)) == OK);
						else
						{
							if (!second)
							{
								second = true;
								if (keyCompare(&tmpNewKey, &itrKey, BTree->key_type) < 0)
									assert((left->insertKey(&tmpNewKey, BTree->key_type, tmpNewPageId, tmpRid)) == OK);
								else
									assert((right->insertKey(&tmpNewKey, BTree->key_type, tmpNewPageId, tmpRid)) == OK);
								newKey = itrKey;
								right->setLeftLink(curPage);
							}
							else
								assert((right->insertKey(&itrKey, BTree->key_type, curPage, tmpRid)) == OK);
						}
						if ((tmpIndexPage->get_next(itr, &itrKey, curPage)) != OK) break;
					}
					assert(second);

					tmpIndexPage->init(pageNo);
					tmpIndexPage->setLeftLink(left->getLeftLink());
					assert((left->get_first(itr, &itrKey, curPage)) == OK);
					while (1)
					{
						assert((tmpIndexPage->insertKey(&itrKey, BTree->key_type, curPage, tmpRid)) == OK);
						if ((left->get_next(itr, &itrKey, curPage)) != OK) break;
					}

					newPageId = pageId2;

					assert((MINIBASE_BM->unpinPage(pageNo, true)) == OK); //unpinPage
					assert((MINIBASE_BM->unpinPage(pageId1, true)) == OK);
					assert((MINIBASE_BM->unpinPage(pageId2, true)) == OK);
					assert((MINIBASE_BM->freePage(pageId1)) == OK);

					return OK;
					//Index split end
				}

				assert((MINIBASE_BM->unpinPage(pageNo, true)) == OK); //unpinPage
				return status;
			}
			else if (tmpSortedPage->get_type() == LEAF) //LeafPage
			{
				BTLeafPage *tmpLeafPage = (BTLeafPage*)tmpPage;
				if (workType == 1)
				{
					//leaf delete begin
					status = tmpLeafPage->deleteRec(key, BTree->key_type);
					assert((MINIBASE_BM->unpinPage(pageNo, true)) == OK); //unpinPage
					return status;
					//leaf delete end
				}
				else
				{
					RID tmpRid;
					if ((tmpLeafPage->get_data_rid(const_cast<void*>(key), BTree->key_type, tmpRid)) == OK) 
					{
						//leaf insert dupulicate begin
						assert((MINIBASE_BM->unpinPage(pageNo, true)) == OK); //unpinPage
						return OK;
						//leaf insert dupulicate end
					}
					else
					{
						status = tmpLeafPage->insertRec(key, BTree->key_type, rid, tmpRid);
						if (status == OK)
						{
							//leaf insert OK begin
							assert((MINIBASE_BM->unpinPage(pageNo, true)) == OK); //unpinPage
							return OK;
							//leaf insert OK end
						}
						else
						{
							//leaf split begin
							int cnt = tmpLeafPage->numberOfRecords();
							assert(cnt >= 2);
							cnt /= 2;

							PageId pageId1, pageId2;
							Page *tmp1, *tmp2;
							assert((MINIBASE_BM->newPage(pageId1, tmp1)) == OK);
							assert((MINIBASE_BM->newPage(pageId2, tmp2)) == OK);
							BTLeafPage *left = (BTLeafPage*)tmp1;
							BTLeafPage *right = (BTLeafPage*)tmp2;
							left->init(pageId1);
							right->init(pageId2);
							right->setNextPage(tmpLeafPage->getNextPage());

							RID itr, tmpRid;
							Keytype itrKey;
							RID dataRid;
							assert((tmpLeafPage->get_first(itr, &itrKey, dataRid)) == OK);
							bool second = false;
							while (1)
							{
								if (cnt-- > 0)
									assert((left->insertRec(&itrKey, BTree->key_type, dataRid, tmpRid)) == OK);
								else
								{
									if (!second)
									{
										second = true;
										if (keyCompare(key, &itrKey, BTree->key_type) < 0)
											assert((left->insertRec(key, BTree->key_type, rid, tmpRid)) == OK);
										else
											assert((right->insertRec(key, BTree->key_type, rid, tmpRid)) == OK);
									}
									assert((right->insertRec(&itrKey, BTree->key_type, dataRid, tmpRid)) == OK);
								}
								if ((tmpLeafPage->get_next(itr, &itrKey, dataRid)) != OK) break;
							}
							assert(second);

							tmpLeafPage->init(pageNo);
							tmpLeafPage->setNextPage(pageId2);
							assert((left->get_first(itr, &itrKey, dataRid)) == OK);
							while (1)
							{
								assert((tmpLeafPage->insertRec(&itrKey, BTree->key_type, dataRid, tmpRid)) == OK);
								if ((left->get_next(itr, &itrKey, dataRid)) != OK) break;
							}

							newPageId = pageId2;
							right->get_first(itr, &newKey, tmpRid);

							assert((MINIBASE_BM->unpinPage(pageNo, true)) == OK); //unpinPage
							assert((MINIBASE_BM->unpinPage(pageId1, true)) == OK);
							assert((MINIBASE_BM->unpinPage(pageId2, true)) == OK);
							assert((MINIBASE_BM->freePage(pageId1)) == OK);

							return OK;
							//leaf split end
						}
					}
				}
			}
			else assert(false);
		}
		// delete leaf entry <key,rid> from the appropriate leaf
		// you need not implement merging of pages when occupancy
		// falls below the minimum threshold (unless you want extra credit!)

		IndexFileScan *new_scan(const void *lo_key = NULL, const void *hi_key = NULL)
		{
			if (debug)
			{
				cout << "->  sys    new scan" << endl;
			}
			PageId scanPageId = BTree->root;
			while (1)
			{
				Page *tmp;
				assert((MINIBASE_BM->pinPage(scanPageId, tmp)) == OK);
				SortedPage *sortedPage = (SortedPage*)tmp;
				if ((sortedPage->get_type() == INDEX))
				{
					BTIndexPage *indexPage = (BTIndexPage*)tmp;
					PageId old = scanPageId;
					scanPageId = indexPage->getLeftLink();
					assert((MINIBASE_BM->unpinPage(old, true)) == OK);
				}
				else
				{
					assert((MINIBASE_BM->unpinPage(scanPageId, true)) == OK);
					fileScan = new BTreeFileScan();
					Keytype *ttt = (Keytype*)lo_key;
					fileScan->init(scanPageId, lo_key, hi_key, BTree->key_type, BTree->keysize);
					return fileScan;
				}
			}
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

		int keysize() {return BTree->keysize;}
};

#endif

