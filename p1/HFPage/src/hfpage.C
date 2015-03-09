#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "heapfile.h"
#include "buf.h"
#include "db.h"


// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
    nextPage = prevPage = INVALID_PAGE;
    curPage = pageNo;
    slotCnt = 0;
    usedPtr = MAX_SPACE - DPFIXED;
    freeSpace = MAX_SPACE - DPFIXED + sizeof(slot_t);
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
    int i;

    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;

    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
         << ", slotCnt=" << slotCnt << endl;

    for (i=0; i < slotCnt; i++) {
        cout << "slot["<< i <<"].offset=" << slot[i].offset
             << ", slot["<< i << "].length=" << slot[i].length << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{
    return prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{
    prevPage = pageNo;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
    nextPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
    return nextPage;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
    if (available_space() < recLen) return DONE;
    usedPtr -= recLen;
    freeSpace -= recLen;
    int i;
    for (i = 0; i <= slotCnt; ++i)
    {
        if (i == slotCnt)
        {
            slot[i].offset = -1;
            ++slotCnt;
            freeSpace -= sizeof(slot_t);
        }
        if (slot[i].offset == -1)
        {
            slot[i].offset = usedPtr;
            slot[i].length = recLen;
            rid.pageNo = page_no();
            rid.slotNo = i;
			break;
        }
    }
    memmove(data + usedPtr, recPtr, recLen);
    return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
    int p = rid.pageNo;
    int q = rid.slotNo;
    if (p != page_no()) return FAIL;
    if (q < 0 || q >= slotCnt || slot[q].offset == -1) return FAIL;
    int i;
    for (i = 0; i < slotCnt; ++i)
        if (slot[i].offset != -1 && slot[i].offset < slot[q].offset)
            slot[i].offset += slot[q].length;
    memmove(data + usedPtr + slot[q].length, data + usedPtr, slot[q].offset - usedPtr);
    usedPtr += slot[q].length;
    freeSpace += slot[q].length;
    slot[q].offset = slot[q].length = -1;
    while (slotCnt > 0 && slot[slotCnt - 1].offset == -1)
    {
        --slotCnt;
        freeSpace += sizeof(slot_t);
    }
    return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
    int i;
    for (i = 0; i < slotCnt; ++i)
        if (slot[i].offset != -1)
        {
            firstRid.pageNo = page_no();
            firstRid.slotNo = i;
            return OK;
        }
    return DONE;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
    int p = curRid.pageNo;
    int q = curRid.slotNo;
    if (p != page_no()) return FAIL;
    if (q < 0 || q >= slotCnt || slot[q].offset == -1) return FAIL;
    int i;
    for (i = q + 1; i < slotCnt; ++i)
        if (slot[i].length != -1)
        {
            nextRid.pageNo = page_no();
            nextRid.slotNo = i;
            return OK;
        }
    return DONE;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
    int p = rid.pageNo;
    int q = rid.slotNo;
    if (p != page_no()) return FAIL;
    if (q < 0 || q >= slotCnt || slot[q].offset == -1) return FAIL;
    memmove(recPtr, data + slot[q].offset, slot[q].length);
    recLen = slot[q].length;
    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
    int p = rid.pageNo;
    int q = rid.slotNo;
    if (p != page_no()) return FAIL;
    if (q < 0 || q >= slotCnt || slot[q].offset == -1) return FAIL;
    recPtr = data + slot[q].offset;
    recLen = slot[q].length;
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    int ret = freeSpace - sizeof(slot_t), i;
    for (i = 0; i < slotCnt; ++i)
        if (slot[i].offset == -1)
        {
            ret += sizeof(slot_t);
            break;
        }
	if (ret < 0) ret = 0;
    return ret;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    return slotCnt == 0;
}



