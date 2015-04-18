#ifndef __SORT__
#define __SORT__

#include "minirel.h"
#include "new_error.h"
#include "scan.h"
#include "heapfile.h"
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <algorithm>
#include <string>
#include <cassert>

#define    PAGESIZE    MINIBASE_PAGESIZE
#define    NAMELEN     10

using namespace std;

int compare(const void *a, const void *b);

class Sort
{
	public:
		static int shiftSize;
		static AttrType type;
		static TupleOrder order;

		void writeToTemp(int cur, int tot)
		{
			RID rid;
			Status s;

			assert(cur % tot == 0);
			qsort(sortArea, cur / tot, tot, compare);

			string tmpFile = getName();
			tempFiles.push_back(tmpFile);
			HeapFile output(tmpFile.c_str(), s);
			assert(s == OK);
			for (int i = 0; i < cur; i += tot)
				assert(output.insertRecord(sortArea + i, tot, rid) == OK);
		}
		//Number of fields in input records
		/*Array containing field types of input records i.e. index of in[] ranges from 0 to (len_in - 1)*/
		//Array containing field sizes of input records
		/*The nubmer of the field to sort on fld_no ranges from 0 to (len_in - 1)*/
		//ASCENDING, DESCENDING
		//Number of buffer pages available
		Sort(char *inFile, char *outFile, int len_in, AttrType in[], short str_sizes[],
				int fld_no,	TupleOrder sort_order, int amt_of_buf, Status& s)
		{
			order = sort_order;
			int tot = 0;
			shiftSize = 0;
			for (int i = 0; i < len_in; ++i)
			{
				if (i < fld_no)
					shiftSize += str_sizes[i];
				else if (i == fld_no)
					type = in[i];
				tot += str_sizes[i];
			}

			{
				sortArea = new char[PAGESIZE * amt_of_buf];

				HeapFile input(inFile, s);
				assert(s == OK);
				Scan scan(&input, s);
				assert(s == OK);

				RID rid;
				int length, cur = 0;
				while (scan.getNext(rid, sortArea + cur, length) == OK)
				{
					assert(length <= tot);
					cur += tot;
					if (cur + tot > PAGESIZE * amt_of_buf)
					{
						writeToTemp(cur, tot);
						cur = 0;
					}
				}
				if (cur) writeToTemp(cur, tot);

				delete sortArea;
			}

			while ((int)tempFiles.size() > 1)
			{
				vector<string> tmp;
				tmp.swap(tempFiles);
				vector<bool> over;
				vector<char*> tmpArea;
				vector<HeapFile*> hf;
				vector<Scan*> scan;
				for (int i = 0; i < (int)tmp.size(); ++i)
				{
					over.push_back(false);
					tmpArea.push_back(NULL);
					hf.push_back(NULL);
					scan.push_back(NULL);
				}
				for (int l = 0, r = 0; l < (int)tmp.size();)
				{
					while (r + 1 < (int)tmp.size() && r - l + 3 <= amt_of_buf) ++r;
					RID rid;
					Status s;
					int length;

					string str = getName();
					tempFiles.push_back(str);
					HeapFile output(str.c_str(), s);
					assert(s == OK);

					for (int j = l; j <= r; ++j)
					{
						tmpArea[j] = new char[tot];
						hf[j] = new HeapFile(tmp[j].c_str(), s);
						assert(s == OK);
						scan[j] = new Scan(hf[j], s);
						assert(s == OK);
						if (scan[j]->getNext(rid, tmpArea[j], length) == OK)
							assert(length <= tot);
						else
							over[j] = true;
					}

					while (1)
					{
						int k = l;
						for (int j = l + 1; j <= r; ++j)
						{
							if (over[j]) continue;
							if (over[k])
							{
								k = j;
								continue;
							}
							if (compare(tmpArea[j], tmpArea[k]) < 0)
								k = j;
						}
						if (over[k]) break;
						assert(output.insertRecord(tmpArea[k], tot, rid) == OK);
						if (scan[k]->getNext(rid, tmpArea[k], length) == OK)
							assert(length <= tot);
						else
						{
							over[k] = true;
							delete tmpArea[k];
							delete scan[k];
							hf[k]->deleteFile();
							delete hf[k];
						}
					}

					l = r + 1;
					r = l;
				}
			}

			if ((int)tempFiles.size() == 1)
			{
				string str = tempFiles[0];
				HeapFile from(str.c_str(), s);
				assert(s == OK);
				HeapFile to(outFile, s);
				assert(s == OK);
				Scan scan(&from, s);
				assert(s == OK);
				char* area = new char[tot];
				int length;
				RID rid;

				while (scan.getNext(rid, area, length) == OK)
				{
					assert(length <= tot);
					assert(to.insertRecord(area, tot, rid) == OK);
				}

				from.deleteFile();

	/*			Scan gxx(&to, s);
				assert(s == OK);
				while (gxx.getNext(rid, area, length) == OK)
				{
					cerr << area + shiftSize << endl;
				}*/

				delete area;
			}
			else assert((int)tempFiles.size() == 0);

			//finish
			s = OK;
		}

		~Sort()
		{
		}

	private:
		char *sortArea;
		vector<string> tempFiles;
		int count = 0;

		string getName()
		{
			string s = "";
			int x = count++;
			while (x)
			{
				s += (char)('0' + x % 10);
				x /= 10;
			}
			reverse(s.begin(), s.end());
			if (s == "") s = "0";
			return "gxx" + s;
		}
		/*
		   unsigned short rec_len;            // length of record.

		   char *outFileName;        // name of the output file name.

		// make names for temporary heap files.
		void makeHFname(char *name, int passNum, int HFnum);

		// first pass.
		Status firstPass(char *inFile, int bufferNum, int& tempHFnum);

		// pass after the first.
		Status followingPass(int passNum, int oldHFnum, int bufferNum, int& newHFnum);

		// merge.
		Status merge(Scan *scan[], int runNum, HeapFile *outHF);

		// find the "smallest" record from runs.
		Status popup(char* record, int *runFlag, int runNum, int &runId);*/
};

int Sort::shiftSize = 0;
AttrType Sort::type = attrString;
TupleOrder Sort::order = Ascending; //Descending

int compare(const void *a, const void *b)
{
	if (Sort::type == attrString)
	{
		int val = strcmp((char*)a + Sort::shiftSize, (char*)b + Sort::shiftSize);
		if (Sort::order == Ascending);
		else if (Sort::order == Descending) val = -val;
		else assert(false);
		return val;
	}
	else if (Sort::type == attrInteger)
	{
		assert(false);
		return *((int*)a) < *((int*)b);
	}
	else assert(false);
}

#endif
