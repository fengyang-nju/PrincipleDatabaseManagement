#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

/**
 * Notes:
 * 1. The index of file number starts from 0;
 *
 * 2. The record format as following:
 * number of fields in this record[1byte]
 * + null-indicator[num_of_attribute/8]
 * + offset_positions[num_of_attribute 4 * byte, offset value starts from the tail of offset_position,
 *   the offset_positions specify the end position of the field. Please cite the 5th page of the ppt lecture 3. ]
 * + [values]
 *
 * 3. Page format as following:
 * record values[as the record format]+ free space + record directory[start slot position[1 byte] + record size in slot [1 byte], total 2 bytes]
 * + record number [1 byte] + next available slot index [1 byte, start from 0]
 * */

#define PAGE_SIZE 4096
#define SLOT_SIZE 32

// 4094 = (32 + 2) * 120 + 14
#define MAX_OF_RECORD 120
#define PAGE_NUM unsigned


#include <string>
#include <climits>
#include <stdio.h>
#include <sys/stat.h>
#include <iostream>
#include <string.h>
#include <stdlib.h>

using namespace std;

class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                     			// Access to the _pf_manager instance

    RC createFile    (const string &fileName);                         	// Create a new file
    RC destroyFile   (const string &fileName);                         	// Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle); 	// Open a file
    RC closeFile     (FileHandle &fileHandle);                         	// Close a file

    /*add method*/
    bool exist	(const string &fileName);  //check the existence of the file
protected:
    PagedFileManager();                                   				// Constructor
    ~PagedFileManager();                                  				// Destructor

private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
    // variables to keep the counter for each operation
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;
	
    FileHandle();                                                    	// Default constructor
    ~FileHandle();                                                   	// Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables

    /* My Methods */
    FILE* pFile;

    /* My methods */
    /**
     * Notes:
     * 1. The index of file number starts from 0;
     * */
    RC appendNewPage();
    RC loadPageHeaderInfos(PageNum pageNum, byte& recordNum, byte& nextAvailableSlotIndex);  // load the pageHeader information into data
    void loadPageHeaderInfos(const void *pageData, byte& recordNum, byte& nextAvailableSlotIndex);  // override
    void updatePageHeaderInfos(const void *pageData, byte slotNum, byte nextAvailableSlotIndex);  // update the page header information of the last two bytes of the page data



private:
    /**
     * Move the operation pointer of the file to the beginning of the page specified by the page Index
     * @param pageIndex; starts from 0
     * */
    RC moveFileOpPointer(unsigned pageIndex);
}; 

#endif
