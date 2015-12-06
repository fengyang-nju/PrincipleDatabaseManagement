#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <math.h>

#include "../rbf/pfm.h"

using namespace std;

// Record ID
typedef struct
{
  unsigned pageNum;	// page number
  unsigned slotNum; // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// = 
           LT_OP,      // <
           LE_OP,      // <=
           GT_OP,      // >
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP	   // no condition
} CompOp;

enum DataType {
	API = 0,
	RECORD
};

/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
  RBFM_ScanIterator() {};
  ~RBFM_ScanIterator() {};

  // Never keep the results in the memory. When getNextRecord() is called, 
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data);
  bool hasNextRecord();
  RC createNewScanIterator(FileHandle &fileHandle,
	      const vector<Attribute> &recordDescriptor,
	      const string &conditionAttribute,
	      const CompOp compOp,                  // comparision type such as "<" and "="
	      const void *value,                    // used in the comparison
	      const vector<string> &attributeNames);
  RC getNextProperRID(RID &rid);

  bool dataComparator(AttrType dataType, void* leftOperator, const void* rightOperator, const CompOp compOp);
  RC close();

private :
  bool getData(RID& rid, void* data);


private:
  FileHandle fileHandle;
  vector<Attribute> recordDescriptor;
  Attribute conditionAttribute;
  CompOp compOp;
  vector<string> attributeNames;
  const void* value;
  RID nextRID;
  RID finalRID;
  PageNum currentPageNum;
  byte currentPageRecordNum;
};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first, 
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
  // This method will be mainly used for debugging/testing. 
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

  /* My Methods*/
public:
  RC readRecordInStoredFormat(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);  //read the data base on the offset position information

  void readDataContent(void* data, int startOffset, int endOffset, void* value);  //read the data base on the offset position information
  void loadSlotItemInfos(const void *data, int slotIndex, byte& startSlotIndex, byte& occupiedSlotNum); // load the slot item information from slot directory
  void insertSlotItemInfo(const void *pageData, byte startSlotIndex, byte occupiedSlotNum); // append the slot item information at the tail of page

  static unsigned calculateRecordLength(const vector<Attribute> &recordDescriptor, const void *data, DataType type);
  RC printRecordInStoreFormat(const vector<Attribute> &recordDescriptor, const void *recordData);

  /* added in the 2nd project */
  //startSlotIndex, the start slot index of the data
  //
  RC moveDataRight(const void* pageData, byte fromWhereSlotIndex, byte dataLength, byte toWhereSlotIndex);
  RC moveDataLeft(const void* pageData, byte fromWhereSlotIndex, byte dataLength, byte toWhereSlotIndex);

  RC getRealRIDFromIndex(RID &realRID, void* pageData, const RID &indexRID);
  RC updateRIDList(const void* pageData, byte updateStartIndex, byte changeSize);

  RC readAttributesFromStoredRecord(const void* recordDataInStoredFormat, const vector<Attribute> &recordDescriptor,
			const vector<string> &attributeName, void *data);

  RC readAttributesFromAPIRecord(const void* recordDataInAPIFormat, const vector<Attribute> &recordDescriptor,
  			const vector<string> &attributeName, void *data);

//  RC  RecordBasedFileManager::assembleRecordIntoAttributesData(void* recordData, void *data,
//  		vector<Attribute> recordDescriptor, vector<string> attributeNames, DataType recordType);

public:
  static void readInteger(void* data, int offset, int& value);
  static void readFloat(void* data, int offset, float& value);
  static void readVarchar(void* data, int offset, int& valueLength, char* value);
  static void readInteger(void* data, int& value); //override
  static void readFloat(void* data, float& value); //override
  static void readVarchar(void* data, int& valueLength, char* value); //override

  static void convertAPIData2StoreFormat(const vector<Attribute> &recordDescriptor, const void *APIData, void* recordData);
  static void convertStoreFormat2APIData(const vector<Attribute> &recordDescriptor, const void *recordData, void* APIData);

  byte locateInsertSlotLocation(FileHandle &fileHandle, byte occupiedSlotNum, RID &rid);

public:

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
  PagedFileManager * _pf_manager;
};

#endif
