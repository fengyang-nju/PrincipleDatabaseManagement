#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	_pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
    if(_rbf_manager){
        delete _rbf_manager;
        _rbf_manager = 0;
    }
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return _pf_manager->createFile(fileName.c_str());
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName.c_str());
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName.c_str(),fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

	if(recordDescriptor.size() == 0){
		cout << "Record Descriptor is empty" << endl;
		return -1;
	}

	void* recordData = (byte*)malloc(PAGE_SIZE);
	this->convertAPIData2StoreFormat(recordDescriptor,data,recordData);

	// calculate recordLength, occupiedCells and insertLocation
	int recordLength = this->calculateRecordLength(recordDescriptor, recordData, RECORD);

	byte occupiedSlotNum = (char)(recordLength / SLOT_SIZE) + 1;
	byte insertSlotIndex = locateInsertSlotLocation(fileHandle, occupiedSlotNum, rid);

	if(insertSlotIndex < 0){
		return -1;
	}

	byte *pageData = new byte[PAGE_SIZE];
	if(fileHandle.readPage(rid.pageNum, pageData)){
		return -1;
	}

	memcpy((void *)(pageData + insertSlotIndex * SLOT_SIZE), (char *)recordData, recordLength);

	this->insertSlotItemInfo(pageData, insertSlotIndex, occupiedSlotNum);
	fileHandle.updatePageHeaderInfos(pageData,rid.slotNum+1,insertSlotIndex+occupiedSlotNum);

	if(fileHandle.writePage(rid.pageNum, pageData) < 0){
		return -1;
	}

	delete[] pageData;
	return 0;

}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	byte *pageData = new byte[PAGE_SIZE];
    if(fileHandle.readPage(rid.pageNum, pageData) < 0){
    	return -1;
	}
	byte startSlotIndex;
	byte occupiedSlotNum;
	this->loadSlotItemInfos(pageData, rid.slotNum, startSlotIndex, occupiedSlotNum);

	byte* recordContent = new byte[PAGE_SIZE];
	memcpy(recordContent, (byte*)pageData + startSlotIndex * SLOT_SIZE,occupiedSlotNum * SLOT_SIZE);
	byte* apiDataTemp = new byte[PAGE_SIZE];
	this->convertStoreFormat2APIData(recordDescriptor,recordContent,apiDataTemp);

	unsigned APIDataSize = this->calculateRecordLength(recordDescriptor,apiDataTemp,API);
	//unsigned storeRecordSize = APIDataSize+sizeof(int) + sizeof(int)*recordDescriptor.size();

	memcpy(data, apiDataTemp, APIDataSize);

	delete[] pageData;
	delete[] recordContent;
	delete[] apiDataTemp;
	return 0;
}


// Attention: the format of the data is the API format, that from the test case!!!
RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    if(recordDescriptor.size() == 0){
        return -1;
    }

    unsigned desFieldNum = recordDescriptor.size();
    unsigned offset = 0;

    unsigned nullIndicatorByteSize = ceil(desFieldNum / 8.0);
    unsigned nullBitsOffset = 0;

    offset+=sizeof(byte)*nullIndicatorByteSize;

    byte* nullBits = ((byte*)data+nullBitsOffset);
    unsigned char nullIndicatorPointer = 0x80; //initially, it's 10000000, in each of the following iteration, the "1" will be moved toward right.
    for(unsigned i = 0; i < desFieldNum; i++){
    	if(nullIndicatorPointer == 0){
    		nullIndicatorPointer = 0x80;
    		nullBitsOffset +=1;
    		nullBits = ((byte*)data+nullBitsOffset);
        }

    	if((*nullBits & nullIndicatorPointer) != 0){ // NULL
    	    cout << recordDescriptor[i].name << ": " << "NULL";
    	}else{  // not null
    		void* value;
    		if(recordDescriptor[i].type == TypeInt){
    			value = (int*)malloc(sizeof(int));
    			this->readInteger((byte*)data+offset, *(int*)value);
    			cout << recordDescriptor[i].name << ": " << *(int*)value <<" ";
    			offset+=sizeof(int);
			}else if(recordDescriptor[i].type == TypeReal){
    			value = (float*)malloc(sizeof(float));
    			this->readFloat((byte*)data+offset, *(float*)value);
				cout << recordDescriptor[i].name << ": " << *(float*)value <<" ";
				offset+=sizeof(float);
			}else{
				value = (char*)malloc(recordDescriptor[i].length);
    			char *charData = new char[PAGE_SIZE];
				int varcharLength = 0;
				this->readVarchar((byte*)data+offset,varcharLength,charData);
				cout << recordDescriptor[i].name << ": " << (char*)charData <<" ";
				delete[] charData;
				offset+=sizeof(int);
				offset+=varcharLength;
			}
			free(value);
    	}
    	nullIndicatorPointer = nullIndicatorPointer>>1;
    }
    cout<<endl;
    return 0;
}

RC RecordBasedFileManager::printRecordInStoreFormat(const vector<Attribute> &recordDescriptor, const void *recordData){
	byte* apiData = new byte[PAGE_SIZE];
	this->convertStoreFormat2APIData(recordDescriptor,recordData,apiData);
	this->printRecord(recordDescriptor,apiData);
	delete[] apiData;
	return 0;
}


void RecordBasedFileManager::readDataContent(void* data, int startOffset, int endOffset, void* value){
	memcpy(value, (byte*)data + startOffset, endOffset - startOffset);
}

void RecordBasedFileManager::readInteger(void* data, int offset, int& value){
	memcpy(&value, (byte*)data + offset, sizeof(int));
}

void RecordBasedFileManager::readInteger(void* data, int& value){
	this->readInteger(data,0,value);
}

void RecordBasedFileManager::readFloat(void* data, int offset, float& value){
    memcpy(&value, (byte*)data + offset, sizeof(float));
}

void RecordBasedFileManager::readFloat(void* data, float& value){
	this->readFloat(data,0,value);
}

void RecordBasedFileManager::readVarchar(void* data, int offset, int& valueLength, char* value){
	this->readInteger(data,offset,valueLength);
    memcpy((void*)value, (char*)data + offset + sizeof(int), valueLength);
    value[valueLength] = '\0';
}

void RecordBasedFileManager::readVarchar(void* data, int& valueLength, char* value){
	this->readVarchar(data,0,valueLength,value);
}

void RecordBasedFileManager::loadSlotItemInfos(const void *data, int slotIndex, byte& startSlotIndex, byte& occupiedSlotNum){
    /*Please refer the page format firstly*/
	int startOffset = PAGE_SIZE - 2 * sizeof(byte) - (slotIndex + 1) * (2 * sizeof(byte));
    memcpy(&startSlotIndex, (byte*)data + startOffset, sizeof(byte));
    memcpy(&occupiedSlotNum, (byte*)data + startOffset + sizeof(byte), sizeof(byte));
}

void RecordBasedFileManager::insertSlotItemInfo(const void *pageData, byte startSlotIndex, byte occupiedSlotNum){
    byte currentSlotNum = -1;
    memcpy(&currentSlotNum, (byte*)pageData+PAGE_SIZE - sizeof(byte)*2, sizeof(byte));
	int insertPosOffset = PAGE_SIZE - 2 * sizeof(byte) - (currentSlotNum + 1) * (2 * sizeof(byte));
	memcpy((byte*)pageData + insertPosOffset, &startSlotIndex, sizeof(byte));
	memcpy((byte*)pageData + insertPosOffset + sizeof(byte), &occupiedSlotNum, sizeof(byte));
}

unsigned RecordBasedFileManager::calculateRecordLength(const vector<Attribute> &recordDescriptor, const void *data, DataType type){
    if(recordDescriptor.size() == 0){
        return -1;
    }

    unsigned desFieldNum = recordDescriptor.size();
    unsigned nullIndicatorByteSize = ceil(desFieldNum / 8.0);
    unsigned nullBitsOffset = 0; //the null-indicator offset, initially, we read the 1st byte
    unsigned valueOffset = 0;
    valueOffset+=sizeof(byte)*nullIndicatorByteSize;  // the data offset, null-indicator space

    if(type==RECORD){
    	valueOffset+=sizeof(int); // the attribute size, one integer
    	valueOffset+=sizeof(int)*(desFieldNum); // the value position space;
    	nullBitsOffset+=sizeof(int); //For the record type, the nullBitsOffset should start from the 5th byte;
    }

    byte* nullBits = ((byte*)data+nullBitsOffset);
    unsigned char nullIndicatorPointer = 0x80; //initially, it's 10000000, in each of the following iteration, the "1" will be moved toward right.

    for(unsigned i = 0; i < desFieldNum; i++){
    	if(nullIndicatorPointer == 0){
    		nullIndicatorPointer = 0x80;
    		nullBitsOffset +=1;
    		nullBits = ((byte*)data+nullBitsOffset);
        }

    	if((*nullBits & nullIndicatorPointer) != 0){ // NULL
    		valueOffset+=0;
    	}else{  // not null
    		if(recordDescriptor[i].type == TypeInt){
    			valueOffset+=sizeof(int);
			}else if(recordDescriptor[i].type == TypeReal){
    			valueOffset+=sizeof(float);
			}else{
	    		void* value;
				value = (char*)malloc(recordDescriptor[i].length);
    			char *charData = new char[PAGE_SIZE];
				int varcharLength = 0;
				this->readVarchar((byte*)data+valueOffset,varcharLength,charData);
				delete[] charData;
				valueOffset+=sizeof(int);
				valueOffset+=varcharLength;
				free(value);
			}
    	}
    	nullIndicatorPointer = nullIndicatorPointer>>1;
    }

    return valueOffset;
}

void RecordBasedFileManager::convertAPIData2StoreFormat(const vector<Attribute> &recordDescriptor, const void *APIData, void* recordData){
	unsigned desFieldNum = recordDescriptor.size();
	unsigned apiDataOffset = 0;
	unsigned recordDataOffset = 0;

	//copy the size of field into the beginning of recordData;
	memcpy((byte*)recordData+recordDataOffset,&desFieldNum,sizeof(unsigned));
	recordDataOffset+=sizeof(unsigned);

	unsigned nullIndicatorByteSize = ceil(desFieldNum / 8.0);

	//copy the null-indicator into the record data;
	memcpy((byte*)recordData+recordDataOffset,APIData,sizeof(byte)*nullIndicatorByteSize);
	recordDataOffset+=sizeof(byte)*nullIndicatorByteSize;
	apiDataOffset+=sizeof(byte)*nullIndicatorByteSize;
	unsigned nullBitsOffset = 0;

	byte* nullBits = ((byte*)APIData+nullBitsOffset);
	unsigned char nullIndicatorPointer = 0x80; //initially, it's 10000000, in each of the following iteration, the "1" will be moved toward right.

	int* fieldPositionSpace = new int[desFieldNum];
	unsigned dataValueLength = 0;
	for(unsigned i = 0; i < desFieldNum; i++){
		if(nullIndicatorPointer == 0){
			nullIndicatorPointer = 0x80;
			nullBitsOffset +=1;
			nullBits = ((byte*)APIData+nullBitsOffset);
		}

		if((*nullBits & nullIndicatorPointer) != 0){ // NULL
			apiDataOffset+=0;
			dataValueLength+=0;
		}else{  // not null
			if(recordDescriptor[i].type == TypeInt){
				apiDataOffset+=sizeof(int);
				dataValueLength+=sizeof(int);
			}else if(recordDescriptor[i].type == TypeReal){
				apiDataOffset+=sizeof(float);
				dataValueLength+=sizeof(float);
			}else{
				void* value;
				value = (char*)malloc(recordDescriptor[i].length);
				char *charData = new char[PAGE_SIZE];
				int varcharLength = 0;
				this->readVarchar((byte*)APIData+apiDataOffset,varcharLength,charData);
				delete[] charData;
				apiDataOffset+=sizeof(int);
				apiDataOffset+=varcharLength;
				dataValueLength+=sizeof(int);
				dataValueLength+=varcharLength;

				free(value);
			}
		}
		fieldPositionSpace[i] = dataValueLength;
		nullIndicatorPointer = nullIndicatorPointer>>1;
	}

	//copy the offset_position into the record data
	memcpy((byte*)recordData+recordDataOffset,fieldPositionSpace,sizeof(int)*desFieldNum);
	delete[] fieldPositionSpace;
	recordDataOffset+=sizeof(int)*desFieldNum;

	//copy the data values into the record data
	memcpy((byte*)recordData+recordDataOffset,(byte*)APIData+sizeof(byte)*nullIndicatorByteSize,dataValueLength);
}

void RecordBasedFileManager::convertStoreFormat2APIData(const vector<Attribute> &recordDescriptor, const void *recordData, void* APIData){
	unsigned desFieldNum = recordDescriptor.size();
	unsigned apiDataOffset = 0;
	unsigned recordDataOffset = sizeof(unsigned);

	unsigned nullIndicatorByteSize = ceil(desFieldNum / 8.0);

	//copy the null-indicator into the api data;
	memcpy((byte*)APIData+apiDataOffset,(byte*)recordData+recordDataOffset,sizeof(byte)*nullIndicatorByteSize);
	recordDataOffset+=sizeof(byte)*nullIndicatorByteSize;
	apiDataOffset+=sizeof(byte)*nullIndicatorByteSize;

	unsigned lastValueEndPositionOffset = recordDataOffset + 4*(desFieldNum-1);
	int lastValueEndPosition = *(int*)((byte*)recordData+lastValueEndPositionOffset);
	recordDataOffset+=sizeof(byte)*4*desFieldNum;

	memcpy((byte*)APIData+apiDataOffset, (byte*)recordData+recordDataOffset,lastValueEndPosition);
}

byte RecordBasedFileManager::locateInsertSlotLocation(FileHandle &fileHandle,
		byte occupiedSlotNum, RID &rid) {
	PageNum totalNum = fileHandle.getNumberOfPages();

	byte firstAvailableSlotIndex = -1;
	byte slotNum = -1;
	PageNum currentPageIndex = 0;
	if (totalNum == 0) {
		fileHandle.appendNewPage();
		RC rc = fileHandle.loadPageHeaderInfos(currentPageIndex, slotNum,
				firstAvailableSlotIndex);
		if (rc < 0) {
			return -1;
		}

		if (firstAvailableSlotIndex + occupiedSlotNum < MAX_OF_RECORD) {
			// assign RID with current pageNum and next slot Num
			rid.pageNum = currentPageIndex;
			rid.slotNum = slotNum;
			if (rid.slotNum < 0) {
				return -1;
			}

			return firstAvailableSlotIndex;
		}
	}

	//search from the tail
	currentPageIndex = totalNum - 1;
	RC rc = fileHandle.loadPageHeaderInfos(currentPageIndex, slotNum,
			firstAvailableSlotIndex);
	if (rc < 0) {
		return -1;
	}

	if (firstAvailableSlotIndex + occupiedSlotNum < MAX_OF_RECORD) {
		// assign RID with current pageNum and next slot Num
		rid.pageNum = currentPageIndex;
		rid.slotNum = slotNum;
		if (rid.slotNum < 0) {
			return -1;
		}

		return firstAvailableSlotIndex;
	}

	currentPageIndex = 0;
	while (totalNum != 0 && currentPageIndex < (totalNum - 1)) {
		fileHandle.loadPageHeaderInfos(currentPageIndex, slotNum,
				firstAvailableSlotIndex);
		if (firstAvailableSlotIndex < 0) {
			return -1;
		}

		if (firstAvailableSlotIndex + occupiedSlotNum < MAX_OF_RECORD) {
			// assign RID with current pageNum and next slot Num
			rid.pageNum = currentPageIndex;
			rid.slotNum = slotNum;
			if (rid.slotNum < 0) {
				return -1;
			}

			return firstAvailableSlotIndex;
		}
		currentPageIndex++;
	}

	fileHandle.appendNewPage();
	currentPageIndex = fileHandle.getNumberOfPages() - 1;
	rc = fileHandle.loadPageHeaderInfos(currentPageIndex, slotNum,
			firstAvailableSlotIndex);
	if (rc < 0) {
		return -1;
	}

	if (firstAvailableSlotIndex + occupiedSlotNum < MAX_OF_RECORD) {
		// assign RID with current pageNum and next slot Num
		rid.pageNum = currentPageIndex;
		rid.slotNum = slotNum;
		if (rid.slotNum < 0) {
			return -1;
		}

		return firstAvailableSlotIndex;
	}
	return -1;
}
