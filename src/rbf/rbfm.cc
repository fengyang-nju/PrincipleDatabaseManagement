#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
	_pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager() {
	if (_rbf_manager) {
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

RC RecordBasedFileManager::openFile(const string &fileName,
		FileHandle &fileHandle) {
	return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	return _pf_manager->closeFile(fileHandle);;
}

RC RBFM_ScanIterator::createNewScanIterator(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
		const void *value,                    // used in the comparison
		const vector<string> &attributeNames) {

	this->compOp = compOp;
	//memcpy(this->value,value);
	this->value = value;
	this->attributeNames = attributeNames;
	this->fileHandle = fileHandle;
	this->recordDescriptor = recordDescriptor;

	for (unsigned i = 0; i < recordDescriptor.size(); i++) {
		if (strcmp(conditionAttribute.c_str(), recordDescriptor[i].name.c_str())
				== 0) {
			this->conditionAttribute = recordDescriptor[i];
			break;
		}
	}

	this->nextRID.pageNum = 0;
	this->nextRID.slotNum = 0; //initialization

	if(this->fileHandle.getNumberOfPages() > 0){
		PageNum finalPageNum = this->fileHandle.getNumberOfPages() - 1;
		void* finalPageData = (byte*) malloc(PAGE_SIZE);
		this->fileHandle.readPage(finalPageNum, finalPageData);

		byte recordNum, nextAvailableSlotIndex;
		this->fileHandle.loadPageHeaderInfos(finalPageData, recordNum,
				nextAvailableSlotIndex);
		this->finalRID.pageNum = finalPageNum;


		this->finalRID.slotNum = recordNum - 1;

		this->currentPageNum = 0;
		void* firstPageData = (byte*) malloc(PAGE_SIZE);
		this->fileHandle.readPage(this->currentPageNum, firstPageData);
		this->fileHandle.loadPageHeaderInfos(firstPageData, recordNum,
					nextAvailableSlotIndex);
		this->currentPageRecordNum = recordNum;
		free(firstPageData);
		free(finalPageData);
	}else{
		PageNum finalPageNum = 0;
		this->finalRID.pageNum = finalPageNum;
		this->finalRID.slotNum = 0;
		this->currentPageNum = 0;
		this->currentPageRecordNum = 0;
	}
	return 0;

}

bool RBFM_ScanIterator::getData(RID& rid, void* data) {
	void* dataContent = (byte*) malloc(PAGE_SIZE);
	if (RecordBasedFileManager::instance()->readRecordInStoredFormat(fileHandle,
			recordDescriptor, rid, dataContent)!=0){
		free(dataContent);
		return -1;
	}
	//for debug
//	RecordBasedFileManager::instance()->printRecordInStoreFormat(
//			recordDescriptor, dataContent);

	RecordBasedFileManager::instance()->readAttributesFromStoredRecord(dataContent,
			recordDescriptor, this->attributeNames, data);
	free(dataContent);
	return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
	while (hasNextRecord()) {
//		if(this->finalRID.slotNum == this->nextRID.slotNum){
//			cout<<"==================="<<endl;
//		}

		if (this->compOp == NO_OP) {
			rid.pageNum = this->nextRID.pageNum;
			rid.slotNum = this->nextRID.slotNum;
			this->getNextProperRID(this->nextRID);
			if(this->getData(rid, data)==0){
				return 0;
			}else{
				continue;
			}
			return 0;
		} else {
			void* leftValue = (byte*) malloc(PAGE_SIZE);
			if(RecordBasedFileManager::instance()->readAttribute(
					this->fileHandle, this->recordDescriptor, this->nextRID,
					this->conditionAttribute.name, leftValue)==0){
				if (this->dataComparator(this->conditionAttribute.type,
						(byte*)leftValue+1, this->value, this->compOp)) {
					rid.pageNum = this->nextRID.pageNum;
					rid.slotNum = this->nextRID.slotNum;

					//for debug
					//cout<< "rid.pageNum  = "<<rid.pageNum  << " rid.slotNum = "<<rid.slotNum<<endl;

					this->getNextProperRID(this->nextRID);
					free(leftValue);
					if(this->getData(rid, data)==0){
						return 0;
					}else{
						continue;
					}
					return 0;
				}
			}
			free(leftValue);
		}
		if(this->getNextProperRID(this->nextRID)!=0)
			return RBFM_EOF;
	}
	return RBFM_EOF;
}

RC RBFM_ScanIterator::close() {
	return fclose(this->fileHandle.pFile);
}

bool RBFM_ScanIterator::dataComparator(AttrType dataType, void* leftOperator,
		const void* rightOperator, const CompOp compOp) {
	if (compOp == NO_OP)
		return true;
	if (dataType == TypeVarChar) {
		char* left = (char*) ((byte*)leftOperator+sizeof(unsigned));
		char* right = (char*) ((byte*)rightOperator+sizeof(unsigned));
		if (compOp == EQ_OP) {
			return strcmp(left, right) == 0;
		}
		if (compOp == LT_OP) {
			return strcmp(left, right) < 0;
		}
		if (compOp == GT_OP) {
			return strcmp(left, right) > 0;
		}
		if (compOp == LE_OP) {
			return strcmp(left, right) <= 0;
		}
		if (compOp == GE_OP) {
			return strcmp(left, right) >= 0;
		}
		if (compOp == NE_OP) {
			return strcmp(left, right) != 0;
		}
		return true;
	} else if (dataType == TypeInt) {

		int left = *(int*) leftOperator;
		int right = *(int*) rightOperator;
		if (compOp == EQ_OP) {
			return left == right;
		}
		if (compOp == LT_OP) {
			return left < right;
		}
		if (compOp == LE_OP) {
			return left <= right;
		}
		if (compOp == GT_OP) {
			return left > right;
		}
		if (compOp == GE_OP) {
			return left >= right;
		}
		if (compOp == NE_OP) {
			return left != right;
		}
		return true;
	} else if (dataType == TypeReal) {
		float left = *(float*) leftOperator;
		float right = *(float*) rightOperator;
		if (compOp == EQ_OP) {
			return left == right;
		}
		if (compOp == LT_OP) {
			return left < right;
		}
		if (compOp == LE_OP) {
			return left <= right;
		}
		if (compOp == GT_OP) {
			return left > right;
		}
		if (compOp == GE_OP) {
			return left >= right;
		}
		if (compOp == NE_OP) {
			return left != right;
		}
		return true;
	} else {
		return false;
	}
}

RC RBFM_ScanIterator::getNextProperRID(RID &rid) {

	if (rid.pageNum == this->currentPageNum
			&& rid.slotNum < (this->currentPageRecordNum - 1)) {
		//increase the slot index
		rid.slotNum++;
	} else if (rid.pageNum == this->currentPageNum
			&& rid.slotNum == (this->currentPageRecordNum - 1)) {
		if (rid.pageNum != this->finalRID.pageNum) {
			//increase the page number and reset the slot index
			PageNum nextPageNum = rid.pageNum + 1;
			void* pageData = (byte*) malloc(PAGE_SIZE);
			this->fileHandle.readPage(nextPageNum, pageData);
			byte recordNum, nextAvailableSlotIndex;
			this->fileHandle.loadPageHeaderInfos(pageData, recordNum,
					nextAvailableSlotIndex);

			this->currentPageRecordNum = recordNum;
			this->currentPageNum = nextPageNum;
			rid.pageNum = this->currentPageNum;
			rid.slotNum = 0;

			free(pageData);
			return 0;
		} else {
			//reach the final rid;
			rid.slotNum++;
			return -1;
		}
	}
	return 0;
}

bool RBFM_ScanIterator::hasNextRecord() {
	//for debug
//	if(this->finalRID.slotNum == this->nextRID.slotNum){
//		cout<<"==================="<<endl;
//	}
	if(this->finalRID.pageNum==0 && this->finalRID.pageNum ==0 && this->finalRID.slotNum==0)
		return false;

	if (this->finalRID.pageNum > this->nextRID.pageNum)
		return true;
	else if (this->finalRID.pageNum == this->nextRID.pageNum
			&& (this->finalRID.slotNum) >= this->nextRID.slotNum)
		return true;

	return false;
}

RC RecordBasedFileManager::readAttributesFromAPIRecord(const void* recordDataInAPIFormat, const vector<Attribute> &recordDescriptor,
			const vector<string> &attributeName, void *data){
	byte* recordDataInStoreFormat = new byte[PAGE_SIZE];
	this->convertAPIData2StoreFormat(recordDescriptor,recordDataInAPIFormat,recordDataInStoreFormat);
	this->readAttributesFromStoredRecord(recordDataInStoreFormat, recordDescriptor, attributeName, data);
	delete[] recordDataInStoreFormat;
	return 0;
}

//the vector<Attribute> recordDescriptor should be all attributes;
RC RecordBasedFileManager::readAttributesFromStoredRecord(
		const void* recordDataInStoredFormat,
		const vector<Attribute> &recordDescriptor,
		const vector<string> &attributeName, void *data) {

	//for debug
	//this->printRecordInStoreFormat(recordDescriptor, recordDataInStoredFormat);

	unsigned storedFieldSize = *(unsigned*) recordDataInStoredFormat;

	//unsigned nameSize = attributeName.size();
	byte nullIndicator = 0;
	byte standardNullIndicator = 0x80;

	unsigned nullIndicatorOffset = 0;
	unsigned i, j;

	unsigned dataOffset = ceil(attributeName.size()/8.0);
	for (i = 0; i < attributeName.size(); i++) {
		if (i != 0 && i % 8 == 0) {
			memcpy((byte*) data + nullIndicatorOffset, &nullIndicator,
					sizeof(byte));
			nullIndicator = 0;
			standardNullIndicator = 0x80;
			nullIndicatorOffset++;
		}
		for (j = 0; j < recordDescriptor.size(); j++) {
			if (strcmp(recordDescriptor[j].name.c_str(),
					attributeName[i].c_str()) == 0) {
				break;
			}
		}

		if (j == recordDescriptor.size()) {
			//setNull
			nullIndicator |= standardNullIndicator;
		} else {
			unsigned start = sizeof(unsigned) + ceil(storedFieldSize / 8.0)	+ (storedFieldSize * sizeof(unsigned));
			unsigned valueStartOffset;
			unsigned offset;

			if (j == 0) {
				valueStartOffset = 0;
			} else {
				offset = sizeof(unsigned) + ceil(storedFieldSize / 8.0)	+ (j - 1) * sizeof(unsigned);
				memcpy(&valueStartOffset,
						(byte*) recordDataInStoredFormat + offset,
						sizeof(unsigned));
			}

			offset = sizeof(unsigned) + ceil(storedFieldSize / 8.0)
					+ j * sizeof(unsigned);
			unsigned valueEndOffset;
			memcpy(&valueEndOffset, (byte*) recordDataInStoredFormat + offset,
					sizeof(unsigned));

			if (valueEndOffset == valueStartOffset) {
				//it is null
				nullIndicator |= standardNullIndicator;
			} else {
				memcpy((byte*)data+dataOffset,
						(byte*) recordDataInStoredFormat + start
								+ valueStartOffset,
						valueEndOffset - valueStartOffset);
			}
			dataOffset += valueEndOffset - valueStartOffset;
		}
		standardNullIndicator = standardNullIndicator >> 1;
	}
	memcpy((byte*) data + nullIndicatorOffset, &nullIndicator, sizeof(byte));
	return 0;
}

//the record descriptor contains the full attribute list, including the deleted ones.
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid,
		const string &attributeName, void *data) {

	//for debug
	//cout << "attributeName = " << attributeName << endl;

	byte* recordData = (byte*) malloc(PAGE_SIZE);
	if(this->readRecordInStoredFormat(fileHandle, recordDescriptor, rid,recordData)!=0){
		free(recordData);
		return -1;
	}
	//for debug
	//this->printRecordInStoreFormat(recordDescriptor,recordData);

	unsigned i = 0;
	for (i = 0; i < recordDescriptor.size(); i++) {
		if (strcmp(recordDescriptor[i].name.c_str(), attributeName.c_str())
				== 0) {
			break;
		}
	}

	byte nullIndicator = 0;
	unsigned storedFieldSize = *((unsigned*)recordData);

	unsigned nullIndicatorByteSize = ceil(storedFieldSize / 8.0);

	if (recordDescriptor.size() > storedFieldSize || i >= storedFieldSize) {
		return 0;
	} else {
		unsigned start = sizeof(unsigned) + ceil(storedFieldSize / 8.0)+ (storedFieldSize * sizeof(unsigned));
		unsigned offset;

		unsigned valueStartOffset;
		if(i==0){
			valueStartOffset=0;
		}else{
			memcpy(&valueStartOffset, (byte*) recordData + sizeof(unsigned) +nullIndicatorByteSize  +(i-1)*sizeof(unsigned),sizeof(unsigned));
		}

//		if (i == 0) {
//			valueStartOffset = 0;
//		} else {
//			offset = sizeof(unsigned) + ceil(storedFieldSize / 8.0)+ (i - 1) * sizeof(unsigned);
//			unsigned valueStartOffset;
//			memcpy(&valueStartOffset, recordData + offset, sizeof(unsigned));
//		}

		offset = sizeof(unsigned) + ceil(storedFieldSize / 8.0) + i * sizeof(unsigned);
		unsigned valueEndOffset;
		memcpy(&valueEndOffset, recordData + offset, sizeof(unsigned));
		if (valueEndOffset == valueStartOffset) {
			nullIndicator |= 128;
		} else {
			memcpy((byte*) data + sizeof(byte), recordData + start + valueStartOffset,
					valueEndOffset - valueStartOffset);
		}
	}
	memcpy(data, &nullIndicator, 1);
	free(recordData);
	return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
		const void *value,                    // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RBFM_ScanIterator &rbfm_ScanIterator) {

	return -1;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {
	byte* pageData = (byte*) malloc(PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, pageData) != 0) {
		return -1;
	}

	//for debug
//	byte* recordInfo = new byte[PAGE_SIZE];
//	this->readRecord(fileHandle,recordDescriptor,rid,recordInfo);
//	this->printRecord(recordDescriptor,recordInfo);

	byte startSlotIndex, occupiedSlotSize;
	this->loadSlotItemInfos(pageData, rid.slotNum, startSlotIndex,
			occupiedSlotSize);

	if (occupiedSlotSize == 0)
		return -1;

	if (startSlotIndex < 0) {
		RID realRID;
		this->getRealRIDFromIndex(realRID, pageData, rid);
		this->deleteRecord(fileHandle, recordDescriptor, realRID);
		startSlotIndex &= 127;
	}
	//if the occupiedSlotSize<-1, which means the
	byte recordNum, nextAvailableSlotIndex;
	fileHandle.loadPageHeaderInfos(pageData, recordNum, nextAvailableSlotIndex);
	this->moveDataLeft(pageData, startSlotIndex + occupiedSlotSize,
			nextAvailableSlotIndex, startSlotIndex);
	//negative means delete some space
	this->updateRIDList(pageData, rid.slotNum, -occupiedSlotSize);

	fileHandle.writePage(rid.pageNum, pageData);
	free(pageData);

	//for debug
//	pageData = (byte*) malloc(PAGE_SIZE);
//	RID tmp;
//	tmp.pageNum = rid.pageNum;
//	tmp.slotNum = rid.slotNum+1;
//	this->readRecord(fileHandle,recordDescriptor,tmp,pageData);
//	this->printRecord(recordDescriptor,pageData);
//	free(pageData);

	return 0;
}

RC RecordBasedFileManager::getRealRIDFromIndex(RID &realRID, void* pageData,
		const RID &indexRID) {
	byte startSlotIndex, occupiedSlotSize;
	this->loadSlotItemInfos(pageData, indexRID.slotNum, startSlotIndex,
			occupiedSlotSize);
	if (startSlotIndex < 0) {
		startSlotIndex &= 127;
		PageNum dataPageNum;
		PageNum dataRIDNum;
		memcpy(&dataPageNum, (byte*) pageData + startSlotIndex * SLOT_SIZE, sizeof(PageNum));
		memcpy(&dataRIDNum, (byte*) pageData + startSlotIndex * SLOT_SIZE + sizeof(PageNum), sizeof(PageNum));
		realRID.pageNum = dataPageNum;
		realRID.slotNum = dataRIDNum;
	} else {
		cout
				<< "Error, the startSlot Index is le to 0, we should not resolve it!"
				<< endl;
		return -1;
	}
	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data,
		const RID &rid) {
	if (recordDescriptor.size() == 0) {
		cout << "Record Descriptor is empty" << endl;
		return -1;
	}
	void* newRecordData = (byte*) malloc(PAGE_SIZE);
	this->convertAPIData2StoreFormat(recordDescriptor, data, newRecordData);

	void* originalData = (byte*) malloc(PAGE_SIZE);
	this->readRecord(fileHandle, recordDescriptor, rid, originalData);
	void* originalStoredData = (byte*) malloc(PAGE_SIZE);
	this->convertAPIData2StoreFormat(recordDescriptor, originalData,
			originalStoredData);

	// calculate recordLength, occupiedCells and insertLocation
	int newRecordLength = this->calculateRecordLength(recordDescriptor, newRecordData, RECORD);
	byte newOccupiedSlotNum = (byte) (newRecordLength / SLOT_SIZE) + 1;

	byte *pageData = new byte[PAGE_SIZE];
	if (fileHandle.readPage(rid.pageNum, pageData) < 0) {
		return -1;
	}
	byte recordNum, nextAvailableSlotIndex;
	fileHandle.loadPageHeaderInfos(pageData, recordNum, nextAvailableSlotIndex);

	byte recordStartSlotIndex, oldOccupiedSlotNum;
	this->loadSlotItemInfos(pageData, rid.slotNum, recordStartSlotIndex,
			oldOccupiedSlotNum);

	if (oldOccupiedSlotNum < newOccupiedSlotNum) {
		//it maybe need to set up a new page
		//cout<<"recordStartSlotIndex = "<<recordStartSlotIndex<<" newOccupiedSlotNum = "<<newOccupiedSlotNum<<" oldOccupiedSlotNum = "<<oldOccupiedSlotNum<<endl;
		if ((MAX_OF_RECORD - nextAvailableSlotIndex)
				< (newOccupiedSlotNum - oldOccupiedSlotNum)) {
			//move the page data into another place;
			RID realRID;
			if(this->insertRecord(fileHandle, recordDescriptor, data, realRID)!=0)
			{
				cout<< "insert record failed in updating record!"<<endl;
			}
			memcpy(pageData + recordStartSlotIndex * SLOT_SIZE, &realRID.pageNum, sizeof(unsigned));
			memcpy(pageData + recordStartSlotIndex * SLOT_SIZE + sizeof(unsigned), &realRID.slotNum, sizeof(unsigned));

			this->moveDataLeft(pageData,recordStartSlotIndex + oldOccupiedSlotNum,nextAvailableSlotIndex, recordStartSlotIndex + 1);
			this->updateRIDList(pageData, rid.slotNum,	1 - oldOccupiedSlotNum);
			//change the slot item info into index style;
			recordStartSlotIndex |= 128;
			unsigned offset = PAGE_SIZE - 2 * sizeof(byte) 	- (rid.slotNum + 1) * 2 * sizeof(byte);
			memcpy(pageData + offset, &recordStartSlotIndex, sizeof(byte));

		} else {
			//move the page data here
				this->moveDataRight(pageData,recordStartSlotIndex + oldOccupiedSlotNum,nextAvailableSlotIndex, recordStartSlotIndex+newOccupiedSlotNum);
				memcpy(pageData + recordStartSlotIndex * SLOT_SIZE, newRecordData, newRecordLength);
				this->updateRIDList(pageData, rid.slotNum, newOccupiedSlotNum - oldOccupiedSlotNum);
		}
	} else {
		//don't need to set up a new page
		if(oldOccupiedSlotNum != newOccupiedSlotNum){
			memcpy(pageData + recordStartSlotIndex * SLOT_SIZE, newRecordData, newRecordLength);
			this->moveDataLeft(pageData, recordStartSlotIndex + oldOccupiedSlotNum,
					nextAvailableSlotIndex, recordStartSlotIndex + newOccupiedSlotNum);
			this->updateRIDList(pageData, rid.slotNum, newOccupiedSlotNum - oldOccupiedSlotNum);
		}else{
			memcpy(pageData + recordStartSlotIndex * SLOT_SIZE, newRecordData, newRecordLength);
		}
	}
	if (fileHandle.writePage(rid.pageNum, pageData) < 0) {
		cout<<"file write failed!"<<endl;
		return -1;
	}

	delete[] pageData;
	pageData = 0;
	free(newRecordData);
	free(originalData);
	free(originalStoredData);
	return 0;
}

// the changeSize could be negative or positive, positive means move right, negative means move left
RC RecordBasedFileManager::updateRIDList(const void* pageData,
		byte updateStartIndex, byte changeSize) {
	//update next available length of the page;
	unsigned offset = PAGE_SIZE - 1;
	byte nextAvailableTemp;
	memcpy(&nextAvailableTemp, (byte*) pageData + offset, sizeof(byte));
	nextAvailableTemp = nextAvailableTemp + changeSize;
	memcpy((byte*) pageData + offset, &nextAvailableTemp, sizeof(byte));

	byte recordNum;
	offset = PAGE_SIZE - 2;
	memcpy(&recordNum, (byte*) pageData + offset, sizeof(byte));

	//then, we should update the occupied length of the startRIDIndex;
	offset = PAGE_SIZE - (updateStartIndex + 1) * 2 - 2 * sizeof(byte) + 1;
	byte slotLength = 0;
	memcpy(&slotLength, (byte*) pageData + offset, sizeof(byte));
	slotLength = slotLength + changeSize;
	memcpy((byte*) pageData + offset, &slotLength, sizeof(byte));

	//update the startIndex of the following data;
	byte followingStartSlot = 0;
	for (byte i = updateStartIndex + 1; i < recordNum; i++) {
		offset = PAGE_SIZE - (i + 1) * sizeof(byte) * 2 - 2 * sizeof(byte);
		memcpy(&followingStartSlot, (byte*) pageData + offset,
				sizeof(byte));
		bool isIndexFlag = followingStartSlot < 0;
		if (isIndexFlag < 0) {
			followingStartSlot &= 127;
		}
		followingStartSlot = followingStartSlot + changeSize;
		if (isIndexFlag < 0) {
			followingStartSlot |= 128;
		}
		memcpy((char *) pageData + offset, &followingStartSlot, sizeof(byte));
	}
	return 0;
}

RC RecordBasedFileManager::moveDataRight(const void* pageData,
		byte fromWhereSlotIndex, byte dataLengthInSlot, byte toWhereSlotIndex) {

	unsigned fromIndex = fromWhereSlotIndex * SLOT_SIZE;
	unsigned toIndex = toWhereSlotIndex * SLOT_SIZE;
	unsigned allDataSize = dataLengthInSlot * SLOT_SIZE;
	unsigned movingSize = allDataSize - fromIndex;
	unsigned needSetBlankDataSize = toIndex - fromIndex;

	if (fromIndex > toIndex) {
		cout << "fromIndex = " << fromIndex << " allDataSize = " << allDataSize
				<< " toIndex = " << toIndex << " movingSize = " << movingSize
				<< " needSetBlankDataSize = " << needSetBlankDataSize << endl;
		return -1;
	}

	byte* tmpData = new byte[PAGE_SIZE];
	memcpy(tmpData, (byte*) pageData + fromIndex, movingSize);
	memcpy((byte*) pageData + toIndex, tmpData, movingSize);
	delete[] tmpData;
	tmpData = 0;

	memset((byte*) pageData + fromIndex, 0, needSetBlankDataSize);

	return 0;
}

RC RecordBasedFileManager::moveDataLeft(const void* pageData,
		byte fromWhereSlotIndex, byte dataLengthInSlot, byte toWhereSlotIndex) {

	unsigned fromIndex = fromWhereSlotIndex * SLOT_SIZE;
	unsigned toIndex = toWhereSlotIndex * SLOT_SIZE;
	unsigned allDataSize = dataLengthInSlot * SLOT_SIZE;
	unsigned movingSize = allDataSize - fromIndex;
	unsigned needSetBlankDataSize = fromIndex - toIndex;

	if (fromWhereSlotIndex <= toWhereSlotIndex) {
		cout << "fromIndex = " << fromIndex << " allDataSize = " << allDataSize
				<< " toIndex = " << toIndex << " movingSize = " << movingSize
				<< " needSetBlankDataSize = " << needSetBlankDataSize << endl;
		return -1;
	}

	byte* tmpData = new byte[PAGE_SIZE];
	memcpy(tmpData, (byte*) pageData + fromIndex, movingSize);
	memcpy((byte*) pageData + toIndex, tmpData, movingSize);
	delete[] tmpData;
	tmpData = 0;
	memset((byte*) pageData + toIndex + movingSize, 0, needSetBlankDataSize);
	return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

	if (recordDescriptor.size() == 0) {
		cout << "Record Descriptor is empty" << endl;
		return -1;
	}

	void* recordData = (byte*) malloc(PAGE_SIZE);
	this->convertAPIData2StoreFormat(recordDescriptor, data, recordData);

	// calculate recordLength, occupiedCells and insertLocation
	int recordLength = this->calculateRecordLength(recordDescriptor, recordData,
			RECORD);

	byte occupiedSlotNum = (byte) (recordLength / SLOT_SIZE) + 1;
	byte insertSlotIndex = locateInsertSlotLocation(fileHandle, occupiedSlotNum,
			rid);

	if (insertSlotIndex < 0) {
		cout<<"Didn't find the proper insert position!"<<endl;
		return -1;
	}

	byte *pageData = new byte[PAGE_SIZE];
	if (fileHandle.readPage(rid.pageNum, pageData)!=0) {
		cout<<"Read page failed! pageNum = "<< rid.pageNum <<endl;
		return -1;
	}

	memcpy((void *) (pageData + insertSlotIndex * SLOT_SIZE),
			(char *) recordData, recordLength);

	this->insertSlotItemInfo(pageData, insertSlotIndex, occupiedSlotNum);
	fileHandle.updatePageHeaderInfos(pageData, rid.slotNum + 1,
			insertSlotIndex + occupiedSlotNum);

	if (fileHandle.writePage(rid.pageNum, pageData) < 0) {
		cout<< "write page failed, pageNum = " <<rid.pageNum<<endl;
		return -1;
	}

	delete[] pageData;
	return 0;

}

RC RecordBasedFileManager::readRecordInStoredFormat(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	byte *pageData = new byte[PAGE_SIZE];
	if (fileHandle.readPage(rid.pageNum, pageData) < 0) {
		delete[] pageData;
		cout<<"read page failed!"<<endl;
		return -1;
	}
	byte startSlotIndex;
	byte occupiedSlotNum;
	this->loadSlotItemInfos(pageData, rid.slotNum, startSlotIndex,
			occupiedSlotNum);

	if(occupiedSlotNum==0){
		delete[] pageData;
		//cout<<"this record is deleted! startSlotIndex = "<<startSlotIndex<< " rid.pageNum = "<<rid.pageNum << " rid.slotNum = "<<rid.slotNum<<endl;
		return -1;
	}

	if (startSlotIndex < 0) {
		RID realRID;
		this->getRealRIDFromIndex(realRID, pageData, rid);
		if (this->readRecordInStoredFormat(fileHandle, recordDescriptor,
				realRID, data) == 0) {
			delete[] pageData;
			return 0;
		} else {
			delete[] pageData;
			return -1;
		}

	}

	byte* recordContent = new byte[PAGE_SIZE];
	memcpy(recordContent, (byte*) pageData + startSlotIndex * SLOT_SIZE,
			occupiedSlotNum * SLOT_SIZE);
	unsigned length = this->calculateRecordLength(recordDescriptor,
			recordContent, RECORD);
	memcpy(data, (byte*) recordContent, length);

	delete[] pageData;
	delete[] recordContent;
	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	byte *pageData = new byte[PAGE_SIZE];
	if (fileHandle.readPage(rid.pageNum, pageData) < 0) {
		delete[] pageData;
		return -1;
	}
	byte startSlotIndex;
	byte occupiedSlotNum;
	this->loadSlotItemInfos(pageData, rid.slotNum, startSlotIndex,
			occupiedSlotNum);

	if(occupiedSlotNum==0){
		//for debug
		//cout<<"this record already deleted!"<<endl;
		delete[] pageData;
		return -1;
	}

	if (startSlotIndex < 0) {
		//encounter the
		RID realRID;
		this->getRealRIDFromIndex(realRID, pageData, rid);
		delete[] pageData;
		return this->readRecord(fileHandle, recordDescriptor, realRID, data);
	}

	byte* recordContent = new byte[PAGE_SIZE];
	memcpy(recordContent, (byte*) pageData + startSlotIndex * SLOT_SIZE,
			occupiedSlotNum * SLOT_SIZE);
	this->convertStoreFormat2APIData(recordDescriptor, recordContent, data);

	delete[] pageData;
	delete[] recordContent;
	return 0;
}

// Attention: the format of the data is the API format, that from the test case!!!
RC RecordBasedFileManager::printRecord(
		const vector<Attribute> &recordDescriptor, const void *data) {
	if (recordDescriptor.size() == 0) {
		cout << "Record Descriptor is empty!" << endl;
		return -1;
	}

	unsigned desFieldNum = recordDescriptor.size();
	unsigned offset = 0;

	unsigned nullIndicatorByteSize = ceil(desFieldNum / 8.0);
	unsigned nullBitsOffset = 0;

	offset += sizeof(byte) * nullIndicatorByteSize;

	byte* nullBits = ((byte*) data + nullBitsOffset);
	unsigned char nullIndicatorPointer = 0x80; //initially, it's 10000000, in each of the following iteration, the "1" will be moved toward right.
	for (unsigned i = 0; i < desFieldNum; i++) {
		if (nullIndicatorPointer == 0) {
			nullIndicatorPointer = 0x80;
			nullBitsOffset += 1;
			nullBits = ((byte*) data + nullBitsOffset);
		}

		if ((*nullBits & nullIndicatorPointer) != 0) { // NULL
			cout << recordDescriptor[i].name << ": " << "NULL";
		} else {  // not null
			void* value;
			if (recordDescriptor[i].type == TypeInt) {
				value = (int*) malloc(sizeof(int));
				this->readInteger((byte*) data + offset, *(int*) value);
				cout << recordDescriptor[i].name << ": " << *(int*) value
						<< " ";
				offset += sizeof(int);
			} else if (recordDescriptor[i].type == TypeReal) {
				value = (float*) malloc(sizeof(float));
				this->readFloat((byte*) data + offset, *(float*) value);
				cout << recordDescriptor[i].name << ": " << *(float*) value
						<< " ";
				offset += sizeof(float);
			} else {
				value = (char*) malloc(recordDescriptor[i].length);
				char *charData = new char[PAGE_SIZE];
				int varcharLength = 0;
				this->readVarchar((byte*) data + offset, varcharLength,
						charData);
				cout << recordDescriptor[i].name << ": " << (char*) charData
						<< " ";
				delete[] charData;
				offset += sizeof(int);
				offset += varcharLength;
			}
			free(value);
		}
		nullIndicatorPointer = nullIndicatorPointer >> 1;
	}
	cout << endl;
	return 0;
}

RC RecordBasedFileManager::printRecordInStoreFormat(
		const vector<Attribute> &recordDescriptor, const void *data) {
//	byte* apiData = new byte[PAGE_SIZE];
//	this->convertStoreFormat2APIData(recordDescriptor,recordData,apiData);
//	this->printRecord(recordDescriptor,apiData);
//	delete[] apiData;
//	return 0;

	if (recordDescriptor.size() == 0) {
		cout << "Record Descriptor is empty!" << endl;
		return -1;
	}
	unsigned desFieldNum = *(unsigned*)data;
	cout<<"StoredFieldNum = " <<desFieldNum <<" The number of attributes and storedField equals = " << (desFieldNum == recordDescriptor.size())<<endl;

	unsigned offset = 0;
	unsigned nullIndicatorByteSize = ceil(desFieldNum / 8.0);


	offset += sizeof(byte) * nullIndicatorByteSize;

	unsigned nullBitsOffset = 1;
	byte nullBits = *((byte*) data + nullBitsOffset);
	unsigned char nullIndicatorPointer = 0x80; //initially, it's 10000000, in each of the following iteration, the "1" will be moved toward right.

	unsigned dataStartOffset = sizeof(unsigned) + nullIndicatorByteSize + desFieldNum*sizeof(unsigned);
	for (unsigned i = 0; i < desFieldNum; i++) {
		if (nullIndicatorPointer == 0) {
			nullIndicatorPointer = 0x80;
			nullBitsOffset += 1;
			nullBits = *((byte*) data + nullBitsOffset);
		}

		unsigned valueStartOffset;
		if(i==0){
			valueStartOffset=0;
		}else{
			memcpy(&valueStartOffset, (byte*) data + sizeof(unsigned) + nullIndicatorByteSize+(i-1)*sizeof(unsigned),sizeof(unsigned));
		}

		unsigned valueEndOffset;
		memcpy(&valueEndOffset, (byte*) data + sizeof(unsigned) + nullIndicatorByteSize+i*sizeof(unsigned),sizeof(unsigned));

		byte* value = (byte*) malloc(PAGE_SIZE);
		memset(value,0,PAGE_SIZE);

		memcpy(value,(byte*)data+dataStartOffset+valueStartOffset,valueEndOffset-valueStartOffset);

		cout<<"startOffset = "<<valueStartOffset<<" endOffset = "<<valueEndOffset;

		if ((nullBits & nullIndicatorPointer) != 0) { // NULL
			cout << recordDescriptor[i].name << ": " << "NULL";
		} else {  // not null

			if (recordDescriptor[i].type == TypeInt) {
				cout << recordDescriptor[i].name << ": " << *(int*) value
						<< " ";
				offset += sizeof(int);
			} else if (recordDescriptor[i].type == TypeReal) {
				cout << recordDescriptor[i].name << ": " << *(float*) value
						<< " ";
				offset += sizeof(float);
			} else {
				int varcharLength = 0;
				cout << recordDescriptor[i].name << ": " << *(unsigned*) value
						<< " - " << (char*)(value+sizeof(unsigned));
				offset += sizeof(int);
				offset += varcharLength;
			}
			free(value);
		}
		nullIndicatorPointer = nullIndicatorPointer >> 1;
		cout<<endl;
	}
	cout << endl;
	return 0;
}

void RecordBasedFileManager::readDataContent(void* data, int startOffset,
		int endOffset, void* value) {
	memcpy(value, (byte*) data + startOffset, endOffset - startOffset);
}

void RecordBasedFileManager::readInteger(void* data, int offset, int& value) {
	memcpy(&value, (byte*) data + offset, sizeof(int));
}

void RecordBasedFileManager::readInteger(void* data, int& value) {
	readInteger(data, 0, value);
}

void RecordBasedFileManager::readFloat(void* data, int offset, float& value) {
	memcpy(&value, (byte*) data + offset, sizeof(float));
}

void RecordBasedFileManager::readFloat(void* data, float& value) {
	readFloat(data, 0, value);
}

void RecordBasedFileManager::readVarchar(void* data, int offset,
		int& valueLength, char* value) {
	readInteger(data, offset, valueLength);
	memcpy((void*) value, (char*) data + offset + sizeof(int), valueLength);
	value[valueLength] = '\0';
}

void RecordBasedFileManager::readVarchar(void* data, int& valueLength,
		char* value) {
	readVarchar(data, 0, valueLength, value);
}

void RecordBasedFileManager::loadSlotItemInfos(const void *data, int slotIndex,
		byte& startSlotIndex, byte& occupiedSlotNum) {
	/*Please refer the page format firstly*/
	int startOffset = PAGE_SIZE - 2 * sizeof(byte)
			- (slotIndex + 1) * (2 * sizeof(byte));
	memcpy(&startSlotIndex, (byte*) data + startOffset, sizeof(byte));
	memcpy(&occupiedSlotNum, (byte*) data + startOffset + sizeof(byte),
			sizeof(byte));
}

void RecordBasedFileManager::insertSlotItemInfo(const void *pageData,
		byte startSlotIndex, byte occupiedSlotNum) {
	byte currentSlotNum = -1;
	memcpy(&currentSlotNum, (byte*) pageData + PAGE_SIZE - sizeof(byte) * 2,
			sizeof(byte));
	int insertPosOffset = PAGE_SIZE - 2 * sizeof(byte)
			- (currentSlotNum + 1) * (2 * sizeof(byte));
	memcpy((byte*) pageData + insertPosOffset, &startSlotIndex, sizeof(byte));
	memcpy((byte*) pageData + insertPosOffset + sizeof(byte), &occupiedSlotNum,
			sizeof(byte));
}

unsigned RecordBasedFileManager::calculateRecordLength(
		const vector<Attribute> &recordDescriptor, const void *data,
		DataType type) {
	if (recordDescriptor.size() == 0) {
		return -1;
	}

	unsigned desFieldNum = recordDescriptor.size();
	unsigned nullIndicatorByteSize = ceil(desFieldNum / 8.0);
	unsigned nullBitsOffset = 0; //the null-indicator offset, initially, we read the 1st byte
	unsigned valueOffset = 0;
	valueOffset += sizeof(byte) * nullIndicatorByteSize; // the data offset, null-indicator space

	if (type == RECORD) {
		valueOffset += sizeof(unsigned); // the attribute size, one integer
		valueOffset += sizeof(int) * (desFieldNum); // the value position space;
		nullBitsOffset += sizeof(int); //For the record type, the nullBitsOffset should start from the 5th byte;
	}

	byte* nullBits = ((byte*) data + nullBitsOffset);
	unsigned char nullIndicatorPointer = 0x80; //initially, it's 10000000, in each of the following iteration, the "1" will be moved toward right.

	for (unsigned i = 0; i < desFieldNum; i++) {
		if (nullIndicatorPointer == 0) {
			nullIndicatorPointer = 0x80;
			nullBitsOffset += 1;
			nullBits = ((byte*) data + nullBitsOffset);
		}

		if ((*nullBits & nullIndicatorPointer) != 0) { // NULL
			valueOffset += 0;
		} else {  // not null
			if (recordDescriptor[i].type == TypeInt) {
				valueOffset += sizeof(int);
			} else if (recordDescriptor[i].type == TypeReal) {
				valueOffset += sizeof(float);
			} else {
				void* value;
				value = (char*) malloc(recordDescriptor[i].length);
				char *charData = new char[PAGE_SIZE];
				int varcharLength = 0;
				readVarchar((byte*) data + valueOffset, varcharLength,
						charData);
				delete[] charData;
				valueOffset += sizeof(int);
				valueOffset += varcharLength;
				free(value);
			}
		}
		nullIndicatorPointer = nullIndicatorPointer >> 1;
	}

	return valueOffset;
}

void RecordBasedFileManager::convertAPIData2StoreFormat(
		const vector<Attribute> &recordDescriptor, const void *APIData,
		void* recordData) {
	unsigned desFieldNum = recordDescriptor.size();
	unsigned apiDataOffset = 0;
	unsigned recordDataOffset = 0;

	//copy the size of field into the beginning of recordData;
	memcpy((byte*) recordData + recordDataOffset, &desFieldNum,
			sizeof(unsigned));
	recordDataOffset += sizeof(unsigned);

	unsigned nullIndicatorByteSize = ceil(desFieldNum / 8.0);

	//copy the null-indicator into the record data;
	memcpy((byte*) recordData + recordDataOffset, APIData,
			sizeof(byte) * nullIndicatorByteSize);
	recordDataOffset += sizeof(byte) * nullIndicatorByteSize;
	apiDataOffset += sizeof(byte) * nullIndicatorByteSize;
	unsigned nullBitsOffset = 0;

	byte* nullBits = ((byte*) APIData + nullBitsOffset);
	unsigned char nullIndicatorPointer = 0x80; //initially, it's 10000000, in each of the following iteration, the "1" will be moved toward right.

	int* fieldPositionSpace = new int[desFieldNum];
	unsigned dataValueLength = 0;
	for (unsigned i = 0; i < desFieldNum; i++) {
		if (nullIndicatorPointer == 0) {
			nullIndicatorPointer = 0x80;
			nullBitsOffset += 1;
			nullBits = ((byte*) APIData + nullBitsOffset);
		}

		if ((*nullBits & nullIndicatorPointer) != 0) { // NULL
			apiDataOffset += 0;
			dataValueLength += 0;
		} else {  // not null
			if (recordDescriptor[i].type == TypeInt) {
				apiDataOffset += sizeof(int);
				dataValueLength += sizeof(int);
			} else if (recordDescriptor[i].type == TypeReal) {
				apiDataOffset += sizeof(float);
				dataValueLength += sizeof(float);
			} else {
				void* value;
				value = (char*) malloc(recordDescriptor[i].length);
				char *charData = new char[PAGE_SIZE];
				int varcharLength = 0;
				readVarchar((byte*) APIData + apiDataOffset,
						varcharLength, charData);
				delete[] charData;
				apiDataOffset += sizeof(int);
				apiDataOffset += varcharLength;
				dataValueLength += sizeof(int);
				dataValueLength += varcharLength;

				free(value);
			}
		}
		fieldPositionSpace[i] = dataValueLength;
		nullIndicatorPointer = nullIndicatorPointer >> 1;
	}

	//copy the offset_position into the record data
	memcpy((byte*) recordData + recordDataOffset, fieldPositionSpace,
			sizeof(int) * desFieldNum);
	delete[] fieldPositionSpace;
	recordDataOffset += sizeof(int) * desFieldNum;

	//copy the data values into the record data
	memcpy((byte*) recordData + recordDataOffset,
			(byte*) APIData + sizeof(byte) * nullIndicatorByteSize,
			dataValueLength);
}

void RecordBasedFileManager::convertStoreFormat2APIData(
		const vector<Attribute> &recordDescriptor, const void *recordData,
		void* APIData) {
	//unsigned desFieldNum = recordDescriptor.size();
	unsigned desFieldNum = *((unsigned*) recordData); //we should get the stored fieldNumber of the stored data;

	unsigned apiDataOffset = 0;
	unsigned recordDataOffset = sizeof(unsigned);

	unsigned nullIndicatorByteSize = ceil(desFieldNum / 8.0);

	//copy the null-indicator into the api data;
	memcpy((byte*) APIData + apiDataOffset,
			(byte*) recordData + recordDataOffset,
			sizeof(byte) * nullIndicatorByteSize);
	recordDataOffset += sizeof(byte) * nullIndicatorByteSize;
	apiDataOffset += sizeof(byte) * nullIndicatorByteSize;

	unsigned lastValueEndPositionOffset = recordDataOffset
			+ 4 * (desFieldNum - 1);
	int lastValueEndPosition = *(int*) ((byte*) recordData
			+ lastValueEndPositionOffset);
	recordDataOffset += sizeof(byte) * 4 * desFieldNum;

	memcpy((byte*) APIData + apiDataOffset,
			(byte*) recordData + recordDataOffset, lastValueEndPosition);
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
