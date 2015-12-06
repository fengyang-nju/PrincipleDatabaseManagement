#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance() {
	if (!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager() {
	_pf_manager = PagedFileManager::instance();
}

IndexManager::~IndexManager() {
	if (_index_manager) {
		delete _index_manager;
		_index_manager = 0;
	}
}

RC IndexManager::createFile(const string &fileName) {
	return _pf_manager->createFile(fileName.c_str());
}

RC IndexManager::destroyFile(const string &fileName) {
	return _pf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
	if (ixfileHandle.pFile != NULL) {
		return -1;
	}
	return _pf_manager->openFile(fileName, ixfileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
	return _pf_manager->closeFile(ixfileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {

	unsigned totalPageNum = ixfileHandle.getNumberOfPages();

	unsigned length = attribute.length;
	if (attribute.type == TypeVarChar) {
		length += sizeof(unsigned);
	}

	RC rc = -1;
	if (totalPageNum == 0) {
		rc = this->initializeIXPage(ixfileHandle, attribute, key, rid);
	} else {
		MyBPTreeIndexNode* root = getRoot(ixfileHandle, attribute);
		if (root == NULL) {
			return -1;
		}
		PAGE_NUM leafPageIndex;
		MyBPTreeLeafNode* leafNode = this->findContainKeyLEAFPage(ixfileHandle,
				attribute, key, root, leafPageIndex);

		unsigned keyIndex = 0;
		for (; keyIndex < leafNode->keys.size(); keyIndex++)
			if (leafNode->keyComparator(attribute.type, key, leafNode->keys[keyIndex]) <= 0) {
				break;
		}
		bool found = (keyIndex != leafNode->keys.size());

		bool exact = false;
		if(found)
			exact = (leafNode->keyComparator(attribute.type, key, leafNode->keys[keyIndex]) ==0);
		//try to check whether we should split nodes
		if(found && exact){
			//append the RID to the existing key
			rc = IndexManager::appendRIDToExsitingKey(ixfileHandle, *leafNode, leafPageIndex, attribute, keyIndex, rid);
		}else {
			//the key don't exist in the leaf
			if (leafNode->meta.freeSpace > (length + sizeof(RID))) {
				rc = basicInsertKeyIntoLeaf(ixfileHandle, *leafNode,leafPageIndex, attribute, keyIndex, key, rid);
			} else {
				//we should consider the split
				rc = insertKeyIntoLeaf(ixfileHandle, *leafNode, leafPageIndex, attribute, key, rid);

			}
		}

		delete root;
		delete leafNode;
	}

	//this->printBtree(ixfileHandle,attribute);

	return rc;
}

RC IndexManager::basicInsertKeyIntoIndexNode(IXFileHandle &ixfileHandle,
		MyBPTreeIndexNode& indexNode, const void *key,
		PAGE_NUM childPageIndex) {
	unsigned keyPos = indexNode.findKeyIndex(key);
	indexNode.insert(keyPos, key, childPageIndex);
	return 0;
}

//this method could be recursive
//should take the recursively split into consideration
RC IndexManager::insertKeyIntoIndexNode(IXFileHandle &ixfileHandle,
		const Attribute &attribute, PAGE_NUM pageIndex, const void *key,
		PAGE_NUM childPageIndex) {

	byte* nodePageData = new byte[PAGE_SIZE];
	ixfileHandle.readPage(pageIndex, nodePageData);
	MyBPTreeIndexNode* indexNode =
			dynamic_cast<MyBPTreeIndexNode*>(this->constructBPTreeNodeByPageData(
					nodePageData, attribute));

	unsigned length = attribute.length;
	if (attribute.type == TypeVarChar) {
		length += sizeof(unsigned);
	}

	if (indexNode->meta.freeSpace > (length + sizeof(PAGE_NUM))) {
		//just run basicInsert
		basicInsertKeyIntoIndexNode(ixfileHandle, *indexNode, key,
				childPageIndex);
		byte* pageData = new byte[PAGE_SIZE];
		indexNode->toPageData(pageData);
		ixfileHandle.writePage(pageIndex, pageData);
		delete[] pageData;

	} else {
		//need split
		if (pageIndex == rootPageIndex) {
			//root split
			/**************************** calculate the related information  *******************/
			//insert the key and dataPageNum into the original keyList;

			unsigned insertKeyIndex = indexNode->findKeyIndex(key);
//			indexNode->keys.insert(indexNode->keys.begin() + insertKeyIndex,key);
//			indexNode->children.insert(indexNode->children.begin() + insertKeyIndex + 1, childPageIndex);
			indexNode->insert(insertKeyIndex, key, childPageIndex);

			//calculate the split point
			unsigned totalKeyNum = indexNode->keys.size();
			unsigned pullUpKeyIndex = (totalKeyNum - 1) / 2; // exclusively, from this index, the following key should belong to the right child;
			const void* pullUpKey = indexNode->keys[pullUpKeyIndex];

			unsigned rightChildKeyNum = totalKeyNum - pullUpKeyIndex; //the most left one will be pulled up to the new root; so I will minus 1 in the recordCount setting
			unsigned leftChildKeyKeyNum = pullUpKeyIndex;

			unsigned leftChildPageNum = ixfileHandle.getNumberOfPages();
			unsigned rightChildPageNum = ixfileHandle.getNumberOfPages() + 1;

			/******************* set the information of the new right child, and write them back to file  *******************/
			MyBPTreeIndexNode* newRightNode = new MyBPTreeIndexNode();
			newRightNode->keyType = attribute;
			newRightNode->meta.pageType = INDEX;
			newRightNode->meta.parentPageIndex = rootPageIndex;
			newRightNode->meta.recordCount = rightChildKeyNum - 1;
			newRightNode->meta.rightSiblingPageIndex = UNDEFINED_PAGE_INDEX;
			newRightNode->meta.leftSiblingPageIndex = leftChildPageNum;
			newRightNode->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader) - (newRightNode->meta.recordCount + 1) * sizeof(PAGE_NUM) - length * newRightNode->meta.recordCount;

			//move the original keys and datas into the newLeafPage
			for (unsigned i = 0; i < rightChildKeyNum; i++) {
				newRightNode->keys.push_back(indexNode->keys[pullUpKeyIndex]);
				newRightNode->children.push_back(indexNode->children[pullUpKeyIndex + 1]);
				indexNode->keys.erase(indexNode->keys.begin() + pullUpKeyIndex);
				indexNode->children.erase(indexNode->children.begin() + pullUpKeyIndex + 1);
			}

			newRightNode->keys.erase(newRightNode->keys.begin());
			newRightNode->updateChildrenParentPageInfo(ixfileHandle,rightChildPageNum);

			/******************* set the information of the new left child, and write them back to file  *******************/
			MyBPTreeIndexNode* newLeftNode = new MyBPTreeIndexNode();
			newLeftNode->keyType = attribute;
			newLeftNode->meta.pageType = INDEX;
			newLeftNode->meta.parentPageIndex = rootPageIndex;
			newLeftNode->meta.recordCount = leftChildKeyKeyNum;
			newLeftNode->meta.leftSiblingPageIndex = UNDEFINED_PAGE_INDEX;
			newLeftNode->meta.rightSiblingPageIndex = rightChildPageNum;
			newLeftNode->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader) 	- (leftChildKeyKeyNum + 1) * sizeof(PAGE_NUM) - length * leftChildKeyKeyNum;

			//the left child should be given the first pageIndex
			newLeftNode->children.push_back(indexNode->children[0]);
			indexNode->children.erase(indexNode->children.begin());

			//move the original keys and datas into the newLeftPage
			//because the first child is already moved to the newLeftNode, we can start from the 0;
			for (unsigned i = 0; i < leftChildKeyKeyNum; i++) {
				newLeftNode->keys.push_back(indexNode->keys[0]);
				newLeftNode->children.push_back(indexNode->children[0]);
				indexNode->keys.erase(indexNode->keys.begin());
				indexNode->children.erase(indexNode->children.begin());
			}

			newLeftNode->updateChildrenParentPageInfo(ixfileHandle,
					leftChildPageNum);

			//for debug
//			newRightNode->printNode();
//			newLeftNode->printNode();

			/******************* set the information of the new rootPage, and write them back to file  *******************/
			//for debug
//			if (indexNode->keys.size() != 0
//					|| indexNode->children.size() != 0) {
//				cerr << "Root split error!" << endl;
//				return -1;
//			}
			indexNode->keys.push_back(pullUpKey);
			indexNode->children.push_back(leftChildPageNum);
			indexNode->children.push_back(rightChildPageNum);

			indexNode->meta.pageType = INDEX;
			indexNode->meta.parentPageIndex = UNDEFINED_PAGE_INDEX;
			indexNode->meta.recordCount = 1;
			indexNode->meta.leftSiblingPageIndex = UNDEFINED_PAGE_INDEX;
			indexNode->meta.rightSiblingPageIndex = UNDEFINED_PAGE_INDEX;
			indexNode->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader) - (1 + 1) * sizeof(PAGE_NUM) - length * 1;

			byte* pageData = new byte[PAGE_SIZE];
			newLeftNode->toPageData(pageData);
			ixfileHandle.appendPage(pageData);
			delete[] pageData;

			pageData = new byte[PAGE_SIZE];
			newRightNode->toPageData(pageData);
			ixfileHandle.appendPage(pageData);
			delete[] pageData;

			pageData = new byte[PAGE_SIZE];
			indexNode->toPageData(pageData);
			ixfileHandle.writePage(rootPageIndex, pageData);
			delete[] pageData;

			delete newLeftNode;
			delete newRightNode;
		} else {
			//ordinary node split, I prefer the right child have more keys;
			/**************************** calculate the related information  *******************/
			//insert the key and dataPageNum into the original keyList;
//			unsigned insertKeyIndex = indexNode->findKeyIndex(key);
//			indexNode->keys.insert(indexNode->keys.begin() + insertKeyIndex,key);
//			indexNode->children.insert(	indexNode->children.begin() + insertKeyIndex + 1, childPageIndex);

			unsigned insertKeyIndex = indexNode->findKeyIndex(key);
			indexNode->insert(insertKeyIndex,key,childPageIndex);

			//calculate the split point
			unsigned totalKeyNum = indexNode->keys.size();
			unsigned pullUpKeyIndex = (totalKeyNum - 1) / 2; // exclusively, from this index, the following key should belong to the right child;
			const void* pullUpKey = indexNode->keys[pullUpKeyIndex];

			unsigned newNodeKeyNum = totalKeyNum - pullUpKeyIndex; //the most left one will be pulled up to parent; so I will minus 1 in the recordCount setting
			unsigned oldNodeKeyNum = pullUpKeyIndex;

			/******************* set the information of the new leaf page, and write them back to file  *******************/
			PAGE_NUM newNodePageNum = ixfileHandle.getNumberOfPages();
			MyBPTreeIndexNode* newNode = new MyBPTreeIndexNode();
			newNode->keyType = attribute;
			newNode->meta.pageType = INDEX;
			newNode->meta.parentPageIndex = indexNode->meta.parentPageIndex;
			newNode->meta.recordCount = newNodeKeyNum - 1;
			newNode->meta.rightSiblingPageIndex =
					indexNode->meta.rightSiblingPageIndex;
			newNode->meta.leftSiblingPageIndex = pageIndex;
			newNode->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader)
					- (newNode->meta.recordCount + 1) * sizeof(PAGE_NUM)
					- length * newNode->meta.recordCount;

			//move the original keys and datas into the newLeafPage
			//newNodeKeyNum+1 means, move the key-indexPage pair fro
			for (unsigned i = 0; i < newNodeKeyNum; i++) {
				newNode->keys.push_back(indexNode->keys[pullUpKeyIndex]);
				newNode->children.push_back(
						indexNode->children[pullUpKeyIndex + 1]);
				indexNode->keys.erase(indexNode->keys.begin() + pullUpKeyIndex);
				indexNode->children.erase(
						indexNode->children.begin() + pullUpKeyIndex + 1);
			}

			//for debug
//			newNode->printNode();
//			indexNode->printNode();

			//move the first item from keyList
			newNode->keys.erase(newNode->keys.begin());

			byte* newNodePageData = new byte[PAGE_SIZE];
			newNode->toPageData(newNodePageData);
			ixfileHandle.appendPage(newNodePageData);
			delete[] newNodePageData;

			//update the leftSibling Index of the rightSibling;
			if (newNode->meta.rightSiblingPageIndex != UNDEFINED_PAGE_INDEX) {
				byte* origRightSiblingPageData = new byte[PAGE_SIZE];
				ixfileHandle.readPage(newNode->meta.rightSiblingPageIndex,
						origRightSiblingPageData);
				IXPageHeader headerTmp;
				this->constructIXPageHeaderSectionFromData(
						origRightSiblingPageData, headerTmp);
				headerTmp.leftSiblingPageIndex = newNodePageNum;
				this->writePageHeaderIntoData(origRightSiblingPageData,
						headerTmp);
				ixfileHandle.writePage(newNode->meta.rightSiblingPageIndex,
						origRightSiblingPageData);
				delete[] origRightSiblingPageData;
			}

			//update the parentIndex of the children
			newNode->updateChildrenParentPageInfo(ixfileHandle, newNodePageNum);

			/******************* set the information of the old leaf page, and write them back to file  *******************/
			indexNode->meta.recordCount = oldNodeKeyNum;
			indexNode->meta.rightSiblingPageIndex = newNodePageNum;
			indexNode->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader)
					- (oldNodeKeyNum + 1) * sizeof(PAGE_NUM)
					- length * oldNodeKeyNum;
			byte* originalLeafPageData = new byte[PAGE_SIZE];
			indexNode->toPageData(originalLeafPageData);
			ixfileHandle.writePage(pageIndex, originalLeafPageData);
			delete[] originalLeafPageData;

			/*******************  in this point, we just need to insert the copyUpKey to the parentIndexPage *******************/
			insertKeyIntoIndexNode(ixfileHandle, attribute,
					newNode->meta.parentPageIndex, pullUpKey, newNodePageNum);
			delete newNode;
		}
	}

	delete indexNode;
	delete[] nodePageData;
	return 0;
}

//this method should take the spliting into consideration
//this method DONT need to take the RID overflow into consideration;
//in our spliting method, we think the right child should have higher priority,
//we can assume the newKey is not in the keyList

//ATTENTION: in this method, the leaf WILL split!
RC IndexManager::insertKeyIntoLeaf(IXFileHandle &ixfileHandle,
		MyBPTreeLeafNode& leafNode, PAGE_NUM leafPageIndex,
		const Attribute &attribute, const void *key, const RID &rid) {

	unsigned length = attribute.length;
	if (attribute.type == TypeVarChar) {
		length += sizeof(unsigned);
	}

	/******************* calculate the related information  *******************/
	//insert the key and dataPageNum into the original keyList;

	unsigned insertKeyIndex = leafNode.findKeyIndex(key);
	leafNode.insert(insertKeyIndex,key,rid);
//	leafNode.keys.insert(leafNode.keys.begin() + insertKeyIndex, key);
//	leafNode.datas.insert(leafNode.datas.begin() + insertKeyIndex, rid);

	//calculate the split point
	unsigned totalKeyNum = leafNode.keys.size();
	unsigned splitPointIndex = totalKeyNum / 2; // Inclusively, from this index, the following key should belong to the right child;
	const void* copyUpKey = leafNode.keys[splitPointIndex];

	unsigned newLeafKeyNum = totalKeyNum - splitPointIndex;
	unsigned oldLeafKeyNum = splitPointIndex;

	/******************* set the information of the new leaf page, and write them back to file  *******************/
	PAGE_NUM rightLeafPageNum = ixfileHandle.getNumberOfPages();
	MyBPTreeLeafNode* rightLeafNode = new MyBPTreeLeafNode();
	rightLeafNode->keyType = attribute;
	rightLeafNode->meta.pageType = LEAF;
	rightLeafNode->meta.parentPageIndex = leafNode.meta.parentPageIndex;
	rightLeafNode->meta.recordCount = newLeafKeyNum;
	rightLeafNode->meta.rightSiblingPageIndex =
			leafNode.meta.rightSiblingPageIndex;
	rightLeafNode->meta.leftSiblingPageIndex = leafPageIndex;
	rightLeafNode->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader)
			- newLeafKeyNum * (length + sizeof(RID));

	//move the original keys and datas into the newLeafPage
	for (unsigned i = 0; i < newLeafKeyNum; i++) {
		rightLeafNode->keys.push_back(leafNode.keys[splitPointIndex]);
		rightLeafNode->datas.push_back(leafNode.datas[splitPointIndex]);

		leafNode.keys.erase(leafNode.keys.begin() + splitPointIndex);
		leafNode.datas.erase(leafNode.datas.begin() + splitPointIndex);
	}

	byte* rightLeafPageData = new byte[PAGE_SIZE];
	rightLeafNode->toPageData(rightLeafPageData);
	ixfileHandle.appendPage(rightLeafPageData);
	//ixfileHandle.writePage(rightLeafPageNum,rightLeafPageData);
	delete[] rightLeafPageData;

	//update the leftSibling Index of the rightSibling;
	if (rightLeafNode->meta.rightSiblingPageIndex != UNDEFINED_PAGE_INDEX) {
		byte* origRightSiblingPageData = new byte[PAGE_SIZE];
		ixfileHandle.readPage(rightLeafNode->meta.rightSiblingPageIndex,
				origRightSiblingPageData);
		IXPageHeader headerTmp;
		this->constructIXPageHeaderSectionFromData(origRightSiblingPageData,
				headerTmp);
		headerTmp.leftSiblingPageIndex = rightLeafPageNum;
		this->writePageHeaderIntoData(origRightSiblingPageData, headerTmp);
		ixfileHandle.writePage(rightLeafNode->meta.rightSiblingPageIndex,
				origRightSiblingPageData);
		delete[] origRightSiblingPageData;
	}

	//update the parentIndex of the children
	rightLeafNode->updateChildrenParentPageInfo(ixfileHandle, rightLeafPageNum);

	/******************* set the information of the old leaf page, and write them back to file  *******************/
	leafNode.meta.recordCount = oldLeafKeyNum;
	leafNode.meta.rightSiblingPageIndex = rightLeafPageNum;
	leafNode.meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader)
			- oldLeafKeyNum * (sizeof(RID) + length);
	byte* originalLeafPageData = new byte[PAGE_SIZE];
	leafNode.toPageData(originalLeafPageData);
	ixfileHandle.writePage(leafPageIndex, originalLeafPageData);
	delete[] originalLeafPageData;

	/*******************  in this point, we just need to insert the copyUpKey to the parentIndexPage *******************/
	insertKeyIntoIndexNode(ixfileHandle, attribute,
			rightLeafNode->meta.parentPageIndex, copyUpKey, rightLeafPageNum);
	delete rightLeafNode;
	return 0;
}

RC IndexManager::appendRIDToExsitingKey(IXFileHandle &ixfileHandle,
		MyBPTreeLeafNode& leafNode, PAGE_NUM leafPageIndex,
		const Attribute &attribute, unsigned index, const RID &rid){
	RID* fakeRID = &leafNode.datas[index];
	if (fakeRID->pageNum == UNDEFINED_PAGE_INDEX) {
		return appendRIDIntoDataPage(ixfileHandle, fakeRID->slotNum, rid);
	} else {
		PAGE_NUM parentPageNum = leafPageIndex;
		PAGE_NUM dataPageNum;
		buildUpNewDataPage(ixfileHandle, parentPageNum, dataPageNum,
				*fakeRID, rid);
		fakeRID->pageNum = UNDEFINED_PAGE_INDEX;
		fakeRID->slotNum = dataPageNum;
		byte* pageData = new byte[PAGE_SIZE];
		leafNode.toPageData(pageData);
		ixfileHandle.writePage(leafPageIndex, pageData);
		delete[] pageData;
	}
	return 0;
}

RC IndexManager::basicInsertKeyIntoLeaf(IXFileHandle &ixfileHandle,MyBPTreeLeafNode& leafNode, PAGE_NUM leafPageIndex,
		const Attribute &attribute, unsigned index, const void *key, const RID &rid) {
	leafNode.insert(index, key, rid);
	byte* pageData = new byte[PAGE_SIZE];
	leafNode.toPageData(pageData);
	ixfileHandle.writePage(leafPageIndex, pageData);
	delete[] pageData;
	return 0;
}

RC IndexManager::buildUpNewDataPage(IXFileHandle &ixfileHandle,
		PAGE_NUM parentPageNum, PAGE_NUM& dataPageNum, const RID oldRID,
		const RID newRID) {
	IXPageHeader header;
	header.pageType = DATA;
	header.parentPageIndex = parentPageNum;
	header.recordCount = 2;
	header.leftSiblingPageIndex = UNDEFINED_PAGE_INDEX;
	header.rightSiblingPageIndex = UNDEFINED_PAGE_INDEX;
	header.freeSpace = PAGE_SIZE - sizeof(IXPageHeader) - sizeof(RID) * 2;
	byte* pageData = new byte[PAGE_SIZE];
	IndexManager::writePageHeaderIntoData(pageData, header);
	memcpy(pageData + sizeof(IXPageHeader), &oldRID, sizeof(RID));
	memcpy(pageData + sizeof(IXPageHeader) + sizeof(RID), &newRID, sizeof(RID));
	dataPageNum = ixfileHandle.getNumberOfPages();
	return ixfileHandle.appendPage(pageData);
}

RC IndexManager::appendRIDIntoDataPage(IXFileHandle &ixfileHandle,
		PAGE_NUM dataPageNum, const RID &rid) {
	IXPageHeader header;
	byte* pageData = new byte[PAGE_SIZE];
	ixfileHandle.readPage(dataPageNum, pageData);
	this->constructIXPageHeaderSectionFromData(pageData, header);
	PAGE_NUM pageIndex = dataPageNum;

	//we should try to check whether there is a duplicated rid in the ridList;

	while (header.rightSiblingPageIndex != UNDEFINED_PAGE_INDEX) {
		pageIndex = header.rightSiblingPageIndex;
		ixfileHandle.readPage(pageIndex, pageData);
		this->constructIXPageHeaderSectionFromData(pageData, header);
	}

	if (header.recordCount != MAX_RID_NUM) {
		memcpy(
				pageData + sizeof(IXPageHeader)
						+ sizeof(RID) * header.recordCount, &rid, sizeof(RID));
		header.freeSpace -= sizeof(RID);
		header.recordCount++;
	} else {
		IXPageHeader newHeader;

		newHeader.pageType = APPEND;
		newHeader.parentPageIndex = pageIndex;
		newHeader.recordCount = 1;
		newHeader.leftSiblingPageIndex = UNDEFINED_PAGE_INDEX;
		newHeader.rightSiblingPageIndex = UNDEFINED_PAGE_INDEX;
		newHeader.freeSpace = PAGE_SIZE - sizeof(IXPageHeader) - sizeof(RID);

		byte* newPageData = new byte[PAGE_SIZE];
		this->writePageHeaderIntoData(newPageData, newHeader);
		memcpy(newPageData + sizeof(IXPageHeader), &rid, sizeof(RID));

		ixfileHandle.appendPage(newPageData);
		delete[] newPageData;

		header.rightSiblingPageIndex = ixfileHandle.getNumberOfPages() - 1;
	}
	this->writePageHeaderIntoData(pageData, header);
	ixfileHandle.writePage(pageIndex, pageData);

	delete[] pageData;
	return 0;
}

RC IndexManager::mergeOrRedistribute(IXFileHandle &ixfileHandle,
		const Attribute &attribute, MyBPTreeNode* node, PAGE_NUM indexPageNum) {

	MyBPTreeIndexNode* parentNode = NULL;
	PAGE_NUM realParentPageIndex = node->meta.parentPageIndex;

	MyBPTreeNode* leftSibling = NULL;
	PAGE_NUM realLeftSiblingPageIndex = node->meta.leftSiblingPageIndex;
	IXPageHeader leftHeader;
	if (realLeftSiblingPageIndex != UNDEFINED_PAGE_INDEX) {
		byte* leftPageData = new byte[PAGE_SIZE];
		ixfileHandle.readPage(realLeftSiblingPageIndex, leftPageData);
		this->constructIXPageHeaderSectionFromData(leftPageData, leftHeader);
		if (leftHeader.parentPageIndex == realParentPageIndex) {
			leftSibling =
					dynamic_cast<MyBPTreeNode*>(this->constructBPTreeNodeByPageData(
							leftPageData, attribute));
		}
		delete[] leftPageData;
	}

	MyBPTreeNode* rightSibling = NULL;
	PAGE_NUM realRightSiblingPageIndex = node->meta.rightSiblingPageIndex;
	IXPageHeader rightHeader;
	if (realRightSiblingPageIndex != UNDEFINED_PAGE_INDEX) {
		byte* rightPageData = new byte[PAGE_SIZE];
		ixfileHandle.readPage(realRightSiblingPageIndex, rightPageData);
		this->constructIXPageHeaderSectionFromData(rightPageData, rightHeader);
		if (rightHeader.parentPageIndex == realParentPageIndex) {
			rightSibling =(dynamic_cast<MyBPTreeNode*>(this->constructBPTreeNodeByPageData(rightPageData, attribute)));
		}
		delete[] rightPageData;
	}

	byte* parentData = new byte[PAGE_SIZE];
	ixfileHandle.readPage(realParentPageIndex, parentData);
	parentNode =dynamic_cast<MyBPTreeIndexNode*>(this->constructBPTreeNodeByPageData(parentData, attribute));
	delete[] parentData;

	unsigned parentKeyIndex = 0;
	for (; parentKeyIndex < parentNode->children.size(); parentKeyIndex++) {
		if (parentNode->children[parentKeyIndex] == indexPageNum) {
			break;
		}
	}

	if (parentKeyIndex == parentNode->children.size()) {
		// for debug
//		cerr<< "Didn't find the pageIndex in the parentNode, there must be some errors!"<< endl;
		return -1;
	}

	if (rightSibling == NULL && leftSibling == NULL) {
		//for debug condition 1, dont have any siblings, should be error
//		cerr<< "It should be in the initializing, we just need to delete the basic item"<< endl;
		return -1;
	} else if (leftSibling == NULL && rightSibling != NULL) {
		//condition 2; left one is empty, right one works
		PAGE_NUM rightSiblingPageIndex =parentNode->children[parentKeyIndex + 1];
		PAGE_NUM leftSiblingPageIndex = indexPageNum;

		if(parentKeyIndex == (parentNode->children.size() -1)){
			cerr<< "System Error! Find the most right child but it has one rightSibling"<< endl;
			return -1;
		}

		if (shouldMerge(node->meta.pageType,node->keys.size(), rightSibling->keys.size(),	 attribute)) {
			this->mergeNodes(ixfileHandle, attribute, parentNode,
					realParentPageIndex, node, leftSiblingPageIndex, rightSibling,
					rightSiblingPageIndex, parentKeyIndex);
		} else {
			this->redistributeBPlusTree(ixfileHandle, attribute, parentNode,
					realParentPageIndex, node, leftSiblingPageIndex, rightSibling,
					rightSiblingPageIndex, parentKeyIndex);
		}

		//write right page information into disk
		byte* pageData = new byte[PAGE_SIZE];
		rightSibling->toPageData(pageData);
		ixfileHandle.writePage(realRightSiblingPageIndex, pageData);
		delete[] pageData;
		delete rightSibling;
	} else if (leftSibling != NULL && rightSibling == NULL) {
		PAGE_NUM leftSiblingPageIndex = parentNode->children[parentKeyIndex-1];
		PAGE_NUM rightSiblingPageIndex = indexPageNum;

		if(parentKeyIndex == 0){
			cerr<< "System Error! Find the most right child but it has one rightSibling"<< endl;
			return -1;
		}
		parentKeyIndex -=1;

		if (shouldMerge(node->meta.pageType, leftSibling->keys.size(), node->keys.size(),  attribute)) {
			this->mergeNodes(ixfileHandle, attribute, parentNode,
					realParentPageIndex, leftSibling, leftSiblingPageIndex, node,
			rightSiblingPageIndex, parentKeyIndex);
		} else {
			this->redistributeBPlusTree(ixfileHandle, attribute, parentNode,
					realParentPageIndex, leftSibling, leftSiblingPageIndex, node,
			rightSiblingPageIndex, parentKeyIndex);
		}

		//write left page information into disk
		byte* pageData = new byte[PAGE_SIZE];
		leftSibling->toPageData(pageData);
		ixfileHandle.writePage(realLeftSiblingPageIndex, pageData);
		delete[] pageData;
		delete leftSibling;
	} else {
		//condition 4; should try to find the larger one to merge or distribute;
		if (rightSibling->keys.size() < leftSibling->keys.size()) {
			PAGE_NUM leftSiblingPageIndex = parentNode->children[parentKeyIndex
					- 1];
			PAGE_NUM rightSiblingPageIndex = indexPageNum;

			if(parentKeyIndex == 0){
				cerr<< "System Error! Find the most right child but it has one rightSibling"<< endl;
				return -1;
			}
			parentKeyIndex -=1;

			if (shouldMerge(node->meta.pageType, leftSibling->keys.size(),
					node->keys.size(), attribute)) {
				this->mergeNodes(ixfileHandle, attribute, parentNode,
						realParentPageIndex, leftSibling, leftSiblingPageIndex,
						node, rightSiblingPageIndex, parentKeyIndex);
			} else {
				this->redistributeBPlusTree(ixfileHandle, attribute, parentNode,
						realParentPageIndex, leftSibling, leftSiblingPageIndex,
						node, rightSiblingPageIndex, parentKeyIndex);
			}
		} else {
			PAGE_NUM rightSiblingPageIndex = parentNode->children[parentKeyIndex
					+ 1];
			PAGE_NUM leftSiblingPageIndex = indexPageNum;

			if (shouldMerge(node->meta.pageType, leftSibling->keys.size(),
					node->keys.size(), attribute)) {
				this->mergeNodes(ixfileHandle, attribute, parentNode,
						realParentPageIndex, node, leftSiblingPageIndex,
						rightSibling, rightSiblingPageIndex, parentKeyIndex);
			} else {
				this->redistributeBPlusTree(ixfileHandle, attribute, parentNode,
						realParentPageIndex, node, leftSiblingPageIndex,
						rightSibling, rightSiblingPageIndex, parentKeyIndex);
			}
		}

		//write right page information into disk
		byte* pageData = new byte[PAGE_SIZE];
		rightSibling->toPageData(pageData);
		ixfileHandle.writePage(realRightSiblingPageIndex, pageData);
		delete[] pageData;

		//write left page information into disk
		pageData = new byte[PAGE_SIZE];
		leftSibling->toPageData(pageData);
		ixfileHandle.writePage(realLeftSiblingPageIndex, pageData);
		delete[] pageData;

		delete leftSibling;
		delete rightSibling;
	}

	byte* pageData = new byte[PAGE_SIZE];
	parentNode->toPageData(pageData);
	ixfileHandle.writePage(realParentPageIndex, pageData);
	delete[] pageData;
	delete parentNode;
	//for debug
	//this->printBtree(ixfileHandle,attribute);
	return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {

	MyBPTreeIndexNode* root = getRoot(ixfileHandle, attribute);
	if (root == NULL) {
		return -1;
	}

	PageNum leafPageIndex;
	MyBPTreeLeafNode* node = this->findContainKeyLEAFPage(ixfileHandle,
			attribute, key, root, leafPageIndex);
	basicDeleteKeyFromLeaf(ixfileHandle, *node, attribute, key, rid);

	if (node->meta.freeSpace > HALF_PAGE) {
		this->mergeOrRedistribute(ixfileHandle, attribute, node, leafPageIndex);
	}

	byte* pageData = new byte[PAGE_SIZE];
	node->toPageData(pageData);
	ixfileHandle.writePage(leafPageIndex, pageData);
	delete[] pageData;

	delete root;
	delete node;
	return 0;
}

//the keyIndex is the one map to the child is left, keyIndex+1 is right
//the right one has a relatively higher priority
RC IndexManager::redistributeBPlusTree(IXFileHandle &ixfileHandle,
		const Attribute &attribute, MyBPTreeIndexNode* parentNode,
		PAGE_NUM parentPageIndex, MyBPTreeNode* left, PAGE_NUM leftPageIndex,
		MyBPTreeNode* right, PAGE_NUM rightPageIndex, unsigned keyIndex) {

	const void* newKey;

	unsigned length = attribute.length;
	if(attribute.type == TypeVarChar){
		length += 4;
	}

	if (left->meta.pageType == LEAF) {

		MyBPTreeLeafNode* leftTmp = dynamic_cast<MyBPTreeLeafNode*>(left);
		MyBPTreeLeafNode* rightTmp = dynamic_cast<MyBPTreeLeafNode*>(right);

//		leftTmp->printNode();
//		rightTmp->printNode();

		unsigned leftSize = leftTmp->meta.recordCount;
		unsigned rightSize = rightTmp->meta.recordCount;
		unsigned newLeftSize = (leftSize + rightSize) / 2;

		//cerr << "leftSize = "<<leftSize <<" rightSize = "<< rightSize<< endl;

		//the size should not be the same, if it is the same, then there should be some error!
		if (leftSize == rightSize) {
			cerr << "Error exists in the redistributing leaf node! leftSize = "<<leftSize <<" rightSize = "<< rightSize<< endl;
			return -1;
		}

		if (leftSize > rightSize) {
			while (leftTmp->keys.size() > newLeftSize) {
				rightTmp->keys.insert(rightTmp->keys.begin(), leftTmp->keys[leftTmp->keys.size()-1]);
				leftTmp->keys.erase(leftTmp->keys.begin() + leftTmp->keys.size()-1);

				rightTmp->datas.insert(rightTmp->datas.begin(), leftTmp->datas[leftTmp->datas.size()-1]);
				leftTmp->datas.erase(leftTmp->datas.begin() + leftTmp->datas.size()-1);
			}

		} else {
			while (leftTmp->keys.size() < newLeftSize) {
				leftTmp->keys.push_back(rightTmp->keys[0]);
				rightTmp->keys.erase(rightTmp->keys.begin());
				leftTmp->datas.push_back(rightTmp->datas[0]);
				rightTmp->datas.erase(rightTmp->datas.begin());
			}
		}
		leftTmp->meta.recordCount = newLeftSize;
		leftTmp->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader) -newLeftSize * (sizeof(RID) + length);

		rightTmp->meta.recordCount = leftSize + rightSize - newLeftSize;
		rightTmp->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader)- rightTmp->meta.recordCount * (sizeof(RID) + length);

		newKey = rightTmp->keys[0];
		parentNode->deleteKey(keyIndex);
		parentNode->insert(keyIndex,newKey,rightPageIndex);

//		leftTmp->printNode();
//		rightTmp->printNode();

	} else {

		cout<< left->meta.pageType <<" "<< right->meta.pageType<< " " <<parentNode->meta.pageType<<endl;

		MyBPTreeIndexNode* leftTmp = dynamic_cast<MyBPTreeIndexNode*>(left);
		MyBPTreeIndexNode* rightTmp = dynamic_cast<MyBPTreeIndexNode*>(right);

		//for debug
		//befor redistributed
//		leftTmp->printNode();
//		rightTmp->printNode();
//		parentNode->printNode();

		unsigned leftSize = leftTmp->meta.recordCount;
		unsigned rightSize = rightTmp->meta.recordCount;

		//we need pull the key down
		unsigned newLeftSize = (leftSize + rightSize) / 2;
		unsigned newRightSize = leftSize + rightSize - newLeftSize;

		//the size should not be the same, if it is the same, then there should be some error!
		if (leftSize == rightSize) {
			cerr << "Error exists in the redistributing index node ! leftSize = "<<leftSize <<" rightSize = "<< rightSize<< endl;
			return -1;
		}

		//push the leftSibling's key and indexPages into tmp;
		vector<const void*> keysTmp;
		vector<PAGE_NUM> childrenTmp;
		unsigned i = 0;
		for (; i < leftTmp->keys.size(); i++) {
			keysTmp.push_back(leftTmp->keys[i]);
			childrenTmp.push_back(leftTmp->children[i]);
		}

		const void* tmp = parentNode->keys[keyIndex];
		childrenTmp.push_back(leftTmp->children[i]);//push the lastest one index entry
		keysTmp.push_back(tmp); //"borrow" the key from parent;
		parentNode->keys.erase(parentNode->keys.begin()+keyIndex);

//		byte* tmp = new byte[length];
//		memcpy(tmp,parentNode->keys[keyIndex],length);
//		childrenTmp.push_back(leftTmp->children[i]);//push the lastest one index entry
//		keysTmp.push_back(tmp); //"borrow" the key from parent;
//		parentNode->deleteKey(keyIndex);

		//clean the keys and children of leftTmp
		leftTmp->keys.clear();
		leftTmp->children.clear();

		//push the key and children information of right index node
		i = 0;
		for (; i < rightTmp->keys.size(); i++) {
			keysTmp.push_back(rightTmp->keys[i]);
			childrenTmp.push_back(rightTmp->children[i]);
		}
		childrenTmp.push_back(rightTmp->children[i]);

		//clean the keys and children of rightTmp
		rightTmp->keys.clear();
		rightTmp->children.clear();

		unsigned j = 0;
		//firstly, push the first one node into left
		leftTmp->children.push_back(childrenTmp[0]);
		childrenTmp.erase(childrenTmp.begin());
		//then, push the following ones
		for (; j < newLeftSize; j++) {
			leftTmp->keys.push_back(keysTmp[0]);
			leftTmp->children.push_back(childrenTmp[0]);
			keysTmp.erase(keysTmp.begin());
			childrenTmp.erase(childrenTmp.begin());
		}

		// push the ones into right Tmp
		j = 0;  //plus one include the borrowed key;
		for (; j < newRightSize + 1; j++) {
			rightTmp->keys.push_back(keysTmp[0]);
			rightTmp->children.push_back(childrenTmp[0]);
			keysTmp.erase(keysTmp.begin());
			childrenTmp.erase(childrenTmp.begin());
		}

		if (keysTmp.size() != 0 || childrenTmp.size() != 0) {
			cerr<< "System error, there are some mistakes in the redistribution calculation!"<< endl;
			return -1;
		}

		//get the new key
		newKey = rightTmp->keys[0];
		rightTmp->keys.erase(rightTmp->keys.begin());

		leftTmp->updateChildrenParentPageInfo(ixfileHandle,leftPageIndex);
		rightTmp->updateChildrenParentPageInfo(ixfileHandle,rightPageIndex);

		leftTmp->meta.recordCount = newLeftSize;
		leftTmp->meta.freeSpace = PAGE_SIZE	- sizeof(IXPageHeader) - newLeftSize * (sizeof(PAGE_NUM) + length)	- sizeof(PAGE_NUM);

		rightTmp->meta.recordCount = newRightSize;
		rightTmp->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader) - newRightSize * (sizeof(PAGE_NUM) + length) - sizeof(PAGE_NUM);

		parentNode->keys.insert(parentNode->keys.begin()+keyIndex,newKey);

		cout<<"===================================================================================================================================="<<endl;
		//for debug
		//after redistributed
//		leftTmp->printNode();
//		rightTmp->printNode();
//		parentNode->printNode();
	}

	return 0;
}


//memory operation, no disk IO on node
RC IndexManager::deleteNodeFromIndexNode(IXFileHandle &ixfileHandle,
		const Attribute &attribute, MyBPTreeIndexNode* node,
		PAGE_NUM indexPageNum, unsigned keyIndex) {

	unsigned length = attribute.length;
	if(attribute.type == TypeVarChar){
		length += 4;
	}

	if (node->meta.parentPageIndex == UNDEFINED_PAGE_INDEX) {
		if (node->keys.size() == 1) {

			if (indexPageNum != rootPageIndex || keyIndex != 0) {
				cerr<< "System error! The page number and page information didnt match!"<< endl;
			}

			PAGE_NUM leftIndex = node->children[0];
			byte* leftPageData = new byte[PAGE_SIZE];
			ixfileHandle.readPage(leftIndex, leftPageData);
			IXPageHeader headerTmp;
			this->constructIXPageHeaderSectionFromData(leftPageData, headerTmp);
			//if the height of the tree is 1, we should not delete the root, but revert to the initialized status;
			if (headerTmp.pageType == INDEX) {

				//delete the whole root
				//ATTENTION: According to our algorithm, in this situation, the right sibling should be null;
				MyBPTreeIndexNode* leftTmp =
						dynamic_cast<MyBPTreeIndexNode*>(this->constructBPTreeNodeByPageData(
								leftPageData, attribute));

				delete[] leftPageData;
				leftTmp->updateChildrenParentPageInfo(ixfileHandle,rootPageIndex);
				leftTmp->meta.parentPageIndex = UNDEFINED_PAGE_INDEX;
				byte* pageData = new byte[PAGE_SIZE];
				leftTmp->toPageData(pageData);
				ixfileHandle.writePage(rootPageIndex, pageData);
				delete[] pageData;
				//unsigned leftItemNum = leftTmp->meta.recordCount;
				this->emptifyPage(ixfileHandle, leftIndex);
				delete leftTmp;
			} else {
				node->deleteKey(keyIndex); //if the leftChild is LEAF, it means the tree has become the beginning status; we just need delete the key
				delete[] leftPageData;
			}
		} else {
			//just simply delete the node without checking the freespace
			node->deleteKey(keyIndex);
		}
	} else {
		/******************************************* It is not the root ***************************************************/
		//firstly, simplely delete the key from node;
		node->deleteKey(keyIndex);
		if (node->meta.freeSpace > HALF_PAGE) {
			/******************************************* recursively call this method ***************************************************/
			//find the one sibling could be used to redistribute;
			this->mergeOrRedistribute(ixfileHandle, attribute, node,
					indexPageNum);
		}

		return 0;
	}

	return 0;
}

bool IndexManager::shouldMerge(PAGE_TYPE type, unsigned leftItemNum,
		unsigned rightItemNum, const Attribute& attribute) {
	unsigned avergaeItem = (leftItemNum + rightItemNum) / 2;
	unsigned length = attribute.length;
	if(attribute.type == TypeVarChar){
		length += 4;
	}
	if (type == INDEX) {
		return (PAGE_SIZE - sizeof(IXPageHeader)
				- avergaeItem * (length + sizeof(PAGE_NUM))
				- sizeof(PAGE_NUM)) > HALF_PAGE;
	} else {
		return (PAGE_SIZE - sizeof(IXPageHeader)
				- avergaeItem * (length + sizeof(RID))) > HALF_PAGE;
	}
}

//just directly execute the merge operation, the condition checking should be done in upper layer
RC IndexManager::mergeNodes(IXFileHandle &ixfileHandle,
		const Attribute &attribute, MyBPTreeIndexNode* parent,
		PAGE_NUM parentIndex, MyBPTreeNode* left, PAGE_NUM leftIndex,
		MyBPTreeNode* right, PAGE_NUM rightIndex, unsigned keyIndex) {

	if (left->meta.parentPageIndex != right->meta.parentPageIndex) {
		cerr << "They are not brothers, could not be merged!!!!" << endl;
		return -1;
	}

	unsigned length = attribute.length;
	if(attribute.type == TypeVarChar){
		length += 4;
	}

	if (left->meta.pageType == LEAF) {

		MyBPTreeLeafNode* leftTmp = dynamic_cast<MyBPTreeLeafNode*>(left);
		MyBPTreeLeafNode* rightTmp = dynamic_cast<MyBPTreeLeafNode*>(right);

//		leftTmp->printNode();
//		rightTmp->printNode();

		unsigned leftItemNum = leftTmp->meta.recordCount
				+ rightTmp->meta.recordCount;

		//const void* deleteKey = rightTmp->keys[0];

		//we should update the parent information of the datas in the current left item;
		rightTmp->updateChildrenParentPageInfo(ixfileHandle, leftIndex);

		//push all of the keys and datas from right to left;
		for (unsigned i = 0; i < rightTmp->keys.size();) {
			leftTmp->keys.push_back(rightTmp->keys[0]);
			leftTmp->datas.push_back(rightTmp->datas[0]);
			rightTmp->keys.erase(rightTmp->keys.begin());
			rightTmp->datas.erase(rightTmp->datas.begin());
		}

		leftTmp->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader)
				- leftItemNum * (sizeof(RID) + length);
		leftTmp->meta.rightSiblingPageIndex =
				rightTmp->meta.rightSiblingPageIndex;
		leftTmp->meta.recordCount = leftItemNum;

//		byte* pageData = new byte[PAGE_SIZE];
//		leftTmp->toPageData(pageData);
//		ixfileHandle.writePage(leftIndex, pageData);
//		delete[] pageData;

		//update the leftSibling information of the right sibling of the rightNode;
		if(rightTmp->meta.rightSiblingPageIndex!=UNDEFINED_PAGE_INDEX){
			byte* pageData = new byte[PAGE_SIZE];
			pageData = new byte[PAGE_SIZE];
			ixfileHandle.readPage(rightTmp->meta.rightSiblingPageIndex, pageData);
			IXPageHeader headerTmp;
			this->constructIXPageHeaderSectionFromData(pageData, headerTmp);
			headerTmp.leftSiblingPageIndex = leftIndex;
			this->writePageHeaderIntoData(pageData,headerTmp);
			ixfileHandle.writePage(rightTmp->meta.rightSiblingPageIndex, pageData);
			delete[] pageData;
		}

		if(rightTmp->keys.size()!=0)
		{
			cerr<<"Error Message! There are still some elements in the rightSibling, but we are trying to set it into Empty!"<<endl;
		}

		//currently, the rightTmp should be empty
		rightTmp->meta.pageType = EMPTY;
		rightTmp->meta.recordCount = 0;
		rightTmp->meta.parentPageIndex = UINT_MAX;
		rightTmp->meta.leftSiblingPageIndex = UINT_MAX;
		rightTmp->meta.rightSiblingPageIndex = UINT_MAX;
		rightTmp->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader);

//		leftTmp->printNode();

	} else {
		MyBPTreeIndexNode* leftTmp = dynamic_cast<MyBPTreeIndexNode*>(left);
		MyBPTreeIndexNode* rightTmp = dynamic_cast<MyBPTreeIndexNode*>(right);

		unsigned leftItemNum = leftTmp->meta.recordCount
				+ rightTmp->meta.recordCount + 1; //plus one, which is pull from the parent

		//we should update the parent information of the datas in the current left item;
		rightTmp->updateChildrenParentPageInfo(ixfileHandle, leftIndex);

		byte* tmp = new byte[length];
		memcpy(tmp,parent->keys[keyIndex],length);
		leftTmp->keys.push_back(tmp);


		leftTmp->children.push_back(rightTmp->children[0]);
		rightTmp->children.erase(rightTmp->children.begin());

		for (unsigned i = 0; i < rightTmp->keys.size();) {
			leftTmp->keys.push_back(rightTmp->keys[0]);
			leftTmp->children.push_back(rightTmp->children[0]);
			rightTmp->keys.erase(rightTmp->keys.begin());
			rightTmp->children.erase(rightTmp->children.begin());
		}

		leftTmp->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader)
				- leftItemNum * (sizeof(PAGE_NUM) + length)
				- sizeof(PAGE_NUM);
		leftTmp->meta.rightSiblingPageIndex =
				rightTmp->meta.rightSiblingPageIndex;
		leftTmp->meta.recordCount = leftItemNum;

		//update the leftSibling information of the right sibling of the rightNode;
		if(rightTmp->meta.rightSiblingPageIndex!=UNDEFINED_PAGE_INDEX){
			byte* pageData = new byte[PAGE_SIZE];
			ixfileHandle.readPage(rightTmp->meta.rightSiblingPageIndex, pageData);
			IXPageHeader headerTmp;
			this->constructIXPageHeaderSectionFromData(pageData, headerTmp);
			headerTmp.leftSiblingPageIndex = leftIndex;
			this->writePageHeaderIntoData(pageData,headerTmp);
			ixfileHandle.writePage(rightTmp->meta.rightSiblingPageIndex, pageData);
			delete[] pageData;
		}

		if(rightTmp->keys.size()!=0)
		{
			cerr<<"Error Message! There are still some elements in the rightSibling, but we are trying to set it into Empty!"<<endl;
		}

		//currently, the rightTmp should be empty
		rightTmp->meta.pageType = EMPTY;
		rightTmp->meta.recordCount = 0;
		rightTmp->meta.parentPageIndex = UINT_MAX;
		rightTmp->meta.leftSiblingPageIndex = UINT_MAX;
		rightTmp->meta.rightSiblingPageIndex = UINT_MAX;
		rightTmp->meta.freeSpace = PAGE_SIZE - sizeof(IXPageHeader);
	}

	deleteNodeFromIndexNode(ixfileHandle, attribute, parent, parentIndex,
			keyIndex);
}

RC IndexManager::basicDeleteKeyFromLeaf(IXFileHandle &ixfileHandle,
		MyBPTreeLeafNode& leafNode, const Attribute &attribute, const void *key,
		const RID &rid) {

	unsigned index = 0;

	for (; index < leafNode.keys.size(); index++) {
		if (leafNode.keyComparator(attribute.type, key, leafNode.keys[index])
				== 0) {
			break;
		}
	}

	if (index == leafNode.keys.size()) {
		cerr << "Didn't find the key, there should be some error in the system!"
				<< endl;
		return -1;
	}

	RID* fakeRID = &leafNode.datas[index];
	if (fakeRID->pageNum == UNDEFINED_PAGE_INDEX) {
		RC rc = deleteRIDFromDataPage(ixfileHandle, fakeRID->slotNum, rid);
		if (rc != 0) {
			cerr << "System error, delete rid failed!" << endl;
			return -1;
		}

		byte* pageData = new byte[PAGE_SIZE];
		ixfileHandle.readPage(fakeRID->slotNum, pageData);
		IXPageHeader header;
		this->constructIXPageHeaderSectionFromData(pageData, header);
		if (header.recordCount == 0) {
			leafNode.deleteKey(index);
		}

		delete[] pageData;
		return 0;
	} else {
		leafNode.deleteKey(index);
		return 0;
	}
}

RC IndexManager::deleteRIDFromDataPage(IXFileHandle &ixfileHandle,
		PAGE_NUM dataPageNum, const RID &rid) {

	//load all rid from pageData
	byte* pageData = new byte[PAGE_SIZE];
	ixfileHandle.readPage(dataPageNum, pageData);

	//find the index of the rid;
	vector<RID> ridList;
	this->getAllRIDFromDataPage(ixfileHandle, pageData, ridList);

	unsigned index = 0;
	for (; index < ridList.size(); index++) {
		if (ridList[index].pageNum == rid.pageNum
				&& ridList[index].slotNum == rid.slotNum) {
			break;
		}
	}

	if (index == ridList.size()) {
		cerr << "Didn't find the rid!" << endl;
		return -1;
	}

	//calculate the original total page number;
	unsigned totalPageNum = ceil(((double) ridList.size()) / MAX_RID_NUM);

	//delete the rid from original rid list;
	ridList.erase(ridList.begin() + index);

	//write ridList into DATA and APPEND pages;
	PAGE_NUM currentPageIndex = dataPageNum;
	unsigned currentPageRIDNum;
	IXPageHeader header;
	byte* dataTmp = new byte[PAGE_SIZE];
	for (unsigned pageIndex = 0; pageIndex < totalPageNum; pageIndex++) {
		unsigned offset = sizeof(IXPageHeader);
		ixfileHandle.readPage(currentPageIndex, dataTmp);
		this->constructIXPageHeaderSectionFromData(dataTmp, header);

		if ((ridList.size() - MAX_RID_NUM * pageIndex) > MAX_RID_NUM)
			currentPageRIDNum = MAX_RID_NUM;
		else
			currentPageRIDNum = ridList.size() - MAX_RID_NUM * pageIndex;

		if (currentPageRIDNum > 0) {
			memcpy(pageData + offset, &(ridList[MAX_RID_NUM * pageIndex]),
					sizeof(RID) * currentPageRIDNum);
			header.recordCount = currentPageRIDNum;
			header.freeSpace = PAGE_SIZE - sizeof(RID) * currentPageRIDNum
					- sizeof(IXPageHeader);
			this->writePageHeaderIntoData(pageData, header);
			ixfileHandle.writePage(currentPageIndex, pageData);
			currentPageIndex = header.rightSiblingPageIndex;
		} else {
			if (header.recordCount != 1) {
				cerr << "System error in the deletetion, currentPageIndex = "
						<< currentPageIndex << endl;
				return -1;
			}

			//update the right sibling information of the last page
			if (header.pageType == APPEND) {
				PAGE_NUM previousAppendPage = header.parentPageIndex;
				ixfileHandle.readPage(previousAppendPage, dataTmp);
				this->constructIXPageHeaderSectionFromData(dataTmp, header);
				header.rightSiblingPageIndex = UNDEFINED_PAGE_INDEX;
				this->writePageHeaderIntoData(dataTmp, header);
				ixfileHandle.writePage(previousAppendPage, dataTmp);
			}
			//delete the empty page;
			this->emptifyPage(ixfileHandle, currentPageIndex);
		}
	}

	delete[] dataTmp;
	delete[] pageData;
	return 0;
}


RC IndexManager::emptifyPage(IXFileHandle &ixfileHandle, PAGE_NUM pageNum) {
	cout<<"Emptify Page method was called!"<<endl;

	byte* pageData = new byte[PAGE_SIZE];
	ixfileHandle.readPage(pageNum, pageData);
	IXPageHeader header;
	this->constructIXPageHeaderSectionFromData(pageData, header);
	memset(pageData, 0, PAGE_SIZE);
	header.pageType = EMPTY;
	header.recordCount = 0;
	header.parentPageIndex = UINT_MAX;
	header.leftSiblingPageIndex = UINT_MAX;
	header.rightSiblingPageIndex = UINT_MAX;
	header.freeSpace = PAGE_SIZE - sizeof(IXPageHeader);
	memcpy(pageData, &header, sizeof(IXPageHeader));
	ixfileHandle.writePage(pageNum, pageData);
	delete[] pageData;
	return 0;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator) {
	MyBPTreeIndexNode* root = getRoot(ixfileHandle, attribute);
	if (root == NULL) {
		return -1;
	}
	PAGE_NUM lowKeyPageIndex, highKeyPageIndex;

	unsigned lowKeyIndex, highKeyIndex;

	MyBPTreeLeafNode* lowLeaf;
	MyBPTreeLeafNode* highLeaf;

	if (lowKey != NULL) {
		lowLeaf = findContainKeyLEAFPage(ixfileHandle, attribute, lowKey, root,
				lowKeyPageIndex);
		lowKeyIndex = lowLeaf->findKeyIndex(lowKey);

		bool found = (lowKeyIndex != lowLeaf->keys.size());
		bool exact = false;

		if (found) {
			exact = lowLeaf->keyComparator(attribute.type, lowKey,
					lowLeaf->keys[lowKeyIndex]) == 0;
		}

		if (!found) {
			MyBPTreeLeafNode* rightTmp = lowLeaf;
			lowKeyPageIndex = lowLeaf->meta.rightSiblingPageIndex;
			byte* pageData = new byte[PAGE_SIZE];
			ixfileHandle.readPage(lowKeyPageIndex, pageData);
			lowLeaf =
					dynamic_cast<MyBPTreeLeafNode*>(IndexManager::constructBPTreeNodeByPageData(
							pageData, attribute));
			lowKeyIndex = 0;
			delete rightTmp;
			delete[] pageData;
		} else {
			if (exact && !lowKeyInclusive) {
				if (lowKeyIndex == lowLeaf->keys.size()) {
					MyBPTreeLeafNode* rightTmp = lowLeaf;
					lowKeyPageIndex = lowLeaf->meta.rightSiblingPageIndex;
					byte* pageData = new byte[PAGE_SIZE];
					ixfileHandle.readPage(lowKeyPageIndex, pageData);
					lowLeaf =
							dynamic_cast<MyBPTreeLeafNode*>(IndexManager::constructBPTreeNodeByPageData(
									pageData, attribute));
					lowKeyIndex = 0;
					delete rightTmp;
					delete[] pageData;
				} else {
					lowKeyIndex++;
				}
			}
		}

	} else {
		lowLeaf = findLowestKeyLEAFPage(ixfileHandle, attribute, root,
				lowKeyPageIndex);
		//for debug
		//lowLeaf->printNode();
		lowKeyIndex = 0;
	}

	if (lowLeaf->keys.size() == 0) {
		MyBPTreeLeafNode* lowTmp = lowLeaf;
		PAGE_NUM leftPageIndex = lowLeaf->meta.rightSiblingPageIndex;
		byte* pageData = new byte[PAGE_SIZE];
		ixfileHandle.readPage(leftPageIndex, pageData);

		lowLeaf =
				dynamic_cast<MyBPTreeLeafNode*>(IndexManager::constructBPTreeNodeByPageData(
						pageData, attribute));
		lowKeyPageIndex = leftPageIndex;
		lowKeyIndex = 0;

		delete lowTmp;
		delete[] pageData;
	}

	if (highKey != NULL) {
		highLeaf = findContainKeyLEAFPage(ixfileHandle, attribute, highKey,
				root, highKeyPageIndex);
		highKeyIndex = highLeaf->findKeyIndex(highKey);

		bool found = (highKeyIndex != highLeaf->keys.size());
		bool exact = false;

		if (found) {
			exact = highLeaf->keyComparator(attribute.type, highKey,
					highLeaf->keys[highKeyIndex]) == 0;
		}

		if (!found) {
			highKeyIndex = highLeaf->keys.size() - 1;
		} else {
			if (!exact) {
				if (highKeyIndex != 0) {
					highKeyIndex--;
				} else {
					MyBPTreeLeafNode* rightTmp = highLeaf;
					highKeyPageIndex = highLeaf->meta.leftSiblingPageIndex;
					byte* pageData = new byte[PAGE_SIZE];
					ixfileHandle.readPage(highKeyPageIndex, pageData);
					highLeaf =
							dynamic_cast<MyBPTreeLeafNode*>(IndexManager::constructBPTreeNodeByPageData(
									pageData, attribute));
					highKeyIndex = highLeaf->keys.size() - 1;
					delete rightTmp;
					delete[] pageData;
				}
			} else {
				if (!highKeyInclusive) {
					if (highKeyIndex != 0) {
						highKeyIndex--;
					} else {
						MyBPTreeLeafNode* rightTmp = highLeaf;
						highKeyPageIndex = highLeaf->meta.leftSiblingPageIndex;
						byte* pageData = new byte[PAGE_SIZE];
						ixfileHandle.readPage(highKeyPageIndex, pageData);
						highLeaf =
								dynamic_cast<MyBPTreeLeafNode*>(IndexManager::constructBPTreeNodeByPageData(
										pageData, attribute));
						highKeyIndex = highLeaf->keys.size() - 1;
						delete rightTmp;
						delete[] pageData;
					}
				}
			}
		}
	} else {
		highLeaf = findHighestKeyLEAFPage(ixfileHandle, attribute, root,
				highKeyPageIndex);
		highKeyIndex = highLeaf->keys.size() - 1;
	}

	if (lowLeaf->meta.leftSiblingPageIndex == highKeyPageIndex) {
		cerr << "Initialization failed!" << endl;
		return -1;
	}

	delete lowLeaf;
	delete highLeaf;

	ix_ScanIterator.initialize(ixfileHandle, lowKeyPageIndex, highKeyPageIndex,
			lowKeyIndex, highKeyIndex, attribute);
	return 0;
}

MyBPTreeIndexNode* IndexManager::getRoot(IXFileHandle &ixfileHandle,
		const Attribute &attribute) const {
	byte* rootData = new byte[PAGE_SIZE];
	PAGE_NUM rootIndex = 0;
	RC rc = ixfileHandle.readPage(rootIndex, rootData);
	if (rc != 0) {
		cerr << "Read page failed, there is no page in the B tree!" << endl;
		delete[] rootData;
		return NULL;
	}
	MyBPTreeIndexNode* root =
			dynamic_cast<MyBPTreeIndexNode*>(constructBPTreeNodeByPageData(
					rootData, attribute));
	delete[] rootData;
	return root;
}

//delete the returned node pointer
MyBPTreeLeafNode* IndexManager::findContainKeyLEAFPage(
		IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key,
		MyBPTreeIndexNode* root, PAGE_NUM& pageIndex) {

	PAGE_NUM childIndex = root->findKeyIndex(key);
	pageIndex = root->children[childIndex];

	byte* pageData = new byte[PAGE_SIZE];
	ixfileHandle.readPage(pageIndex, pageData);
	MyBPTreeNode* childNode = constructBPTreeNodeByPageData(pageData,
			attribute);

	while (childNode->meta.pageType != LEAF) {
		MyBPTreeIndexNode* tmpNode = dynamic_cast<MyBPTreeIndexNode*>(childNode);
		childIndex = tmpNode->findKeyIndex(key);
		pageIndex = tmpNode->children[childIndex];
		ixfileHandle.readPage(pageIndex, pageData);
		delete tmpNode;
		childNode = constructBPTreeNodeByPageData(pageData, attribute);
	}
	delete[] pageData;
	MyBPTreeLeafNode* resNode = dynamic_cast<MyBPTreeLeafNode*>(childNode);
	//delete resNode;
	return resNode;
}

MyBPTreeLeafNode* IndexManager::findHighestKeyLEAFPage(
		IXFileHandle &ixfileHandle, const Attribute &attribute,
		MyBPTreeIndexNode* root, PAGE_NUM& pageIndex) {
	PAGE_NUM childIndex = root->children[root->keys.size()];
	byte* pageData = new byte[PAGE_SIZE];
	ixfileHandle.readPage(childIndex, pageData);
	MyBPTreeNode* childNode = constructBPTreeNodeByPageData(pageData,
			attribute);
	pageIndex = childIndex;
	while (childNode->meta.pageType != LEAF) {
		MyBPTreeIndexNode* tmpNode = dynamic_cast<MyBPTreeIndexNode*>(childNode);
		childIndex = tmpNode->children[tmpNode->keys.size()];
		ixfileHandle.readPage(childIndex, pageData);
		delete tmpNode;
		childNode = constructBPTreeNodeByPageData(pageData, attribute);
		pageIndex = childIndex;
	}
	delete[] pageData;
	return dynamic_cast<MyBPTreeLeafNode*>(childNode);
}

MyBPTreeLeafNode* IndexManager::findLowestKeyLEAFPage(
		IXFileHandle &ixfileHandle, const Attribute &attribute,
		MyBPTreeIndexNode* root, PAGE_NUM& pageIndex) {
	PAGE_NUM childIndex = root->children[0];
	byte* pageData = new byte[PAGE_SIZE];
	ixfileHandle.readPage(childIndex, pageData);
	MyBPTreeNode* childNode = constructBPTreeNodeByPageData(pageData,
			attribute);
	pageIndex = childIndex;

	while (childNode->meta.pageType != LEAF) {
		MyBPTreeIndexNode* tmpNode = dynamic_cast<MyBPTreeIndexNode*>(childNode);
		childIndex = tmpNode->children[0];
		ixfileHandle.readPage(childIndex, pageData);
		delete tmpNode;
		childNode = constructBPTreeNodeByPageData(pageData, attribute);
		pageIndex = childIndex;
	}
	delete[] pageData;
	return dynamic_cast<MyBPTreeLeafNode*>(childNode);
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle,
		const Attribute &attribute) const {
	MyBPTreeIndexNode* root = getRoot(ixfileHandle, attribute);
	if(root->meta.recordCount>0){
		cout << "{" << "\n";
		printBPlusTreeNodes(ixfileHandle, root, attribute, "");
		cout << "\n}" << endl;
	}else{
		byte* pageData = new byte[PAGE_SIZE];
		ixfileHandle.readPage(root->children[0], pageData);
		MyBPTreeLeafNode* firstChild = dynamic_cast<MyBPTreeLeafNode*>(this->constructBPTreeNodeByPageData(pageData,attribute));
		cout << "{" << "\n";
		printBPlusTreeNodes(ixfileHandle,firstChild , attribute, "");
		cout << "\n}" << endl;
		delete firstChild;
	}
	delete root;
}

void IndexManager::printBPlusTreeNodes(IXFileHandle &ixfileHandle,
		MyBPTreeNode* node, const Attribute &attribute, string tabs) const {
	if (node->meta.pageType == INDEX) {
		MyBPTreeIndexNode* indexNode = dynamic_cast<MyBPTreeIndexNode*>(node);
		cout << "\"key\":[";
		for (unsigned keyIndex = 0; keyIndex < node->keys.size(); keyIndex++) {
			cout << "\"";
			if (attribute.type == TypeInt) {
				cout << *(int*) (indexNode->keys[keyIndex]) << "\"";
			} else if (attribute.type == TypeReal) {
				cout << *(float*) (indexNode->keys[keyIndex]) << "\"";
			} else if (attribute.type == TypeVarChar) {
				// cout<< ((byte*)(indexNode->keys[keyIndex])+sizeof(unsigned)) <<"\"";
				unsigned length;
				length = attribute.length + sizeof(unsigned);

				//length = *((unsigned*) indexNode->keys[keyIndex]);
				char* value = new char[length+1];
				memset(value,0,length+1);
				memcpy(value,(byte*) indexNode->keys[keyIndex], length);
				cout << (value + 4) << "\"";
				//cout<< value + sizeof(unsigned) <<"==============";
				delete[] value;
			}

			if (keyIndex != (node->keys.size() - 1)) {
				cout << ",";
			} else {
				cout << "],\n";
			}
		}
		cout << tabs << "\"children\": [\n";
		for (unsigned childIndex = 0; childIndex < (indexNode->keys.size() + 1);
				childIndex++) {
			byte* pageData = new byte[PAGE_SIZE];
			RC rc = ixfileHandle.readPage(indexNode->children[childIndex],
					pageData);
			if (rc != 0) {
				//for debug;
				cerr << "Error!" << endl;
				return;
			}

			MyBPTreeNode* childNode = constructBPTreeNodeByPageData(pageData,
					attribute);
			if (childNode->meta.recordCount > 0) {
				cout << tabs + "\t" << "{";
				printBPlusTreeNodes(ixfileHandle, childNode, attribute,
						tabs + "\t");
				cout << "}";

				if (childIndex != (indexNode->keys.size())) {
					cout << ",\n";
				} else {
					cout << "\n";
				}
			}
			delete childNode;
			delete[] pageData;
		}
		cout << tabs << "]";
	} else {
		MyBPTreeLeafNode* indexNode = dynamic_cast<MyBPTreeLeafNode*>(node);
		//cout<< tabs <<"\"key\":[";
		cout << "\"key\":[";
		for (unsigned keyIndex = 0; keyIndex < indexNode->keys.size();
				keyIndex++) {
			cout << "\"";
			if (attribute.type == TypeInt) {
				cout << *(int*) (indexNode->keys[keyIndex]);
			} else if (attribute.type == TypeReal) {
				cout << *(float*) (indexNode->keys[keyIndex]);
			} else if (attribute.type == TypeVarChar) {
				unsigned length;
				length = attribute.length + sizeof(unsigned);

				//length = *((unsigned*) indexNode->keys[keyIndex]);
				char* value = new char[length+1];
				memset(value,0,length+1);
				memcpy(value,(byte*) indexNode->keys[keyIndex], length);
				cout << (value + 4) << "\"";
				//cout<< value + sizeof(unsigned) <<"==============";
				delete[] value;
			}
			cout << ":[";

			RID fakeRID = (indexNode->datas)[keyIndex];
			vector<RID> ridList;
			if (fakeRID.pageNum == UNDEFINED_PAGE_INDEX) {

				PAGE_NUM pageIndex = fakeRID.slotNum;
				byte* pageData = new byte[PAGE_SIZE];
				RC rc = ixfileHandle.readPage(pageIndex, pageData);
				if (rc != 0) {
					//for debug;
					cerr << "Error!" << endl;
					return;
				}

				this->getAllRIDFromDataPage(ixfileHandle, pageData, ridList);
				delete[] pageData;
			} else {
				ridList.push_back(fakeRID);
			}
			for (unsigned ridIndex = 0; ridIndex < ridList.size(); ridIndex++) {
				cout << "(" << ridList[ridIndex].pageNum << ","
						<< ridList[ridIndex].slotNum << ")";
				if (ridIndex != (ridList.size() - 1)) {
					cout << ",";
				} else {
					cout << "]\"";
				}
			}

			if (keyIndex != (indexNode->keys.size() - 1)) {
				cout << ",";
			}
		}
		cout << "]";
	}
}

void IndexManager::getAllRIDFromDataPage(IXFileHandle &ixfileHandle,
		byte* pageData, vector<RID>& ridList) {

	IXPageHeader header;
	constructIXPageHeaderSectionFromData(pageData, header);

	if (header.pageType != DATA && header.pageType != APPEND) {
		//cerr << "System error! header.pageType = " << header.pageType << endl;
		return;
	}

	for (unsigned ridIndex = 0; ridIndex < header.recordCount; ridIndex++) {
		RID rid;
		memcpy(&rid, pageData + sizeof(header) + ridIndex * sizeof(RID),
				sizeof(RID));
		ridList.push_back(rid);
	}
	if (header.rightSiblingPageIndex != UINT_MAX) {
		byte* dataTmp = new byte[PAGE_SIZE];
		ixfileHandle.readPage(header.rightSiblingPageIndex, dataTmp);
		getAllRIDFromDataPage(ixfileHandle, dataTmp, ridList);
		delete[] dataTmp;
	}
}

MyBPTreeNode* IndexManager::constructBPTreeNodeByPageData(byte* pageData,
		const Attribute &attr) {
	IXPageHeader header;
	constructIXPageHeaderSectionFromData(pageData, header);

	unsigned length = attr.length;
	if (attr.type == TypeVarChar) {
		length += sizeof(unsigned);
	}
	if (header.pageType == INDEX) {
		MyBPTreeIndexNode* node = new MyBPTreeIndexNode();
		node->keyType = attr;
		node->meta = header;

		unsigned recordNum = node->meta.recordCount;
		unsigned offset = sizeof(IXPageHeader);
		unsigned pageIndexTmp;

		for (unsigned i = 0; i < recordNum; i++) {
			memcpy(&pageIndexTmp, pageData + offset, sizeof(PAGE_NUM));
			node->children.push_back(pageIndexTmp);
			offset += sizeof(PAGE_NUM);
			byte* keyTmp = new byte[length];
			memcpy(keyTmp, pageData + offset, length);
			node->keys.push_back(keyTmp);
			offset += length;
		}
		memcpy(&pageIndexTmp, pageData + offset, sizeof(PAGE_NUM));
		node->children.push_back(pageIndexTmp);
		return node;
	} else if (header.pageType == LEAF) {
		MyBPTreeLeafNode* node = new MyBPTreeLeafNode();
		node->keyType = attr;
		node->meta = header;

		unsigned recordNum = node->meta.recordCount;
		unsigned offset = sizeof(IXPageHeader);

		for (unsigned i = 0; i < recordNum; i++) {
			byte* keyTmp = new byte[length];
			memcpy(keyTmp, pageData + offset, length);
			node->keys.push_back(keyTmp);
			offset += length;
			RID rid;
			memcpy(&rid, pageData + offset, sizeof(RID));
			offset += sizeof(RID);
			node->datas.push_back(rid);
		}
		return node;
	} else {
		//for debug; should be error;
//		cerr
//				<< "System Error, the header of page contains error, failed to construct!"
//				<< endl;
		return NULL;
	}
}

RC IndexManager::initializeIXPage(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {
	PAGE_NUM rootIndex = 0, leftIndex = 1;

	unsigned length = attribute.length;
	if(attribute.type == TypeVarChar){
		length += sizeof(PAGE_NUM);
	}

	byte* headerData = new byte[PAGE_SIZE];
	IXPageHeader rootHeader;
	rootHeader.pageType = INDEX;
	rootHeader.recordCount = 0;
	rootHeader.parentPageIndex = UNDEFINED_PAGE_INDEX;
	rootHeader.leftSiblingPageIndex = UNDEFINED_PAGE_INDEX;
	rootHeader.rightSiblingPageIndex = UNDEFINED_PAGE_INDEX;
	rootHeader.freeSpace = PAGE_SIZE - sizeof(IXPageHeader) - sizeof(PAGE_NUM); // no key in root
	this->writePageHeaderIntoData(headerData, rootHeader);
	memcpy(headerData + sizeof(IXPageHeader), &leftIndex, sizeof(PAGE_NUM));
	RC rc = ixfileHandle.appendPage(headerData);
	if (rc != 0) {
		cerr << "Writing the header information failed!" << endl;
		return -1;
	}

	//the left child should be empty, but it should be there
	byte* leftChildData = new byte[PAGE_SIZE];
	IXPageHeader leftChildHeader;
	leftChildHeader.pageType = LEAF;
	leftChildHeader.recordCount = 1;
	leftChildHeader.parentPageIndex = rootIndex;
	leftChildHeader.leftSiblingPageIndex = UNDEFINED_PAGE_INDEX;
	leftChildHeader.rightSiblingPageIndex = UNDEFINED_PAGE_INDEX;
	leftChildHeader.freeSpace = PAGE_SIZE - sizeof(IXPageHeader) - sizeof(RID)
			- length;
	this->writePageHeaderIntoData(leftChildData, leftChildHeader);
	memcpy(leftChildData + sizeof(IXPageHeader), key, length);
	memcpy(leftChildData + sizeof(IXPageHeader) + length, &rid, sizeof(RID));
	rc = ixfileHandle.appendPage(leftChildData);
	if (rc != 0) {
		cerr << "Writing the leftData information failed!" << endl;
		return -1;
	}
	delete[] headerData;
	delete[] leftChildData;
	return 0;
}

void IndexManager::constructIXPageHeaderSectionFromData(byte* data,
		IXPageHeader& header) {
	memcpy(&header, data, sizeof(IXPageHeader));
}

void IndexManager::writePageHeaderIntoData(byte *data, IXPageHeader& header) {
	memcpy(data, &header, sizeof(IXPageHeader));
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {

	if (this->currentPointLeafNode != NULL) {
		if (this->hasNextEntry()) {
			unsigned length = attr.length;
			if(attr.type == TypeVarChar){
				length += 4;
			}
			memcpy(key, this->currentPointLeafNode->keys[this->currentKeyIndex],length);
			RID tmp = this->currentKeyRidList[this->currentRidIndex];
			rid.pageNum = tmp.pageNum;
			rid.slotNum = tmp.slotNum;
			//this->increaseIndex();
			if (this->increaseIndex() == 0)
				return 0;
			else {
				this->currentRidIndex++;
				return 0;
			}
		} else {
			return IX_EOF;
		}
	} else {
		return -1;
	}
}

RC IX_ScanIterator::increaseIndex() {
	if (this->currentRidIndex < (this->currentKeyRidList.size() - 1)) {
		this->currentRidIndex++;
		return 0;
	} else {
		if (this->currentKeyIndex < this->currentPageKeyMaxIndex) {
			this->currentKeyIndex++;
			this->currentRidIndex = 0;
			RID rid = this->currentPointLeafNode->datas[this->currentKeyIndex];
			this->currentKeyRidList.clear();
			if (rid.pageNum == UNDEFINED_PAGE_INDEX) {
				PAGE_NUM dataPageNum = rid.slotNum;
				byte* dataPageData = new byte[PAGE_SIZE];
				ixfileHandle.readPage(dataPageNum, dataPageData);
				IndexManager::getAllRIDFromDataPage(this->ixfileHandle,
						dataPageData, this->currentKeyRidList);
				delete[] dataPageData;
			} else {
				this->currentKeyRidList.push_back(rid);
			}
			return 0;
		} else {
			if (this->currentPageIndex != this->highLeafPage) {
				this->currentPageIndex =
						this->currentPointLeafNode->meta.rightSiblingPageIndex;
				currentRidIndex = 0;
				currentKeyIndex = 0;

				byte* pageData = new byte[PAGE_SIZE];
				ixfileHandle.readPage(this->currentPageIndex, pageData);

				delete this->currentPointLeafNode;
				this->currentPointLeafNode =
						dynamic_cast<MyBPTreeLeafNode*>(IndexManager::constructBPTreeNodeByPageData(
								pageData, this->attr));
				if (this->currentPageIndex == this->highLeafPage) {
					this->currentPageKeyMaxIndex = this->highKeyIndex;
				} else {
					this->currentPageKeyMaxIndex =
							this->currentPointLeafNode->meta.recordCount - 1;
				}
				delete[] pageData;

				RID rid =
						this->currentPointLeafNode->datas[this->currentKeyIndex];
				this->currentKeyRidList.clear();
				if (rid.pageNum == UNDEFINED_PAGE_INDEX) {
					PAGE_NUM dataPageNum = rid.slotNum;
					byte* dataPageData = new byte[PAGE_SIZE];
					ixfileHandle.readPage(dataPageNum, dataPageData);
					IndexManager::getAllRIDFromDataPage(this->ixfileHandle,
							dataPageData, this->currentKeyRidList);
					delete[] dataPageData;
				} else {
					this->currentKeyRidList.push_back(rid);
				}
				return 0;
			} else {
				return -1;
			}
		}
	}
	return -1;
}

RC IX_ScanIterator::close() {
	delete this->currentPointLeafNode;
	this->currentPointLeafNode = NULL;
	this->currentKeyRidList.clear();
	return 0;
}

bool IX_ScanIterator::hasNextEntry() {
	if (this->currentPageIndex != this->highLeafPage) {
		return true;
	} else {
		if (this->currentKeyIndex != this->currentPageKeyMaxIndex) {
			return true;
		} else {
			if (this->currentRidIndex < this->currentKeyRidList.size())
				return true;
		}
	}
	return false;
}

void IX_ScanIterator::initialize(IXFileHandle &ixfileHandle,
		unsigned lowLeafPage, unsigned highLeafPage, unsigned lowKeyIndex,
		unsigned highKeyIndex, const Attribute& attr) {
	this->attr = attr;

	this->lowKeyIndex = lowKeyIndex;
	this->highKeyIndex = highKeyIndex;
	this->lowLeafPage = lowLeafPage;
	this->highLeafPage = highLeafPage;

	this->ixfileHandle = ixfileHandle;

	this->currentRidIndex = 0;
	this->currentKeyIndex = lowKeyIndex;
	this->currentPageIndex = lowLeafPage;

	byte* pageData = new byte[PAGE_SIZE];
	ixfileHandle.readPage(this->currentPageIndex, pageData);
	this->currentPointLeafNode =
			dynamic_cast<MyBPTreeLeafNode*>(IndexManager::constructBPTreeNodeByPageData(
					pageData, this->attr));

	if(currentPointLeafNode == NULL){
		delete[] pageData;
		return;
	}

	RID rid = this->currentPointLeafNode->datas[this->currentKeyIndex];
	if (rid.pageNum == UNDEFINED_PAGE_INDEX) {
		PAGE_NUM dataPageNum = rid.slotNum;
		byte* dataPageData = new byte[PAGE_SIZE];
		ixfileHandle.readPage(dataPageNum, dataPageData);
		IndexManager::getAllRIDFromDataPage(this->ixfileHandle, dataPageData,
				this->currentKeyRidList);
		delete[] dataPageData;
	} else {
		this->currentKeyRidList.push_back(rid);
	}

	if (this->currentPageIndex == this->highLeafPage) {
		this->currentPageKeyMaxIndex = this->highKeyIndex;
	} else {
		this->currentPageKeyMaxIndex =
				this->currentPointLeafNode->meta.recordCount - 1;
	}

	delete[] pageData;
}

IXFileHandle::IXFileHandle() {
	ixReadPageCounter = 0;
	ixWritePageCounter = 0;
	ixAppendPageCounter = 0;
	this->pFile = NULL;
}

IXFileHandle::~IXFileHandle() {
	if (this->pFile != NULL) {
		pFile = NULL;
	}
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount,
		unsigned &writePageCount, unsigned &appendPageCount) {
	return FileHandle::collectCounterValues(readPageCount, writePageCount,
			appendPageCount);
}

MyBPTreeNode::MyBPTreeNode() {

}

MyBPTreeNode::~MyBPTreeNode() {
	if(this->keys.size()>0){
		for(unsigned i=0;i<keys.size();i++){
			byte* keyTmp = (byte*)this->keys[i];
			delete[] keyTmp;
		}
		this->keys.clear();
	}
}

unsigned MyBPTreeNode::findKeyIndex(const void* key) //return the index of which larger than the given key
		{
	unsigned i = 0;
	for (; i < this->keys.size(); i++)
		if (this->keyComparator(keyType.type, key, this->keys[i]) < 0) {
			break;
		}
	return i;
}

/**************************The Definition of MyBPTreeIndexNode***********************************************/

MyBPTreeIndexNode::MyBPTreeIndexNode() {
}

//MyBPTreeIndexNode::MyBPTreeIndexNode(byte* pageData, IXPageHeader header, const Attribute& attr){
//	unsigned length = attr.length;
//	if(attr.type == TypeVarChar)
//		length += sizeof(unsigned);
//
//	this->keyType = attr;
//	this->meta = header;
//
//	unsigned recordNum = this->meta.recordCount;
//	unsigned offset = sizeof(IXPageHeader);
//	unsigned pageIndexTmp;
//
//	for (unsigned i = 0; i < recordNum; i++) {
//		memcpy(&pageIndexTmp, pageData + offset, sizeof(PAGE_NUM));
//		this->children.push_back(pageIndexTmp);
//		offset += sizeof(PAGE_NUM);
//		byte* keyTmp = new byte[length];
//		memcpy(keyTmp, pageData + offset, length);
//		this->keys.push_back(keyTmp);
//		offset += length;
//	}
//	memcpy(&pageIndexTmp, pageData + offset, sizeof(PAGE_NUM));
//	this->children.push_back(pageIndexTmp);
//}


MyBPTreeIndexNode::~MyBPTreeIndexNode() {

}

void MyBPTreeIndexNode::toPageData(void* pageData) {
	unsigned length = this->keyType.length;
	if(this->keyType.type == TypeVarChar){
		length += sizeof(unsigned);
	}

	memcpy((byte*) pageData, &this->meta, sizeof(IXPageHeader));
	unsigned offset = sizeof(IXPageHeader);
	memcpy((byte*) pageData + offset, &this->children[0], sizeof(PAGE_NUM));
	offset += sizeof(PAGE_NUM);
	for (unsigned i = 0; i < this->meta.recordCount; i++) {
		if(this->keyType.type == TypeVarChar){
			unsigned valueLength = *(unsigned*)this->keys[i];
			char* valueTmp = new char[length];
			memset(valueTmp,0,length);
			memcpy(valueTmp,this->keys[i],valueLength+sizeof(unsigned));
			memcpy((byte*) pageData + offset, valueTmp, length);
			offset += length;
			delete[] valueTmp;
		}else{
			memcpy((byte*) pageData + offset, this->keys[i], length);
			offset += length;
		}
		memcpy((byte*) pageData + offset, &this->children[i + 1], sizeof(PAGE_NUM));
		offset += sizeof(PAGE_NUM);
		//for debug
		//cout << *(int*)this->keys[i] <<" - "<<this->children[i+1] <<endl;
	}
}

void MyBPTreeIndexNode::insert(unsigned position, const void* key,
		PAGE_NUM childPageIndex) {
	unsigned length = this->keyType.length;
	if(this->keyType.type == TypeVarChar){
		length += sizeof(unsigned);
	}

	byte* keyTmp = new byte[length];
	memcpy(keyTmp, key, length);
	this->keys.insert(keys.begin() + position,keyTmp);
	this->children.insert(this->children.begin() + position + 1,childPageIndex);
	this->meta.freeSpace = this->meta.freeSpace - sizeof(PAGE_NUM)- length;
	this->meta.recordCount++;
}

void MyBPTreeIndexNode::updateChildrenParentPageInfo(IXFileHandle &ixfileHandle,
		PAGE_NUM newParentPageIndex) {
	for (unsigned i = 0; i < this->children.size(); i++) {
		byte* pageData = new byte[PAGE_SIZE];
		IXPageHeader headerTmp;
		ixfileHandle.readPage(this->children[i], pageData);
		IndexManager::constructIXPageHeaderSectionFromData(pageData, headerTmp);
		headerTmp.parentPageIndex = newParentPageIndex;
		IndexManager::writePageHeaderIntoData(pageData, headerTmp);
		ixfileHandle.writePage(this->children[i], pageData);
		delete[] pageData;
	}
}

void MyBPTreeIndexNode::deleteKey(int keyIndex) {
	unsigned length = this->keyType.length;
	if(this->keyType.type == TypeVarChar){
		length += sizeof(unsigned);
	}
	byte* keyTmp = (byte*)this->keys[keyIndex];
	this->keys.erase(this->keys.begin() + keyIndex);
	this->children.erase(this->children.begin() + keyIndex + 1);
	delete[] keyTmp;

	this->meta.freeSpace = this->meta.freeSpace + sizeof(PAGE_NUM) + length;
	this->meta.recordCount--;
}

/**************************The Definition of MyBPTreeLeafNode***********************************************/

MyBPTreeLeafNode::MyBPTreeLeafNode(){

}

//MyBPTreeLeafNode::MyBPTreeLeafNode(byte* pageData, IXPageHeader header, const Attribute& attr){
//	unsigned length = attr.length;
//	if(attr.type == TypeVarChar)
//		length += sizeof(unsigned);
//
//	this->keyType = attr;
//	this->meta = header;
//	unsigned recordNum = this->meta.recordCount;
//	unsigned offset = sizeof(IXPageHeader);
//
//	for (unsigned i = 0; i < recordNum; i++) {
//		byte* keyTmp = new byte[length];
//		memcpy(keyTmp, pageData + offset, length);
//		this->keys.push_back(keyTmp);
//		offset += length;
//		RID rid;
//		memcpy(&rid, pageData + offset, sizeof(RID));
//		offset += sizeof(RID);
//		this->datas.push_back(rid);
//	}
//}

MyBPTreeLeafNode::~MyBPTreeLeafNode() {
}

unsigned MyBPTreeLeafNode::findKeyIndex(const void* key) //return the index of which larger than the given key
		{
	unsigned i = 0;
	for (; i < this->keys.size(); i++)
		if (this->keyComparator(keyType.type, key, this->keys[i]) <= 0) {
			break;
		}
	return i;
}

void MyBPTreeLeafNode::insert(unsigned position, const void* key,
		const RID& rid) {
	unsigned length = this->keyType.length;
	if(this->keyType.type == TypeVarChar){
		length += sizeof(unsigned);
	}

	byte* keyTmp = new byte[length];
	memcpy(keyTmp, key, length);
	this->keys.insert(keys.begin() + position,keyTmp);
	this->datas.insert(datas.begin() + position,rid);
	this->meta.recordCount++;
	this->meta.freeSpace = this->meta.freeSpace - sizeof(RID) - length;
}

void MyBPTreeLeafNode::deleteKey(int keyIndex) {
	unsigned length = this->keyType.length;
	if(this->keyType.type == TypeVarChar){
		length += sizeof(unsigned);
	}
	byte* keyTmp = (byte*)this->keys[keyIndex];
	this->keys.erase(keys.begin() + keyIndex);
	this->datas.erase(datas.begin() + keyIndex);
	delete[] keyTmp;
	this->meta.recordCount--;
	this->meta.freeSpace = this->meta.freeSpace + sizeof(RID)+ length;
}

void MyBPTreeLeafNode::updateChildrenParentPageInfo(IXFileHandle &ixfileHandle,
		PAGE_NUM newParentPageIndex) {
	for (unsigned i = 0; i < this->datas.size(); i++) {
		RID* rid = &datas[i];
		if (rid->pageNum == UNDEFINED_PAGE_INDEX) {
			PAGE_NUM dataPageIndex = rid->slotNum;
			byte* pageData = new byte[PAGE_SIZE];
			IXPageHeader headerTmp;
			ixfileHandle.readPage(dataPageIndex, pageData);
			IndexManager::constructIXPageHeaderSectionFromData(pageData,
					headerTmp);
			headerTmp.parentPageIndex = newParentPageIndex;
			IndexManager::writePageHeaderIntoData(pageData, headerTmp);
			ixfileHandle.writePage(dataPageIndex, pageData);
			delete[] pageData;
		}
	}
}

void MyBPTreeLeafNode::toPageData(void* pageData) {
	unsigned length = this->keyType.length;
	if(this->keyType.type == TypeVarChar){
		length += sizeof(unsigned);
	}

	memcpy((byte*) pageData, &this->meta, sizeof(IXPageHeader));
	unsigned offset = sizeof(IXPageHeader);
	for (unsigned i = 0; i < this->meta.recordCount; i++) {
		if(this->keyType.type == TypeVarChar){
			unsigned valueLength = *(unsigned*)this->keys[i];
			char* valueTmp = new char[length];
			memset(valueTmp,0,length);
			memcpy(valueTmp,this->keys[i],valueLength+sizeof(unsigned));
			memcpy((byte*) pageData + offset, valueTmp, length);
			offset += length;
			delete[] valueTmp;
		}else {
			memcpy((byte*) pageData + offset, this->keys[i], length);
			offset += length;
		}

		memcpy((byte*) pageData + offset, &this->datas[i], sizeof(RID));
		offset += sizeof(RID);
	}
}
