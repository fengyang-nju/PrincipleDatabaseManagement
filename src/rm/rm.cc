#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance() {
	if (!_rm) {
		_rm = new RelationManager();

		FileHandle fileHandle;
		if (_rm->rbfm->openFile(CATELOG_FILE_NAME, fileHandle) == 0) {
			_rm->importTableMapper();
			_rm->rbfm->closeFile(fileHandle);
		}

	}

	return _rm;
}

RelationManager::RelationManager() {
	if (!rbfm){
		this->rbfm = RecordBasedFileManager::instance();
		this->ixm = IndexManager::instance();
	}
}

RelationManager::~RelationManager() {
}

bool RM_ScanIterator::hasNextTuple() {
	return this->rbfm_scanner.hasNextRecord();
}

RC RM_ScanIterator::close() {
	return this->rbfm_scanner.close();
}

RC RM_ScanIterator::createNewScanner(FileHandle fileHandle,
		vector<Attribute> attrs, const string &conditionAttribute,
		const CompOp compOp, const void *value,
		const vector<string> &attributeNames) {
	return this->rbfm_scanner.createNewScanIterator(fileHandle, attrs,
			conditionAttribute, compOp, value, attributeNames);
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	return this->rbfm_scanner.getNextRecord(rid, data);
}

RC RelationManager::importTableMapper() {

	FileHandle tableFile;
	if (this->rbfm->openFile(CATELOG_FILE_NAME, tableFile) != 0) {
		return -1;
	}

	vector<Attribute> attrs;
	this->loadTableDescriptors(attrs);
	vector<string> attrsNames;
	for (unsigned i = 0; i < attrs.size(); i++)
		attrsNames.push_back(attrs[i].name);

	vector<Attribute> columnsAttrs;
	this->loadColumnsDescriptors(columnsAttrs);
	vector<string> columnAttrNames;
	for (unsigned i = 0; i < columnsAttrs.size(); i++)
		columnAttrNames.push_back(columnsAttrs[i].name);

	vector<Attribute> indexesAttrs;
	this->loadIndexesDescriptors(indexesAttrs);
	vector<string> indexAttrNames;
	for (unsigned i = 0; i < indexesAttrs.size(); i++)
		indexAttrNames.push_back(indexesAttrs[i].name);

	RM_ScanIterator tableScanner;
	tableScanner.createNewScanner(tableFile, attrs, "", NO_OP, NULL,
			attrsNames);

	while (tableScanner.hasNextTuple()) {
		RID rid;
		byte* data = (byte*) malloc(PAGE_SIZE);
		if (tableScanner.getNextTuple(rid, data) == 0) {
			MyTable* table = new MyTable();
			this->constructTableFromAPIData(table, data);
			table->rid.pageNum = rid.pageNum;
			table->rid.slotNum = rid.slotNum;

			this->table_mapper[table->name] = table;

			FileHandle colFile;
			if (this->rbfm->openFile(ATTR_FILE_NAME, colFile) != 0) {
				return -1;
			}

			RM_ScanIterator columnScanner;
			columnScanner.createNewScanner(colFile, columnsAttrs, "table-id",
					EQ_OP, &(table->id), columnAttrNames);
			//cout<<"new table columns===================================="<< table->name<<endl;
			while (columnScanner.hasNextTuple()) {
				RID colrid;
				byte* colData = (byte*) malloc(PAGE_SIZE);
				if (columnScanner.getNextTuple(colrid, colData) == 0) {
					Column* col = new Column();

					this->constructColumnFromAPIData(col, colData);
					col->rid.pageNum = colrid.pageNum;
					col->rid.slotNum = colrid.slotNum;
					//cout<< "col name = " << col->name <<endl;
					/////////////////////////////////////////////////////////
					FileHandle ixFile;
					if (this->rbfm->openFile(INDEX_FILE_NAME, ixFile) != 0) {
						return -1;
					}

					RM_ScanIterator ixScanner;
					ixScanner.createNewScanner(ixFile, columnsAttrs, "attr-name",
										EQ_OP, &(col->name), indexAttrNames);
					while(ixScanner.hasNextTuple()){
						RID ixrid;
						byte* ixData = (byte*) malloc(PAGE_SIZE);
						if (ixScanner.getNextTuple(ixrid, ixData) == 0) {
							MyIndex* myIndex = new MyIndex(table->id,table->name.c_str(),col->name.c_str(),(table->name+"_"+col->name).c_str());
							myIndex->rid.pageNum = ixrid.pageNum;
							myIndex->rid.slotNum = ixrid.slotNum;
							col->index = myIndex;
							free(ixData);
							break;
						}
						free(ixData);
					}

					ixScanner.close();
					/////////////////////////////////////////////////////////
					table->column_list.push_back(col);
				}
				free(colData);
			}
			columnScanner.close();
			this->table_mapper[table->name] = table;
		}
		free(data);
	}
	tableScanner.close();


	//for debug
//	for(std::map<string, MyTable*>::iterator it = this->table_mapper.begin();
//			it != this->table_mapper.end(); it++){
//		cout<<(*it).second->column_list.size()<<endl;
//	}

	return 0;
}

void RelationManager::constructColumnFromAPIData(Column* column, void* data) {
//	byte nullPointer = *(byte*)data;
//	memcpy(&id,(byte*)data+offset,sizeof(unsigned));
	unsigned offset = 1;

	unsigned id;
	memcpy(&id, (byte*) data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	column->tableId = id;

	unsigned nameLength;
	memcpy(&nameLength, (byte*) data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);

	char* name = (char*) malloc(PAGE_SIZE);
	memset(name, 0, PAGE_SIZE);
	memcpy(name, (byte*) data + offset, nameLength);
	offset += nameLength;
	column->name = name;
	free(name);

	AttrType columnType;
	memcpy(&columnType, (byte*) data + offset, sizeof(AttrType));
	offset += sizeof(unsigned);
	column->columnType = columnType;

	unsigned length;
	memcpy(&length, (byte*) data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	column->length = length;

	unsigned position;
	memcpy(&position, (byte*) data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	column->position = position;

	unsigned deleteFlag;
	memcpy(&deleteFlag, (byte*) data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	column->deleteFLag = deleteFlag;
	//column = new Column(id,nameValue,columnType,length,position,deleteFlag);
}

void RelationManager::constructTableFromAPIData(MyTable* table, void* data) {
	unsigned offset = 1;
	unsigned id;
	memcpy(&id, (byte*) data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);

	unsigned nameLength;
	memcpy(&nameLength, (byte*) data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);

	char* name = (char*) malloc(PAGE_SIZE);
	memset(name, 0, PAGE_SIZE);
	memcpy(name, (byte*) data + offset, nameLength);
	offset += nameLength;
	table->name = name;

	free(name);

	unsigned filenameLength;
	memcpy(&filenameLength, (byte*) data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);

	char* filename = (char*) malloc(PAGE_SIZE);
	memset(filename, 0, PAGE_SIZE);
	memcpy(filename, (byte*) data + offset, filenameLength);
	offset += filenameLength;
	table->filename = filename;
	free(filename);

	table->id = id;
	//cout<<table->name<<endl;
}

void RelationManager::loadTableDescriptors(vector<Attribute>& tablesAttrs) {
	Attribute tableId;
	tableId.name = "table-id";
	tableId.type = TypeInt;
	tableId.length = sizeof(int);
	tablesAttrs.push_back(tableId);
	Attribute tableName;
	tableName.name = "table-name";
	tableName.type = TypeVarChar;
	tableName.length = 50;
	tablesAttrs.push_back(tableName);
	Attribute fileName;
	fileName.name = "file-name";
	fileName.type = TypeVarChar;
	fileName.length = 50;
	tablesAttrs.push_back(fileName);
}

void RelationManager::loadColumnsDescriptors(vector<Attribute>& colsAttrs) {
	Attribute colId;
	colId.name = "table-id";
	colId.type = TypeInt;
	colId.length = sizeof(int);
	colsAttrs.push_back(colId);
	Attribute columnName;
	columnName.name = "column-name";
	columnName.type = TypeVarChar;
	columnName.length = 50;
	colsAttrs.push_back(columnName);
	Attribute columnType;
	columnType.name = "column-type";
	columnType.type = TypeInt;
	columnType.length = sizeof(TypeInt);
	colsAttrs.push_back(columnType);
	Attribute columnLength;
	columnLength.name = "column-length";
	columnLength.type = TypeInt;
	columnLength.length = sizeof(int);
	colsAttrs.push_back(columnLength);
	Attribute columnPosition;
	columnPosition.name = "column-position";
	columnPosition.type = TypeInt;
	columnPosition.length = sizeof(int);
	colsAttrs.push_back(columnPosition);
	Attribute deletedFlag; // 1 stands for deteled
	deletedFlag.name = "is-deleted";
	deletedFlag.type = TypeInt;
	deletedFlag.length = sizeof(int);
	colsAttrs.push_back(deletedFlag);
}

void RelationManager::loadIndexesDescriptors(vector<Attribute>& ixAttrs){
	Attribute tableId;
	tableId.name = "table-id";
	tableId.type = TypeInt;
	tableId.length = sizeof(int);
	ixAttrs.push_back(tableId);

	Attribute tableName;
	tableName.name = "table-name";
	tableName.type = TypeVarChar;
	tableName.length = 50;
	ixAttrs.push_back(tableName);

	Attribute attrName;
	attrName.name = "attr-name";
	attrName.type = TypeVarChar;
	attrName.length = 50;
	ixAttrs.push_back(attrName);

	Attribute fileName;
	fileName.name = "file-name";
	fileName.type = TypeVarChar;
	fileName.length = 100;
	ixAttrs.push_back(fileName);
}

RC RelationManager::createCatalog() {

	/*
	 *  Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
	 Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
	 * */

	/*Firstly, create files */
	this->createSysTableFiles();

	FileHandle tableMetaHandle;
	if (rbfm->openFile(CATELOG_FILE_NAME, tableMetaHandle) != 0) {
		return -1;
	}

	FileHandle colMetaHandle;
	if (rbfm->openFile(ATTR_FILE_NAME, colMetaHandle) != 0) {
		return -1;
	}


	FileHandle ixMetaHandle;
	if (rbfm->openFile(INDEX_FILE_NAME, ixMetaHandle) != 0) {
		return -1;
	}
	/*Construct the Tables meta infomration */
	vector<Attribute> tablesAttrs;
	this->loadTableDescriptors(tablesAttrs);

	/*Construct the Columns meta information */
	vector<Attribute> colsAttrs;
	this->loadColumnsDescriptors(colsAttrs);

	/*Construct the Indices meta information */
	vector<Attribute> indexesAttrs;
	this->loadIndexesDescriptors(indexesAttrs);

	//construction system data structure
	//create the table file
	MyTable* tableMeta = new MyTable(this->table_mapper.size() + 1, CATELOG_FILE_NAME, CATELOG_FILE_NAME);
	byte* data = (byte*) malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);
	tableMeta->toAPIRecordFormat(data);

	RID rid;
	if (this->rbfm->insertRecord(tableMetaHandle, tablesAttrs, data, rid)
			!= 0) {
		return -1;
	}
	free(data);

	Column *tableIdCol = new Column(tableMeta->id, "table-id", TypeInt,
			sizeof(int), 1, 0);
	tableMeta->column_list.push_back(tableIdCol);
	Column *tableNameCol = new Column(tableMeta->id, "table-name", TypeVarChar,
			50, 2, 0);
	tableMeta->column_list.push_back(tableNameCol);
	Column *filenameCol = new Column(tableMeta->id, "file-name", TypeVarChar,
			50, 3, 0);
	tableMeta->column_list.push_back(filenameCol);
	this->table_mapper[tableMeta->name] = tableMeta;

	list<Column*>::const_iterator iterator;
	for (iterator = tableMeta->column_list.begin();
			iterator != tableMeta->column_list.end(); ++iterator) {
		byte* data = (byte*) malloc(PAGE_SIZE);
		memset(data, 0, PAGE_SIZE);
		(*iterator)->toAPIRecordFormat(data);
		if (this->rbfm->insertRecord(colMetaHandle, colsAttrs, data, rid)
				!= 0) {
			return -1;
		}
		free(data);
	}


	//create column file
	MyTable* colMeta = new MyTable(this->table_mapper.size() + 1, ATTR_FILE_NAME, ATTR_FILE_NAME);

	data = (byte*) malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);
	colMeta->toAPIRecordFormat(data);

	//for debug
	//this->rbfm->printRecord(tablesAttrs, data);

	if (this->rbfm->insertRecord(tableMetaHandle, tablesAttrs, data, rid)
			!= 0) {
		return -1;
	}
	free(data);

	colMeta->rid.pageNum = rid.pageNum;
	colMeta->rid.slotNum = rid.slotNum;

	Column *colTableIdCol = new Column(colMeta->id, "table-id", TypeInt, sizeof(int), 1, 0);
	colMeta->column_list.push_back(colTableIdCol);
	Column *colColNameCol = new Column(colMeta->id, "column-name", TypeVarChar, 50, 2, 0);
	colMeta->column_list.push_back(colColNameCol);
	Column *colColType = new Column(colMeta->id, "column-type", TypeInt,sizeof(int), 3, 0);
	colMeta->column_list.push_back(colColType);
	Column *colColLength = new Column(colMeta->id, "column-length", TypeInt, sizeof(int), 4, 0);
	colMeta->column_list.push_back(colColLength);
	Column *colColPosition = new Column(colMeta->id, "column-position", TypeInt, sizeof(int), 5, 0);
	colMeta->column_list.push_back(colColPosition);
	Column *colColDeleteFlag = new Column(colMeta->id, "is-deleted", TypeInt, sizeof(int), 6, 0);
	colMeta->column_list.push_back(colColDeleteFlag);
	this->table_mapper[colMeta->name] = colMeta;

	for (iterator = colMeta->column_list.begin();
			iterator != colMeta->column_list.end(); ++iterator) {
		byte* data = (byte*) malloc(PAGE_SIZE);
		memset(data, 0, PAGE_SIZE);
		(*iterator)->toAPIRecordFormat(data);

		//for debug
		//this->rbfm->printRecord(colsAttrs, data);

		if (this->rbfm->insertRecord(colMetaHandle, colsAttrs, data, rid)
				!= 0) {
			return -1;
		}
		free(data);
		(*iterator)->rid.pageNum = rid.pageNum;
		(*iterator)->rid.slotNum = rid.slotNum;
	}

	//create index table
	MyTable* ixMeta = new MyTable(this->table_mapper.size() + 1, INDEX_FILE_NAME, INDEX_FILE_NAME);

	data = (byte*) malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);
	ixMeta->toAPIRecordFormat(data);

	if (this->rbfm->insertRecord(tableMetaHandle, tablesAttrs, data, rid)!= 0) {
		return -1;
	}
	free(data);

	ixMeta->rid.pageNum = rid.pageNum;
	ixMeta->rid.slotNum = rid.slotNum;

	Column *ixTableIdCol = new Column(ixMeta->id, "table-id", TypeInt, sizeof(int), 1, 0);
	ixMeta->column_list.push_back(ixTableIdCol);
	Column *ixTableNameCol = new Column(ixMeta->id, "table-name", TypeVarChar, 50, 2, 0);
	ixMeta->column_list.push_back(ixTableNameCol);
	Column *ixAttrNameCol = new Column(ixMeta->id, "attr-name", TypeVarChar, 50, 3, 0);
	ixMeta->column_list.push_back(ixAttrNameCol);
	Column *ixFileNameCol = new Column(ixMeta->id, "file-name", TypeVarChar, 100, 4, 0);
	ixMeta->column_list.push_back(ixFileNameCol);
	this->table_mapper[ixMeta->name] = ixMeta;

	for (iterator = ixMeta->column_list.begin();
			iterator != ixMeta->column_list.end(); ++iterator) {
		byte* data = (byte*) malloc(PAGE_SIZE);
		memset(data, 0, PAGE_SIZE);
		(*iterator)->toAPIRecordFormat(data);


		//for debug
		//this->rbfm->printRecord(colsAttrs, data);
		if (this->rbfm->insertRecord(colMetaHandle, colsAttrs, data, rid)!= 0) {
			return -1;
		}
		free(data);
		(*iterator)->rid.pageNum = rid.pageNum;
		(*iterator)->rid.slotNum = rid.slotNum;
	}

	return 0;
}

RC RelationManager::deleteCatalog() {
	FileHandle fileHandle;
	if (rbfm->openFile(CATELOG_FILE_NAME, fileHandle) == 0) {
		rbfm->closeFile(fileHandle);
		rbfm->destroyFile(CATELOG_FILE_NAME);
	}

	if (rbfm->openFile(ATTR_FILE_NAME, fileHandle) == 0) {
		rbfm->closeFile(fileHandle);
		rbfm->destroyFile(ATTR_FILE_NAME);
	}

	if (rbfm->openFile(INDEX_FILE_NAME, fileHandle) == 0) {
		rbfm->closeFile(fileHandle);
		rbfm->destroyFile(INDEX_FILE_NAME);
	}

	this->table_mapper.clear();
	return 0;
}

RC RelationManager::createSysTableFiles() {
	//Create Tables
	FileHandle fileHandle;
	if (rbfm->openFile(CATELOG_FILE_NAME, fileHandle) == 0) {
		return -1;
	} else {
		rbfm->createFile(CATELOG_FILE_NAME);
	}

	//Create Columns
	if (rbfm->openFile(ATTR_FILE_NAME, fileHandle) == 0) {
		return -1;
	} else {
		rbfm->createFile(ATTR_FILE_NAME);
	}

	if (rbfm->openFile(INDEX_FILE_NAME, fileHandle) == 0) {
		return -1;
	} else {
		rbfm->createFile(INDEX_FILE_NAME);
	}
	return 0;
}

RC RelationManager::createTable(const string &tableName,
		const vector<Attribute> &attrs) {

	//Create the file
	FileHandle newTableHandle;
	if (rbfm->openFile(tableName, newTableHandle) == 0) {
	    cerr << "****Table Created: " << tableName << " ****" << endl << endl;
		return -1;
	} else {
		rbfm->createFile(tableName);
	}

	//insert into the table catalog
	//for debug
	//cout<<tableName<<endl;

	MyTable* myTable = new MyTable(this->table_mapper.size() + 1, tableName,
			tableName);
	vector<Attribute> tableAttr;
	this->loadTableDescriptors(tableAttr);

	byte* data = (byte*) malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);
	myTable->toAPIRecordFormat(data);

	//for debug
//	this->rbfm->printRecord(tableAttr, data);
//	byte* storedData = new byte[PAGE_SIZE];
//	this->rbfm->convertAPIData2StoreFormat(tableAttr,data,storedData);
//	this->rbfm->printRecordInStoreFormat(tableAttr,storedData);

	FileHandle fileHandle;
	RID rid;
	if (this->rbfm->openFile(CATELOG_FILE_NAME, fileHandle) == 0) {
		rbfm->insertRecord(fileHandle, tableAttr, data, rid);
	}
	free(data);
	myTable->rid.pageNum = rid.pageNum;
	myTable->rid.slotNum = rid.slotNum;

	//insert the attributes
	FileHandle attrFileHandle;
	if (rbfm->openFile(ATTR_FILE_NAME, attrFileHandle) != 0) {
		return -1;
	}
	vector<Attribute> attrsTable;
	this->getAttributes(ATTR_FILE_NAME, attrsTable);

	vector<Attribute>::const_iterator iterator;
	int positionIndex = 1;
	for (iterator = attrs.begin(); iterator != attrs.end(); ++iterator) {
		Column * pCol = new Column(myTable->id, (*iterator).name.c_str(),
				(*iterator).type, (*iterator).length, positionIndex, 0);

		data = (byte*) malloc(PAGE_SIZE);
		memset(data, 0, PAGE_SIZE);
		pCol->toAPIRecordFormat(data);

		//for debug
//		this->rbfm->printRecord(attrsTable, data);

		if (this->rbfm->insertRecord(attrFileHandle, attrsTable, data, rid)
				!= 0) {
			return -1;
		}
		free(data);
		pCol->rid.pageNum = rid.pageNum;
		pCol->rid.slotNum = rid.slotNum;
		myTable->column_list.push_back(pCol);
		positionIndex++;
	}

	this->table_mapper[myTable->name] = myTable;
	return 0;
}

RC RelationManager::deleteTable(const string &tableName) {

	if(strcmp(tableName.c_str(),CATELOG_FILE_NAME)==0 ||
			strcmp(tableName.c_str(),ATTR_FILE_NAME)==0 ){
		return -1;
	}

	MyTable* table = table_mapper[tableName];
	if(!table)
		return -1;
	//destroy table file;
	if (this->rbfm->destroyFile(table->filename) != 0) {
		return -1;
	}

	//delete column records here
	list<Column*> colList = table->column_list;
	vector<Attribute> colAttrs;
	this->loadColumnsDescriptors(colAttrs);
	FileHandle columnFileHandle;
	if (this->rbfm->openFile(ATTR_FILE_NAME, columnFileHandle) != 0) {
		return -1;
	}
	for (list<Column*>::iterator colIte = colList.begin();
			colIte != colList.end(); colIte++) {
		this->rbfm->deleteRecord(columnFileHandle, colAttrs, (*colIte)->rid);
		//delete (*colIte);
	}
	this->rbfm->closeFile(columnFileHandle);

	//detele the table record here;
	vector<Attribute> tableAttrs;
	this->loadTableDescriptors(tableAttrs);
	FileHandle tableFileHandle;
	if (this->rbfm->openFile(CATELOG_FILE_NAME, tableFileHandle) != 0) {
		return -1;
	}
	this->rbfm->deleteRecord(tableFileHandle, tableAttrs, table->rid);
	delete table;

	this->table_mapper.erase(tableName);
	return 0;
}

RC RelationManager::getAttributes(const string &tableName,
		vector<Attribute> &attrs) {
	//for debug
	//cout<<tableName<<endl;

	MyTable* table = this->table_mapper[tableName];
	if (!table) {
		return -1;
	}
	list<Column*> columnList = table->column_list;
	list<Column*>::const_iterator iterator;
	for (iterator = columnList.begin(); iterator != columnList.end();
			++iterator) {
		if((*iterator)->deleteFLag!=1){
			this->convertColumnIntoAttribute(*iterator, attrs, false);
		}
	}
	return 0;
}

RC RelationManager::getAllAttributes(const string &tableName,
		vector<Attribute> &attrs) {
	MyTable* table = this->table_mapper[tableName];
	if (!table) {
		return -1;
	}
	list<Column*> columnList = table->column_list;
	list<Column*>::const_iterator iterator;
	for (iterator = columnList.begin(); iterator != columnList.end();
			++iterator) {
		this->convertColumnIntoAttribute(*iterator, attrs, true);
	}
	return 0;
}

RC RelationManager::convertColumnIntoAttribute(Column* col,
		vector<Attribute> &attrs, bool showDeleted) {
	Attribute attr;
	if (col->deleteFLag == 0 || showDeleted) {
		attr.length = col->length;
		attr.name = col->name;
		attr.type = col->columnType;
		//for debug
		//cout<< col->name <<endl;
		attrs.push_back(attr);
	}
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data,
		RID &rid) {

	if(strcmp(tableName.c_str(),CATELOG_FILE_NAME)==0 ||
			strcmp(tableName.c_str(),ATTR_FILE_NAME)==0 ){
		return -1;
	}


	MyTable* table = this->table_mapper[tableName];
	if (!table){
		cout<<"table not exist!"<<endl;
		return -1;
	}
	vector<Attribute> attrs;
	this->getAttributes(tableName, attrs);

	FileHandle fileHandle;
	if (this->rbfm->openFile(table->filename, fileHandle) != 0){
		cout<<"open file failed! filename = "<<table->filename<<endl;
		return -1;
	}

	if(this->rbfm->insertRecord(fileHandle, attrs, data, rid)==0){
		this->rbfm->closeFile(fileHandle);
		return insertIndexesWithTuple(tableName, data,rid);
	}
	return -1;
}

RC RelationManager::insertIndexesWithTuple(const string &tableName, const void *tupleData, RID &rid){
	MyTable* table = this->table_mapper[tableName];
	if (!table){
		cout<<"table not exist!"<<endl;
		return -1;
	}

	list<Column*> indexList = table->column_list;
	vector<Attribute> tableDescriptor;
	for(list<Column*>::iterator it = indexList.begin(); it!= indexList.end(); it++){
		this->convertColumnIntoAttribute((*it), tableDescriptor, false);
	}

	for(list<Column*>::iterator it = indexList.begin(); it!= indexList.end(); it++){
		if((*it)->index!=NULL){
			vector<string> attrsName;
			attrsName.push_back((*it)->name);

			byte* keyValue = new byte[PAGE_SIZE];
			this->rbfm->readAttributesFromAPIRecord(tupleData, tableDescriptor, attrsName, keyValue);
			IXFileHandle ixfileHandle;
			//cout<<"filename = "<<(*it)->index->filename<<endl;
			if (this->rbfm->openFile((*it)->index->filename, ixfileHandle) != 0){
				cout<<"open file failed! filename = "<<(*it)->index->filename<<endl;
						return -1;
			}else{
				byte nullIndicator = *(byte*)keyValue;
				if(nullIndicator==0){
					Attribute attr;
					attr.length = (*it)->length;
					attr.name =  (*it)->name;
					attr.type =  (*it)->columnType;
					RC rc = this->ixm->insertEntry(ixfileHandle,attr,keyValue+sizeof(byte),rid);
					if(rc!=0)
						return -1;

				}else{
					cerr<<"We dont process the NULL value as key!"<<endl;
					return -1;
				}
				this->ixm->closeFile(ixfileHandle);
			}
			delete[] keyValue;
		}
	}
	return 0;
}

RC RelationManager::deleteIndexesWithTuple(const string &tableName, const vector<Attribute> &attrs, const RID &rid){
	byte* recordData = new byte[PAGE_SIZE];

	RC rc = this->readTuple(tableName,rid,recordData);
	if(rc!=0)
		return -1;

	MyTable* table = this->table_mapper[tableName];
	if (!table){
		cout<<"table not exist!"<<endl;
		return -1;
	}

	list<Column*> indexList = table->column_list;
	vector<Attribute> tableDescriptor;
	for(list<Column*>::iterator it = indexList.begin(); it!= indexList.end(); it++){
		this->convertColumnIntoAttribute((*it), tableDescriptor, false);
	}

	for(list<Column*>::iterator it = indexList.begin(); it!= indexList.end(); it++){
		if((*it)->index!=NULL){
			vector<string> attrsName;
			attrsName.push_back((*it)->name);

			byte* keyValue = new byte[PAGE_SIZE];
			this->rbfm->readAttributesFromAPIRecord(recordData, tableDescriptor, attrsName, keyValue);
			IXFileHandle ixfileHandle;
			if (this->rbfm->openFile(table->filename, ixfileHandle) != 0){
				cout<<"open file failed! filename = "<<table->filename<<endl;
						return -1;
			}else{
				byte nullIndicator = *(byte*)keyValue;
				if(nullIndicator==0){
					Attribute attr;
					attr.length = (*it)->length;
					attr.name =  (*it)->name;
					attr.type =  (*it)->columnType;
					RC rc = this->ixm->deleteEntry(ixfileHandle,attr,keyValue+sizeof(byte),rid);
					if(rc!=0)
						return -1;

				}else{
					cerr<<"We dont process the NULL value as key!"<<endl;
					return -1;
				}
			}
			delete[] keyValue;
		}
	}
	delete[] recordData;
	return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
	MyTable* table = this->table_mapper[tableName];
	vector<Attribute> attrs;
	this->getAttributes(tableName, attrs);
	FileHandle fileHandle;

	// for debug
	//cout<<"delete rid = " << rid.pageNum <<" slot = "<<rid.slotNum<<endl;
	if (this->rbfm->openFile(table->filename, fileHandle) != 0) {
		cerr<<"open file operation failed!"<<endl;
		return -1;
	}

	if (this->deleteIndexesWithTuple(table->name, attrs, rid) != 0) {
		cerr<<"The delete record operation failed!"<<endl;
		return -1;
	}

	if (this->rbfm->deleteRecord(fileHandle, attrs, rid) != 0) {
		cerr<<"The delete record operation failed!"<<endl;
		return -1;
	}

	if (this->rbfm->closeFile(fileHandle) != 0) {
		cerr<<"Close file failed!"<<endl;
		return -1;
	}

	return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data,
		const RID &rid) {
	MyTable* table = this->table_mapper[tableName];
	if (!table)
		return -1;
	vector<Attribute> attrs;
	this->getAttributes(tableName, attrs);
	FileHandle fileHandle;

	if (this->rbfm->openFile(table->filename, fileHandle) != 0) {
		return -1;
	}

	if (this->rbfm->updateRecord(fileHandle, attrs, data, rid) != 0) {
		return -1;
	}

	this->rbfm->closeFile(fileHandle);
	table=0;
	return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
		void *data) {
	MyTable* table = this->table_mapper[tableName];
	if (!table){
		cout<<"table is null"<<endl;
		return -1;
	}
	vector<Attribute> attrs;
	this->getAttributes(tableName, attrs);
	FileHandle fileHandle;
	if (this->rbfm->openFile(table->filename, fileHandle) != 0) {
		cout<<"open file failed!"<<endl;
		return -1;
	}

	if (this->rbfm->readRecord(fileHandle, attrs, rid, data) != 0) {
		return -1;
	}
	return this->rbfm->closeFile(fileHandle);
}

RC RelationManager::printTuple(const vector<Attribute> &attrs,
		const void *data) {
	return this->rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
		const string &attributeName, void *data) {

	list<Column*> columnList = this->table_mapper[tableName]->column_list;
	list<Column*>::const_iterator iterator;
	for (iterator = columnList.begin(); iterator != columnList.end();
			++iterator) {
		if (strcmp(attributeName.c_str(), (*iterator)->name.c_str()) == 0
				&& (*iterator)->deleteFLag == 1) {
			cerr<< "The attribute is deleted"<<endl;
			return -1;
		}
	}

	FileHandle fileHandle;
	if (this->rbfm->openFile(tableName, fileHandle) != 0) {
		cerr<< "open file failed!"<<endl;
		return -1;
	}

	vector<Attribute> attrs;
	this->getAllAttributes(tableName, attrs);
	RC rc = this->rbfm->readAttribute(fileHandle, attrs, rid, attributeName,
			data);
	this->rbfm->closeFile(fileHandle);
	return rc;

}

RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute, const CompOp compOp,
		const void *value, const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator) {

	MyTable* table = this->table_mapper[tableName];
	FileHandle fileHandle;
	if (this->rbfm->openFile(table->filename, fileHandle) != 0) {
		return -1;
	}

	vector<Attribute> attrs;
	list<Column*>::iterator iterator;
	for (iterator = table->column_list.begin();
			iterator != table->column_list.end(); iterator++) {
		this->convertColumnIntoAttribute(*iterator, attrs, true);
	}

	return rm_ScanIterator.createNewScanner(fileHandle, attrs,
			conditionAttribute, compOp, value, attributeNames);
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName){

	if(this->table_mapper.size()==0){
		RC rc = this->createCatalog();
		if(rc!=0){
			importTableMapper();
		}
	}

	MyTable* table = this->table_mapper[tableName];
	if(table==NULL){
		return -1;
	}else{
		Column* col = table->findAttributeByName(attributeName);
		if(col==NULL)
			return -1;
		else{
			IndexManager::instance()->createFile(tableName+"_"+attributeName);
			RC rc = this->initializeIndexFile(table, col);
			if(rc != 0)
				return rc;
			//push the index into table
			MyIndex* myIndex = new MyIndex(table->id,tableName.c_str(),attributeName.c_str(),(tableName+"_"+attributeName).c_str());
			col->index = myIndex;

			byte* indexData = new byte[PAGE_SIZE];
			RID ridTmp;
			myIndex->toAPIRecordFormat(indexData);
			this->insertTuple(INDEX_FILE_NAME,indexData,ridTmp);
			delete[] indexData;
			return 0;
		}
	}
	return -1;
}


RC RelationManager::initializeIndexFile(MyTable* table, Column* col){

	string indexFileName = table->name+"_"+col->name;
    IXFileHandle ixfileHandle;

    vector<Attribute> attrVectorTmp;
    this->convertColumnIntoAttribute(col,attrVectorTmp,false);
    Attribute attrTmp = attrVectorTmp[0];

	/*Initialize record scanner*/
	RM_ScanIterator rmsi;
	vector<string> attrs;
	attrs.push_back(col->name);
	RC rc = this->scan(table->name, "", NO_OP, NULL, attrs, rmsi);

	if(rc == -1)
		return rc;

	/*prepare for the index insertation*/
	rc = this->ixm->openFile(indexFileName, ixfileHandle);
	if(rc == -1)
			return rc;

	while(rmsi.hasNextTuple()){
		RID rid;
		byte* value = new byte[PAGE_SIZE];
		rmsi.getNextTuple(rid,value);

		byte nullIndicator = *((byte*)value+sizeof(byte));
		if(nullIndicator==0)
			ixm->insertEntry(ixfileHandle, attrTmp, (byte*)value+sizeof(byte), rid);  //we dont need the offset
		else
			cerr<<"The return value of this attribute is Null, pageNo = "<< rid.pageNum <<" slotNo = "<<rid.slotNum<<endl;
		delete[] value;
	}

	return 0;
}

// indexScan returns an iterator to allow the caller to go through qualified entries in index
RC RelationManager::indexScan(const string &tableName,
                       const string &attributeName,
                       const void *lowKey,
                       const void *highKey,
                       bool lowKeyInclusive,
                       bool highKeyInclusive,
                       RM_IndexScanIterator &rm_IndexScanIterator){
	IXFileHandle ixfileHandle;
	RC rc = ixm->openFile(tableName+"_"+attributeName,ixfileHandle);
	if(rc!=0)
		return rc;

	MyTable* table = this->table_mapper[tableName];
	Column* col = table->findAttributeByName(attributeName);
	vector<Attribute> attrs;
	this->convertColumnIntoAttribute(col, attrs, false);
	Attribute attr = attrs[0];
	ixm->scan(ixfileHandle, attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_scanner);
}


// Extra credit work
RC RelationManager::dropAttribute(const string &tableName,
		const string &attributeName) {
	MyTable* table = this->table_mapper[tableName];
	if(!table)
		return -1;

	vector<Attribute> attrs;
	this->getAllAttributes(tableName,attrs);

	FileHandle fileHandle;
	if(this->rbfm->openFile(tableName,fileHandle)!=0){
		return -1;
	}


	list<Column*> columnList = table->column_list;
	list<Column*>::const_iterator iterator;
	for (iterator = columnList.begin(); iterator != columnList.end();
			++iterator) {
		if (strcmp((*iterator)->name.c_str(), attributeName.c_str()) == 0) {
			(*iterator)->deleteFLag = 1;
			//we should export this information into file
			void* data = (byte*)malloc(PAGE_SIZE);
			(*iterator)->toAPIRecordFormat(data);
			this->rbfm->updateRecord(fileHandle,attrs,data,(*iterator)->rid);
			free(data);
			return 0;
		}
	}
	this->rbfm->closeFile(fileHandle);
	return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName,
		const Attribute &attr) {
	MyTable* table = this->table_mapper[tableName];
	if(!table)
		return -1;

	list<Column*> columnList = table->column_list;
	Column* col = new Column();
	col->name=attr.name;
	col->columnType = attr.type;
	col->deleteFLag =0;
	col->position = columnList.size()+1;
	col->tableId = table->id;
	col->length = attr.length;

	vector<Attribute> attrs;
	this->getAllAttributes(tableName,attrs);

	FileHandle fileHandle;
	if(this->rbfm->openFile(tableName,fileHandle)!=0){
		return -1;
	}

	void* data = (byte*)malloc(PAGE_SIZE);
	col->toAPIRecordFormat(data);
	RID rid;
	if(this->rbfm->insertRecord(fileHandle,attrs,data,rid)!=0){
		free(data);
		col->rid.pageNum = rid.pageNum;
		col->rid.slotNum = rid.slotNum;
	}

	columnList.push_back(col);
	free(data);
	this->rbfm->closeFile(fileHandle);
	return 0;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
	int rc = this->ix_scanner.getNextEntry(rid, key);
	return rc;
};

RC RM_IndexScanIterator::close() {
	return this->ix_scanner.close();
}

RC MyTable::toAPIRecordFormat(void* data) {

	byte nullIndicator = 0;
	int offset = 0;
	memcpy((byte*) data + offset, &nullIndicator, sizeof(byte));
	offset += sizeof(char);
	int tableId = this->id;
	memcpy((byte *) data + offset, &tableId, sizeof(int));
	offset += sizeof(int);
	int length = this->name.length();
	memcpy((byte *) data + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset, this->name.c_str(), length);
	offset += length;

	length = this->filename.length();
	memcpy((byte *) data + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset, this->filename.c_str(), length);
	offset += length;

	return 0;
}

RC Column::toAPIRecordFormat(void* data) {
	int offset = 0;
	unsigned short nullIndicator = 0;
	memcpy((byte*) data + offset, &nullIndicator, sizeof(byte));
	offset += sizeof(byte);

	int tableId = this->tableId;
	memcpy((byte *) data + offset, &tableId, sizeof(int));
	offset += sizeof(int);
	int namelength = this->name.length();
	memcpy((byte *) data + offset, &namelength, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset, this->name.c_str(), namelength);
	offset += namelength;
	memcpy((byte *) data + offset, &this->columnType, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset, &this->length, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset, &this->position, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset, &this->deleteFLag, sizeof(int));
	offset += sizeof(int);
	return 0;
}

RC MyIndex::toAPIRecordFormat(void* data){
	int offset = 0;
	unsigned short nullIndicator = 0;
	memcpy((byte*) data + offset, &nullIndicator, sizeof(byte));
	offset += sizeof(byte);

	int tableId = this->tableId;
	memcpy((byte *) data + offset, &tableId, sizeof(int));
	offset += sizeof(int);

	int namelength = this->tablename.length();
	memcpy((byte *) data + offset, &namelength, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset, this->tablename.c_str(), namelength);
	offset += namelength;

	namelength = this->attrname.length();
	memcpy((byte *) data + offset, &namelength, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset, this->attrname.c_str(), namelength);
	offset += namelength;

	namelength = this->filename.length();
	memcpy((byte *) data + offset, &namelength, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset, this->filename.c_str(), namelength);
	offset += namelength;
	return 0;
}
