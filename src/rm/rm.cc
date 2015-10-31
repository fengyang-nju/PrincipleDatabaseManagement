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
	if (!rbfm)
		this->rbfm = RecordBasedFileManager::instance();
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

	FileHandle colFile;
	if (this->rbfm->openFile(ATTR_FILE_NAME, colFile) != 0) {
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

			//cout<<table->filaname<<endl;

			this->table_mapper[table->name] = table;

//			void* idValue = malloc(sizeof(unsigned));
//			memcpy(idValue,&(table->id),sizeof(unsigned));
//			cout<<"idValue"<<idValue<<endl;
			RM_ScanIterator columnScanner;
			columnScanner.createNewScanner(colFile, columnsAttrs, "table-id",
					EQ_OP, &(table->id), columnAttrNames);

			while (columnScanner.hasNextTuple()) {
				RID colrid;
				byte* colData = (byte*) malloc(PAGE_SIZE);
				if (columnScanner.getNextTuple(colrid, colData) == 0) {
					Column* col = new Column();

					//for debug
//					cout<<"colrid.slotNum"<<colrid.slotNum<<endl;
//					vector<Attribute> colAttr;
//					this->loadColumnsDescriptors(colAttr);
//					this->rbfm->printRecord(colAttr,colData);

					this->constructColumnFromAPIData(col, colData);
					col->rid.pageNum = colrid.pageNum;
					col->rid.slotNum = colrid.slotNum;
					table->column_list.push_back(col);
				}
				free(colData);
			}
			//columnScanner.close();
			this->table_mapper[table->name] = table;
			//free(idValue);
		}
		free(data);
	}
	tableScanner.close();
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
	table->filaname = filename;
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

	/*Construct the Tables meta infomration */
	vector<Attribute> tablesAttrs;
	this->loadTableDescriptors(tablesAttrs);

	/*Construct the Columns meta information */
	vector<Attribute> colsAttrs;
	this->loadColumnsDescriptors(colsAttrs);

	//construction system data structure
	MyTable* tableMeta = new MyTable(this->table_mapper.size() + 1,
	CATELOG_FILE_NAME, CATELOG_FILE_NAME);
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

	MyTable* colMeta = new MyTable(this->table_mapper.size() + 1,
	ATTR_FILE_NAME, ATTR_FILE_NAME);

	data = (byte*) malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);
	colMeta->toAPIRecordFormat(data);

	//for debug
	this->rbfm->printRecord(tablesAttrs, data);

	if (this->rbfm->insertRecord(tableMetaHandle, tablesAttrs, data, rid)
			!= 0) {
		return -1;
	}
	free(data);

	colMeta->rid.pageNum = rid.pageNum;
	colMeta->rid.slotNum = rid.slotNum;

	Column *colTableIdCol = new Column(colMeta->id, "table-id", TypeInt,
			sizeof(int), 1, 0);
	colMeta->column_list.push_back(colTableIdCol);
	Column *colColNameCol = new Column(colMeta->id, "column-name", TypeVarChar,
			50, 2, 0);
	colMeta->column_list.push_back(colColNameCol);
	Column *colColType = new Column(colMeta->id, "column-type", TypeInt,
			sizeof(int), 3, 0);
	colMeta->column_list.push_back(colColType);
	Column *colColLength = new Column(colMeta->id, "column-length", TypeInt,
			sizeof(int), 4, 0);
	colMeta->column_list.push_back(colColLength);
	Column *colColPosition = new Column(colMeta->id, "column-position", TypeInt,
			sizeof(int), 4, 0);
	colMeta->column_list.push_back(colColPosition);
	Column *colColDeleteFlag = new Column(colMeta->id, "is-deleted", TypeInt,
			sizeof(int), 4, 0);
	colMeta->column_list.push_back(colColDeleteFlag);
	this->table_mapper[colMeta->name] = colMeta;

	for (iterator = colMeta->column_list.begin();
			iterator != colMeta->column_list.end(); ++iterator) {
		byte* data = (byte*) malloc(PAGE_SIZE);
		memset(data, 0, PAGE_SIZE);
		(*iterator)->toAPIRecordFormat(data);

		//for debug
		this->rbfm->printRecord(colsAttrs, data);

		if (this->rbfm->insertRecord(colMetaHandle, colsAttrs, data, rid)
				!= 0) {
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
	return 0;
}

RC RelationManager::createTable(const string &tableName,
		const vector<Attribute> &attrs) {

	//Create the file
	FileHandle newTableHandle;
	if (rbfm->openFile(tableName, newTableHandle) == 0) {
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
		this->rbfm->printRecord(attrsTable, data);

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
	if (this->rbfm->destroyFile(table->filaname) != 0) {
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
		attrs.push_back(attr);
	}
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data,
		RID &rid) {
	MyTable* table = this->table_mapper[tableName];
	if (!table){
		cout<<"table not exist!"<<endl;
		return -1;
	}
	vector<Attribute> attrs;
	this->getAttributes(tableName, attrs);
	FileHandle fileHandle;

	if (this->rbfm->openFile(table->filaname, fileHandle) != 0){
		cout<<"open file failed! filename = "<<table->filaname<<endl;
		return -1;
	}

	if(this->rbfm->insertRecord(fileHandle, attrs, data, rid)==0){
		return this->rbfm->closeFile(fileHandle);
	}
	return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
	MyTable* table = this->table_mapper[tableName];
	vector<Attribute> attrs;
	this->getAttributes(tableName, attrs);
	FileHandle fileHandle;

	if (this->rbfm->openFile(table->filaname, fileHandle) != 0) {
		return -1;
	}

	if (this->rbfm->deleteRecord(fileHandle, attrs, rid) != 0) {
		return -1;
	}

	this->rbfm->closeFile(fileHandle);

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

	if (this->rbfm->openFile(table->filaname, fileHandle) != 0) {
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
	if (this->rbfm->openFile(table->filaname, fileHandle) != 0) {
		cout<<"open file failed!"<<endl;
		return -1;
	}

	if (this->rbfm->readRecord(fileHandle, attrs, rid, data) != 0) {
		//for debug
		//cout<<"read record failed! rid.page = "<< rid.pageNum <<" rid.slotNum = " <<rid.slotNum<<endl;
		return -1;
	}
	return this->rbfm->closeFile(fileHandle);
}

//RC RelationManager::readTuple(const string &tableName, const RID &rid,
//		void *data) {
//	MyTable* table = this->table_mapper[tableName];
//	if (!table){
//		cout<<"table is null"<<endl;
//		return -1;
//	}
//	vector<Attribute> attrs;
//	vector<Attribute> allAttrs;
//	this->getAttributes(tableName, attrs);
//	//this->getAllAttributes(allAttrs);
//
//	FileHandle fileHandle;
//	if (this->rbfm->openFile(table->filaname, fileHandle) != 0) {
//		cout<<"open file failed!"<<endl;
//		return -1;
//	}
//
//	if (this->rbfm->readRecord(fileHandle, attrs, rid, data) != 0) {
//		cout<<"read record failed! rid.page = "<< rid.pageNum <<" rid.slotNum = " <<rid.slotNum<<endl;
//		return -1;
//	}
//
//	list<Column*> columnList = this->table_mapper[tableName]->column_list;
//
//	list<Column*>::const_iterator iterator;
//	unsigned attrCounter = 0;
//	void* dataRes = new byte[PAGE_SIZE];
//
//	for (iterator = columnList.begin(); iterator != columnList.end();++iterator) {
//		if((*iterator)->deleteFLag!=1){
//			attrCounter++;
//		}
//	}
//
//	if(this->rbfm->closeFile(fileHandle)!=0){
//		delete[] dataRes;
//	}
//
//	return 0;
//}

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
			return -1;
		}
	}

	FileHandle fileHandle;
	if (this->rbfm->openFile(tableName, fileHandle) != 0) {
		return -1;
	}

	vector<Attribute> attrs;
	this->getAllAttributes(tableName, attrs);
	return this->rbfm->readAttribute(fileHandle, attrs, rid, attributeName,
			data);

//
//	for(int i=0;i<attrs.size();i++){
//		if(strcmp(attributeName.c_str(),attrs[i].name.c_str())==0 || )
//	}
}

RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute, const CompOp compOp,
		const void *value, const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator) {

	MyTable* table = this->table_mapper[tableName];
	FileHandle fileHandle;
	if (this->rbfm->openFile(table->filaname, fileHandle) != 0) {
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

	length = this->filaname.length();
	memcpy((byte *) data + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset, this->filaname.c_str(), length);
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
