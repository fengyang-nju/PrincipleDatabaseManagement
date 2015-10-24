#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance() {
	if (!_rm)
		_rm = new RelationManager();

	return _rm;
}

RelationManager::RelationManager() {
	if (!rbfm)
		this->rbfm = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager() {
}

RC RelationManager::createCatalog() {

	/*
	 *  Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
	 Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
	 * */

	//rbfmHandler = RecordBasedFileManager::instance();
	/*Construct the Tables meta infomration */
	vector<Attribute> tablesAttrs;
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
	MyTable* tableMeta = new MyTable(this->table_list.size()+1,tableName,tableName);
	void* data = tableMeta->toAPIRecordFormat();
	rbfm->destroyFile(tableMeta->filaname);
	rbfm->createFile(tableMeta->filaname);
	FileHandle fileHandle;
	RID rid;
	if(rbfm->openFile(tableMeta->filaname,fileHandle)){
		rbfm->insertRecord(fileHandle,tablesAttrs,data,rid);
	}else{
		return -1;
	}
	this->table_list.push_back(tableMeta);


	/*Construct the Columns meta information */
	vector<Attribute> colsAttrs;
	colsAttrs.push_back(tableId);
	Attribute columnName;
	columnName.name = "column-name";
	columnName.type = TypeVarChar;
	columnName.length = 20;
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

	MyTable* columnMeta = new MyTable(this->table_list.size()+1,tableName,tableName);
	void* data = columnMeta->toAPIRecordFormat();
	rbfm->destroyFile(columnMeta->filaname);
	rbfm->createFile(columnMeta->filaname);
	FileHandle fileHandle;
	RID rid;
	if(rbfm->openFile(columnMeta->filaname,fileHandle)){
		rbfm->insertRecord(fileHandle,tablesAttrs,data,rid);
	}else{
		return -1;
	}
	this->table_list.push_back(columnMeta);


	this->createTable(CATELOG_FILE_NAME, tablesAttrs);

	return 0;
}

RC RelationManager::updateCatelogMetaData() {

}

RC RelationManager::exportCatalog() {

}

RC RelationManager::importCatalog() {

}

RC RelationManager::deleteCatalog() {
	return -1;
}

RC RelationManager::createTable(const string &tableName,
		const vector<Attribute> &attrs) {

	MyTable* myTable = new MyTable(this->table_list.size()+1,tableName,tableName);
	void* data = myTable->toAPIRecordFormat();

	FileHandle fileHandle;
	if(this->rbfm->openFile(tableName,fileHandle)){

	}

	this->table_list.push_back(myTable);


	return -1;
}


RC RelationManager::deleteTable(const string &tableName) {
	return -1;
}

RC RelationManager::getAttributes(const string &tableName,
		vector<Attribute> &attrs) {
	return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data,
		RID &rid) {
	return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
	return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data,
		const RID &rid) {
	return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
		void *data) {
	return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs,
		const void *data) {
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
		const string &attributeName, void *data) {
	return -1;
}

RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute, const CompOp compOp,
		const void *value, const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator) {
	return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName,
		const string &attributeName) {
	return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName,
		const Attribute &attr) {
	return -1;
}

void* MyTable::toAPIRecordFormat() {

	char* data = new char[PAGE_SIZE];
	int offset = 0;

	unsigned short nullIndicator = 0;
	memcpy((byte*)data + offset, &nullIndicator, sizeof(byte));
	offset += sizeof(char);
	int tableId = 0;
	memcpy((byte *) data + offset, &tableId, sizeof(int));
	offset += sizeof(int);
	int length = 50;
	memcpy((byte *) data + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset, this->name.c_str(), length);
	offset += length;
	memcpy((byte *) data + offset, &length, length);
	offset += length;
	memcpy((byte *) data + offset, this->filaname.c_str(), length);
	offset += length;

	return data;
}
