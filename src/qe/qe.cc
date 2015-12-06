#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
	this->iter = input;
	this->condition = condition;
	input->getAttributes(this->attrs);
	for (unsigned i = 0; i < this->attrs.size(); i++) {
		this->attrNames.push_back(this->attrs[i].name);
	}

	for (lCondAttrIndex = 0; lCondAttrIndex < this->attrNames.size(); lCondAttrIndex++) {
		if (strcmp(this->condition.lhsAttr.c_str(), this->attrNames[lCondAttrIndex].c_str()) == 0)
			break;
	}

	if (this->condition.bRhsIsAttr) {
		for (rCondAttrIndex = 0; rCondAttrIndex < this->attrNames.size();rCondAttrIndex++) {
			if (strcmp(this->condition.rhsAttr.c_str(), this->attrNames[rCondAttrIndex].c_str()) == 0)
				break;
		}

		if (this->attrs[lCondAttrIndex].type
				!= this->attrs[rCondAttrIndex].type) {
			cerr << "Data type didnt match! Error!" << endl;
			//	attrType = NULL;
		} else {
			attrType = attrs[lCondAttrIndex].type;
		}
	} else {
		rCondAttrIndex = UINT_MAX;

		if (this->attrs[lCondAttrIndex].type != this->condition.rhsValue.type) {
			cerr << "Data type didnt match! Error!" << endl;
			//	attrType = NULL;
		} else {
			attrType = attrs[lCondAttrIndex].type;
		}
	}

}

Filter::~Filter() {

}

RC Filter::getNextTuple(void *data) {
	while (true) {
		byte* tmp = new byte[PAGE_SIZE];
		//cout<<"==========================="<<endl;
		if (this->iter->getNextTuple(tmp) != QE_EOF) {
			//for debug
			if (compare(tmp)) {
				unsigned length = this->getDataLength(tmp, this->attrs);

				memcpy(data, tmp, length);
				delete[] tmp;
				return 0;
			}
		} else {
			delete[] tmp;
			return QE_EOF;
		}
	}
}

bool Filter::compare(void *data) {
	//TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
	byte* nullMem = new byte[PAGE_SIZE];
	memset(nullMem, 0, PAGE_SIZE);

	byte* leftValue = new byte[PAGE_SIZE];
	memset(leftValue, 0, PAGE_SIZE);
	this->getValueAt(data, lCondAttrIndex, leftValue);

	if (memcmp(leftValue, nullMem, PAGE_SIZE) == 0) {
		delete[] nullMem;
		delete[] leftValue;
		return false;
	}

	bool res;
	if (this->condition.bRhsIsAttr) {
		byte* rightValue = new byte[PAGE_SIZE];
		this->getValueAt(data, rCondAttrIndex, rightValue);
		if (memcmp(rightValue, nullMem, PAGE_SIZE) == 0) {
			res = false;
		} else {
			res = this->dataComparator(attrType, leftValue, rightValue,
					this->condition.op);
		}
		delete[] rightValue;
	} else {
		res = this->dataComparator(attrType, leftValue,
				this->condition.rhsValue.data, this->condition.op);
	}

	delete[] nullMem;
	delete[] leftValue;
	return res;
}

void Filter::getValueAt(void *data, unsigned index, void* value) const {

	unsigned sizeOfNullIndicator = ceil(this->attrs.size() / 8.0);

	byte nullIndicatorValue = 1 << (7 - (index % 8));
	byte nullIndicatorFromData;
	memcpy(&nullIndicatorFromData, (byte*) data + index / 8, sizeof(byte));

	if ((nullIndicatorValue & nullIndicatorFromData) != 0) {
		return;
	}

	unsigned offset = sizeOfNullIndicator;
	unsigned nullIndicatorOffset = 0;
	unsigned short standardNullIndicator = 0x80;
	byte nullIndicator = 0;

	for (unsigned i = 0; i < index; i++) {
		if (i % 8 == 0) {
			memcpy(&nullIndicator, (byte*) data + nullIndicatorOffset,
					sizeof(byte));
			nullIndicatorOffset++;
			standardNullIndicator = 0x80;
		}

		if ((nullIndicator & standardNullIndicator) == 0) {
			if (this->attrs[i].type == TypeInt
					|| this->attrs[i].type == TypeReal)
				offset += sizeof(unsigned);
			else {
				unsigned varLength = *(unsigned*) ((byte*) data + offset);
				offset += varLength;
				offset += sizeof(unsigned);
			}
		}

		standardNullIndicator = standardNullIndicator >> 1;
	}

	if (this->attrs[index].type == TypeInt
			|| this->attrs[index].type == TypeReal)
		memcpy(value, (byte*) data + offset, sizeof(unsigned));
	else {
		unsigned varLength = *(unsigned*) ((byte*) data + offset);
		memcpy(value, (byte*) data + offset, sizeof(unsigned) + varLength);
	}
}

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const {
	for (unsigned i = 0; i < this->attrs.size(); i++) {
		attrs.push_back(this->attrs[i]);
	}
}

Project::Project(Iterator *input,                    // Iterator of input R
		const vector<string> &attrNames) {

	this->iter = input;
	input->getAttributes(this->allAttrs);

	for (unsigned i = 0; i < attrNames.size(); i++) {
		this->projectedAttrNames.push_back(attrNames[i]);
	}

	for (unsigned i = 0; i < attrNames.size(); i++) {
		for (unsigned j = 0; j < this->allAttrs.size(); j++) {
			if (strcmp(this->allAttrs[j].name.c_str(), 	this->projectedAttrNames[i].c_str()) == 0) {
				this->projectedAttrPositions.push_back(j);
				break;
			}
		}
	}

	if (this->projectedAttrNames.size()
			!= this->projectedAttrPositions.size()) {
		cerr << "System error, some attributes were not found in the attrList"
				<< endl;
	}
}   // vector containing attribute names

Project::~Project() {

}

RC Project::getNextTuple(void *data) {
	byte* tmp = new byte[PAGE_SIZE];
	if (this->iter->getNextTuple(tmp) != QE_EOF) {

		unsigned i, j;
		j = 0;
		unsigned dataOffset = ceil(this->allAttrs.size() / 8.0);
		unsigned resOffset = ceil(this->projectedAttrPositions.size() / 8.0);

		unsigned dataNullIndicatorOffset = 0;
		unsigned resNullIndicatorOffset = 0;

		unsigned short standardNullIndicator = 0x80;

		unsigned short dataNullIndicator = 0;
		unsigned short resNullIndicator = 0;

		for (i = 0; i < projectedAttrPositions.size(); i++) {
			if (i != 0 && i % 8 == 0) {
				memcpy((byte*) data + resNullIndicatorOffset, &resNullIndicator,
						sizeof(byte));
				resNullIndicatorOffset++;
				resNullIndicator = 0;
			}

			for (; j < projectedAttrPositions[i]; j++) {
				if (j % 8 == 0) {
					memcpy(&dataNullIndicator,
							(byte*) tmp + dataNullIndicatorOffset,
							sizeof(byte));
					dataNullIndicatorOffset++;
					standardNullIndicator = 0x80;
				}

				if ((dataNullIndicator & standardNullIndicator) == 0) {
					if (this->allAttrs[i].type == TypeInt
							|| this->allAttrs[i].type == TypeReal)
						dataOffset += sizeof(unsigned);
					else {
						unsigned varLength = *(unsigned*) ((byte*) data
								+ dataOffset);
						dataOffset += varLength;
						dataOffset += sizeof(unsigned);
					}
				}
				standardNullIndicator = standardNullIndicator >> 1;
			}

			if (standardNullIndicator == 0) {
				standardNullIndicator = 0x80;
				memcpy(&dataNullIndicator,
						(byte*) tmp + dataNullIndicatorOffset, sizeof(byte));
			}

			if ((dataNullIndicator & standardNullIndicator) == 0) {
				unsigned copyLength = 0;
				if (this->allAttrs[i].type == TypeInt
						|| this->allAttrs[i].type == TypeReal)
					copyLength += sizeof(unsigned);
				else {
					unsigned varLength =
							*(unsigned*) ((byte*) data + dataOffset);
					copyLength += varLength;
					copyLength += sizeof(unsigned);
				}
				memcpy((byte*) data + resOffset, (byte*) tmp + dataOffset,
						copyLength);
				dataOffset += copyLength;
				resOffset += copyLength;
			} else {
				resNullIndicator |= standardNullIndicator;
			}
			j++;
		}
		memcpy((byte*) data + resNullIndicatorOffset, &resNullIndicator,
				sizeof(byte));
		delete[] tmp;
		return 0;
	} else {
		delete[] tmp;
		return QE_EOF;
	}
}

// For attribute in vector<Attribute>, name it as rel.attr
//void Project::getAttributes(vector<Attribute> &attrs) const {
//
//}

BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
		TableScan *rightIn,           // TableScan Iterator of input S
		const Condition &condition,   // Join condition
		const unsigned numPages // # of pages that can be loaded into memory,
								//   i.e., memory block size (decided by the optimizer)
		) {

	this->outer = leftIn;
	this->inner = new TableScan(rightIn->rm, rightIn->tableName);
	this->condition = condition;
	bufferSize = numPages * PAGE_SIZE;
	blockData = new byte[bufferSize];

	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);

	for (lCondAttrIndex = 0; lCondAttrIndex < this->leftAttrs.size();
			lCondAttrIndex++) {
		if (strcmp(this->condition.lhsAttr.c_str(),
				this->leftAttrs[lCondAttrIndex].name.c_str()) == 0)
			break;
	}

	if (this->condition.bRhsIsAttr) {
		for (rCondAttrIndex = 0; rCondAttrIndex < this->rightAttrs.size(); rCondAttrIndex++) {
			if (strcmp(this->condition.rhsAttr.c_str(), this->rightAttrs[rCondAttrIndex].name.c_str()) == 0)
				break;
		}

		if (this->leftAttrs[lCondAttrIndex].type != this->rightAttrs[rCondAttrIndex].type) {
			cerr << "Data type didnt match! Error!" << endl;
			//	attrType = NULL;
		} else {
			attrType = leftAttrs[lCondAttrIndex].type;
		}
	} else {
		rCondAttrIndex = UINT_MAX;
		if (this->leftAttrs[lCondAttrIndex].type
				!= this->condition.rhsValue.type) {
			cerr << "Data type didnt match! Error!" << endl;
			//	attrType = NULL;
		} else {
			attrType = leftAttrs[lCondAttrIndex].type;
		}
	}

	this->maxRecrodLength = ceil(this->leftAttrs.size()/8.0);
	for(unsigned i=0;i<this->leftAttrs.size();i++){
		if (this->leftAttrs[i].type == TypeInt || this->leftAttrs[i].type == TypeReal)
			this->maxRecrodLength += sizeof(unsigned);
		else {
			this->maxRecrodLength += this->leftAttrs[i].length;
			this->maxRecrodLength += sizeof(unsigned);
		}
	}

	currentBlockMapperIndex = 0;
	currentProperVectorIndex = 0;
	loadBlock();
}

BNLJoin::~BNLJoin() {
	delete[] blockData;
	blockMapper.clear();
}

RC BNLJoin::getNextTuple(void *data) {

	if(this->blockMapper.size() ==0)
		return -1;
	else{
		map<KeyType, vector<Pair> >::iterator it = blockMapper.begin();

		KeyType currentKey = it->first;
		Pair currentPair = (it->second)[currentProperVectorIndex];

		byte* innerDataTmp = new byte[PAGE_SIZE];

		while(inner->getNextTuple(innerDataTmp)==0){
			byte* rightAttrValue = new byte[PAGE_SIZE];
			if(this->rCondAttrIndex != UINT_MAX){
				JoinUtil::getValueAt(innerDataTmp,this->rightAttrs,this->rCondAttrIndex,rightAttrValue);
			}else{
				if(this->condition.rhsValue.type == TypeInt || this->condition.rhsValue.type==TypeReal)
					memcpy(rightAttrValue,this->condition.rhsValue.data, sizeof(unsigned));
				else{
					unsigned length = *(unsigned*)(this->condition.rhsValue.data);
					memcpy(rightAttrValue,this->condition.rhsValue.data, sizeof(unsigned)+length);
				}
			}
			if(this->dataComparator(this->attrType,currentKey.data,rightAttrValue,this->condition.op)){
				byte* outerDataTmp = new byte[PAGE_SIZE];
				memcpy(outerDataTmp,this->blockData+currentPair.pos,currentPair.length);
				JoinUtil::combineData(outerDataTmp, this->leftAttrs, innerDataTmp, this->rightAttrs, (byte*)data);
				delete[] outerDataTmp;
				delete[] rightAttrValue;
				delete[] innerDataTmp;
				return 0;
			}
			delete[] rightAttrValue;
		}
		delete[] innerDataTmp;

		TableScan* tmp= new TableScan(this->inner->rm, this->inner->tableName);
		delete this->inner;
		this->inner = tmp;

		if(currentProperVectorIndex<it->second.size()-1){
			currentProperVectorIndex++;
			return getNextTuple(data);
		}else{
			this->blockMapper.erase(it->first);
			if(this->blockMapper.size() ==0){
				this->loadBlock();
				currentProperVectorIndex=0;
			}
			return getNextTuple(data);
		}
	}

}

RC BNLJoin::loadBlock() {
	delete[] blockData;
	blockMapper.clear();
	blockData = new byte[bufferSize];

	byte* data = new byte[PAGE_SIZE];
	unsigned currentPos = 0;

	while ((bufferSize-maxRecrodLength)>currentPos && outer->getNextTuple(data)==0) {
		unsigned dataLength = RecordBasedFileManager::calculateRecordLength(this->leftAttrs,data,API);
		memcpy(blockData+currentPos, data, dataLength);

		byte* value = new byte[PAGE_SIZE];
		JoinUtil::getValueAt(data, this->leftAttrs, this->lCondAttrIndex, value);
		KeyType* key = new KeyType(this->attrType, value);
		vector<Pair> container = blockMapper[*key];
		Pair pair(currentPos,dataLength);
		currentPos += dataLength;

		if (!container.empty()) {
			container.push_back(pair);
		} else {
			vector<Pair> containerTmp;
			containerTmp.push_back(pair);
			blockMapper[*key] = containerTmp;
		}

		delete key;
		delete[] value;
	}
	currentBlockMapperIndex = 0;
	currentProperVectorIndex = 0;
	return 0;
}

// For attribute in vector<Attribute>, name it as rel.attr
void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
	for(unsigned i=0; i< this->leftAttrs.size(); i++)
		attrs.push_back(this->leftAttrs[i]);

	for(unsigned i=0; i< this->rightAttrs.size(); i++)
		attrs.push_back(this->rightAttrs[i]);
}



INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
       IndexScan *rightIn,          // IndexScan Iterator of input S
       const Condition &condition   // Join condition
){
	this->outer = leftIn;
	this->condition = condition;
	this->inner = new IndexScan(rightIn->rm,rightIn->tableName,rightIn->attrName);

	leftIn->getAttributes(this->leftAttrs);
	rightIn->getAttributes(this->rightAttrs);

	this->lCondAttrIndex = JoinUtil::findAttributePos(this->leftAttrs,condition.lhsAttr);

	if (this->condition.bRhsIsAttr) {
		this->rCondAttrIndex = JoinUtil::findAttributePos(this->rightAttrs,condition.rhsAttr);
		if (this->leftAttrs[lCondAttrIndex].type != this->rightAttrs[rCondAttrIndex].type) {
			cerr << "Data type didnt match! Error!" << endl;
			//	attrType = NULL;
		} else {
			attrType = leftAttrs[lCondAttrIndex].type;
		}
	} else {
		rCondAttrIndex = UINT_MAX;
		if (this->leftAttrs[lCondAttrIndex].type!= this->condition.rhsValue.type) {
			cerr << "Data type didnt match! Error!" << endl;
			//	attrType = NULL;
		} else {
			attrType = leftAttrs[lCondAttrIndex].type;
		}
	}
	outerValue = new byte[PAGE_SIZE];
	outerData = new byte[PAGE_SIZE];

}


INLJoin::~INLJoin(){
	delete[] outerValue;
	delete[] outerData;
	delete inner;
}

RC INLJoin::getNextTuple(void *data){

	byte* innerDataTmp = new byte[PAGE_SIZE];
	while(inner->getNextTuple(innerDataTmp)!=QE_EOF){
		byte* innerValueTmp = new byte[PAGE_SIZE];
		memset(innerValueTmp,0,PAGE_SIZE);
		if(this->condition.bRhsIsAttr){
			JoinUtil::getValueAt(innerDataTmp,this->rightAttrs,this->rCondAttrIndex,innerValueTmp);
		}else{
			if(this->condition.rhsValue.type == TypeInt || this->condition.rhsValue.type==TypeReal)
				memcpy(innerValueTmp,this->condition.rhsValue.data, sizeof(unsigned));
			else{
				unsigned length = *(unsigned*)(this->condition.rhsValue.data);
				memcpy(innerValueTmp,this->condition.rhsValue.data, sizeof(unsigned)+length);
			}
		}

		if(this->dataComparator(this->attrType,outerValue,innerValueTmp,this->condition.op)){
			JoinUtil::combineData(outerData, this->leftAttrs, innerDataTmp, this->rightAttrs, (byte*)data);
			delete[] innerValueTmp;
			delete[] innerDataTmp;
			return 0;
		}
		delete[] innerValueTmp;
	}

	delete[] innerDataTmp;

	IndexScan* innerTmp = new IndexScan(inner->rm,inner->tableName,inner->attrName);
	delete this->inner;
	this->inner = innerTmp;

	memset(outerData, 0, PAGE_SIZE);
	memset(outerValue, 0, PAGE_SIZE);
	if(this->outer->getNextTuple(outerData)==0){
		JoinUtil::getValueAt(outerData,this->leftAttrs,this->lCondAttrIndex,outerValue);
		return getNextTuple(data);
	}else{
		return QE_EOF;
	}
}


// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	for(unsigned i=0; i< this->leftAttrs.size(); i++)
		attrs.push_back(this->leftAttrs[i]);

	for(unsigned i=0; i< this->rightAttrs.size(); i++)
		attrs.push_back(this->rightAttrs[i]);
}


Aggregate::Aggregate(Iterator *input,          // Iterator of input R
          Attribute aggAttr,        // The attribute over which we are computing an aggregate
          AggregateOp op            // Aggregate operation
){
	this->it = input;
	this->aggAttr = aggAttr;
	this->op = op;

	this->it->getAttributes(this->allAttrs);
	this->attrPos = JoinUtil::findAttributePos(this->allAttrs,aggAttr.name);
	this->found = false;

	groupAttrPos = UINT_MAX;
}

// Optional for everyone: 5 extra-credit points
// Group-based hash aggregation
Aggregate::Aggregate(Iterator *input,             // Iterator of input R
          Attribute aggAttr,           // The attribute over which we are computing an aggregate
          Attribute groupAttr,         // The attribute over which we are grouping the tuples
          AggregateOp op              // Aggregate operation
){
	this->it = input;
	this->aggAttr = aggAttr;
	this->groupAttr = groupAttr;
	this->op = op;

	this->it->getAttributes(this->allAttrs);
	this->attrPos = JoinUtil::findAttributePos(this->allAttrs,aggAttr.name);
	this->groupAttrPos = JoinUtil::findAttributePos(this->allAttrs,groupAttr.name);

	mapperIndex = 0;
	getAggregateResWithGroup();
}

Aggregate::~Aggregate(){

}

RC Aggregate::getNextTuple(void *data){
	if(this->groupAttrPos == UINT_MAX){
		if(found){
			return QE_EOF;
		}else{
			return getAggregateResWithoutGroup(data);
		}
	}else{


		if(mapperIndex == this->groupMapper.size()){
			delete[] resData;
			return QE_EOF;
		}

		memcpy(data, this->resData + mapperIndex * 9, 9);
		mapperIndex++;
	}
}

RC Aggregate::getAggregateResWithGroup(){
	if(attrPos == this->allAttrs.size() || groupAttrPos ==  this->allAttrs.size()){
			cerr<<"didnt find the attriubte;" <<endl;
			return -2; //indicate didnt find the attriubte;
	}

	byte* dataTmp = new byte[PAGE_SIZE];

	byte* valueTmp = new byte[sizeof(unsigned)];
	memset(valueTmp,0,sizeof(unsigned));
	byte* groupValueTmp = new byte[sizeof(unsigned)];
	memset(groupValueTmp,0,sizeof(unsigned));
	byte* standardNull = new byte[sizeof(unsigned)];
	memset(standardNull,0,sizeof(unsigned));

	float value = 0;

	float groupValue = 0;

	do {
		if(this->it->getNextTuple(dataTmp)==QE_EOF){
			delete[] dataTmp;
			delete[] valueTmp;
			delete[] groupValueTmp;
			delete[] standardNull;
			return QE_EOF;
		}
		JoinUtil::getValueAt(dataTmp, allAttrs, attrPos, valueTmp);
		JoinUtil::getValueAt(dataTmp, allAttrs, groupAttrPos, groupValueTmp);
	}while(memcmp(valueTmp,standardNull,sizeof(unsigned))==0);

	if(this->aggAttr.type == TypeInt){
		value = (float)*(int*)valueTmp;
	}else if(this->aggAttr.type == TypeReal){
		value = *(float*)valueTmp;
	}

	if(this->groupAttr.type == TypeReal)
		groupValue = *(float*)groupValueTmp;
	else if(this->groupAttr.type == TypeInt){
		groupValue = (float)(*(int*)groupValueTmp);
	}

	processGroupMapContent(groupValue, value);

	while(this->it->getNextTuple(dataTmp)!=QE_EOF){
		JoinUtil::getValueAt(dataTmp, allAttrs, attrPos, valueTmp);
		JoinUtil::getValueAt(dataTmp, allAttrs, groupAttrPos, groupValueTmp);
		if(memcmp(valueTmp,standardNull,sizeof(unsigned))==0 || memcmp(groupValueTmp,standardNull,sizeof(unsigned))==0)
			continue;

		if(this->aggAttr.type == TypeInt){
			value = (float)*(int*)valueTmp;
		}else if(this->aggAttr.type == TypeReal){
			value = *(float*)valueTmp;
		}

		if(this->groupAttr.type == TypeReal)
			groupValue = *(float*)groupValueTmp;
		else if(this->groupAttr.type == TypeInt){
			groupValue = (float)(*(int*)groupValueTmp);
		}

		processGroupMapContent(groupValue, value);
		memset(valueTmp,0,sizeof(float));
		memset(groupValueTmp,0,sizeof(float));
	}

	delete[] dataTmp;
	delete[] valueTmp;
	delete[] groupValueTmp;
	delete[] standardNull;

	// 9 = nullIndicator + sizeof(float) + sizeof(float)
	resData = new byte[this->groupMapper.size() * (9)];
	unsigned offset = 0;
	byte nullIndicator = 0;
	for(map<float,float>::const_iterator it = groupMapper.begin(); it != groupMapper.end(); ++it )
	{
	   float key = it->first;
	   if(this->op == MAX || this->op == MIN || this->op == COUNT || this->op == SUM){
		   memcpy(resData+offset,&nullIndicator, sizeof(byte));
		   offset+=sizeof(byte);
		   if(this->groupAttr.type == TypeInt){
		      int valueInt = (int)key;
		      memcpy(resData+offset,&valueInt, sizeof(int));
		   }else{
		      memcpy(resData+offset,&key, sizeof(float));
		   }
		   offset+=4;
		   memcpy(resData+offset,&groupMapper[key], sizeof(float));
		   offset+=sizeof(float);
	   }else if(this->op == AVG){
		   memcpy(resData+offset,&nullIndicator, sizeof(byte));
		   offset+=sizeof(byte);
		   if(this->groupAttr.type == TypeInt){
			   int valueInt = (int)key;
			   memcpy(resData+offset,&valueInt, sizeof(int));
		   }else{
			   memcpy(resData+offset,&key, sizeof(float));
		   }
		   offset+=4;
		   float valueAvg = groupMapper[key] / gourpMapperFreq[key];
		   memcpy(resData+offset,&valueAvg, sizeof(float));
		   offset+=sizeof(float);
	   }
	}

	return 0;
}

RC Aggregate::getAggregateResWithoutGroup(void* data){

	if(attrPos == this->allAttrs.size()){
		cerr<<"didnt find the attriubte;" <<endl;
		return -2; //indicate didnt find the attriubte;
	}

	byte* dataTmp = new byte[PAGE_SIZE];
	byte* valueTmp = new byte[sizeof(unsigned)];
	memset(valueTmp,0,sizeof(unsigned));

	byte* standardNull = new byte[sizeof(unsigned)];
	memset(standardNull,0,sizeof(unsigned));

	float value = 0;
	float count = 0.0;
	float sum = 0;

	do {
		if(this->it->getNextTuple(dataTmp)==QE_EOF){
			delete[] dataTmp;
			delete[] valueTmp;
			delete[] standardNull;
			return QE_EOF;
		}
		JoinUtil::getValueAt(dataTmp, allAttrs, attrPos, valueTmp);
	}while(memcmp(valueTmp,standardNull,sizeof(unsigned))==0);

	count = count+1;

	if(this->op == AVG || this->op == SUM){
		if(this->aggAttr.type == TypeInt){
			sum += *(int*)valueTmp;
		}else if(this->aggAttr.type == TypeReal){
			sum += *(float*)valueTmp;
		}
	}

	if(this->aggAttr.type == TypeReal)
		value = *(float*)valueTmp;
	else if(this->aggAttr.type == TypeInt){
		value = (float)(*(int*)valueTmp);
	}

	while(this->it->getNextTuple(dataTmp)!=QE_EOF){
		JoinUtil::getValueAt(dataTmp, allAttrs, attrPos, valueTmp);
		if(memcmp(valueTmp,standardNull,sizeof(unsigned))==0)
			continue;

		count = count+1;
		if(this->op == MAX||this->op == MIN){
			if(this->aggAttr.type == TypeReal){
				float tmp = *(float*)valueTmp;
				if(this->op == MAX){
					if(value<tmp)
						value = tmp;
				}else{
					if(value>tmp)
						value = tmp;
				}
			}else if(this->aggAttr.type == TypeInt){
				float tmp = (float)*(int*)valueTmp;
				if(this->op == MAX){
					if(value<tmp)
						value = tmp;
				}else{
					if(value>tmp)
						value = tmp;
				}
			}
		}else if(this->op == AVG || this->op == SUM){
			if(this->aggAttr.type == TypeInt){
				sum += *(int*)valueTmp;
			}else if(this->aggAttr.type == TypeReal){
				sum += *(float*)valueTmp;
			}
		}else if(this->op == COUNT){
			//do nothing
		}

		memset(valueTmp,0,sizeof(float));
	}

	if(this->op == MAX||this->op == MIN){
		memcpy((byte*)data+1, &value, sizeof(float));
	}else if(this->op == COUNT){
		memcpy((byte*)data+1, &count, sizeof(float));
	}else if(this->op == AVG){
		float avgValue = sum/count;
		memcpy((byte*)data+1, &avgValue, sizeof(float));
	}else if(this->op == SUM){
		memcpy((byte*)data+1, &sum, sizeof(float));
	}
	byte nullIndicator = 0;
	memcpy(data,&nullIndicator,sizeof(byte));
	found =true;
	delete[] dataTmp;
	delete[] valueTmp;
	delete[] standardNull;
	return 0;
}


// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const{
	Attribute attr;
	attr.type = this->groupAttr.type;
	attr.length = this->groupAttr.length;

	if(this->op == MAX){
		attr.name = "MAX("+this->groupAttr.name+")";
	}else if(this->op == MIN){
		attr.name = "MIN("+this->groupAttr.name+")";
	}else if(this->op == AVG){
		attr.name = "AVG("+this->groupAttr.name+")";
	}else if(this->op == SUM){
		attr.name = "SUM("+this->groupAttr.name+")";
	}else if(this->op == COUNT){
		attr.name = "COUNT("+this->groupAttr.name+")";
	}
	attrs.push_back(attr);
};


//// ... the rest of your implementations go here
GHJoin::GHJoin(Iterator *leftIn,               // Iterator of input R
           Iterator *rightIn,               // Iterator of input S
           const Condition &condition,      // Join condition (CompOp is always EQ)
           const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
     ){

}

GHJoin::~GHJoin(){

}

RC GHJoin::getNextTuple(void *data){

	return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void GHJoin::getAttributes(vector<Attribute> &attrs) const{

}




