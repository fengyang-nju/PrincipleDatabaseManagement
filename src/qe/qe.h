#ifndef _qe_h_
#define _qe_h_

#include <vector>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum {
	MIN = 0, MAX, COUNT, SUM, AVG
} AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

class Pair {
public:
	unsigned pos;
	unsigned length;
	Pair(unsigned pos, unsigned length) {
		this->pos = pos;
		this->length = length;
	}

	Pair& operator=(const Pair &p) {
		this->pos = p.pos;
		this->length = p.length;
		return *this;
	}

	bool operator==(const Pair &p) const {
		return this->pos == p.pos && this->length == p.length;
	}
};

class KeyType {

public:
	AttrType type;
	byte* data;

	KeyType(AttrType type, byte* data) {
		this->type = type;
		if (type == TypeInt) {
			this->data = new byte[sizeof(unsigned)];
			memcpy(this->data, data, sizeof(unsigned));
		} else if (type == TypeReal) {
			this->data = new byte[sizeof(unsigned)];
			memcpy(this->data, data, sizeof(float));
		} else {
			unsigned rightLength = *(unsigned*) (data);
			this->data = new byte[rightLength + sizeof(unsigned)];
			memcpy(this->data, data, rightLength + sizeof(unsigned));
		}
	}

	KeyType(const KeyType &p) {
		this->type = p.type;
		if (type == TypeInt) {
			this->data = new byte[sizeof(unsigned)];
			memcpy(this->data, p.data, sizeof(unsigned));
		} else if (type == TypeReal) {
			this->data = new byte[sizeof(unsigned)];
			memcpy(this->data, p.data, sizeof(float));
		} else {
			unsigned rightLength = *(unsigned*) (p.data);
			this->data = new byte[rightLength + sizeof(unsigned)];
			memcpy(this->data, p.data, rightLength + sizeof(unsigned));
		}
	}

	~KeyType() {
		delete[] data;
	}

	KeyType& operator=(const KeyType &p) {
		this->type = p.type;

		if (type == TypeInt) {
			memcpy(this->data, p.data, sizeof(unsigned));
		} else if (type == TypeReal) {
			memcpy(this->data, p.data, sizeof(float));
		} else {
			unsigned rightLength = *(unsigned*) (p.data);
			this->data = new byte[rightLength + sizeof(unsigned)];
			memcpy(this->data, p.data, rightLength + sizeof(unsigned));
		}
		return *this;
	}

	bool operator==(const KeyType &p) const {
		if (type == TypeInt) {
			int left = *(int*) (data);
			int right = *(int*) (p.data);
			return left == right;
		} else if (type == TypeReal) {
			float left = *(float*) (data);
			float right = *(float*) (p.data);
			return left == right;
		} else {
			unsigned leftLength = *(unsigned*) data;
			string leftValue((byte*) data + sizeof(unsigned), leftLength);
			unsigned rightLength = *(unsigned*) (p.data);
			string rightValue((byte*) (p.data) + sizeof(unsigned), rightLength);
			return strcmp(leftValue.c_str(), rightValue.c_str()) == 0;
		}
	}

	bool operator>(const KeyType &p) const {
		if (type == TypeInt) {
			int left = *(int*) (data);
			int right = *(int*) (p.data);
			return left > right;
		} else if (type == TypeReal) {
			float left = *(float*) (data);
			float right = *(float*) (p.data);
			return left > right;
		} else {
			unsigned leftLength = *(unsigned*) data;
			string leftValue((byte*) data + sizeof(unsigned), leftLength);
			unsigned rightLength = *(unsigned*) (p.data);
			string rightValue((byte*) (p.data) + sizeof(unsigned), rightLength);
			return strcmp(leftValue.c_str(), rightValue.c_str()) > 0;
		}
	}

	bool operator>=(const KeyType &p) const {
		if (type == TypeInt) {
			int left = *(int*) (data);
			int right = *(int*) (p.data);
			return left >= right;
		} else if (type == TypeReal) {
			float left = *(float*) (data);
			float right = *(float*) (p.data);
			return left >= right;
		} else {
			unsigned leftLength = *(unsigned*) data;
			string leftValue((byte*) data + sizeof(unsigned), leftLength);

			unsigned rightLength = *(unsigned*) (p.data);
			string rightValue((byte*) (p.data) + sizeof(unsigned), rightLength);
			return strcmp(leftValue.c_str(), rightValue.c_str()) >= 0;
		}
	}

	bool operator<(const KeyType &p) const {
		if (type == TypeInt) {
			int left = *(int*) (data);
			int right = *(int*) (p.data);
			return left < right;
		} else if (type == TypeReal) {
			float left = *(float*) (data);
			float right = *(float*) (p.data);
			return left < right;
		} else {
			unsigned leftLength = *(unsigned*) data;
			string leftValue((byte*) data + sizeof(unsigned), leftLength);

			unsigned rightLength = *(unsigned*) (p.data);
			string rightValue((byte*) (p.data) + sizeof(unsigned), rightLength);
			return strcmp(leftValue.c_str(), rightValue.c_str()) < 0;
		}
	}

	bool operator<=(const KeyType &p) const {
		if (type == TypeInt) {
			int left = *(int*) (data);
			int right = *(int*) (p.data);
			return left <= right;
		} else if (type == TypeReal) {
			float left = *(float*) (data);
			float right = *(float*) (p.data);
			return left <= right;
		} else {
			unsigned leftLength = *(unsigned*) data;
			string leftValue((byte*) data + sizeof(unsigned), leftLength);
			unsigned rightLength = *(unsigned*) (p.data);
			string rightValue((byte*) (p.data) + sizeof(unsigned), rightLength);
			return strcmp(leftValue.c_str(), rightValue.c_str()) <= 0;
		}
	}
};

struct Value {
	AttrType type;          // type of value
	void *data;         // value
};

struct Condition {
	string lhsAttr;        // left-hand side attribute
	CompOp op;             // comparison operator
	bool bRhsIsAttr; // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
	string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
	Value rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};

class JoinUtil{
public:
	static void getValueAt(void *data, vector<Attribute>& attrs, unsigned index,
			void* value) {

		unsigned sizeOfNullIndicator = ceil(attrs.size() / 8.0);

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
				if (attrs[i].type == TypeInt || attrs[i].type == TypeReal)
					offset += sizeof(unsigned);
				else {
					unsigned varLength = *(unsigned*) ((byte*) data + offset);
					offset += varLength;
					offset += sizeof(unsigned);
				}
			}

			standardNullIndicator = standardNullIndicator >> 1;
		}

		if (attrs[index].type == TypeInt || attrs[index].type == TypeReal)
			memcpy(value, (byte*) data + offset, sizeof(unsigned));
		else {
			unsigned varLength = *(unsigned*) ((byte*) data + offset);
			memcpy(value, (byte*) data + offset, sizeof(unsigned) + varLength);
		}
	};

	static unsigned findAttributePos(vector<Attribute> &attrs, const string name){
		for(unsigned i = 0;i<attrs.size();i++){
			//cout<<attrs[i].name<<endl;
			if(strcmp(attrs[i].name.c_str(),name.c_str())==0)
				return i;
		}
		return UINT_MAX;
	};

	static void combineData(byte* leftData, vector<Attribute> &leftAttrs, byte* rightData, vector<Attribute> &rightAttrs, byte* combinedRes){
		unsigned totalSize = leftAttrs.size() + rightAttrs.size();
		unsigned resOffset = ceil(totalSize/8.0);

		unsigned short standardNullIndicator = 0x80;
		unsigned short nullIndicator = 0;
		unsigned short nullIndicatorOffset = 0;

		unsigned leftLength = ceil(leftAttrs.size()/8.0);
		unsigned rightLength = ceil(rightAttrs.size()/8.0);

		unsigned short resNullIndicator = 0;
		unsigned resNullIndicatorOffset = 0;


		memcpy(&nullIndicator,leftData,sizeof(byte));
		nullIndicatorOffset++;
		for(unsigned i=0;i<leftAttrs.size();i++){
			if(i!=0&&i%8==0){
				memcpy(combinedRes+resNullIndicatorOffset, &resNullIndicator, sizeof(byte));
				resNullIndicatorOffset++;

				memcpy(&nullIndicator,leftData + nullIndicatorOffset,sizeof(byte));
				standardNullIndicator = 0x80;
				nullIndicatorOffset++;
			}
			if((nullIndicator&standardNullIndicator)==0){
				if (leftAttrs[i].type == TypeInt || leftAttrs[i].type == TypeReal)
					leftLength += sizeof(unsigned);
				else {
					unsigned varLength =
					*(unsigned*) ((byte*) leftData + leftLength);
					leftLength += varLength;
					leftLength += sizeof(unsigned);
				}
			}else{
				resNullIndicator |= standardNullIndicator;
			}
			standardNullIndicator = standardNullIndicator>>1;
		}

		if(standardNullIndicator == 0){
			memcpy(combinedRes+resNullIndicatorOffset, &resNullIndicator, sizeof(byte));
			resNullIndicatorOffset++;
			resNullIndicator = 0;
			standardNullIndicator = 0x80;
		}

		unsigned short rightStandardNullIndicator = 0x80;
		nullIndicatorOffset = 0;
		nullIndicator = 0;

		for(unsigned i=0;i<rightAttrs.size();i++){
			if(i%8==0){
				memcpy(&nullIndicator,rightData + nullIndicatorOffset,sizeof(byte));
				rightStandardNullIndicator = 0x80;
				nullIndicatorOffset++;
			}
			if((nullIndicator&rightStandardNullIndicator)==0){
				if (rightAttrs[i].type == TypeInt || rightAttrs[i].type == TypeReal)
					rightLength += sizeof(unsigned);
				else {
					unsigned varLength = *(unsigned*) ((byte*) rightData + rightLength);
					rightLength += varLength;
					rightLength += sizeof(unsigned);
				}
			}else{
				resNullIndicator |= standardNullIndicator;
			}

			standardNullIndicator = standardNullIndicator >>1;
			rightStandardNullIndicator = rightStandardNullIndicator >>1;
			if(standardNullIndicator == 0){
				memcpy(combinedRes+resNullIndicatorOffset, &resNullIndicator, sizeof(byte));
				resNullIndicatorOffset++;
				resNullIndicator = 0;
				standardNullIndicator = 0x80;
			}
		}
		memcpy(combinedRes+resNullIndicatorOffset, &resNullIndicator, sizeof(byte));

		unsigned leftNullSize = ceil(leftAttrs.size()/8.0);
		unsigned leftCopyLength = leftLength-leftNullSize;
		memcpy(combinedRes+resOffset,leftData+leftNullSize,leftCopyLength);

		unsigned rightNullSize = ceil(rightAttrs.size()/8.0);
		unsigned rightCopyLength = rightLength - rightNullSize;
		memcpy(combinedRes+resOffset+leftCopyLength,rightData+rightNullSize,rightCopyLength);
	};
};

class Iterator {
	// All the relational operators and access methods are iterators.
public:
	virtual RC getNextTuple(void *data) = 0;
	virtual void getAttributes(vector<Attribute> &attrs) const = 0;
	virtual ~Iterator() {
	}
	;

	virtual unsigned getDataLength(void *data, vector<Attribute> &attrs) {
		unsigned offset = ceil(attrs.size() / 8.0);
		for (unsigned i = 0; i < attrs.size(); i++) {
			if (attrs[i].type == TypeInt || attrs[i].type == TypeReal)
				offset += sizeof(unsigned);
			else {
				unsigned varLength = *(unsigned*) ((byte*) data + offset);
				offset += varLength;
				offset += sizeof(unsigned);
			}
		}
		return offset;
	}

public:
	static bool dataComparator(AttrType dataType, void* leftOperator,
			const void* rightOperator, const CompOp compOp) {

		byte* nullMem = new byte[PAGE_SIZE];
		memset(nullMem, 0, PAGE_SIZE);
		if(memcmp(leftOperator,nullMem,PAGE_SIZE) ==0 ||
				memcmp(rightOperator,nullMem,PAGE_SIZE) ==0){
			delete[] nullMem;
			return false;
		}
		delete[] nullMem;


		if (rightOperator == NULL || leftOperator == NULL)
			return false;

		if (compOp == NO_OP)
			return true;
		if (dataType == TypeVarChar) {
			unsigned leftLength = *(unsigned*)(byte*)leftOperator;
			char* left = (char*) ((byte*) leftOperator + sizeof(unsigned));
			string leftString(left,leftLength);

			unsigned rightLength = *(unsigned*)(byte*)rightOperator;
			char* right = (char*) ((byte*) rightOperator + sizeof(unsigned));
			string rightString(right,rightLength);

			if (compOp == EQ_OP) {
				return strcmp(leftString.c_str(), rightString.c_str()) == 0;
			}
			if (compOp == LT_OP) {
				return strcmp(leftString.c_str(), rightString.c_str()) < 0;
			}
			if (compOp == GT_OP) {
				return strcmp(leftString.c_str(), rightString.c_str()) > 0;
			}
			if (compOp == LE_OP) {
				return strcmp(leftString.c_str(), rightString.c_str())  <= 0;
			}
			if (compOp == GE_OP) {
				return strcmp(leftString.c_str(), rightString.c_str())  >= 0;
			}
			if (compOp == NE_OP) {
				return strcmp(leftString.c_str(), rightString.c_str())  != 0;
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
};

class TableScan: public Iterator {
	// A wrapper inheriting Iterator over RM_ScanIterator
public:
	RelationManager &rm;
	RM_ScanIterator *iter;
	string tableName;
	vector<Attribute> attrs;
	vector<string> attrNames;
	RID rid;

	TableScan(RelationManager &rm, const string &tableName, const char *alias =
	NULL) :
			rm(rm) {
		//Set members
		this->tableName = tableName;

		// Get Attributes from RM
		rm.getAttributes(tableName, attrs);

		// Get Attribute Names from RM
		unsigned i;
		for (i = 0; i < attrs.size(); ++i) {
			// convert to char *
			attrNames.push_back(attrs.at(i).name);
		}

		// Call RM scan to get an iterator
		iter = new RM_ScanIterator();
		rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

		// Set alias
		if (alias)
			this->tableName = alias;
	}
	;

	// Start a new iterator given the new compOp and value
	void setIterator() {
		iter->close();
		delete iter;
		iter = new RM_ScanIterator();
		rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
	}
	;

	RC getNextTuple(void *data) {
		return iter->getNextTuple(rid, data);
	}
	;

	void getAttributes(vector<Attribute> &attrs) const {
		attrs.clear();
		attrs = this->attrs;
		unsigned i;

		// For attribute in vector<Attribute>, name it as rel.attr
		for (i = 0; i < attrs.size(); ++i) {
			string tmp = tableName;
			tmp += ".";
			tmp += attrs.at(i).name;
			attrs.at(i).name = tmp;
		}
	}
	;

	~TableScan() {
		iter->close();
	}
	;
};

class IndexScan: public Iterator {
	// A wrapper inheriting Iterator over IX_IndexScan
public:
	RelationManager &rm;
	RM_IndexScanIterator *iter;
	string tableName;
	string attrName;
	vector<Attribute> attrs;
	char key[PAGE_SIZE];
	RID rid;

	IndexScan(RelationManager &rm, const string &tableName,
			const string &attrName, const char *alias = NULL) :
			rm(rm) {
		// Set members
		this->tableName = tableName;
		this->attrName = attrName;

		// Get Attributes from RM
		rm.getAttributes(tableName, attrs);

		// Call rm indexScan to get iterator
		iter = new RM_IndexScanIterator();
		rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

		// Set alias
		if (alias)
			this->tableName = alias;
	}
	;

	// Start a new iterator given the new key range
	void setIterator(void* lowKey, void* highKey, bool lowKeyInclusive,
			bool highKeyInclusive) {
		iter->close();
		delete iter;
		iter = new RM_IndexScanIterator();
		rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
				highKeyInclusive, *iter);
	}
	;

	RC getNextTuple(void *data) {
		int rc = iter->getNextEntry(rid, key);
		if (rc == 0) {
			rc = rm.readTuple(tableName.c_str(), rid, data);
		}
		return rc;
	}
	;

	void getAttributes(vector<Attribute> &attrs) const {
		attrs.clear();
		attrs = this->attrs;
		unsigned i;

		// For attribute in vector<Attribute>, name it as rel.attr
		for (i = 0; i < attrs.size(); ++i) {
			string tmp = tableName;
			tmp += ".";
			tmp += attrs.at(i).name;
			attrs.at(i).name = tmp;
		}
	}
	;

	~IndexScan() {
		iter->close();
	}
	;
};

class Filter: public Iterator {

private:
	Iterator *iter;
	string tableName;
	vector<Attribute> attrs;
	vector<string> attrNames;
	Condition condition;
	AttrType attrType;

	unsigned lCondAttrIndex;
	unsigned rCondAttrIndex;

	// Filter operator
public:
	Filter(Iterator *input,               // Iterator of input R
			const Condition &condition     // Selection condition
			);
	~Filter();

	RC getNextTuple(void *data);

	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;

private:
	bool compare(void *data);
	void getValueAt(void *data, unsigned index, void* value) const;
};

class Project: public Iterator {
	// Projection operator

private:
	Iterator *iter;
	vector<Attribute> allAttrs;

	vector<string> projectedAttrNames;
	AttrType attrType;

	vector<unsigned> projectedAttrPositions;
public:
	Project(Iterator *input,                    // Iterator of input R
			const vector<string> &attrNames); // vector containing attribute names
	~Project();

	RC getNextTuple(void *data);
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const {
		for(unsigned i=0;i<projectedAttrPositions.size();i++){
			attrs.push_back(allAttrs[projectedAttrPositions[i]]);
		}
	};
	//void getValueAt(void *data, unsigned index, void* value, bool& nullFlag) const;
};

// Optional for the undergraduate solo teams: 5 extra-credit points
class BNLJoin: public Iterator {
	// Block nested-loop join operator

private:
	Iterator *outer;

	string tableName;

	vector<Attribute> leftAttrs;
	vector<Attribute> rightAttrs;

	Condition condition;
	AttrType attrType;

	TableScan *inner;

	unsigned lCondAttrIndex;
	unsigned rCondAttrIndex;

	unsigned bufferSize;

	byte* blockData;
	map<KeyType, vector<Pair> > blockMapper;

	unsigned maxRecrodLength;

	unsigned currentBlockMapperIndex;
	unsigned currentProperVectorIndex;

public:
	BNLJoin(Iterator *leftIn,            // Iterator of input R
			TableScan *rightIn,           // TableScan Iterator of input S
			const Condition &condition,   // Join condition
			const unsigned numPages // # of pages that can be loaded into memory,
									//   i.e., memory block size (decided by the optimizer)
			);

	~BNLJoin();

	RC getNextTuple(void *data);
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;

	RC loadBlock();
};

class INLJoin: public Iterator {
	// Index nested-loop join operator
public:
	IndexScan *inner; //indexed ones
	Iterator *outer;
	Condition condition;

	unsigned lCondAttrIndex;
	unsigned rCondAttrIndex;

	vector<Attribute> leftAttrs;
	vector<Attribute> rightAttrs;

	AttrType attrType;

	byte* outerData;
	byte* outerValue;
public:
	INLJoin(Iterator *leftIn,           // Iterator of input R
			IndexScan *rightIn,          // IndexScan Iterator of input S
			const Condition &condition   // Join condition
			);
	~INLJoin();

	RC getNextTuple(void *data);
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;
};

// Optional for everyone. 10 extra-credit points
class GHJoin: public Iterator {
	// Grace hash join operator
public:
	GHJoin(Iterator *leftIn,               // Iterator of input R
			Iterator *rightIn,               // Iterator of input S
			const Condition &condition,  // Join condition (CompOp is always EQ)
			const unsigned numPartitions // # of partitions for each relation (decided by the optimizer)
			);
	~GHJoin();

	RC getNextTuple(void *data);
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;
};

class Aggregate: public Iterator {


public:
	Iterator *it;
	Attribute aggAttr;
	AggregateOp op;

	Attribute groupAttr;


	vector<Attribute> allAttrs;
	bool found;
	unsigned attrPos;
	unsigned groupAttrPos;

	map<float,float> groupMapper;
	map<float,float> gourpMapperFreq;
	byte* resData;
	unsigned mapperIndex;

	// Aggregation operator
public:
	// Mandatory for graduate teams/solos. Optional for undergrad solo teams: 5 extra-credit points
	// Basic aggregation
	Aggregate(Iterator *input,          // Iterator of input R
			Attribute aggAttr, // The attribute over which we are computing an aggregate
			AggregateOp op            // Aggregate operation
			);

	// Optional for everyone: 5 extra-credit points
	// Group-based hash aggregation
	Aggregate(Iterator *input,             // Iterator of input R
			Attribute aggAttr, // The attribute over which we are computing an aggregate
			Attribute groupAttr, // The attribute over which we are grouping the tuples
			AggregateOp op              // Aggregate operation
			);
	~Aggregate();

	RC getNextTuple(void *data);
	// Please name the output attribute as aggregateOp(aggAttr)
	// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
	// output attrname = "MAX(rel.attr)"
	void getAttributes(vector<Attribute> &attrs) const;

private:
	RC getAggregateResWithoutGroup(void* data);

	RC getAggregateResWithGroup();

	//take the precision into consideration, we store the sum value into the groupMapper, even though the op == AVG
	RC processGroupMapContent(float insertKey, float value){
		bool contains = false;
		for(map<float,float>::const_iterator it = groupMapper.begin(); it != groupMapper.end(); ++it )
		{
		   float key = it->first;
		   if(key == insertKey){
			   if(this->op == MAX){
				   if(groupMapper[key] < value)
					   groupMapper[key] = value;
			   }else if(this->op == MIN){
				   if(groupMapper[key] > value)
					   groupMapper[key] = value;
			   }else if(this->op == AVG || this->op ==SUM){
				   groupMapper[key] = groupMapper[key] + value;
				   gourpMapperFreq[key] = gourpMapperFreq[key] + 1;
			   }else if(this->op == COUNT){
				   groupMapper[key] = groupMapper[key] + 1;
			   }
			   contains = true;
			   break;
		   }
		}

		if(!contains){
     	   if(this->op == AVG || this->op ==SUM){
			   groupMapper[insertKey] = value;
			   gourpMapperFreq[insertKey] = 1;
		   }else if(this->op == COUNT){
			   groupMapper[insertKey] =  1;
		   }else {
			   groupMapper[insertKey] = value;
		   }
		}
	};
};

#endif
