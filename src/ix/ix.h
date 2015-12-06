#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <list>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

#define rootPageIndex 0



class IXFileHandle;
class IXPageHeader;
class MyBPTreeNode;
class MyBPTreeIndexNode;
class MyBPTreeLeafNode;
class IX_ScanIterator;

# define HALF_PAGE 2048
# define MAX_RID_NUM 509
# define UNDEFINED_PAGE_INDEX UINT_MAX
//# define UINT_MAX 4294967295

enum PAGE_TYPE{INDEX=0, LEAF, DATA, APPEND, EMPTY};

/* Page formats*/
/* As declared in the PAGE_TYPE enumeration, we defined four types of pages:
 * If the corresponding sibling is NULL, we just need to set it into UINT_MAX, which we treat as a special value;
 *
 * INDEX:
 * Contains the index information, format:
 * HEADER-SECTION + firstChildIndex[PAGE_NUM, 4bytes] + [ key, ChildIndex[PAGE_NUM, 4bytes]];
 *
 * LEAF:
 * Contains the leaf information, we use the key-ridlist format;
 * HEADER-SECTION + { key + rid [ 2 * 4 bytes] };
 *
 * DATA:
 * Contains the ridList, format; The number of rids that could be stored in one page should be (4096 - 24)/8 = 509
 * HEADER-SECTION{parent: the key page, rightSiblingPage: the appendPageIndex} + { rid [ 2 * 4 bytes] };
 *
 * APPEND:
 * Contains the ridList, format;
 * HEADER-SECTION{parent: the parent DATA PageIndex or the previous APPEND PageIndex, rightSibling: the next APPEND PageIndex} + { rid [ 2 * 4 bytes] };
 * */

class IXPageHeader{
public:
	PAGE_TYPE pageType;
	unsigned recordCount;
	unsigned freeSpace;
	PageNum parentPageIndex;
	PageNum leftSiblingPageIndex;
	PageNum rightSiblingPageIndex;
};

class MyBPTreeNode{
public:
	MyBPTreeNode();
	virtual ~MyBPTreeNode();

	int keyComparator(AttrType dataType, const void* leftKey, const void* rightKey){
		if(leftKey==NULL)
			return 1;
		if(rightKey==NULL)
			return -1;

		if (dataType == TypeVarChar) {

			unsigned leftLength = *(unsigned*)leftKey;
			char* leftValue = new char[PAGE_SIZE];
			memset(leftValue, 0 ,PAGE_SIZE);
			memcpy(leftValue, (byte*)leftKey+sizeof(unsigned), leftLength);

			unsigned rightLength = *(unsigned*)rightKey;
			char* rightValue = new char[PAGE_SIZE];
			memset(rightValue, 0 ,PAGE_SIZE);
			memcpy(rightValue, (byte*)rightKey+sizeof(unsigned), rightLength);

			string leftValueTmp(leftValue);
			string rightValueTmp(rightValue);

			delete[] leftValue;
			delete[] rightValue;

			return leftValueTmp.compare(rightValueTmp);

		} else if (dataType == TypeInt) {
			int left = *(int*) leftKey;
			int right = *(int*) rightKey;
			return left-right;
		} else if (dataType == TypeReal) {
			float left = *(float*) leftKey;
			float right = *(float*) rightKey;
			if(left>right)
				return 1;
			if(left==right)
				return 0;

			if(left<right)
				return -1;
		}
	}; // left larger than right return positive;

	virtual unsigned findKeyIndex(const void* key); //return the index of which larger than the given key

	//pure virtual
	virtual void deleteKey(int keyIndex)=0;  //delete key from the
	virtual void toPageData(void* pageData)=0;

public:
	Attribute keyType;
	IXPageHeader meta;
	vector<const void*> keys;
};

class MyBPTreeIndexNode : public MyBPTreeNode{
public:
	MyBPTreeIndexNode();

	virtual ~MyBPTreeIndexNode();

	virtual void insert(unsigned position, const void* key, PAGE_NUM childPageIndex);
	void updateChildrenParentPageInfo(IXFileHandle &ixfileHandle, PAGE_NUM newParentPageIndex);

	void printNode(){
		cout<< "[ pageNum = "<<this->children[0]<<" ] ";


		for(unsigned i=0;i<this->meta.recordCount;i++){
			cout <<" key = ";
			if(keyType.type==TypeInt)
				cout<< *(int*)this->keys[i];
			else if(keyType.type==TypeReal)
				cout<< *(float*)this->keys[i];
			else
				cout<< *(char*)this->keys[i];
			cout<<"[ pageNum = "<<this->children[i+1]<<" ] ";
		}
		cout<<endl;
	};

	//inherit from MyBPTreeNode
	virtual void deleteKey(int keyIndex);  //delete key from the
	virtual void toPageData(void* pageData);
public:
	vector<PAGE_NUM> children;
};

class MyBPTreeLeafNode : public MyBPTreeNode{
public:
	MyBPTreeLeafNode();

	virtual ~MyBPTreeLeafNode();

	void printNode(){
			for(unsigned i=0;i<this->meta.recordCount;i++){
				cout <<" key = ";
				if(keyType.type==TypeInt)
					cout<< *(int*)this->keys[i];
				else if(keyType.type==TypeReal)
					cout<< *(float*)this->keys[i];
				else
					cout<< *(char*)this->keys[i];
				//cout<<"[ RID = "<<this->datas[i].pageNum << " - " << this->datas[i].slotNum<<" ] ";
			}
			cout<<endl;
	};

	virtual unsigned findKeyIndex(const void* key); //return the index of which larger than the given key

	virtual void insert(unsigned position, const void* key, const RID& rid);

	void updateChildrenParentPageInfo(IXFileHandle &ixfileHandle, PAGE_NUM newParentPageIndex);
	//inherit from MyBPTreeNode
	virtual void deleteKey(int keyIndex);  //delete key from the
	virtual void toPageData(void* pageData);

public:
	vector<RID> datas; //the <key,rid> pageNum; we apply the <key, ridList> style to store the rids.
};

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

        /******************************************************* My Methods ********************************************************/
public:
        static MyBPTreeNode* constructBPTreeNodeByPageData(byte* pageData, const Attribute& attr);

        static void constructIXPageHeaderSectionFromData(byte *data, IXPageHeader& header);

        static void getAllRIDFromDataPage(IXFileHandle &ixfileHandle, byte* pageData, vector<RID>& ridList);

        static void writePageHeaderIntoData(byte *data, IXPageHeader& header);

        MyBPTreeLeafNode* findContainKeyLEAFPage(IXFileHandle &ixfileHandle, const Attribute &attribute,
        		const void* key, MyBPTreeIndexNode* root, PAGE_NUM& pageIndex);
        MyBPTreeLeafNode* findLowestKeyLEAFPage(IXFileHandle &ixfileHandle, const Attribute &attribute,
        		MyBPTreeIndexNode* root, PAGE_NUM& pageIndex);
        MyBPTreeLeafNode* findHighestKeyLEAFPage(IXFileHandle &ixfileHandle, const Attribute &attribute,
        		MyBPTreeIndexNode* root, PAGE_NUM& pageIndex);

        MyBPTreeIndexNode* getRoot(IXFileHandle &ixfileHandle,const Attribute &attribute) const;

        RC initializeIXPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        RC appendRIDIntoDataPage(IXFileHandle &ixfileHandle,PAGE_NUM dataPageNum, const RID &rid);

        RC buildUpNewDataPage(IXFileHandle &ixfileHandle,PAGE_NUM parentPageNum, PAGE_NUM& dataPageNum, const RID oldRID, const RID newRID);

        void printBPlusTreeNodes(IXFileHandle &ixfileHandle, MyBPTreeNode* node, const Attribute &attribute, string tabs)const;

        bool shouldMerge(PAGE_TYPE type, unsigned leftItemNum, unsigned rightItemNum, const Attribute& attribute);

        RC deleteNodeFromIndexNode(IXFileHandle &ixfileHandle, const Attribute &attribute,
        		MyBPTreeIndexNode* node, PAGE_NUM indexPageNum,unsigned keyIndex);

        RC mergeOrRedistribute(IXFileHandle &ixfileHandle, const Attribute &attribute, MyBPTreeNode* node, PAGE_NUM indexPageNum);

        RC redistributeBPlusTree(IXFileHandle &ixfileHandle, const Attribute &attribute,
        		MyBPTreeIndexNode* parentNode, PAGE_NUM parentPageIndex, MyBPTreeNode* left, PAGE_NUM leftPageIndex,
        		MyBPTreeNode* right, PAGE_NUM rightPageIndex, unsigned keyIndex);

        RC mergeNodes(IXFileHandle &ixfileHandle, const Attribute &attribute, MyBPTreeIndexNode* parent, PAGE_NUM parentIndex,
        		MyBPTreeNode* left, PAGE_NUM leftIndex, MyBPTreeNode* right, PAGE_NUM rightIndex, unsigned keyIndex);

        RC basicDeleteKeyFromLeaf(IXFileHandle &ixfileHandle, MyBPTreeLeafNode& leafNode, const Attribute &attribute, const void *key, const RID &rid);

        RC deleteRIDFromDataPage(IXFileHandle &ixfileHandle,PAGE_NUM dataPageNum, const RID &rid);

        RC emptifyPage(IXFileHandle &ixfileHandle, PAGE_NUM pageNum);

        RC appendRIDToExsitingKey(IXFileHandle &ixfileHandle,
        		MyBPTreeLeafNode& leafNode, PAGE_NUM leafPageIndex,
        		const Attribute &attribute, unsigned index, const RID &rid);

        RC basicInsertKeyIntoLeaf(IXFileHandle &ixfileHandle, MyBPTreeLeafNode& leafNode,
        		PAGE_NUM leafPageIndex, const Attribute &attribute, unsigned index, const void *key, const RID &rid);

        RC insertKeyIntoLeaf(IXFileHandle &ixfileHandle, MyBPTreeLeafNode& leafNode, PAGE_NUM leafPageIndex,
        		const Attribute &attribute, const void *key, const RID &rid);

        RC basicInsertKeyIntoIndexNode(IXFileHandle &ixfileHandle, MyBPTreeIndexNode& indexNode,
        		const void *key, PAGE_NUM childPageIndex);

        RC insertKeyIntoIndexNode(IXFileHandle &ixfileHandle, const Attribute &attribute,
        		PAGE_NUM pageIndex, const void *key, PAGE_NUM childPageIndex);

        void updateChildrenParentPageInfo(IXFileHandle &ixfileHandle, PAGE_NUM newParentPageIndex);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
    	PagedFileManager *_pf_manager;
};


class IXFileHandle : public FileHandle{
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

	/****************************My Method*************************/
};

class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        bool hasNextEntry();

        RC increaseIndex();

        void initialize(IXFileHandle &ixfileHandle, unsigned lowLeafPage, unsigned highLeafPage,
        		unsigned lowKeyIndex, unsigned highKeyIndex, const Attribute& attr);

    public:
        MyBPTreeLeafNode* currentPointLeafNode;
        vector<RID> currentKeyRidList;
        unsigned currentRidIndex;
        unsigned currentPageIndex;
        unsigned currentKeyIndex;

        unsigned currentPageKeyMaxIndex;

        Attribute attr;
        IXFileHandle ixfileHandle;
        unsigned lowLeafPage;
        unsigned highLeafPage;
        unsigned highKeyIndex;
        unsigned lowKeyIndex;
};




#endif
