#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
    if(_pf_manager){
        delete _pf_manager;
        _pf_manager = 0;
    }
}


RC PagedFileManager::createFile(const string &fileName)
{
	const char* name = fileName.c_str();
	if(exist(name)){
		return -1;
	}
	fopen(name,"wb");
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	return remove(fileName.c_str());
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	 const char *name = fileName.c_str();
	 if(!exist(name)){
		return -1;
	 }
	 FILE* pFile = fopen(name, "rb+");
	 fileHandle.pFile = pFile;
	 return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	return fclose(fileHandle.pFile);
}

bool PagedFileManager::exist(const string &fileName){
	struct stat buffer;
    if(stat(fileName.c_str(), &buffer) == 0)
    	return true;
    else
    	return false;
}

FileHandle::FileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
	this->pFile = NULL;
}


FileHandle::~FileHandle()
{
    if(this->pFile){
        this->pFile = NULL;
    }
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if(!moveFileOpPointer(pageNum)){
        return -1;
    }

    if(fread(data, PAGE_SIZE, 1, this->pFile) != 1){
    	cout<<"Read page data Failed!"<<endl;
        return -1;
    }

    readPageCounter++;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if(moveFileOpPointer(pageNum)<0){
        return -1;
    }

    if(fwrite(data, PAGE_SIZE, 1, pFile) == 0){
        this->writePageCounter++;
        return 0;
    }

    return -1;
}


RC FileHandle::appendPage(const void *data)
{
	if(fseek(pFile, 0, SEEK_END) < 0){
	    return -1;
	}
	if(fwrite(data, PAGE_SIZE, 1, this->pFile) == 0){
		this->appendPageCounter++;
	    return 0;
	}
	return -1;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = this->readPageCounter;
	writePageCount = this->writePageCounter;
	appendPageCount = this->appendPageCounter;
	return 0;
}

unsigned FileHandle::getNumberOfPages()
{
    if((fseek(this->pFile, 0, SEEK_END)) < 0){
        return -1;
    }
    return ftell(this->pFile) / PAGE_SIZE;
}

RC FileHandle::moveFileOpPointer(unsigned pageIndex)
{
    if(pageIndex >= getNumberOfPages()){
        cout<<"Move file operation pointer failed!"<<endl;
    	return -1;
    }
    return fseek(this->pFile, pageIndex * PAGE_SIZE, SEEK_SET);
}

void FileHandle::loadPageHeaderInfos(PageNum pageNum, byte& recordNum, byte& nextAvailableSlotIndex){
	void* pageData = (byte*)malloc(PAGE_SIZE);
	this->readPage(pageNum,pageData);
	this->loadPageHeaderInfos(pageData,recordNum,nextAvailableSlotIndex);
}

void FileHandle::loadPageHeaderInfos(const void *pageData, byte& recordNum, byte& nextAvailableSlotIndex){
	memcpy(&nextAvailableSlotIndex,(byte*)pageData + PAGE_SIZE - sizeof(byte),  sizeof(byte));
	memcpy(&recordNum, (byte*)pageData + PAGE_SIZE - 2*sizeof(byte), sizeof(byte));
}

void FileHandle::updatePageHeaderInfos(const void *pageData, byte slotNum, byte nextAvailableSlotIndex){
	memcpy((byte*)pageData + PAGE_SIZE - sizeof(byte), &nextAvailableSlotIndex, sizeof(byte));
	memcpy((byte*)pageData + PAGE_SIZE - 2*sizeof(byte), &slotNum, sizeof(byte));
}

void FileHandle::appendNewPage(){
	byte* pageData = new byte[PAGE_SIZE];
	byte nextAvailableSlotIndex = 0;
	byte slotNum = 0;
	memcpy((byte*)pageData + PAGE_SIZE - sizeof(byte), &nextAvailableSlotIndex, sizeof(byte));
	memcpy((byte*)pageData + PAGE_SIZE - 2*sizeof(byte), &slotNum, sizeof(byte));
	delete[] pageData;
}

