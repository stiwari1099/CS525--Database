#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<limits.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

typedef struct Page{

	SM_PageHandle data;
	PageNumber pageNum; 
	int dirtyBit;
	int fixCount;
	int lastUsed;

} Frame;

int LRU_Counter = 0;
int startIndex = -1;
int readCount = 0;
int bufferSize = 0;
int writeCount = 0;

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData){

	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;

	Frame *frame = malloc(sizeof(Frame)*numPages);

	for(int i = 0; i < numPages; i++){
	 	frame[i].pageNum = -1;
	 	frame[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
	 	frame[i].dirtyBit = 0;
	 	frame[i].fixCount = 0;
	 	frame[i].lastUsed = 0;
	}

	bm->mgmtData = frame;
	writeCount = 0;
	
	return RC_OK;	
}


RC shutdownBufferPool(BM_BufferPool *const bm){

	if(bm->pageFile == NULL){
		return RC_ERROR;
	}
	Frame *frame = (Frame *)bm->mgmtData;
	forceFlushPool(bm);
	
	for(int i = 0; i < bm->numPages; i++){
		if(frame[i].fixCount != 0){
			return RC_ERROR;
		}
	}

	bm->pageFile = NULL;
	bm->numPages = -1;
	bm->strategy = -1;
	readCount = 0;
	writeCount = 0;
	LRU_Counter = 0;

	free(bm->mgmtData);
	return RC_OK;
}


RC FIFO(BM_BufferPool *const bm, int pageNumber){

	Frame *frame = (Frame *)bm->mgmtData;

	// Append page to array if buffer is not full
	for(int i = 0; i < bm->numPages; i++){

		if(frame[i].pageNum == -1){
			SM_PageHandle memPage = malloc(sizeof(char)*PAGE_SIZE);
			SM_FileHandle fileHandle;
			// openPageFile is used to initialize fileHandle
			openPageFile(bm->pageFile, &fileHandle);
			// Read page content into memPage
			readBlock(pageNumber , &fileHandle, memPage);

			// Assign page values in buffer
			frame[i].pageNum = pageNumber;
			frame[i].data = malloc(sizeof(char)*PAGE_SIZE);
			frame[i].data = memPage;
			frame[i].dirtyBit = 0;
			frame[i].fixCount = 1;
	 		frame[i].lastUsed = LRU_Counter;
	 		LRU_Counter++;
	 		readCount++;
	 		startIndex++;
	 		return RC_OK;
		}
	}

	int index = -1;
	int counter = bm->numPages;
	int i = startIndex;

	// Get the index of first frame which has fixCount value 0
	while(counter > 0){
		i = i % (bm->numPages);
		if(frame[i].fixCount == 0){
			index = i;
			break;
		}
		counter--;
		i++;
	}

	// If index is -1 then all pages are currently being used and FIFO can not remove any frame
	if(index == -1){
		//printf("All pages in the buffer are currently being used!!\n");
		return RC_ERROR;
	}

	startIndex = index + 1;
	BM_PageHandle *page = malloc(sizeof(BM_PageHandle));
	page->pageNum = frame[index].pageNum;
	page->data = frame[index].data;
	// Write page to the disk before moved out of buffer
	forcePage(bm, page);

	SM_PageHandle memPage = malloc(sizeof(char)*PAGE_SIZE);
	SM_FileHandle fileHandle;
	// openPageFile is used to initialize fileHandle
	openPageFile(bm->pageFile, &fileHandle);
	// Read page content into memPage
	readBlock(pageNumber , &fileHandle, memPage);

	// Assign page values in buffer
	frame[index].pageNum = pageNumber;
	frame[index].data = malloc(sizeof(char)*PAGE_SIZE);
	frame[index].data = memPage;
	frame[index].dirtyBit = 0;
	frame[index].fixCount = 1;
	frame[index].lastUsed = LRU_Counter;
	LRU_Counter++;
	readCount++;

	return RC_OK;
}


RC LRU(BM_BufferPool *const bm, int pageNumber){
	long int minValue = INT_MAX;
	int index = -1;
	Frame *frame = (Frame *)bm->mgmtData;

	// Check for empty slot in buffer
	for(int i = 0; i < bm->numPages; i++){
		if(frame[i].pageNum == -1){
			index = i;
			break;
		}
	}

	// If no empty slot found then get index of least recently used page from buffer
	if(index == -1){
		for(int i = 0; i < bm->numPages; i++){
			if(minValue > frame[i].lastUsed && frame[i].fixCount == 0){
				index = i;
				minValue = frame[i].lastUsed;
			}
		}
	}

	// If index is -1 then all pages are currently being used and LRU can not remove any frame
	if(index == -1){
		//printf("All pages in the buffer are currently being used!!\n");
		return RC_ERROR;
	}

	BM_PageHandle *page = malloc(sizeof(BM_PageHandle));
	page->pageNum = frame[index].pageNum;
	page->data = frame[index].data;
	forcePage(bm, page);


	SM_PageHandle memPage = malloc(sizeof(char)*PAGE_SIZE);
	SM_FileHandle fileHandle;
	// openPageFile is used to initialize fileHandle
	openPageFile(bm->pageFile, &fileHandle);
	// Read page content into memPage
	readBlock(pageNumber , &fileHandle, memPage);

	// Assign page values in buffer
	frame[index].pageNum = pageNumber;
	frame[index].data = memPage;
	frame[index].dirtyBit = 0;
	frame[index].fixCount = 1;
	frame[index].lastUsed = LRU_Counter;
	LRU_Counter++;
	readCount++;

	return RC_OK;
}


RC forceFlushPool(BM_BufferPool *const bm){
	// Check if buffer is initialized or not
	if(bm->pageFile == NULL){
		return RC_ERROR;
	}
	Frame *frame = (Frame *)bm->mgmtData;

	for(int i = 0; i < bm->numPages; i++){
		// Only write those pages whose dirty bit is 1 and not currently being used
		if(frame[i].fixCount == 0 && frame[i].dirtyBit == 1){
			SM_FileHandle fileHandle;
			// openPageFile is used to initialize fileHandle
			openPageFile(bm->pageFile, &fileHandle);
			// writeBlock writes page to disk
			writeBlock(frame[i].pageNum, &fileHandle, frame[i].data);
			// Set dirty bit to 0 as the page is no longer dirty
			frame[i].dirtyBit = 0;
			writeCount++;
		}
	}
	
	return RC_OK;
}


RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page){
	Frame *frame = (Frame *)bm->mgmtData;

	// Iterate through the buffer and set dirty bit
	for(int i = 0; i < bm->numPages; i++){
		if(frame[i].pageNum == page->pageNum){
			frame[i].dirtyBit = 1;
			return RC_OK;
		}
	}

	return RC_ERROR;
}


RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page){
	Frame *frame = (Frame *)bm->mgmtData;

	// Iterate through the buffer and decrement fix count	
	for(int i = 0; i < bm->numPages; i++){
		if(frame[i].pageNum == page->pageNum){
			frame[i].fixCount--;
			return RC_OK;
		}
	}

	return RC_ERROR;
}


RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page){
	Frame *frame = (Frame *)bm->mgmtData;

	for(int i = 0; i < bm->numPages; i++){
		if(frame[i].pageNum == page->pageNum && frame[i].dirtyBit == 1){
			SM_FileHandle fileHandle;
			// openPageFile is used to initialize fileHandle
			openPageFile(bm->pageFile, &fileHandle);
			// writeBlock writes page to disk
			writeBlock(frame[i].pageNum, &fileHandle, frame[i].data);
			// Set dirty bit to 0 as the page is no longer dirty
			frame[i].dirtyBit = 0;
			writeCount++;
			return RC_OK;
		}
	}

	return RC_ERROR;
}


RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
	if(bm->pageFile == NULL){
		return RC_ERROR;
	}
	Frame *frame = (Frame *)bm->mgmtData;

	// Check if page number is valid
	if(pageNum < 0){
		return RC_ERROR;
	}

	// Check if the page is already present in buffer
	for(int i = 0; i < bm->numPages; i++){
		if(frame[i].pageNum == pageNum){
			page->pageNum = pageNum;
			page->data = frame[i].data;
			frame[i].fixCount++;
			frame[i].lastUsed = LRU_Counter;
			LRU_Counter++;
			return RC_OK;
		}
	}

	// If page is not in buffer
	switch(bm->strategy){

		case RS_FIFO:
				if(FIFO(bm, pageNum) != RC_OK){
					return RC_ERROR;
				}
				break;

		case RS_LRU: 
				if(LRU(bm, pageNum) != RC_OK){
					return RC_ERROR;
				}
				break;

		case RS_LRU_K:
				printf("LRU - K is not implemented.\n");
				break;

		default:
			printf("No appropriate page replacement strategy found!\n");
			break;

	}

	// Read data from buffer into page handle
	for(int i = 0; i < bm->numPages; i++){
		if(frame[i].pageNum == pageNum){
			page->pageNum = pageNum;
			page->data = frame[i].data;
			break;
		}
	}

	return RC_OK;
}


PageNumber *getFrameContents(BM_BufferPool *const bm){
	// Declare array of type PageNumber
	PageNumber *frameContents = malloc(sizeof(int)*bm->numPages);
	Frame *frame = (Frame *)bm->mgmtData;

	// Set array value to page number of the frames
	for(int i = 0; i < bm->numPages; i++){
		if(frame[i].pageNum == -1){
			frameContents[i] = NO_PAGE;
		}else{
			frameContents[i] = frame[i].pageNum;
		}
	}

	return frameContents;
}


bool *getDirtyFlags(BM_BufferPool *const bm){
	// Declare boolean array
	bool *dirtyFlags = malloc(sizeof(bool) * (bm->numPages));
	Frame *frame = (Frame *)bm->mgmtData;

	// Set array values as per dirty bit
	for(int i = 0; i < bm->numPages; i++){
		if(frame[i].dirtyBit == 1){
			dirtyFlags[i] = true;
		}else{
			dirtyFlags[i] = false;
		}
	}

	return dirtyFlags;
}

int *getFixCounts(BM_BufferPool *const bm){
	// Declare int array
	int *fixCountArray = malloc(sizeof(int)*bm->numPages);
	Frame *frame = (Frame *)bm->mgmtData;
	
	// Set array values equal to fix count values
	for(int i = 0; i < bm->numPages; i++){
		fixCountArray[i] = frame[i].fixCount;
	}

	return fixCountArray;
}

int getNumReadIO (BM_BufferPool *const bm){
	return readCount;
}

int getNumWriteIO (BM_BufferPool *const bm){
	return writeCount;
}