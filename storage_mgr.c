#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>

#include "storage_mgr.h"

FILE *fp;

void initStorageManager(){
	printf("##############################################################################\n");
	printf("CS 525 - Programming Assignment 3: Record Manager\n");
	printf("Submitted by:\n");
	printf("Ruturaj Joshi - A20497857\n");
	printf("Prashant Bhutani - A20488431\n");
	printf("Shubham Tiwari - A20499153\n");
	printf("##############################################################################\n");
}


RC createPageFile(char *fileName){

	fp = fopen(fileName, "w+");

	if(fp != NULL){
		char *ch = malloc(PAGE_SIZE*sizeof(char));
		int counter = PAGE_SIZE;
		while(counter > 0){
			strcat(ch, "\0");
			counter--;
		}
		// Check if write operation was successful
		if(fwrite(ch, sizeof(char), PAGE_SIZE, fp) != PAGE_SIZE){
			return RC_WRITE_FAILED;
		}
		fclose(fp);
		// Release allocated memory
		free(ch); 
		return RC_OK;
	}

	return RC_FILE_NOT_FOUND;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle){
	fp = fopen(fileName, "r");

	if(fp != NULL){
		fHandle -> fileName = fileName;
		fHandle -> curPagePos = 0;
		
		// Calculate size of file using to fseek() and ftell()
		// fseek() moves pointer to the end of file and ftell() returns the position of file pointer
		// which is size of file in bytes

		if(fseek(fp, 0L, SEEK_END) != 0){
			return RC_FILE_NOT_FOUND;
		}
		fHandle -> totalNumPages = ftell(fp)/PAGE_SIZE;
		fclose(fp);
		return RC_OK;
	}

	return RC_FILE_NOT_FOUND;
}

RC closePageFile(SM_FileHandle *fHandle){

	// ftell() return positive integer if file is open else it returns negative integer
	if(ftell(fp) > 0){
		fclose(fp);
	}

	return RC_OK;;
}


RC destroyPageFile(char *fileName){
	// access() with F_OK checks if file can be accessed or not. If it returns 0 then file exist otherwise not.
	// Refered url: https://pubs.opengroup.org/onlinepubs/009695299/functions/access.html
	if(access(fileName, F_OK) == 0){
		remove(fileName);
		return RC_OK;
	}

	return RC_FILE_NOT_FOUND;
}


RC readBlock(int pageNum , SM_FileHandle * fHandle, SM_PageHandle memPage){

	// Check if pageNum is valid 
	if(pageNum > fHandle -> totalNumPages || pageNum < 0){
		return RC_READ_NON_EXISTING_PAGE;
	}

	fp = fopen(fHandle -> fileName, "r");
	if(fp != NULL){
		// Move file pointer to the required page number
		if(fseek(fp, PAGE_SIZE*pageNum, SEEK_SET) != 0){
			return RC_READ_NON_EXISTING_PAGE;
		}

		// Read the page contents into memPage
		fread(memPage, PAGE_SIZE, sizeof(char), fp);
		// Set current page to pageNum
		fHandle -> curPagePos = pageNum;
		fclose(fp);
		return RC_OK;
	}

	return RC_FILE_NOT_FOUND;
}


int getBlockPos(SM_FileHandle *fHandle ){
	return fHandle -> curPagePos;
}


RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
	// Utilizing readBlock() function with page number 0
	return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle , SM_PageHandle memPage){
	int prevPage = (fHandle -> curPagePos) - 1;
	
	if(prevPage >= 0){
		// Utilizing readBlock() function with page number = current page - 1
		return readBlock(prevPage, fHandle, memPage);
	}

	return RC_READ_NON_EXISTING_PAGE;
}

RC readCurrentBlock(SM_FileHandle *fHandle ,SM_PageHandle memPage){
	// Utilizing readBlock() function with current page number
	return readBlock(fHandle -> curPagePos, fHandle, memPage);
}


RC readNextBlock(SM_FileHandle *fHandle ,SM_PageHandle memPage){
	// Utilizing readBlock() function with (current page number + 1)
	return readBlock((fHandle -> curPagePos) + 1, fHandle, memPage);
}

RC readLastBlock( SM_FileHandle *fHandle ,SM_PageHandle memPage){
	// Utilizing readBlock() function with last page number
	return readBlock(fHandle -> totalNumPages, fHandle, memPage);
}

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
		
	fp = fopen(fHandle->fileName, "r+");
	int startPos = pageNum*PAGE_SIZE;

	if(fp != NULL){
		if(pageNum == 0){

			fseek(fp, startPos, SEEK_SET);	

			for(int i = 0; i < PAGE_SIZE; i++){	
				fputc(memPage[i], fp);
			}

			fHandle->curPagePos = ftell(fp);
			fclose(fp);
			 
		}else {	

			fHandle->curPagePos = startPos;
			fclose(fp);
			writeCurrentBlock(fHandle, memPage);

		}

	}else{

		return RC_FILE_NOT_FOUND;
	}
	
	return RC_OK;
}


RC writeCurrentBlock(SM_FileHandle *fHandle , SM_PageHandle memPage){
	// Utilize writeBlock() with current page number
	return writeBlock(fHandle -> curPagePos, fHandle, memPage);
}


RC appendEmptyBlock(SM_FileHandle *fHandle){
	// Using calloc here as it initializes the memory block with null
	// Note: malloc doesn't initialize the memroy block
	char *emptyBlock = calloc(PAGE_SIZE, sizeof(char));
	// Write empty block with writeBlock() using pageNum = (total pages + 1)
	if(writeBlock(fHandle -> totalNumPages + 1, fHandle, emptyBlock) == RC_OK){
		free(emptyBlock);
		return RC_OK;
	}
	free(emptyBlock);
	return RC_WRITE_FAILED;
}


RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle){
	int totalPages = fHandle -> totalNumPages;
	// If we file has less pages than the capacity then append empty block till capacity is fulfilled 
	if(totalPages < numberOfPages){
		for(int i = 0; i < (numberOfPages - totalPages); i++){
			if(appendEmptyBlock(fHandle) != RC_OK){
				return RC_WRITE_FAILED;
			}
		}
	}
	return RC_OK;
}