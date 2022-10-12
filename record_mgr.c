#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

#define ATTRIBUTE_SIZE 15

typedef struct MetaDataMgr{

	int recordCount;
	int firstFreePage;
	int scanCounter;
	BM_PageHandle bm_pageHandle;	
	BM_BufferPool bm;	
	RID recordID;
	Expr *condition;

} MetaDataMgr;


MetaDataMgr *metaDataMgr;

RC initRecordManager(void *mgmtData){
	return RC_OK;
}

RC shutdownRecordManager(){

	free(metaDataMgr);
	return RC_OK;
}

void addSchemaData(char* bm_pageHandle, Schema *schema){

	int cnt = 0;

	while(cnt < schema->numAttr){

		strncpy(bm_pageHandle, schema->attrNames[cnt], ATTRIBUTE_SIZE);
		bm_pageHandle += ATTRIBUTE_SIZE;
	
	    *(int*)bm_pageHandle = (int)schema->dataTypes[cnt];
	    bm_pageHandle += sizeof(int);

	    *(int*)bm_pageHandle = schema->typeLength[cnt];
	    bm_pageHandle += sizeof(int);

	    cnt++;   	
	}

}

RC createTable(char *name, Schema *schema){

	// check if file already exist
	if(access(name, F_OK) == 0){
		return RC_ERROR;
	}

	metaDataMgr = (MetaDataMgr*) malloc(sizeof(MetaDataMgr));

	char data[PAGE_SIZE];
	char *bm_pageHandle = data;
	SM_FileHandle fileHandle;
	 
	// Add record count in metadata
	*(int*)bm_pageHandle = 0; 
	bm_pageHandle += sizeof(int);
	
	// First free page is page 1 since we are using page 0 for metadata 
	*(int*)bm_pageHandle = 1;
	bm_pageHandle += sizeof(int);

	// Add number of attributes
	*(int *)bm_pageHandle = schema->numAttr;
	bm_pageHandle += sizeof(int);

	// Add key size of the attributes
	*(int *)bm_pageHandle = schema->keySize;
	bm_pageHandle += sizeof(int);

	// Add schema details to metadata
	addSchemaData(bm_pageHandle, schema);

	// Initialize bufferpool
	initBufferPool(&metaDataMgr->bm, name, 100, RS_FIFO, NULL);

	// Add metadata to file
	if(createPageFile(name) == RC_OK && openPageFile(name, &fileHandle) == RC_OK && writeBlock(0, &fileHandle, data) == RC_OK && closePageFile(&fileHandle) == RC_OK){

		return RC_OK;
	}

	free(bm_pageHandle);

	return RC_ERROR;
}


Schema* readSchema(SM_PageHandle bm_pageHandle, int atrrCount){

	Schema *schema = (Schema*)malloc(sizeof(Schema));
	schema->dataTypes = (DataType*)malloc(sizeof(DataType)*atrrCount);
	schema->attrNames = (char**)malloc(sizeof(char*)*atrrCount);	
	schema->typeLength = (int*)malloc(sizeof(int)*atrrCount);

	schema->numAttr = atrrCount;

    int cnt = 0;

	while(cnt < atrrCount){

		schema->attrNames[cnt]= (char*)malloc(ATTRIBUTE_SIZE);
		strncpy(schema->attrNames[cnt], bm_pageHandle, ATTRIBUTE_SIZE);
		bm_pageHandle += ATTRIBUTE_SIZE;
	   
		schema->dataTypes[cnt]= *(int*) bm_pageHandle;
		bm_pageHandle += sizeof(int);

		schema->typeLength[cnt]= *(int*)bm_pageHandle;
		bm_pageHandle += sizeof(int);

		cnt++;
	}

	return schema;
}

RC openTable(RM_TableData *rel, char *name){

	SM_PageHandle bm_pageHandle;    
	int attributeCount;
	rel->mgmtData = metaDataMgr;
	rel->name = name;
    
    // Pin Page since we are reading metadata from it
	pinPage(&metaDataMgr->bm, &metaDataMgr->bm_pageHandle, 0);
	
	// Set pointer location to start
	bm_pageHandle = metaDataMgr->bm_pageHandle.data;
	
	// Get total number of records
	metaDataMgr->recordCount= *(int*)bm_pageHandle;
	bm_pageHandle += sizeof(int);

	// Get first free page
	metaDataMgr->firstFreePage= *(int*) bm_pageHandle;
    bm_pageHandle += sizeof(int);
	
	// Get number of attributes of schema
    attributeCount = *(int*)bm_pageHandle;
	bm_pageHandle += sizeof(int);

	// Get schema details
	rel->schema = readSchema(bm_pageHandle, attributeCount);	

	// Unpin page since we have retrived metadata and we no longer need the page 0
	unpinPage(&metaDataMgr->bm, &metaDataMgr->bm_pageHandle);

	return RC_OK;
} 
  
RC closeTable(RM_TableData *rel){
	// Remove pages from bufferpool
	metaDataMgr = rel->mgmtData;
	shutdownBufferPool(&metaDataMgr->bm);
	return RC_OK;
}

RC deleteTable(char *name){
	// Delete the file
	destroyPageFile(name);
	return RC_OK;
}

int getNumTuples(RM_TableData *rel){
	return metaDataMgr->recordCount;
}

// This is a custom function that scans all pages and returns the start pointer of the page that has empty slot
char *findFreeSlot(RID *recordID, int recordSize, int pageNumber){
	int slot = -1;
	char *data, *res;

	while(slot == -1){

		pinPage(&metaDataMgr->bm, &metaDataMgr->bm_pageHandle, pageNumber);
		data = metaDataMgr->bm_pageHandle.data;

		for(int i = 0; i < PAGE_SIZE/recordSize; i++){
			// '*' denotes if slot has record or not
			if(data[i*recordSize] != '*'){
				slot = i;
				recordID->slot = slot;
				recordID->page = pageNumber;
				res = data;
				break;
			}
		}

		unpinPage(&metaDataMgr->bm, &metaDataMgr->bm_pageHandle);
		pageNumber++;

	}

	return res;
}


RC insertRecord(RM_TableData *rel, Record *record){

	char *slotPtr;
	int recordSize = getRecordSize(rel->schema);
	RID *recordID = &record->id; 
	metaDataMgr = rel->mgmtData;
	
	recordID->page = metaDataMgr->firstFreePage;
	slotPtr = findFreeSlot(recordID, recordSize, recordID->page);
	
	// Set slot pointer to start of the slot
	slotPtr += (recordID->slot*recordSize);

	// "*" denotes that the slot has record
	strcpy(slotPtr, "*");
	slotPtr++;

	// Copy record data to slot
	memcpy(slotPtr, record->data + 1, recordSize - 1);

	// Unpin page since we have written data to page and it is no longer needed
	unpinPage(&metaDataMgr->bm, &metaDataMgr->bm_pageHandle);
	metaDataMgr->recordCount++;
	
	return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id){

	metaDataMgr = rel->mgmtData;
	pinPage(&metaDataMgr->bm, &metaDataMgr->bm_pageHandle, id.page);
	
	char *data = metaDataMgr->bm_pageHandle.data;
	data += (id.slot*getRecordSize(rel->schema));
	
	// "#" denotes that the slot is now empty
	strcpy(data, "#");
	metaDataMgr->firstFreePage = id.page;

	unpinPage(&metaDataMgr->bm, &metaDataMgr->bm_pageHandle);

	return RC_OK;
}


RC updateRecord(RM_TableData *rel, Record *record){	

	char *data;
	int recordSize = getRecordSize(rel->schema);
	metaDataMgr = rel->mgmtData;
	
	pinPage(&metaDataMgr->bm, &metaDataMgr->bm_pageHandle, record->id.page);

	data = metaDataMgr->bm_pageHandle.data;
	data += (record->id.slot*recordSize);
	
	// '*' denotes that the slot is not empty
	strcpy(data, "*");
	data++;

	// Copy new record information to the slot
	memcpy(data, record->data + 1, recordSize - 1);
	unpinPage(&metaDataMgr->bm, &metaDataMgr->bm_pageHandle);
	
	return RC_OK;	
}


RC getRecord(RM_TableData *rel, RID id, Record *record){

	int recordSize = getRecordSize(rel->schema);
	metaDataMgr = rel->mgmtData;
	pinPage(&metaDataMgr->bm, &metaDataMgr->bm_pageHandle, id.page);

	char *dataPtr = metaDataMgr->bm_pageHandle.data + (id.slot*recordSize);
	
	// Check if record exist
	if(*dataPtr != '*'){
		return RC_ERROR;
	}
		
	record->id = id;
	char *data = record->data;
	data++;

	// Copy record data from slot to record object
	memcpy(data, dataPtr + 1, recordSize - 1);
	unpinPage(&metaDataMgr->bm, &metaDataMgr->bm_pageHandle);

	return RC_OK;
}

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){

	openTable(rel, "Scanner");

    MetaDataMgr *scanner = (MetaDataMgr*)malloc(sizeof(MetaDataMgr));
	MetaDataMgr *recordMgr;
    	
    // Initilaize the variables required for scan	
    scanner->scanCounter = 0;	
    scanner->recordID.page = 1;
	scanner->recordID.slot = 0;
    scanner->condition = cond;
    recordMgr = rel->mgmtData;
    recordMgr->recordCount = ATTRIBUTE_SIZE;
    scan->rel= rel;
    scan->mgmtData = scanner;

	return RC_OK;
}


RC next(RM_ScanHandle *scan, Record *record){

	char *data;
	Value *result = (Value *)malloc(sizeof(Value));
	MetaDataMgr *scanner = scan->mgmtData;
    Schema *schema = scan->rel->schema;
    MetaDataMgr *recordMgr = scan->rel->mgmtData;
   
	int recordSize = getRecordSize(schema);
	int totalSlots = PAGE_SIZE/recordSize;

	// Execute this loop until we scan all records
	while(scanner->scanCounter <= recordMgr->recordCount)
	{  
		scanner->recordID.slot++;

		// Reset slot number if we scanned all record from the page and move to next page
		if(scanner->recordID.slot >= totalSlots){

			scanner->recordID.slot = 0;
			scanner->recordID.page += 1;

		}

		int temp = -1;

		// Below condition avoids pinning one page multiple times
		if(temp != scanner->recordID.page){
			pinPage(&recordMgr->bm, &scanner->bm_pageHandle, scanner->recordID.page);
		}
		
		// Get start pointer of the page
		data = scanner->bm_pageHandle.data;
		temp = scanner->recordID.page;	

		// Add record offset 
		data += (scanner->recordID.slot*recordSize);

		char *dataPtr = record->data;
		dataPtr++;
		
		// Copy data from slot
		memcpy(dataPtr, data + 1, recordSize - 1);
		// Validate condition against the record
		evalExpr(record, schema, scanner->condition, &result); 
		scanner->scanCounter++;

		// If true we found required record
		if(result->v.boolV == TRUE){
			
			record->id.page = scanner->recordID.page;
			record->id.slot = scanner->recordID.slot;
			unpinPage(&recordMgr->bm, &scanner->bm_pageHandle);	
			free(result);

			return RC_OK;
		}
	}
	
	unpinPage(&recordMgr->bm, &scanner->bm_pageHandle);
	free(result);

	return RC_RM_NO_MORE_TUPLES;
}


RC closeScan(RM_ScanHandle *scan){
	
    free(scan->mgmtData);  	
	return RC_OK;
}


int getRecordSize(Schema *schema){

	int size = 0;

	for(int i = 0; i < schema->numAttr; i++){

		if(schema->dataTypes[i] == DT_STRING){
			size += schema->typeLength[i];
		}else if(schema->dataTypes[i] == DT_INT){
			size += sizeof(int);
		}else if(schema->dataTypes[i] == DT_FLOAT){
			size += sizeof(float);
		}else if(schema->dataTypes[i] == DT_BOOL){
			size += sizeof(bool);
		}

	}
	
	return ++size;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
	
	Schema *sc = (Schema *)malloc(sizeof(Schema));
	sc->numAttr = numAttr;
	sc->attrNames = attrNames;
	sc->dataTypes = dataTypes;
	sc->typeLength = typeLength;
	sc->keySize = keySize;
	sc->keyAttrs = keys;

	return sc; 
}


RC freeSchema(Schema *sc){
	free(sc);
	return RC_OK;
}


RC createRecord(Record **record, Schema *schema){

	Record *newRecord = (Record*)malloc(sizeof(Record));    
	newRecord->data= (char*)malloc(getRecordSize(schema));
	
	// "#" is appended at start since there is no record data
	strcpy(newRecord->data, "#");
	*record = newRecord;

	return RC_OK;
}


RC attrOffset(Schema *schema, int attrNum, int *result){
	*result = 1;

	for(int i = 0; i < attrNum; i++){

		if(schema->dataTypes[i] == DT_STRING){
			*result += schema->typeLength[i];
		}else if(schema->dataTypes[i] == DT_INT){
			*result += sizeof(int);
		}else if(schema->dataTypes[i] == DT_FLOAT){
			*result += sizeof(float);
		}else if(schema->dataTypes[i] == DT_BOOL){
			*result += sizeof(bool);
		}

	}

	return RC_OK;
}


RC freeRecord(Record *record){
	free(record);
	return RC_OK;
}


RC getAttr(Record *record, Schema *schema, int attrNum, Value **value){

	int offset = 0;
	Value *attribute = (Value*)malloc(sizeof(Value));
	char *dataPtr = record->data;

	// Get attribute offset
	attrOffset(schema, attrNum, &offset);

	dataPtr += offset;

	if(attrNum == 1){
		schema->dataTypes[attrNum] = 1;	
	}

	// Check attribute type and retrive data accordingly 
	if(schema->dataTypes[attrNum] == DT_INT){

		int value = 0;
		memcpy(&value, dataPtr, sizeof(int));
		attribute->v.intV = value;
		attribute->dt = DT_INT;

	}else if(schema->dataTypes[attrNum] == DT_FLOAT){

		float value;
	  	memcpy(&value, dataPtr, sizeof(float));
	  	attribute->v.floatV = value;
		attribute->dt = DT_FLOAT;

	}else if(schema->dataTypes[attrNum] == DT_STRING){

		attribute->v.stringV = (char *) malloc(schema->typeLength[attrNum] + 1);
		strncpy(attribute->v.stringV, dataPtr, schema->typeLength[attrNum]);
		attribute->v.stringV[schema->typeLength[attrNum]] = '\0';
		attribute->dt = DT_STRING;

	}else if(schema->dataTypes[attrNum] == DT_BOOL){

		bool value;
		memcpy(&value,dataPtr, sizeof(bool));
		attribute->v.boolV = value;
		attribute->dt = DT_BOOL;

	}else{

		free(attribute);
		return RC_ERROR;
	}

	*value = attribute;

	return RC_OK;
}


RC setAttr(Record *record, Schema *schema, int attrNum, Value *value){
	int offset;
	attrOffset(schema, attrNum, &offset);
	char *dataPtr = record->data;
	dataPtr += offset;

	// Set attribute value in the record based on its data type
	if(schema->dataTypes[attrNum] == DT_STRING){

		strncpy(dataPtr, value->v.stringV, schema->typeLength[attrNum]);
		dataPtr += schema->typeLength[attrNum];

	}else if(schema->dataTypes[attrNum] == DT_INT){

		*(int *) dataPtr = value->v.intV;	  
		dataPtr += sizeof(int);

	}else if(schema->dataTypes[attrNum] == DT_FLOAT){

		*(float *) dataPtr = value->v.floatV;
		dataPtr += sizeof(float);

	}else if(schema->dataTypes[attrNum] == DT_BOOL){

		*(bool *) dataPtr = value->v.boolV;
		dataPtr += sizeof(bool);
	}
		
	return RC_OK;
}
