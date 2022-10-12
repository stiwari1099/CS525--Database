# CS525 - Advanced Database Organization

## Programming Assignment 3: Record Manager

## Contributors
* **Ruturaj Joshi (A20497857)**
* **Prashant Bhutani (A20488431)**
* **Shubham Tiwari (A20499153)**

## Table of contents

* [Project Overview](#Project-Overview)
* [Project Structure](#Project-Structure)
* [How to run this project?](#How-to-run-this-project?)
* [Interface Implementation](#Interface-Implementation)
    * [initRecordManager()](#initRecordManager)
    * [shutdownRecordManager()](#shutdownRecordManager)
    * [addSchemaData()](#addSchemaData)
    * [createTable()](#createTable)
    * [readSchema()](#readSchema)
    * [openTable()](#openTable)
    * [closeTable()](#closeTable)
    * [deleteTable()](#deleteTable)
    * [getNumTuples()](#getNumTuples)
    * [findFreeSlot()](#findFreeSlot)
    * [insertRecord()](#insertRecord)
    * [deleteRecord()](#deleteRecord)
    * [updateRecord()](#updateRecord)
    * [getRecord()](#getRecord)
    * [startScan()](#startScan)
    * [next()](#next)
    * [closeScan()](#closeScan)
    * [getRecordSize()](#getRecordSize)
    * [createSchema()](#createSchema)
    * [freeSchema()](#freeSchema)
    * [createRecord()](#createRecord)
    * [attrOffset()](#attrOffset)
    * [freeRecord()](#freeRecord)
    * [getAttr()](#getAttr)
    * [setAttr()](#setAttr)
  

## Project Overview <a name = "Project-Overview"></a>
The goal of this assignment is to implement a simple record manager that handles tables with a fixed schema and record size.

## Project Structure <a name = "Project-Structure"></a>
* **record_mgr.c** - Implements interfaces from record_mgr.h file.
* **record_mgr.h** - This header file gives interfaces that record manager should implement. 
* **buffer_mgr.c** - Implements interfaces from buffer_mgr.h file.
* **buffer_mgr.h** - This header file gives interfaces that buffer manager should implement. 
* **buffer_mgr_stat.h** - Defines several functions for outputting buffer or page content to stdout or into a string. 
* **buffer_mgr_stat.c** - Implements functions defined in buffer_mgr_stat.h file.
* **dberror.c** - Defines functions for error handling.
* **dberror.h** - Defines error codes and constants.
* **dt.h**  - Defines constants.
* **test_assign3_1_V2.c** - Defines test cases for record manager.
* **storage_mgr.h** - This header file gives interfaces that storage manager should implement. 
* **storage_mgr.c** - Implements interfaces from storage_mgr.h file.
* **test_helper.h** - Defines macros used in the test cases.
* **rm_serializer.c** - Provide serialization functions.
* **test_expr.c** - Define test cases for expressions.


## How to run this project? <a name="How-to-run-this-project?"></a>
```
$ make

// To execute test cases from test_assign3_1_V2.c
$ make run

// Create binary files for test_expr.c
$ make test_expr

// Execute test_expr.c test cases
$ make run_expr

// To clean object files
$ make clean
```

## Interface Implementation <a name = "Interface-Implementation"></a>
### initRecordManager() <a name = "initRecordManager"></a>
---
* This function initializes the record manager.


### shutdownRecordManager() <a name = "shutdownRecordManager"></a>
---
* It deallocates memory for the metadata manager.  


### addSchemaData() <a name = "addSchemaData"></a>
---
* It is a custom function that adds schema data to the first page of the page file where metadata is stored. 


### createTable() <a name = "createTable"></a>
---
* It creates the file for the record manager and stores metadata on page 0.
* Data stored as metadata: record count, first free page, attribute count, schema detials like name, datatype and, length. 


### readSchema() <a name = "readSchema"></a>
---
* It is a custom function that reads data about schema for the record manager file and returns the schema object. 


### openTable() <a name = "openTable"></a>
---
* This function opens the record manager file and reads metadata that is used for further record manager operations.


### closeTable() <a name = "closeTable"></a>
---
* This function removes record manager data from the buffer pool. 


### deleteTable() <a name = "deleteTable"></a>
---
* This function deletes the record manager file.


### getNumTuples() <a name = "getNumTuples"></a>
---
* This function returns number of records from record manager file.


### findFreeSlot() <a name = "findFreeSlot"></a>
---
* This function scans the entire page file for empty slots and returns the page pointer and slot number.


### insertRecord() <a name = "insertRecord"></a>
---
* This function inserts the record into the empty slot. 


### deleteRecord() <a name = "deleteRecord"></a>
---
* This function deletes the record and makes that empty slot available for the new record. 


### updateRecord() <a name = "updateRecord"></a>
---
* This function updates the record attribute values.


### getRecord() <a name = "getRecord"></a>
---
* This function reads specified record from the record manager file.


### startScan() <a name = "startScan"></a>
---
* This function initializes the variables used for scanning record manager data.

### next() <a name = "next"></a>
---
* This function scans all the records and returns the record that satisfies the given condition.

### closeScan() <a name = "closeScan"></a>
---
* This function deallocates memory for the scan objects.


### getRecordSize() <a name = "getRecordSize"></a>
---
* This is helper function that returns record size.


### createSchema() <a name = "createSchema"></a>
---
* This function initializes all schema-related variables.

### freeSchema() <a name = "freeSchema"></a>
---
* This function deallocates memory for the schema.

### createRecord() <a name = "createRecord"></a>
---
* This function creates and returns dummy record. 

### attrOffset() <a name = "attrOffset"></a>
---
* This function returns attribute offset. 	

### freeRecord() <a name = "freeRecord"></a>
---
* This deallocates memory for the record.  

### getAttr() <a name = "getAttr"></a>
---
* This function returns attribute value from the record of the specified attribute. 

### setAttr() <a name = "setAttr"></a>
---
* This function update attribute value from the record of the specified attribute. 
