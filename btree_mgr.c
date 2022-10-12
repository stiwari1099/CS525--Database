#include "btree_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "buffer_mgr.h"
#include <stdlib.h>
#include <string.h>
#include<unistd.h>

int inum = 0;
int maxEle;
int minBit = -1;

typedef struct BtreeMgr{
    char * name;
    BM_PageHandle bm_pageHandle;
    SM_FileHandle fileHandle;
    BM_BufferPool bm;
    int btreeOrder;
    int nodeCount;
}BtreeMgr;

typedef struct BTREE{
    int *key;
    struct BTREE **next;
    RID *id;
} BTree;

BtreeMgr *btreeMgr;
BTree *scan = NULL;
BTree *begin = NULL;

RC initIndexManager (void *mgmtData){
    initStorageManager();
    /*printf("##############################################################################\n");
    printf("CS 525 - Programming Assignment 4: Index Manager\n");
    printf("Submitted by:\n");
    printf("Ruturaj Joshi - A20497857\n");
    printf("Prashant Bhutani - A20488431\n");
    printf("Shubham Tiwari - A20499153\n");
    printf("##############################################################################\n");*/
    return RC_OK;
}

RC shutdownIndexManager(){
    begin = NULL;
    scan = NULL;
    return RC_OK;
}

RC createBtree (char *idxId, DataType keyType, int n){

    if(access(idxId, F_OK) == 0){
        printf("Btree is already present!");
        return RC_ERROR;
    }

    int i=0;
    btreeMgr = (BtreeMgr *)malloc(sizeof(BtreeMgr));
    btreeMgr->name = idxId;
    btreeMgr->btreeOrder = n;
    btreeMgr->nodeCount = 0;

    begin = (BTree*)malloc(sizeof(BTree));
    (*begin).id = malloc(sizeof(int) * n);
    (*begin).next = malloc(sizeof(BTree) * (n + 1));
    (*begin).key = malloc(sizeof(int) * n);
    

    while(i < n + 1){
        begin->next[i] = NULL;
        i += 1;
    }

    maxEle = n;

    if(createPageFile(idxId) != RC_OK){
        return RC_ERROR;
    }

    return RC_OK;
}

RC openBtree(BTreeHandle **tree, char *idxId){

    // Check if file is present or not
    if(access(idxId, F_OK) != 0){
        return RC_ERROR;
    }

    btreeMgr->name = idxId;

    if(openPageFile(idxId, &btreeMgr->fileHandle) == RC_OK){
        return RC_OK;
    }

    return RC_ERROR;
}

RC closeBtree (BTreeHandle *tree){

    if(closePageFile(&btreeMgr->fileHandle)==0){

        free(btreeMgr);
        free(begin);
        return RC_OK;
    }

    return RC_ERROR;
}

RC deleteBtree(char *idxId){

    if(access(idxId, F_OK) != 0){
        printf("Btree doesn't exist!");
        return RC_ERROR;
    }

    if(destroyPageFile(idxId) == RC_OK){
        return RC_OK;
    }

    free(btreeMgr);
    free(begin);

    return RC_ERROR;
}


RC getNumNodes(BTreeHandle *tree, int *result){

    BTree *hssrrtkbkc = (BTree*)malloc(sizeof(BTree));
    int numNodes = 0, i = 0;
    hssrrtkbkc = begin;

    while(hssrrtkbkc != NULL){
        hssrrtkbkc = hssrrtkbkc->next[btreeMgr->btreeOrder];
        numNodes += 1;
    }

    numNodes = 0;
    while(i < btreeMgr->btreeOrder + 2){
        numNodes += 1;
        i += 1;
    }

    *result = numNodes;

    return RC_OK;
}

RC getNumEntries(BTreeHandle *tree, int *result){

    int totalEle = 0;
    BTree *hssrrtkbkc = (BTree*)malloc(sizeof(BTree));
    hssrrtkbkc = begin;

    while(hssrrtkbkc != NULL){

        for(int i = 0; i < btreeMgr->btreeOrder; i++){
            if(hssrrtkbkc->key[i] != 0){
                totalEle++;
            }
        }
        hssrrtkbkc = hssrrtkbkc->next[btreeMgr->btreeOrder];
    }

    *result = totalEle;
    return RC_OK;
}

RC getKeyType(BTreeHandle *tree, DataType *result){
    // Since our Btree is only handles integer key values
    *result = DT_INT;
    return RC_OK;
}


RC findKey(BTreeHandle *tree, Value *key, RID *result){

    BTree *hssrrtkbkc = (BTree*)malloc(sizeof(BTree));
    int got = -1, i;
    for (hssrrtkbkc = begin; hssrrtkbkc != NULL; hssrrtkbkc = hssrrtkbkc->next[btreeMgr->btreeOrder]){
            int i = 0;
            while(i<btreeMgr->btreeOrder){
                    if((*hssrrtkbkc).key[i] == key->v.intV) {
                        result->page = hssrrtkbkc->id[i].page;
                        result->slot = hssrrtkbkc->id[i].slot;
                        return RC_OK;
                    }
                i++;
            }
    }

    if(got == -1){
        return RC_IM_KEY_NOT_FOUND;
    }

    return RC_ERROR;
}

RC insertKey(BTreeHandle *tree, Value *key, RID rid){

    int i = 0;
    BTree *node = (BTree*)malloc(sizeof(BTree));
    (*node).next = malloc(sizeof(BTree) * (btreeMgr->btreeOrder + 1));
    (*node).key = malloc(sizeof(int) * btreeMgr->btreeOrder);
    (*node).id = malloc(sizeof(int) * btreeMgr->btreeOrder);
    BTree *hssrrtkbkc = (BTree*)malloc(sizeof(BTree));
    
    
    int nodeFull = 0;
    hssrrtkbkc = begin;

    while(hssrrtkbkc != NULL){
        nodeFull = 0;
        i = 0;
        while(i < btreeMgr->btreeOrder){
            if ((*hssrrtkbkc).key[i] == 0) {
                (*hssrrtkbkc).next[i] = NULL;
                (*hssrrtkbkc).id[i].page = rid.page;
                nodeFull += 1;
                (*hssrrtkbkc).key[i] = key->v.intV;
                (*hssrrtkbkc).id[i].slot = rid.slot;
            
                break;
            }
            i++;
        }
        
        if ((nodeFull == 0) && (hssrrtkbkc->next[btreeMgr->btreeOrder] == NULL)) {
            (*node).next[btreeMgr->btreeOrder] = NULL;
            (*hssrrtkbkc).next[btreeMgr->btreeOrder] = node;
        }

        hssrrtkbkc = hssrrtkbkc->next[btreeMgr->btreeOrder];
    }
    
    hssrrtkbkc = begin;

    while(hssrrtkbkc != NULL){
        i =0;
        while(i < btreeMgr->btreeOrder){
            
            if((*hssrrtkbkc).key[i] != 0){
                btreeMgr->nodeCount += 1;
            }
            i++;
        }

       hssrrtkbkc = hssrrtkbkc->next[btreeMgr->btreeOrder]; 
    }

    return RC_OK;
}

RC deleteKey(BTreeHandle *tree, Value *key){

    BTree *hssrrtkbkc = (BTree*)malloc(sizeof(BTree));

    for (hssrrtkbkc = begin; hssrrtkbkc != NULL; hssrrtkbkc = hssrrtkbkc->next[btreeMgr->btreeOrder]){
        int i=0;

        while(i < btreeMgr->btreeOrder){
            if ((*hssrrtkbkc).key[i] == (*key).v.intV){
                (*hssrrtkbkc).id[i].slot = 0;
                (*hssrrtkbkc).key[i] = 0;
                (*hssrrtkbkc).id[i].page = 0;

                return RC_OK;
            }
            i++;
        }
    }
    
    return RC_ERROR;
}


RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle){

    *handle = (BT_ScanHandle *)malloc(sizeof(BT_ScanHandle));
    BTree *hssrrtkbkc = begin;
    int c = 0, st, d;
    scan = begin;
    inum = 0;
    int dem = 14, pg;
    
    int totalEle = 0;

    while(hssrrtkbkc != NULL){
        int i = 0;
        while(i<btreeMgr->btreeOrder){
            if ((*hssrrtkbkc).key[i] != 0)
                totalEle += 1;
                i += 1;
            }
            hssrrtkbkc = (*hssrrtkbkc).next[btreeMgr->btreeOrder];
    }

    int key[totalEle], swap;
    int sam = 1, res = 12;
    int elements[btreeMgr->btreeOrder][totalEle];
    int count = 0;

    hssrrtkbkc = begin;

    while(hssrrtkbkc != NULL){
            int i = 0;
            while(i < btreeMgr->btreeOrder){
                elements[1][count] = hssrrtkbkc->id[i].slot;
                key[count] = hssrrtkbkc->key[i];
                elements[0][count] = hssrrtkbkc->id[i].page;
                count += 1;
                i += 1;
            }
        hssrrtkbkc = hssrrtkbkc->next[btreeMgr->btreeOrder];
    }
    
    while(c < count - 1){

            for(int d = 0; d < count - c - 1; d += 1){

                if (key[d] > key[d+1]){

                    if(sam == 1){
                        st = elements[1][d];
                        swap = key[d];
                        pg = elements[0][d];
                    }

                    if(res > sam){

                        key[d] = key[d + 1];
                        key[d + 1] = swap;
                        elements[1][d] = elements[1][d + 1];
                        elements[1][d + 1] = st;
                        elements[0][d] = elements[0][d + 1];
                        elements[0][d + 1] = pg;
                    }
                }
            }
        c++;
    }

    count = 0;
    hssrrtkbkc = begin;

    while(hssrrtkbkc != NULL){

        for(int i = 0; i < btreeMgr->btreeOrder; i += 1){
            (*hssrrtkbkc).key[i] = key[count];
            (*hssrrtkbkc).id[i].slot = elements[1][count];
            (*hssrrtkbkc).id[i].page = elements[0][count];

            count += 1;
        }
        hssrrtkbkc = hssrrtkbkc->next[btreeMgr->btreeOrder];
    }

    return RC_OK;
}

RC nextEntry(BT_ScanHandle *handle, RID *result){

    if((*scan).next[btreeMgr->btreeOrder] != NULL){

        if(btreeMgr->btreeOrder == inum){
            
            scan = (*scan).next[btreeMgr->btreeOrder];
            inum = 0;
            result->page = (*scan).id[inum].page;
            (*result).slot = (*scan).id[inum].slot;
            inum += 1;

        } else{

            result->page = (*scan).id[inum].page;
            (*result).slot = (*scan).id[inum].slot;
            inum += 1;
        }

    }else{

        return RC_IM_NO_MORE_ENTRIES;
    }

    return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle){
    handle->mgmtData = NULL;
    free(handle);
    return RC_OK;
}