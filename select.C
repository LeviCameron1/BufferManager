/*
    Purpose: implement a SELECT query
    @authors: Kohei Tagai (9084551077), Levi Cameron (9081118565),
              Shourya Agrawal (9081614613)
*/

#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[], // select A
		       const attrInfo *attr, // where 
		       const Operator op, 
		       const char *attrValue) //search value
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
    Status status;
    
    AttrDesc attrDescArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        // get attribute catalog tuple
        status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         attrDescArray[i]);
        if (status != OK) { return status; }
    }
    
    // get AttrDesc structure for the projection
    AttrDesc attrDesc;
    int intAttrValue;
    float floatAttrValue;
    
    if (attr->relName != NULL){
        status = attrCat->getInfo(attr->relName,
                                    attr->attrName,
                                    attrDesc);
        switch (attrDesc.attrType) {
            case INTEGER:
                intAttrValue = atoi(attrValue);
                break;
            case FLOAT:
                floatAttrValue = atof(attrValue);
                break;
        }
    }
    
    // get output record length from attrdesc structures
    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += attrDescArray[i].attrLen;
    }

    switch (attrDesc.attrType) {
        case INTEGER:
            status = ScanSelect(result, projCnt, attrDescArray, &attrDesc,
    							    op, (char*)&intAttrValue, reclen);
            break;
        case FLOAT:
            status = ScanSelect(result, projCnt, attrDescArray, &attrDesc,
    							    op, (char*)&floatAttrValue, reclen);
            break;
        case STRING:
            status = ScanSelect(result, projCnt, attrDescArray, &attrDesc,
    							op, attrValue, reclen);
            break;
    }
    
    return status;
}


/*
 * implements select query
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */
const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[], 
			const AttrDesc *attrDesc,
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
    Status status;
    
    // open the result table
    InsertFileScan resultRel(result, status);
    if (status != OK) { return status; }
    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;
    
    HeapFileScan scan(string(projNames[0].relName), status);
    if (status != OK) { return status; }

    RID tmpRid;
    Record tmpRec;
    
    status = scan.startScan(attrDesc->attrOffset,
    						attrDesc->attrLen,
                            (Datatype) attrDesc->attrType,
                            filter,
                            op);
    if (status != OK) { return status; }

    while (scan.scanNext(tmpRid) == OK)
    {
        status = scan.getRecord(tmpRec);
        ASSERT(status == OK);
        
        // copy data into the output record
        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++)
        {
        		memcpy(outputData + outputOffset,
	                  (char *)tmpRec.data + projNames[i].attrOffset,
	                  projNames[i].attrLen);

        		outputOffset += projNames[i].attrLen;
        }

        RID outRID;
        status = resultRel.insertRecord(outputRec, outRID);
		ASSERT(status == OK);
    }
    return OK;
}

