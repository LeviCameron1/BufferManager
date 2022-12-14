#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue) // relname.attr
{
Status status;
HeapFileScan scan(relation, status);
if(status != OK) return status;
// Check if input argument is null
	if(attrName[0] == '\0'){
		status = scan.startScan(0,0,STRING,NULL,op);
	}
	else{ // if input is not null get description of the attribute
		AttrDesc attrDesc;
		attrCat->getInfo(relation,attrName,attrDesc);
		
		if(type == FLOAT){// cast attrValue to type float
			const float filter = atof(attrValue);
			const float *fp = &filter;
			memcpy(&attrValue,&fp, sizeof(attrValue));
			status = scan.startScan(attrDesc.attrOffset,attrDesc.attrLen,type,attrValue,op);
		}else if (type == INTEGER){// cast attrValue to type int
			const int filter = atoi(attrValue);
			const int *fp = &filter;
			memcpy(&attrValue,&fp, sizeof(attrValue));
			status = scan.startScan(attrDesc.attrOffset,attrDesc.attrLen,type,attrValue,op);
		}else{//attrValue is type string
			status = scan.startScan(attrDesc.attrOffset,attrDesc.attrLen,type,attrValue,op);
		}
	}

	RID tmpRID;
	// go through the heap file deleting records that are a match
	while (scan.scanNext(tmpRID) == OK)
    {
		status = scan.deleteRecord();
		assert(status == OK);
	}

return OK;
}


