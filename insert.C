/*
    Purpose: implement a INSERT query
    @authors: Kohei Tagai (9084551077), Levi Cameron (9081118565),
              Shourya Agrawal (9081614613)
*/

#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// part 6
Record rec;
RID rid;
Status status;
AttrDesc* allAtt;
int actualAttrCnt; 

status = attrCat->getRelInfo(relation, actualAttrCnt,allAtt); // get all attributes of that relation
if (status != OK){
	return status;
}

if (actualAttrCnt != attrCnt){ // relation attribute count not matching given attributes
return OK;
}

// to find total space required
int space = 0;
for (int i = 0; i < attrCnt; i++){
	space += allAtt[i].attrLen;
}

char* newRec = new char[space]; // create a char array with required length

for (int i = 0; i < attrCnt;i++){

	if (attrList[i].attrValue == NULL){ // if any are null, reject
		return OK;
	}

	for (int j = 0; j < attrCnt; j++){ // for every i look for a matching j 
		if (strcmp(attrList[i].attrName, allAtt[j].attrName) == 0){ // if order matches

			if (allAtt[j].attrType == STRING){
				memcpy( (char *) (newRec + allAtt[j].attrOffset), (char *) attrList[i].attrValue, allAtt[j].attrLen);
			}
			else if (allAtt[j].attrType == INTEGER){
				int intval = atoi((char *) attrList[i].attrValue);
				memcpy( (char *) (newRec + allAtt[j].attrOffset), &intval, allAtt[j].attrLen);
			}
			else if (allAtt[j].attrType == FLOAT){
				float floatval = atof((char *) attrList[i].attrValue);
				memcpy( (char *) (newRec + allAtt[j].attrOffset),&floatval, allAtt[j].attrLen );
			}
		} 
	}
}

rec.length = space;
rec.data = (void *) newRec;

InsertFileScan *ins = new InsertFileScan(relation,status);
if (status != OK) return status;

ins->insertRecord(rec,rid);
return OK;

}

