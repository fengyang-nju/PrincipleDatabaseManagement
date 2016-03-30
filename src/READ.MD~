# The implementation of database project of the class CS222 Principle Data Management, given by Prof. Michael Carey.

## rbf: Record-Based File Manager

### Internal Record Format

The record format is designed as following:

number of fields in this record[size of unsigned] + null-indicator[num_of_attribute/8] + offset_positions[num_of_attribute 4 * byte, offset value starts from the tail of offset_position, the offset_positions specify the end position of the field. If it is not clear enoughm, please refer the first figure of the 21st page of the ppt lecture 2] + [values]

Attention: the offset_position starts from the tail of the offset_position space, and the value should be the offset of END position of the field.

I store the VarChar field as the format that learnt from the class: length_of_charSequence [integer, 4 bytes] + charSequence

### Page Format
Our page format is designed as following:

Record values[as the record format]+ free space + record directory[start slot position[1 byte] + record size in slot [1 byte], total 2 bytes] + NO. of records [1 byte] + next available slot index [1 byte, start from 0]

In my program, the SLOT_SIZE is 32 bytes. So, the maximum number of records thta could be contained in one page should be 120. [4094 = (32 + 2) * 120 + 14]

### Implementation Details
Due to the difference of record format and the api data format, I implemented two public methods to convert the data format, the headers of two methods are as following:
```
  void convertAPIData2StoreFormat(const vector<Attribute> &recordDescriptor, const void *APIData, void* recordData);
  void convertStoreFormat2APIData(const vector<Attribute> &recordDescriptor, const void *recordData, void* APIData);
```

Furthremore, in order to print the record data, I implemented a method to finish this task:
```
  RC printRecordInStoreFormat(const vector<Attribute> &recordDescriptor, const void *recordData);
```

And in order to calculate the length of the two types of data, a utility method is implemented:
```
  unsigned calculateRecordLength(const vector<Attribute> &recordDescriptor, const void *data, DataType type);
```

Attention: the DataType should be the RECORD or API.

## rm: Relation Manager

Meta-data
Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int, delete-flag:int)

### Internal Record Format
Number of fields in this record[size of unsigned] + null-indicator[num_of_attribute/8] + offset_positions[num_of_attribute 4 * byte, offset value starts from the tail of offset_position, the offset_positions specify the end position of the field. If it is not clear enoughm, please refer the first figure of the 21st page of the ppt lecture 2] + [values]

### UPDATE OPERATION: 
1. If the updated record size is larger than the old one, we will firstly check whether the available size of the current page is enough for the updated record or not. If it is not, the updated record will be insert into the file as an totally new record, and the returned rid will be process as following:
	1. delete the current record information.
	2. conduct the this operation: startSlotIndex |= 128, which could change the first bit of startSlotIndex into 1. In this way, we can convert the startSlotIndex into a number less than 0.
	3. save the return rid.pageNum(unsigned) into the startSlotIndex*SLOT_SIZE, and rid.slotNum(unsigned) into the startSlotIndex*SLOT_SIZE + sizeof(unsigned) 
	4. save the changed startSlotIndex back to the RIDList at the tail of page.

2. If the updated record size is larger than the old one, we will firstly check whether the available size of the current page is enough for the updated record or not. If it is not, we just need to move the following record to the proper position and insert the updated data;

3. If the updated record size is less than the old one, we just move the following record to the proper position, and insert the updated record data into the old position to override the old data.

4. After one of the above operation, we will update the RID list and page header information at the tail of page:
	1. Update the slotItem information of rid.slotNum, of which, we just modify the occupiedSlotNumber.
	2. Update the page header information, modify the nextAvailableSlot Index;
	3. Update the  slotItems following rid.slotNum, of which, we need modify the startSlotIndex;  

5. DELETE OPERATION:
We just need to delete the content and move the following record into the proper position, and change the occupied length of the slotItem into 0;

### Page Format

Our page format is designed as following:
Record values[as the record format]+ free space + record directory[start slot position[1 byte] + record size in slot [1 byte], total 2 bytes] + NO. of records [1 byte] + next available slot index [1 byte, start from 0]

### File Format

Our page format is designed as following:
Record values[as the record format]+ free space + record directory[start slot position[1 byte] + record size in slot [1 byte], total 2 bytes] + NO. of records [1 byte] + next available slot index [1 byte, start from 0]
In my program, the SLOT_SIZE is 32 bytes. So, the maximum number of records thta could be contained in one page should be 120. [4094 = (32 + 2) * 120 + 14]

### Implementation Detail
1. Because the difference of the internal format and API format, we defined an enumeration to clarify the two types {RECORD, API}.
2. We also implemented two methods to conver the two types of data;
3. We implement two methods to extract the required attribute value from the two types of data;

## ix: Index Manager

## qe: Query Engine


