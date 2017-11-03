package com.axc166930.davisbase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.*;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author avinash
 *
 */
public class RandomAccessFileCreation extends RandomAccessFile {

	File file;
	int pageSize, pageType, recordNum, rootNumberOfRecords, rootPos, recordStartPostion = 8, rowId;
	//End of Page 1
	int posPointer;
	//End of RfTbl Page
	int rafTablePosition;
	//End of RfCol Page
	int rafColumnPosition, recordLength, noOfColumns, pageCount = 1;
	Helper help;
	
	public RandomAccessFileCreation(File file, String mode, int pageSize) throws FileNotFoundException {
		super(file, mode);
		this.file = file;
		this.pageSize = pageSize;
		posPointer = pageSize/2;
		rootPos = pageSize;
		rafTablePosition = pageSize;
		rafColumnPosition = pageSize;
		help = new Helper();
	}

	public RandomAccessFileCreation(String file, String mode, int pageSize) throws FileNotFoundException {
		super(file, mode);
		this.pageSize = pageSize;
		posPointer = pageSize/2;
		rootPos = pageSize;
		rafTablePosition = pageSize;
		rafColumnPosition = pageSize;
		help = new Helper();
	}

	
	public void setPageType(int pageType) throws IOException {
		this.pageType = pageType;
		seek(0);
		writeByte(pageType);
	}

	/*public int getNumberOfRecords() throws IOException {
		System.out.println("Inside" + read());
		return read();
	}*/

	/*public void setNumberOfRecords(int numberOfRecords) {
		this.numberOfRecords = numberOfRecords;
	}*/
	
	/**
	 * Inserting into DavisBase_Tables 
	 * @param tableName
	 * @throws IOException
	 */
	public void insertIntoDavisBaseTable(String tableName) throws IOException {
		
		recordLength = 1 + 20;
		
		//Set Number of records
		seek(1);
		recordNum += 1;
		writeByte(recordNum);
			
		
		rafTablePosition = rafTablePosition - recordLength;
		//System.out.println(pos);
		
		//Set Address of last record inserted
		seek(2);
		writeShort(rafTablePosition);
		
		//Set Address for each record
		seek(recordStartPostion);
		writeShort(rafTablePosition);
		recordStartPostion += 2;
		
		seek(rafTablePosition);
		writeByte(++rowId);
		
		seek(rafTablePosition+1);
		writeBytes(tableName);	
		
		noOfColumns = 2;
	}

	/**
	 * Inserting into DavisBase_Columns
	 * @param tableName
	 * @param columnName
	 * @param data_type
	 * @param columnKey
	 * @param ordinal_position
	 * @param is_nullable
	 * @throws IOException
	 */
	public void insertIntoDavisBaseColumn(String tableName, String columnName,
			String data_type, String columnKey, int ordinal_position, String is_nullable) throws IOException {
		
		//recordLength = 1 + tableName.length() + columnName.length() + data_type.length() + 1 + is_nullable.length() + columnKey.length();
		//Set Number of records
		
		//recordLength = rowId + tableName + columnName + serialCode + columnKey + ordinalPosition + IsNullable
		recordLength = 1 + 20 + 20 + 2 + 20 + 1 + 20; //84 bytes, 2 is for serial code
		
		seek(1);
		//System.out.println(recordNum);
		recordNum += 1;
		writeByte(recordNum);
		
		rafColumnPosition = rafColumnPosition - recordLength;
		//System.out.println(rafColumnPosition);
		
		//Set Address of last record inserted
		seek(2);
		writeShort(rafColumnPosition);
		
		seek(recordStartPostion);
		writeShort(rafColumnPosition);
		recordStartPostion += 2;
		
		seek(rafColumnPosition);
		writeByte(++rowId); //Check if it should be int
		seek(rafColumnPosition+1);
		writeBytes(tableName);
		seek(rafColumnPosition+21);
		writeBytes(columnName);
		seek(rafColumnPosition+41);
		int serialCode = help.getSerialCode(data_type);
		writeShort(serialCode);
		seek(rafColumnPosition+43);
		writeBytes(columnKey);
		seek(rafColumnPosition+63);
		writeByte(ordinal_position);
		seek(rafColumnPosition+64);
		writeBytes(is_nullable);
		
		noOfColumns = 7;
	}
	
	/* void insertUserTableEntry(String tableName) throws IOException {
		recordLength = 1 + tableName.length();
		
		seek(1);
		writeByte(++numberOfRecords);
		
		pos = pos - recordLength;
		System.out.println(pos);
		
		
		seek(2);
		writeShort(pos);
		
		//Set Address for each record
		seek(recordStartPostion);
		writeShort(pos);
		recordStartPostion += 2;
		
		
		seek(pos);
		writeByte(++rowId);
		writeBytes(tableName);
	}*/

	/**
	 * Insert into Tables created by user
	 * @param tableName
	 * @param columnDetails
	 * @throws IOException
	 */
	public void insertUserColumsEntry(String tableName, String columnDetails) throws IOException {
		columnDetails = columnDetails.substring(0, columnDetails.length()-1);
		String columns[] = columnDetails.split(",");
		
		//recordLength = 0;
		for (int i = 0; i < columns.length; i++) {
			//colname, data_type, pri, nullable
			seek(1);
			int numberOfRecords = readByte();
			seek(1);
			writeByte(++numberOfRecords);
			++this.recordNum;
			
			rafColumnPosition = rafColumnPosition - recordLength;
			
			//Set Address for each record
			seek(recordStartPostion);
			writeShort(rafColumnPosition);
			recordStartPostion += 2;
			
			seek(rafColumnPosition);
			//writeByte(++rowId);
			writeByte(numberOfRecords);
			seek(rafColumnPosition+1);
			writeBytes(tableName);
			seek(rafColumnPosition+21);
			
			String temp[] = new String[4];
			
			String temp1[] = columns[i].split(" ");
			
			for (int j = 0; j < temp1.length; j++) {
				temp[j] = temp1[j];
			}
			
			writeBytes(temp[0]); //ColumnName
			seek(rafColumnPosition+41);
			
			int serialCode = help.getSerialCode(temp[1]);
			writeShort(serialCode); //DataType
			seek(rafColumnPosition+43);
			
			if(temp[2] != null && temp[2].equalsIgnoreCase("PRI"))
				writeBytes(temp[2]); //ColumnKey
			else {
				writeBytes("");
				temp[3] = temp[2];
			}
			seek(rafColumnPosition+63);
			
			writeByte(i+1);						  //OrdinalPosition
			seek(rafColumnPosition+64);
			
			if(temp[3] != null)
				writeBytes(temp[3]); //IsNullable
			else
				writeBytes("");
		}
		
		seek(2);
		writeShort(rafColumnPosition);
	}
	
	/*public int getSerialCode(String data_type) {
		int serialCode = 0;
		if(data_type.equalsIgnoreCase("int"))
        {
            //record_size=record_size+4;
            serialCode=0x06;
        }
        else if(data_type.equalsIgnoreCase("tinyint"))
        {
            //record_size=record_size+1;
            serialCode=0x04;
        }
        else if(data_type.equalsIgnoreCase("smallint"))
        {
            //record_size=record_size+2;
            serialCode=0x05;
        }
        else if(data_type.equalsIgnoreCase("bigint"))
        {
            //record_size=record_size+8;
            serialCode=0x07;
        }
        else if(data_type.equalsIgnoreCase("real"))
        {
            //record_size=record_size+4;
            serialCode=0x08;
        }
        else if(data_type.equalsIgnoreCase("double"))
        {
            //record_size=record_size+8;
            serialCode=0x09;
        }
        else if(data_type.equalsIgnoreCase("datetime"))
        {
            //record_size=record_size+8;
            serialCode=0x0A;
        }
        else if(data_type.equalsIgnoreCase("date"))
        {
            //record_size=record_size+8;
            serialCode=0x0B;
        }
        else if(data_type.equalsIgnoreCase("text"))
        {
            //record_size=record_size+8;
            serialCode=0x0C;
        }
		
		return serialCode;
	}

	public String getSerialCodeAsString(int SerialCode) {
		String data_type = "";
		if(SerialCode == 6)
        {
            //record_size=record_size+4;
			data_type="int";
        }
        else if(SerialCode == 4)
        {
            //record_size=record_size+1;
        	data_type="tinyint";
        }
        else if(SerialCode == 5)
        {
            //record_size=record_size+2;
        	data_type="smallint";
        }
        else if(SerialCode == 7)
        {
            //record_size=record_size+8;
        	data_type="bigint";
        }
        else if(SerialCode == 8)
        {
            //record_size=record_size+4;
        	data_type="real";
        }
        else if(SerialCode == 9)
        {
            //record_size=record_size+8;
        	data_type= "double";
        }
        else if(SerialCode == 10)
        {
            //record_size=record_size+8;
        	data_type="datetime";
        }
        else if(SerialCode == 11)
        {
            //record_size=record_size+8;
        	data_type="date";
        }
        else if(SerialCode == 12)
        {
            //record_size=record_size+8;
        	data_type="text";
        }
		
		return data_type;
	}*/
	

	/*private int getRecordLengthForDataType(String data_type) {
		int record_size = 0;
		if(data_type.equalsIgnoreCase("int"))
        {
            record_size=record_size+4;
            //serialCode=0x06;
        }
        else if(data_type.equalsIgnoreCase("tinyint"))
        {
            record_size=record_size+1;
            //serialCode=0x04;
        }
        else if(data_type.equalsIgnoreCase("smallint"))
        {
            record_size=record_size+2;
            //serialCode=0x05;
        }
        else if(data_type.equalsIgnoreCase("bigint"))
        {
            record_size=record_size+8;
            //serialCode=0x07;
        }
        else if(data_type.equalsIgnoreCase("real"))
        {
            record_size=record_size+4;
            //serialCode=0x08;
        }
        else if(data_type.equalsIgnoreCase("double"))
        {
            record_size=record_size+8;
            //serialCode=0x09;
        }
        else if(data_type.equalsIgnoreCase("datetime"))
        {
            record_size=record_size+8;
            //serialCode=0x0A;
        }
        else if(data_type.equalsIgnoreCase("date"))
        {
            record_size=record_size+8;
            //serialCode=0x0B;
        }
        else if(data_type.equalsIgnoreCase("text"))
        {
            record_size=record_size+20;
            //serialCode=0x0C;
        }
		
		return record_size;
	}*/

	
	/**
	 * Inserting the records into the file
	 * @param tableName
	 * @param columnList
	 * @param columnValues
	 * @param rfTbl
	 * @param rfCol
	 * @param tree
	 * @throws IOException
	 */
	public void insertRecordIntoTable(String tableName, String[] columnList, String[] columnValues, RandomAccessFileCreation rfTbl, RandomAccessFileCreation rfCol, BPlusTree tree) throws IOException {
		
		int numberOfRecords;
		if(pageCount == 1) {
			seek(1);
			numberOfRecords = readByte();
		} else {
			seek(pageSize + (pageCount-2)*512 + 1);
			numberOfRecords = readByte();
		}
		
		if(((pageSize/2) - ((numberOfRecords*recordLength)+8+(numberOfRecords*2)))> recordLength) {
			//Do the normal insert
		} 
		else //Overflow
		{
			//Creating space for right child
			setLength(pageSize + pageCount*512);
			
			//First overflow
			if(pageCount == 1) 
			{
				//Left child pointing to right child
				seek(4);
				writeInt(pageSize);
				
				//Right child pointing to FFFF
				seek(pageSize + (pageCount-1)*512 + 4);
				//writeInt(Integer.MAX_VALUE);
				writeInt(-1);
			} 
			else 
			{
				//Left child pointing to right child
				seek(pageSize + (pageCount-2)*512 + 4);
				writeInt(pageSize + (pageCount-1)*512);
				
				//Right child pointing to FFFF
				seek(pageSize + (pageCount-1)*512 + 4);
				//writeInt(Integer.MAX_VALUE);
				writeInt(-1);
			}	
			
			//Writing the key where split occurs to the root
			seek(posPointer);
			int rootKey = readInt();
			//System.out.println("Root Key for Split: " + rootKey);
			
			int end = rootPos-1;
			rootPos = end - 4;
			seek(rootPos);
			writeInt(rootKey);
			
			//Increasing the number of records in root
			seek(512+1);
			write(++rootNumberOfRecords);
			
			//Point pos to the end of new page
			posPointer = pageSize + pageCount*512;
			
			//Set Page Type of new leaf page
			//seek(pos-(countPages*512));
			seek(pageSize+(pageCount-1)*512);
			writeByte(13);
			
			//Increment number of pages
			pageCount++;
			
			//Reset numberOfRecords to zero for the newly created page
			recordNum = 0;
			
			recordStartPostion = pageSize + (pageCount - 2)*512 + 8;
			
			
		}
			
			
		if(pageCount == 1) {
			rfCol.seek(2);
			int position = rfCol.readShort() + 1; // this position will give us the table name
			rfCol.seek(position);
			String referenceTableLine = rfCol.readLine().substring(0, 20);
			
			//System.out.println("Our table: " + tableName);
			//System.out.println("Reference Table name: " + referenceTableLine);
			
			int k = 1;
			while(!referenceTableLine.contains(tableName)) { //readLine gives us the table name	
				position = position + 84*(k);
				rfCol.seek(position);
				referenceTableLine = rfCol.readLine().substring(0, 20);
				//System.out.println("Inside Reference Table name: " + referenceTableLine);
				//k++;
			}

			if(!( (columnList.length == 1) && (columnList[0].equals("")) )) {
				boolean flagForPrimaryKey = checkIfPrimaryKeyIsPresent(columnList, rfCol, position);
				if(!flagForPrimaryKey){
					System.out.println("Primary key not present");
					return;
				}
			}
			
			if(!( (columnList.length == 1) && (columnList[0].equals("")) )) {
				int flagForNotNullableKey = checkIfNotNullableKeyIsPresent(columnList, columnValues, rfCol, position);
				if(flagForNotNullableKey == 0){
					System.out.println("Not Nullable Column missing");
					return;
				}else if(flagForNotNullableKey == 2){
					System.out.println("Incorrect value for not nullable column");
					return;
				}
			}
			
			
			boolean testUniqueness = isPrimaryKeyUnique(Integer.parseInt(columnValues[0]));
			if(!testUniqueness) {
				System.out.println("Key is not unique");
				return;
			}
			
			seek(1);
			int numbOfRecords = readByte();
			seek(1);
			writeByte(++numbOfRecords);
			++this.recordNum;
			
			for(int i = columnValues.length - 1; i >= 0; i--){
				
				rfCol.seek(position + 40);
				String data_type = help.getSerialCodeAsString(rfCol.readShort());
				//System.out.println(data_type);
				
				if(data_type.equalsIgnoreCase("int")){
					posPointer = posPointer - 4;
					seek(posPointer);
					if("null".equalsIgnoreCase(columnValues[i]))
						writeInt(0);
					else
						writeInt((Integer.parseInt(columnValues[i])));
					//seek(pos+4);
				} 
				else if(data_type.equalsIgnoreCase("tinyint"))
		        {
					posPointer = posPointer - 1;
					seek(posPointer);
					if("null".equalsIgnoreCase(columnValues[i]))
						writeByte(0);
					else
						writeByte((Integer.parseInt(columnValues[i])));
					
		        }
		        else if(data_type.equalsIgnoreCase("smallint"))
		        {
		        	posPointer = posPointer - 2;
		        	seek(posPointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
						writeShort(0);
					else
						writeShort((Short.parseShort(columnValues[i])));
					
		        }
		        else if(data_type.equalsIgnoreCase("bigint"))
		        {
		        	posPointer = posPointer - 8;
		        	seek(posPointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
						writeLong(0);
					else
						writeLong((Long.parseLong(columnValues[i])));
					
		        }
		        else if(data_type.equalsIgnoreCase("real"))
		        {
		        	posPointer = posPointer - 4;
		        	seek(posPointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
						writeFloat(0);
					else
						writeFloat((Float.parseFloat(columnValues[i])));
					
		        }
		        else if(data_type.equalsIgnoreCase("double"))
		        {
		        	posPointer = posPointer - 8;
		        	seek(posPointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
		        		writeDouble(0);
					else
						writeDouble((Double.parseDouble(columnValues[i])));
		        }
		        else if(data_type.equalsIgnoreCase("datetime"))
		        {
		        	posPointer = posPointer - 8;
		        	seek(posPointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
		        		writeLong(0);
		        	else {
		        		/*String dateParams[] = columnValues[i].split("-");
		        		ZoneId zoneId = ZoneId.of( "America/Chicago");
		        		
		        		 Convert date and time parameters for 1974-05-27 to a ZonedDateTime object 
		        		
		        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
		        		 ZonedDateTime toLocalDate() method will display in a simple format 
		        		System.out.println(zdt.toLocalDate()); */
		        		
		        		/* Convert a ZonedDateTime object to epochSeconds
		        		 * This value can be store 8-byte integer to a binary
		        		 * file using RandomAccessFile writeLong()
		        		 */
		        		long epochSeconds = help.toLong(columnValues[i]);
		        		writeLong ( epochSeconds );
		        		
		        	}
		        		
					
		        }
		        else if(data_type.equalsIgnoreCase("date"))
		        {
		        	posPointer = posPointer - 8;
		        	seek(posPointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
		        		writeLong(0);
		        	else {
		        		/*String dateParams[] = columnValues[i].split("-");
		        		ZoneId zoneId = ZoneId.of( "America/Chicago");
		        		
		        		 Convert date and time parameters for 1974-05-27 to a ZonedDateTime object 
		        		
		        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
		        		 ZonedDateTime toLocalDate() method will display in a simple format 
		        		System.out.println(zdt.toLocalDate()); */
		        		
		        		/* Convert a ZonedDateTime object to epochSeconds
		        		 * This value can be store 8-byte integer to a binary
		        		 * file using RandomAccessFile writeLong()
		        		 */
		        		long epochSeconds = help.toLong(columnValues[i]);
		        		writeLong ( epochSeconds );
		        		
		        	}
					
		        }
		        else if(data_type.equalsIgnoreCase("text"))
		        {
		        	posPointer = posPointer - 20;
		        	seek(posPointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
		        		writeBytes("");
		        	else
		        		writeBytes(columnValues[i]);
					
		        }
				position = position + 84;
				
				}
				//Update Byte 2 in new table
				seek(2);
				writeShort(posPointer);
				
				seek(recordStartPostion);
				writeShort(posPointer);
				recordStartPostion += 2;
				}
			else //Overflow Insert
			{
				rfCol.seek(2);
				int position = rfCol.readShort() + 1; // this position will give us the table name
				rfCol.seek(position);
				String referenceTableLine = rfCol.readLine().substring(0, 20);
				
				//System.out.println("Our table: " + tableName);
				//System.out.println("Reference Table name: " + referenceTableLine);
				
				//tableName.equalsIgnoreCase(referenceTableLine)
				int k = 1;
				while(!referenceTableLine.contains(tableName)) { //readLine gives us the table name	
					position = position + 84*(k);
					rfCol.seek(position);
					referenceTableLine = rfCol.readLine().substring(0, 20);
					//System.out.println("Inside Reference Table name: " + referenceTableLine);
					//k++;
				}

				if(((columnList.length == 1) && (columnList[0].equals("")))) {
					boolean flagForPrimaryKey = checkIfPrimaryKeyIsPresent(columnList, rfCol, position);
					if(!flagForPrimaryKey){
						System.out.println("Primary key not present");
						return;
					}
				}
				
				if(((columnList.length == 1) && (columnList[0].equals("")))) {
					int flagForNotNullableKey = checkIfNotNullableKeyIsPresent(columnList, columnValues, rfCol, position);
					if(flagForNotNullableKey == 0){
						System.out.println("Not Nullable Column missing");
						return;
					}else if(flagForNotNullableKey == 2){
						System.out.println("Incorrect value for not nullable column");
						return;
					}
				}
				
				
				boolean testUniqueness = isPrimaryKeyUnique(Integer.parseInt(columnValues[0]));
				if(!testUniqueness) {
					System.out.println("Key is not unique");
					return;
				}
				
				seek(pageSize + (pageCount- 2)*512 + 1);
				int n = readByte();
				seek(pageSize + (pageCount - 2)*512 + 1);
				writeByte(++n);
				
				for(int i = columnValues.length - 1; i >= 0; i--){
					
					//int position = rfCol.readShort() + (84*(columnValues.length-(i+1)) + 41); 		//columnnumbr*50 + 41 to get serial code of data type
					
					rfCol.seek(position + 40);
					String data_type = help.getSerialCodeAsString(rfCol.readShort());
					//System.out.println(data_type);
					
					if(data_type.equalsIgnoreCase("int")){
						posPointer = posPointer - 4;
						seek(posPointer);
						if("null".equalsIgnoreCase(columnValues[i]))
							writeInt(0);
						else
							writeInt((Integer.parseInt(columnValues[i])));
						//seek(pos+4);
					} 
					else if(data_type.equalsIgnoreCase("tinyint"))
			        {
						posPointer = posPointer - 1;
						seek(posPointer);
						if("null".equalsIgnoreCase(columnValues[i]))
							writeByte(0);
						else
							writeByte((Integer.parseInt(columnValues[i])));
						
			        }
			        else if(data_type.equalsIgnoreCase("smallint"))
			        {
			        	posPointer = posPointer - 2;
			        	seek(posPointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
							writeShort(0);
						else
							writeShort((Short.parseShort(columnValues[i])));
						
			        }
			        else if(data_type.equalsIgnoreCase("bigint"))
			        {
			        	posPointer = posPointer - 8;
			        	seek(posPointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
							writeLong(0);
						else
							writeLong((Long.parseLong(columnValues[i])));
						
			        }
			        else if(data_type.equalsIgnoreCase("real"))
			        {
			        	posPointer = posPointer - 4;
			        	seek(posPointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
							writeFloat(0);
						else
							writeFloat((Float.parseFloat(columnValues[i])));
						
			        }
			        else if(data_type.equalsIgnoreCase("double"))
			        {
			        	posPointer = posPointer - 8;
			        	seek(posPointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
			        		writeDouble(0);
						else
							writeDouble((Double.parseDouble(columnValues[i])));
			        }
			        else if(data_type.equalsIgnoreCase("datetime"))
			        {
			        	posPointer = posPointer - 8;
			        	seek(posPointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
			        		writeBytes("");
			        	else
			        		writeBytes(columnValues[i]);
						
			        }
			        else if(data_type.equalsIgnoreCase("date"))
			        {
			        	posPointer = posPointer - 8;
			        	seek(posPointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
			        		writeBytes("");
			        	else
			        		writeBytes(columnValues[i]);
						
			        }
			        else if(data_type.equalsIgnoreCase("text"))
			        {
			        	posPointer = posPointer - 20;
			        	seek(posPointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
			        		writeBytes("");
			        	else
			        		writeBytes(columnValues[i]);
						
			        }
					position = position + 84;
					
				}
				//Update Byte 2 in new table
				seek(pageSize + (pageCount - 2)*512 + 2);
				writeShort(posPointer);
				
				//Update position of each record
				seek(recordStartPostion);
				writeShort(posPointer);
				recordStartPostion += 2;
			}
			
		} 

	
	/**
	 * Checking the null key is present in the command
	 * @param columnList
	 * @param columnValues
	 * @param rfCol
	 * @param position
	 * @return
	 * @throws IOException
	 */
	private int checkIfNotNullableKeyIsPresent(String[] columnList, String[] columnValues, RandomAccessFileCreation rfCol, int position) throws IOException {
		
		for (int i = noOfColumns-1; i >= 0; i--) {
			//boolean flag = false;
			int status = 0;
			rfCol.seek(position + 63);
			String referenceNotNullableColumnName = null;
			String referenceKey = rfCol.readLine().substring(0, 20);
			//System.out.println("Reference Is Nullable: " + referenceKey);
			
			if(referenceKey != null && referenceKey.contains("no")){
				rfCol.seek(position + 20);
				referenceNotNullableColumnName = rfCol.readLine().substring(0, 20);
				//System.out.println("column details: "+referenceNotNullableColumnName);
				
				
				for (int j = 0; j < columnList.length; j++) {
					if(referenceNotNullableColumnName != null && referenceNotNullableColumnName.contains(columnList[j])) {
						//flag = true;
						status = 1;
						if("null".equalsIgnoreCase(columnValues[j])) {
							System.out.println("Null value is present for not nullable key");
							status = 2;
						}	
							
					}
				}
				if(status == 0 || status == 2){
					return status;
				}
						
			}
			position = position + 84;
		}
		return 1;
	}	


	/**
	 * Checking if the Primary key is unique
	 * @param columnList
	 * @param rfCol
	 * @param position
	 * @return
	 * @throws IOException
	 */
	private boolean checkIfPrimaryKeyIsPresent(String[] columnList, RandomAccessFileCreation rfCol, int position) throws IOException {
		boolean flag = false;
		for (int i = noOfColumns-1; i >= 0; i--) {
			rfCol.seek(position + 42);
			String referencePrimaryColumnName = null;
			String referenceKey = rfCol.readLine().substring(0, 20);
			//System.out.println("Reference: " + referenceKey);
			
			if(referenceKey != null && referenceKey.contains("pri")){
				rfCol.seek(position + 20);
				referencePrimaryColumnName = rfCol.readLine().substring(0, 20);
				//System.out.println("column details: "+referencePrimaryColumnName);
				
				for (int j = 0; j < columnList.length; j++) {
					if(referencePrimaryColumnName != null && referencePrimaryColumnName.contains(columnList[j])) {
						return true;
					}
				}
						
			}
			position = position + 84;
		}
		return flag;
	}

	/**
	 * Determining the records from the table for Select query
	 * @param tableName
	 * @param wildCard
	 * @param deciding_col
	 * @param operator
	 * @param comp_val
	 * @param rfTbl
	 * @param rfCol
	 * @param column8
	 * @param column9
	 * @param column10
	 * @param column11
	 * @throws IOException
	 */
	public void queryFromTable(String tableName, String wildCard, String[] deciding_col, String[] operator, String[] comp_val, String[] comparator, RandomAccessFileCreation rfTbl, RandomAccessFileCreation rfCol, String column8, String column9, String column10, String column11) throws IOException {

		//Find the position where the table name exists in davisbase_columns table
		rfCol.seek(2);
		int position = rfCol.readShort() + 1; // this position will give us the table name
		rfCol.seek(position);
		String referenceTableLine = rfCol.readLine().substring(0, 20);
		
		//System.out.println("Our table: " + tableName);
		//System.out.println("Reference Table name: " + referenceTableLine);
		
		int k = 1;
		while(!referenceTableLine.contains(tableName)) { //readLine gives us the table name	
			position = position + 84*(k);
			rfCol.seek(position);
			referenceTableLine = rfCol.readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + referenceTableLine);
			//k++;
		}
		
		int pageToBeProcessed = 1;
		while(pageToBeProcessed <= pageCount) {
			if(pageToBeProcessed == 1) 
			{
				seek(1);
				int numberOfRecords = readByte();
				helperMethodForSelect(pageToBeProcessed, numberOfRecords, wildCard, deciding_col, operator, comp_val, comparator, rfTbl, rfCol, position, column8, column9, column10, column11);
			}
			else
			{
				seek(pageSize + (pageToBeProcessed - 2)*512 + 1);
				int numberOfRecords = readByte();
				helperMethodForSelect(pageToBeProcessed, numberOfRecords, wildCard, deciding_col, operator, comp_val, comparator, rfTbl, rfCol, position, column8, column9, column10, column11);
			}
			pageToBeProcessed++;
		}
			
	}


	/**
	 * Helper to fetch the records from the table for select command
	 * @param pageToBeProcessed
	 * @param numberOfRecords
	 * @param wildCard
	 * @param deciding_col
	 * @param operator
	 * @param comp_val
	 * @param rfTbl
	 * @param rfCol
	 * @param position
	 * @param column8
	 * @param column9
	 * @param column10
	 * @param column11
	 * @throws IOException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void helperMethodForSelect(int pageToBeProcessed, int numberOfRecords, String wildCard, String[] deciding_col, String[] operator, String[] comp_val, String[] comparator, RandomAccessFileCreation rfTbl, RandomAccessFileCreation rfCol, int position, String column8, String column9, String column10, String column11) throws IOException {
				//Get number of records in table
				//seek(1);
				//int numberOfRecords = readByte();
				ArrayList[] list = new ArrayList[noOfColumns];
				
				String data_type = null;
				int recordStartPosition = 8;
				int numberOfBytes = 0;
				int sum = 0;
				
				String columnNames[] = new String[noOfColumns];
				Map<String, Integer> mapOfOrdinalPostions = new HashMap<String, Integer>();
				Map<String, String> mapOfDataTypes = new HashMap<String, String>();
				
				for (int i = noOfColumns-1; i >= 0; i--) {
					//Get column name
					rfCol.seek(position+20);
					String columnName = rfCol.readLine().substring(0, 20).trim();
					columnNames[i] = columnName;
					
					//Get ordinal position
					rfCol.seek(position+62);
					int ordinalPosition = rfCol.readByte();
					
					//Store it in a map
					mapOfOrdinalPostions.put(columnName, ordinalPosition);
					
					//Get the data type
					rfCol.seek(position + 40);
					data_type = help.getSerialCodeAsString(rfCol.readShort());
					//System.out.println(data_type);
					
					//Store it in a map
					mapOfDataTypes.put(columnName, data_type);
					
					//Create a empty list based on data type
					list[i] = help.getArrayList(data_type);
					
					int temp = numberOfRecords;	
					numberOfBytes = help.getRecordLengthForDataType(data_type);
					sum = sum + numberOfBytes;
					
					int start;
					if(pageToBeProcessed == 1) {
						seek(recordStartPosition);
						start = readShort() + recordLength - sum; 	//recordLength - numberOfBytes
						seek(start);
					} else {
						seek(pageSize + (pageToBeProcessed - 2)*512 + recordStartPosition);
						start = readShort() + recordLength - sum; 	//recordLength - numberOfBytes
						seek(start);
					}
					
					while(temp != 0) {
						
						if(data_type.equalsIgnoreCase("int"))
						{
							list[i].add(readInt());
						} 
						else if(data_type.equalsIgnoreCase("tinyint"))
				        {
							list[i].add(readByte());					
				        }
				        else if(data_type.equalsIgnoreCase("smallint"))
				        {
				        	list[i].add(readShort());
				        }
				        else if(data_type.equalsIgnoreCase("bigint"))
				        {
				        	list[i].add(readLong());
				        }
				        else if(data_type.equalsIgnoreCase("real"))
				        {
				        	list[i].add(readFloat());
				        }
				        else if(data_type.equalsIgnoreCase("double"))
				        {
				        	list[i].add(readDouble());
				        }
				        else if(data_type.equalsIgnoreCase("datetime"))
				        {	
				        	
				        	list[i].add(readLong());
				        }
				        else if(data_type.equalsIgnoreCase("date"))
				        {
				        	list[i].add(readLong());
						 }
				        else if(data_type.equalsIgnoreCase("text"))
				        {
				        	//if((temp == numberOfRecords) && i == (numberOfColumns-1)) {
				        		//list[i].add(readLine());	
				        	//} else {
				        		list[i].add(readLine().substring(0, 20).trim());
				        	//}
				        }
						
						start = start - recordLength;
						seek(start);
						temp--;
					}
					position = position + 84;	
				}
				
				//Print Lists
				ArrayList<Integer> index = new ArrayList<>();
				Integer[] printer = new Integer[numberOfRecords];
				int flagforprint = 0;
				if(deciding_col.length != 0) {
					for(int y = 0; y < deciding_col.length; y++) {
						int deciding_ordinalPosition = mapOfOrdinalPostions.get(deciding_col[y]);
						String deciding_dataType = mapOfDataTypes.get(deciding_col[y]);
						ArrayList deciding_List = list[deciding_ordinalPosition-1];
						for (int j = 0; j < deciding_List.size(); j++) {
							if("<".equals(operator[y])) 
							{
								if(deciding_dataType.equalsIgnoreCase("int")) 
								{
									if((Integer)deciding_List.get(j) < Integer.parseInt(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else if(printer[j]==0 && comparator[y].equalsIgnoreCase("and"))
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
								} 
								else if(deciding_dataType.equalsIgnoreCase("tinyint"))
						        {
									if((Byte)deciding_List.get(j) < Byte.parseByte(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}					
						        }
						        else if(deciding_dataType.equalsIgnoreCase("smallint"))
						        {
						        	if((Short)deciding_List.get(j) < Short.parseShort(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("bigint"))
						        {
						        	if((Long)deciding_List.get(j) < Long.parseLong(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("real"))
						        {
						        	if((Float)deciding_List.get(j) < Float.parseFloat(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("double"))
						        {
						        	if((Double)deciding_List.get(j) < Double.parseDouble(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("datetime"))
						        {	
					        		long epochSeconds = help.toLong(comp_val[y]);
						        	
						        	if((Long)deciding_List.get(j) < epochSeconds){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        } 
						        else if(deciding_dataType.equalsIgnoreCase("date")) 
						        {
					        		long epochSeconds = help.toLong(comp_val[y]);
						        	
						        	if((Long)deciding_List.get(j) < epochSeconds){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
				
							} 
							else if(">".equals(operator[y]))
							{
								if(deciding_dataType.equalsIgnoreCase("int")) 
								{
									if((Integer)deciding_List.get(j) > Integer.parseInt(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
								} 
								else if(deciding_dataType.equalsIgnoreCase("tinyint"))
						        {
									if((Byte)deciding_List.get(j) > Byte.parseByte(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}					
						        }
						        else if(deciding_dataType.equalsIgnoreCase("smallint"))
						        {
						        	if((Short)deciding_List.get(j) > Short.parseShort(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("bigint"))
						        {
						        	if((Long)deciding_List.get(j) > Long.parseLong(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("real"))
						        {
						        	if((Float)deciding_List.get(j) > Float.parseFloat(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("double"))
						        {
						        	if((Double)deciding_List.get(j) > Double.parseDouble(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("datetime"))
						        {	
					        		long epochSeconds = help.toLong(comp_val[y]);
						        	
						        	if((Long)deciding_List.get(j) > epochSeconds){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        } 
						        else if(deciding_dataType.equalsIgnoreCase("date")) 
						        {
					        		long epochSeconds = help.toLong(comp_val[y]);
						        	
						        	if((Long)deciding_List.get(j) > epochSeconds){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
							}
							else 
							{ // = operator
								if(deciding_dataType.equalsIgnoreCase("int")) 
								{
									if((Integer)deciding_List.get(j) == Integer.parseInt(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else if(printer[j]==0 && comparator[y].equalsIgnoreCase("and"))
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
								} 
								else if(deciding_dataType.equalsIgnoreCase("tinyint"))
						        {
									if((Byte)deciding_List.get(j) == Byte.parseByte(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}					
						        }
						        else if(deciding_dataType.equalsIgnoreCase("smallint"))
						        {
						        	if((Short)deciding_List.get(j) == Short.parseShort(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("bigint"))
						        {
						        	if((Long)deciding_List.get(j) == Long.parseLong(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("real"))
						        {
						        	if((Float)deciding_List.get(j) == Float.parseFloat(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("double"))
						        {
						        	if((Double)deciding_List.get(j) == Double.parseDouble(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("datetime"))
						        {	
					        		long epochSeconds = help.toLong(comp_val[y]);
						        	
						        	if((Long)deciding_List.get(j) == epochSeconds){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        } 
						        else if(deciding_dataType.equalsIgnoreCase("date")) 
						        {
					        		long epochSeconds = help.toLong(comp_val[y]);
						        	
						        	if((Long)deciding_List.get(j) == epochSeconds){
										index.add(j);
										if(printer[j] == null) {
											flagforprint = 1;
											printer[j] = 1;
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
						        }
						        else if(deciding_dataType.equalsIgnoreCase("text")) {
									if(((String)deciding_List.get(j)).equals(comp_val[y])){
										index.add(j);
										if(printer[j] == null) {
											if(printer[j]==null  && comparator[y] != null && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 0;
											}else {
												flagforprint = 1;
												printer[j] = 1;
											}
										}
										else {
											if(printer[j]==1 && comparator[y].equalsIgnoreCase("and")) {
												printer[j] = 1;
												flagforprint = 1;
											}
											else if(printer[j]==0 && comparator[y].equalsIgnoreCase("and"))
												printer[j] = 0;
											if(comparator[y].equalsIgnoreCase("or")) {
												printer[j] = 1;
												flagforprint = 1;
											}
										}
									}
								}
							}
							
							if(flagforprint == 0 && comparator[y]!= null && comparator[y].equalsIgnoreCase("and")) {
								printer[j] = 0;
							}else 
								flagforprint = 0;
						}
						
						
					}
					
					if(!("*".equals(wildCard))) {
						String selectColumnNames[] = wildCard.split(",");
						if(pageToBeProcessed == 1) {
							for (int j = 0; j < selectColumnNames.length; j++) {
								System.out.print(selectColumnNames[j] + " ");
							}
							System.out.println();
						}
						
						if(deciding_col.length != 0) {
							
							int m = 0, flag = 0;
							while(m < index.size()){
								for (int z = 0; z < selectColumnNames.length; z++) {
									int temp = mapOfOrdinalPostions.get(selectColumnNames[z]) - 1;
									if(mapOfDataTypes.get(selectColumnNames[z]).equalsIgnoreCase("datetime") || mapOfDataTypes.get(selectColumnNames[z]).equalsIgnoreCase("date"))
									{
										
										/* Define the time zone for Dallas CST */
							        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
							        	
							        	/* Converst Epoch Seconds back to a new ZonedDateTime object 
							        	 * First use RandomAccessFile readLong() to retrieve 8-byte
							        	 * integer from table file, then...
							        	 */
							        	long retreivedEpochSeconds = (long) list[temp].get(index.get(m));
							        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
							        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );
							        	
							        	//list[i].add(zdt2.toLocalDate());
										System.out.print(zdt2.toLocalDate() + " ");
									}
									else
									{
										for( int d =0; d<printer.length; d++) {
											if(printer[d]!= null && printer[d] == 1) {
												System.out.print(list[temp].get(index.get(m)) + " ");
												flag = 1;
											}
										}
									}
										
								}
								if(flag == 1) {
									System.out.println();
									flag = 0;
								}
								m++;
							}
						} else 
						  {
							
							int m = 0, flag = 0;
							while(m < numberOfRecords){
								for (int z = 0; z < selectColumnNames.length; z++) {
									int temp = mapOfOrdinalPostions.get(selectColumnNames[z]) - 1;
									if(mapOfDataTypes.get(selectColumnNames[z]).equalsIgnoreCase("datetime") || mapOfDataTypes.get(selectColumnNames[z]).equalsIgnoreCase("date"))
									{
										
										/* Define the time zone for Dallas CST */
							        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
							        	
							        	/* Convert Epoch Seconds back to a new ZonedDateTime object 
							        	 * First use RandomAccessFile readLong() to retrieve 8-byte
							        	 * integer from table file, then...
							        	 */
							        	long retreivedEpochSeconds = (long) list[temp].get(m);
							        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
							        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );
							        	
							        	//list[i].add(zdt2.toLocalDate());
										System.out.print(zdt2.toLocalDate() + " ");
									}
									else
									{
										for( int d =0; d<printer.length; d++) {
											if(printer[d]!=null && printer[d] == 1) {
												System.out.print(list[temp].get(index.get(m)) + " ");
												flag = 1;
											}
										}
									}
								}
								if(flag == 1) {
									System.out.println();
									flag = 0;
								}
								m++;
							}
						}
					} 
					else 
					{	
						if(pageToBeProcessed == 1) {
							for (int j = 0; j < columnNames.length; j++) {
								System.out.print(columnNames[j] + " ");
							}
							System.out.println();
						}
						
						if (deciding_col.length != 0) {

							int m = 0, flag = 0;
							while (m < index.size()) {
								for (int z = 0; z < columnNames.length; z++) {
									
									if(mapOfDataTypes.get(columnNames[z]).equalsIgnoreCase("datetime") || mapOfDataTypes.get(columnNames[z]).equalsIgnoreCase("date"))
									{
										
										/* Define the time zone for Dallas CST */
							        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
							        	
							        	/* Convert Epoch Seconds back to a new ZonedDateTime object 
							        	 * First use RandomAccessFile readLong() to retrieve 8-byte
							        	 * integer from table file, then...
							        	 */
							        	long retreivedEpochSeconds = (long)list[z].get(index.get(m));
							        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
							        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );
							        	
							        	//list[i].add(zdt2.toLocalDate());
										System.out.print(zdt2.toLocalDate() + " ");
									}
									else
									{
										for( int d =0; d<printer.length; d++) {
											if(printer[d] != null && printer[d] == 1) {
												System.out.print(list[z].get(index.get(m)) + " ");
												flag = 1;
											}
										}
									}
									
								}
								if(flag == 1) {
									System.out.println();
									flag = 0;
								}
								m++;
							}
						} 
						else 
						{
							int m = 0, flag = 0;
							while (m < numberOfRecords) {
								for (int z = 0; z < list.length; z++) {
									if(mapOfDataTypes.get(columnNames[z]).equalsIgnoreCase("datetime") || mapOfDataTypes.get(columnNames[z]).equalsIgnoreCase("date"))
									{
										
										/* Define the time zone for Dallas CST */
							        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
							        	
							        	/* Convert Epoch Seconds back to a new ZonedDateTime object 
							        	 * First use RandomAccessFile readLong() to retrieve 8-byte
							        	 * integer from table file, then...
							        	 */
							        	//System.out.println(list[z].get(m));
							        	long retreivedEpochSeconds = (long) list[z].get(m);
							        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
							        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );
							        	
							        	//list[i].add(zdt2.toLocalDate());
										System.out.print(zdt2.toLocalDate() + " ");
									}
									else
									{
										for( int d =0; d<printer.length; d++) {
											if(printer[d]!=null && printer[d] == 1) {
												System.out.print(list[z].get(m) + " ");
												flag = 1;
											}
										}
									}

								}
								if(flag == 1) {
									System.out.println();
									flag = 0 ;
								}
								m++;
							}
						}		
					}
				} 
				
				if(deciding_col.length == 0) {
					if(!("*".equals(wildCard))) {
						
						String selectColumnNames[] = wildCard.split(",");
						if(pageToBeProcessed == 1) {
							for (int j = 0; j < selectColumnNames.length; j++) {
								System.out.print(selectColumnNames[j] + " ");
							}
							System.out.println();
						}
						
							int m = 0;
							while(m < numberOfRecords){
								for (int z = 0; z < selectColumnNames.length; z++) {
									int temp = mapOfOrdinalPostions.get(selectColumnNames[z]) - 1;
									if(mapOfDataTypes.get(selectColumnNames[z]).equalsIgnoreCase("datetime") || mapOfDataTypes.get(selectColumnNames[z]).equalsIgnoreCase("date"))
									{
										
										/* Define the time zone for Dallas CST */
							        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
							        	
							        	/* Convert Epoch Seconds back to a new ZonedDateTime object 
							        	 * First use RandomAccessFile readLong() to retrieve 8-byte
							        	 * integer from table file, then...
							        	 */
							        	long retreivedEpochSeconds = (long) list[temp].get(m);
							        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
							        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );
							        	
							        	//list[i].add(zdt2.toLocalDate());
										System.out.print(zdt2.toLocalDate() + " ");
									}
									else
									{
										System.out.print(list[temp].get(m) + " ");
									}
								}
								System.out.println();
								m++;
							}
					} 
					else 
					{	
						if(pageToBeProcessed == 1) {
							for (int j = 0; j < columnNames.length; j++) {
								System.out.print(columnNames[j] + " ");
							}
							System.out.println();
						}
						
							int m = 0;
							while (m < numberOfRecords) {
								for (int z = 0; z < list.length; z++) {
									if(mapOfDataTypes.get(columnNames[z]).equalsIgnoreCase("datetime") || mapOfDataTypes.get(columnNames[z]).equalsIgnoreCase("date"))
									{
										
										/* Define the time zone for Dallas CST */
							        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
							        	
							        	/* Convert Epoch Seconds back to a new ZonedDateTime object 
							        	 * First use RandomAccessFile readLong() to retrieve 8-byte
							        	 * integer from table file, then...
							        	 */
							        	//System.out.println(list[z].get(m));
							        	long retreivedEpochSeconds = (long) list[z].get(m);
							        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
							        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );
							        	
							        	//list[i].add(zdt2.toLocalDate());
										System.out.print(zdt2.toLocalDate() + " ");
									}
									else
									{
										System.out.print(list[z].get(m) + " ");
									}

								}
								System.out.println();
								m++;
							}
					}
				}
				
	}
	
	
	/*@SuppressWarnings("rawtypes")
	private ArrayList getArrayList(String data_type) {
		if(data_type.equalsIgnoreCase("int")){
			return new ArrayList<Integer>();
		} 
		else if(data_type.equalsIgnoreCase("tinyint"))
        {			
			return new ArrayList<Integer>();
        }
        else if(data_type.equalsIgnoreCase("smallint"))
        {
        	return new ArrayList<Integer>();
        }
        else if(data_type.equalsIgnoreCase("bigint"))
        {
        	return new ArrayList<Integer>();
        }
        else if(data_type.equalsIgnoreCase("real"))
        {
        	return new ArrayList<Float>();			
        }
        else if(data_type.equalsIgnoreCase("double"))
        {
        	return new ArrayList<Double>();
        }
        else if(data_type.equalsIgnoreCase("datetime"))
        {
        	return new ArrayList<Long>();			
        }
        else if(data_type.equalsIgnoreCase("date"))
        {
        	return new ArrayList<Long>();			
        }
        else if(data_type.equalsIgnoreCase("text"))
        {
        	return new ArrayList<String>();
        } 
        else {
        	return null;
        }
	}*/

	/**
	 * Executing the drop command for the table
	 * @param tableName
	 * @param rfTbl
	 * @param rfCol
	 * @throws IOException
	 */
	public void processDropString(String tableName, RandomAccessFileCreation rfTbl, RandomAccessFileCreation rfCol) throws IOException {
		
		//Find the position where the table name exists in davisbase_table table
		rfTbl.seek(2);
		int rfTblPosition = rfTbl.readShort() + 1; // this position will give us the table name
		rfTbl.seek(rfTblPosition);
		String rfTblReferenceTableLine = rfTbl.readLine().substring(0, 20);
		//System.out.println("Our table: " + tableName);
		//System.out.println("RfTbl Reference Table name: " + rfTblReferenceTableLine);
		
		int m = 1;
		while(!rfTblReferenceTableLine.contains(tableName)) { //readLine gives us the table name	
			rfTblPosition = rfTblPosition + 21*(m);
			rfTbl.seek(rfTblPosition);
			rfTblReferenceTableLine = rfTbl.readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + rfTblReferenceTableLine);
		}
		Map<String, Integer> rfTblMapOfOrdinalPostions = new HashMap<String, Integer>();
		rfTblMapOfOrdinalPostions.put("rowid", 1);
		rfTblMapOfOrdinalPostions.put("table_name", 2);
		//Map<String, String> mapOfDataTypes = new HashMap<String,String>();
		Map<Integer, String> rfTblMapOfDataTypes = new HashMap<Integer, String>();
		rfTblMapOfDataTypes.put(1, "byte");
		rfTblMapOfDataTypes.put(2, "text");
		rfTbl.makeRecordZeroDrop(rfTblPosition - 1, (rfTblPosition -1 + rfTbl.recordLength), rfTblMapOfDataTypes, rfTblMapOfOrdinalPostions);
		rfTbl.recordStartPostion = rfTbl.recordStartPostion - 2;
		rfTbl.rafTablePosition = rfTbl.rafTablePosition + rfTbl.recordLength;
		//-----------------------------------------------------------------------------------------
		
		//Find the position where the table name exists in davisbase_columns table
		rfCol.seek(2);
		int rfColPosition = rfCol.readShort() + 1; // this position will give us the table name
		rfCol.seek(rfColPosition);
		String rfColReferenceTableLine = rfCol.readLine().substring(0, 20);
		
		//System.out.println("Our table: " + tableName);
		//System.out.println("RfCol Reference Table name: " + rfColReferenceTableLine);
		
		int k = 1;
		while(!rfColReferenceTableLine.contains(tableName)) { //readLine gives us the table name	
			rfColPosition = rfColPosition + 84*(k);
			rfCol.seek(rfColPosition);
			rfColReferenceTableLine = rfCol.readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + rfColReferenceTableLine);
			//k++;
		}
		Map<String, Integer> rfColMapOfOrdinalPostions = new HashMap<String, Integer>();
		rfColMapOfOrdinalPostions.put("rowid", 1);
		rfColMapOfOrdinalPostions.put("table_name", 2);
		rfColMapOfOrdinalPostions.put("column_name", 3);
		rfColMapOfOrdinalPostions.put("data_type", 4);
		rfColMapOfOrdinalPostions.put("column_key", 5);
		rfColMapOfOrdinalPostions.put("ordinal_position", 6);
		rfColMapOfOrdinalPostions.put("is_nullable", 7);
		//Map<String, String> mapOfDataTypes = new HashMap<String,String>();
		Map<Integer, String> rfColMapOfDataTypes = new HashMap<Integer, String>();
		rfColMapOfDataTypes.put(1, "BYTE");
		rfColMapOfDataTypes.put(2, "TEXT");
		rfColMapOfDataTypes.put(3, "TEXT");
		rfColMapOfDataTypes.put(4, "SMALLINT");
		rfColMapOfDataTypes.put(5, "TEXT");
		rfColMapOfDataTypes.put(6, "TINYINT");
		rfColMapOfDataTypes.put(7, "TEXT");
		
		for (int i = 0; i < noOfColumns; i++) {
			//position = position - 1;
			rfCol.makeRecordZeroDrop(rfColPosition - 1, (rfColPosition - 1 + rfCol.recordLength), rfColMapOfDataTypes, rfColMapOfOrdinalPostions);
			rfColPosition = rfColPosition + 84;
			rfCol.recordStartPostion = rfCol.recordStartPostion - 2;
			rfCol.rafColumnPosition = rfCol.rafColumnPosition + rfCol.recordLength;
		}
		
	}

	/**
	 * Method to Update records in the table
	 * @param tableName
	 * @param columnToBeUpdated
	 * @param valueToBeSet
	 * @param deciding_col
	 * @param operator
	 * @param comp_val
	 * @param rfTbl
	 * @param rfCol
	 * @throws IOException
	 */
	public void processUpdateString(String tableName, String columnToBeUpdated, String valueToBeSet, String deciding_col, String operator, String comp_val, RandomAccessFileCreation rfTbl, RandomAccessFileCreation rfCol) throws IOException {
		rfCol.seek(2);
		int rfColPosition = rfCol.readShort() + 1; // this position will give us the table name
		rfCol.seek(rfColPosition);
		String rfColReferenceTableLine = rfCol.readLine().substring(0, 20);

		//System.out.println("Our table: " + tableName);
		//System.out.println("RfCol Reference Table name: " + rfColReferenceTableLine);
		
		int k = 1;
		while(!(rfColReferenceTableLine.contains(tableName))) { //readLine gives us the table name	
			rfColPosition = rfColPosition + 84*(k);
			rfCol.seek(rfColPosition);
			rfColReferenceTableLine = rfCol.readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + rfColReferenceTableLine);
		}
		
		String data_type = null;

		String columnNames[] = new String[noOfColumns];
		Map<String, Integer> mapOfOrdinalPostions = new HashMap<String, Integer>();
		Map<Integer, String> mapOfDataTypes = new HashMap<Integer, String>();
		for (int i = noOfColumns - 1; i >= 0; i--) {

			// Get column name
			rfCol.seek(rfColPosition + 20);
			String columnName = rfCol.readLine().substring(0, 20).trim();
			columnNames[i] = columnName;

			// Get ordinal position
			rfCol.seek(rfColPosition + 62);
			int ordinalPosition = rfCol.readByte();

			// Store it in a map
			mapOfOrdinalPostions.put(columnName, ordinalPosition);

			// Get the data type
			rfCol.seek(rfColPosition + 40);
			data_type = help.getSerialCodeAsString(rfCol.readShort());
			//System.out.println(data_type);

			// Store it in a map
			mapOfDataTypes.put(ordinalPosition, data_type);
			rfColPosition = rfColPosition + 84;
		}
		
		int columnToBeUpdatedOrdinalPosition = mapOfOrdinalPostions.get(columnToBeUpdated);
		String columnToBeUpdatedDataType = mapOfDataTypes.get(columnToBeUpdatedOrdinalPosition);
		
		int lengthUpdate = 0;
		for (int i = 1; i < columnToBeUpdatedOrdinalPosition; i++) {
			lengthUpdate += help.getRecordLengthForDataType(mapOfDataTypes.get(i));
			//System.out.println("Length: " + lengthUpdate);
		}
		
		if(deciding_col != null) {
			int decidingOrdinalPosition = mapOfOrdinalPostions.get(deciding_col);
			String decidingDataType = mapOfDataTypes.get(decidingOrdinalPosition);
			int lengthCondition = 0;
			for (int i = 1; i < decidingOrdinalPosition; i++) {
				lengthCondition += help.getRecordLengthForDataType(mapOfDataTypes.get(i));
				//System.out.println("Length: " + lengthCondition);
			}
			
			
			int pageToBeProcessed = 1;
			while(pageToBeProcessed <= pageCount) {
				if(pageToBeProcessed == 1) 
				{
					seek(1);
					int numberOfRecords = readByte();
					helperMethodForUpdate(lengthUpdate, lengthCondition, pageToBeProcessed, numberOfRecords, deciding_col, decidingDataType, columnToBeUpdatedDataType, operator, comp_val, valueToBeSet, rfTbl, rfCol);
				}
				else
				{
					seek(pageSize + (pageToBeProcessed - 2)*512 + 1);
					int numberOfRecords = readByte();
					helperMethodForUpdate(lengthUpdate, lengthCondition, pageToBeProcessed, numberOfRecords, deciding_col, decidingDataType, columnToBeUpdatedDataType, operator, comp_val, valueToBeSet, rfTbl, rfCol);
				}
				pageToBeProcessed++;
			}
				
		}
		else 
		{
			int pageToBeProcessed = 1;
			while(pageToBeProcessed <= pageCount) {
				if(pageToBeProcessed == 1) 
				{
					seek(8);
					int startUpdate = readShort() + lengthUpdate;
					seek(1);
					int numberOfRecords = readByte();
					for (int j = 0; j < numberOfRecords; j++) {
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
						startUpdate = startUpdate - recordLength;
					}
					
				}
				else
				{
					seek(pageSize + (pageToBeProcessed - 2)*512 + 8);
					int startUpdate = readShort() + lengthUpdate;
					
					seek(pageSize + (pageToBeProcessed - 2)*512 + 1);
					int numberOfRecords = readByte();
					
					for (int j = 0; j < numberOfRecords; j++) {
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
						startUpdate = startUpdate - recordLength;
					}
			
				}
				pageToBeProcessed++;
			}
			
		}
		
	}

	/**
	 * Method to show the tables 
	 * @throws IOException
	 */
	public void processShowTableQuery() throws IOException {
		seek(8);
		int start = readShort() + 1;
		seek(1);
		int count = readByte();
		
		while(count != 0) {
			seek(start);
			System.out.println(readLine().substring(0, 20).trim());
			start = start - recordLength;
			count--;
		}
	}

	/**
	 * To delete the record from the table
	 * @param tableName
	 * @param deciding_col
	 * @param operator
	 * @param comp_val
	 * @param rfTbl
	 * @param rfCol
	 * @throws IOException
	 */
	public void deleteFromTable(String tableName, String deciding_col, String operator, String comp_val, RandomAccessFileCreation rfTbl, RandomAccessFileCreation rfCol) throws IOException {
		if (deciding_col != null) {
			// Find the position where the table name exists in davisbase_columns table
			rfCol.seek(2);
			int position = rfCol.readShort() + 1; // this position will give us
													// the table name
			rfCol.seek(position);
			String referenceTableLine = rfCol.readLine().substring(0, 20);

			//System.out.println("Our table: " + tableName);
			//System.out.println("Reference Table name: " + referenceTableLine);

			int k = 1;
			while (!referenceTableLine.contains(tableName)) { // readLine gives
																// us the table
																// name
				position = position + 84 * (k);
				rfCol.seek(position);
				referenceTableLine = rfCol.readLine().substring(0, 20);
				//System.out.println("Inside Reference Table name: "
					//	+ referenceTableLine);
				//k++;
			}
			
			String data_type = null;
			//int recordStartPosition = 8;

			String columnNames[] = new String[noOfColumns];
			Map<String, Integer> mapOfOrdinalPostions = new HashMap<String, Integer>();
			// Map<String, String> mapOfDataTypes = new HashMap<String,String>();
			Map<Integer, String> mapOfDataTypes = new HashMap<Integer, String>();
			for (int i = noOfColumns - 1; i >= 0; i--) {

				// Get column name
				rfCol.seek(position + 20);
				String columnName = rfCol.readLine().substring(0, 20).trim();
				columnNames[i] = columnName;

				// Get ordinal position
				rfCol.seek(position + 62);
				int ordinalPosition = rfCol.readByte();

				// Store it in a map
				mapOfOrdinalPostions.put(columnName, ordinalPosition);

				// Get the data type
				rfCol.seek(position + 40);
				data_type = help.getSerialCodeAsString(rfCol.readShort());
				//System.out.println(data_type);

				// Store it in a map
				mapOfDataTypes.put(ordinalPosition, data_type);
				position = position + 84;
			}
			int decidingOrdinalPosition = mapOfOrdinalPostions.get(deciding_col);
			String decidingDataType = mapOfDataTypes.get(decidingOrdinalPosition);
			int length = 0;
			for (int i = 1; i < decidingOrdinalPosition; i++) {
				length += help.getRecordLengthForDataType(mapOfDataTypes.get(i));
				//System.out.println("Length: " + length);
			}
			
			int pageToBeProcessed = 1;
			while(pageToBeProcessed <= pageCount) {
				if(pageToBeProcessed == 1) 
				{
					seek(1);
					int numberOfRecords = readByte();
					helperMethodForDelete(pageToBeProcessed, numberOfRecords, length, deciding_col, decidingDataType, operator, comp_val, mapOfDataTypes, mapOfOrdinalPostions);
				}
				else
				{
					seek(pageSize + (pageToBeProcessed - 2)*512 + 1);
					int numberOfRecords = readByte();
					helperMethodForDelete(pageToBeProcessed, numberOfRecords, length, deciding_col, decidingDataType, operator, comp_val, mapOfDataTypes, mapOfOrdinalPostions);
				}
				pageToBeProcessed++;
			}
			

		} else {
			int end = pageSize + (pageCount - 1)*512;
			for (int i = 1; i < end; i++) {
				seek(i);
				writeByte(0);
			}
			recordStartPostion = 8;
			recordNum = 0;
			setLength(pageSize);
			posPointer = 512;
			pageCount = 1;
		}
		System.out.println("Delete successfully!");
	}

	public void helperMethodForDelete(int pageToBeProcessed, int numberOfRecords, int length, String deciding_col, String decidingDataType, String operator, String comp_val, Map<Integer, String> mapOfDataTypes, Map<String, Integer> mapOfOrdinalPostions) throws IOException {
		int recordStart = 8;
		int start;
		if(pageToBeProcessed == 1) {
			seek(recordStart);
			start = readShort();
			seek(start);
		} else {
			seek(pageSize + (pageToBeProcessed - 2)*512 + recordStart);
			start = readShort();
			seek(start);
		}
		
		//boolean flag = false;
		for (int i = 0; i < numberOfRecords; i++) {
		//while(start >= pos){
			boolean flag = false;
			seek(start + length);
			if ("<".equals(operator)) {
				if (readInt() < Integer.parseInt(comp_val)) {
					makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
					flag = true;
					//pos = pos + recordLength;
					recordStartPostion = recordStartPostion - 2;
				}
			} else if (">".equals(operator)) {
				if (readInt() > Integer.parseInt(comp_val)) {
					makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);
					flag = true;
					//pos = pos + recordLength;
					recordStartPostion = recordStartPostion - 2;
				}
			} else { // = operator
				if (decidingDataType.equalsIgnoreCase("int")) {
					if (readInt() == Integer.parseInt(comp_val)) {
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);
						flag = true;
						//pos = pos + recordLength;
						recordStartPostion = recordStartPostion - 2;
					}
				} else if (decidingDataType.equalsIgnoreCase("text")) {
					String stringToBecompared = readLine().substring(0, 20).trim();
					if (stringToBecompared.equals(comp_val)) {
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);
						flag = true;
						//pos = pos + recordLength;
						recordStartPostion = recordStartPostion - 2;
					}
				}

			}
			if(!flag) {
				start = start - recordLength;
			} else {
				i--;
			}
		}
	}

	/**
	 * Making record zero for delete command
	 * @param start
	 * @param end
	 * @param mapOfDataTypes
	 * @param mapOfOrdinalPostions
	 * @param pageToBeProcessed
	 * @throws IOException
	 */
	public void makeRecordZero(int start, int end, Map<Integer, String> mapOfDataTypes, Map<String, Integer> mapOfOrdinalPostions, int pageToBeProcessed) throws IOException {
		for (int j = start; j < end; j++) {
			seek(j);
			writeByte(0);
		}
		int address = posPointer;
		for (int i = 1; i <= noOfColumns; i++) {
			seek(address);
			if(mapOfDataTypes.get(i).equalsIgnoreCase("int")){
				int temp = readInt();
				seek(start);
				writeInt(temp);
				start = start + 4;
				address = address + 4;
			}else if(mapOfDataTypes.get(i).equalsIgnoreCase("byte"))
	        {			
				int temp = readByte();
				seek(start);
				writeByte(temp);
				start = start + 1;
				address = address + 1;
	        }
			else if(mapOfDataTypes.get(i).equalsIgnoreCase("tinyint"))
	        {			
				int temp = readByte();
				seek(start);
				writeByte(temp);
				start = start + 1;
				address = address + 1;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("smallint"))
	        {
	        	int temp = readShort();
				seek(start);
				writeInt(temp);
				start = start + 2;
				address = address + 2;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("bigint"))
	        {
	        	long temp = readLong();
				seek(start);
				writeLong(temp);
				start = start + 8;
				address = address + 8;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("real"))
	        {
	        	float temp = readFloat();
				seek(start);
				writeFloat(temp);
				start = start + 4;
				address = address + 4;		
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("double"))
	        {
	        	double temp = readDouble();
				seek(start);
				writeDouble(temp);
				start = start + 8;
				address = address + 8;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("datetime"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				address = address + 20;		
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("date"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				address = address + 20;			
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("text"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				address = address + 20;
	        } 
		}
		
		int s = posPointer;
		for (int j = s; j < (posPointer+recordLength); j++) {
			seek(j);
			writeByte(0);
		}
		
		int pageCurrent = (int)Math.floor(posPointer/512);
		int recordStart = 8;
		
		if(pageCurrent == 0) {
			//Make the address of last record copied as zero
			seek(1);
			int numOfRecords = readByte();
			seek(recordStart + (numOfRecords-1)*2);
			writeShort(0);
			
			//Decrementing number of records
			seek(1);
			int updatedNumOfRecords = readByte();
			seek(1);
			writeByte(updatedNumOfRecords-1);
			
			//Update pos and Update the address of latest record inserted
			posPointer = posPointer + recordLength;
			seek(2);
			writeShort(posPointer);
			
		} else { //If current page is other than page 1
			
			//Update pos and Update the address of latest record inserted
			if((posPointer + recordLength) >= (pageSize + (pageCurrent - 2)*512 + 512)) {
				if(pageCurrent == 2) {
					seek(2);
					posPointer = readShort();
					
					//Set pointer to right child as zero
					seek(4);
					writeInt(0);
					
					setLength(pageSize);
					pageCount--;
				}
				else {
					seek((pageCurrent - 1)* 512 + 2);
					posPointer = readShort();
					
					//Set pointer to right child as zero
					seek(pageSize + (pageCurrent - 2)*512 + 4);
					writeInt(0);
					
					setLength(pageSize + (pageCurrent - 2)*512);
					pageCount--;
				}
			}
			else {
				posPointer = posPointer + recordLength;
				
				//Update the address of latest record inserted			
				seek(pageSize + (pageCurrent - 2)*512 + 2);
				writeShort(posPointer);
				
				//Make the address of last record copied as zero
				seek(pageSize + (pageCurrent - 2)*512 + 1);
				int n = readByte();
				seek(pageSize + (pageCurrent - 2)*512 + recordStart + (n-1)*2);
				writeShort(0);
				
				//Decrementing number of records
				seek(pageSize + (pageCurrent - 2)*512 + 1);
				int updatedNumOfRecords = readByte();
				seek(pageSize + (pageCurrent - 2)*512 + 1);
				writeByte(updatedNumOfRecords-1);
			}
		}
		
		this.recordNum = calculateNumberOfRecords(pageCount);
	}


	/**
	 * Make zero for records for drop command
	 * @param start
	 * @param end
	 * @param mapOfDataTypes
	 * @param mapOfOrdinalPostions
	 * @throws IOException
	 */
	private void makeRecordZeroDrop(int start, int end, Map<Integer, String> mapOfDataTypes, Map<String, Integer> mapOfOrdinalPostions) throws IOException {
		for (int j = start; j < end; j++) {
			seek(j);
			writeByte(0);
		}
		seek(2);
		int address = readShort();
		for (int i = 1; i <= noOfColumns; i++) {
			seek(address);
			if(mapOfDataTypes.get(i).equalsIgnoreCase("int")){
				int temp = readInt();
				seek(start);
				writeInt(temp);
				start = start + 4;
				address = address + 4;
			}else if(mapOfDataTypes.get(i).equalsIgnoreCase("byte"))
	        {			
				int temp = readByte();
				seek(start);
				writeByte(temp);
				start = start + 1;
				address = address + 1;
	        }
			else if(mapOfDataTypes.get(i).equalsIgnoreCase("tinyint"))
	        {			
				int temp = readByte();
				seek(start);
				writeByte(temp);
				start = start + 1;
				address = address + 1;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("smallint"))
	        {
	        	int temp = readShort();
				seek(start);
				writeInt(temp);
				start = start + 2;
				address = address + 2;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("bigint"))
	        {
	        	long temp = readLong();
				seek(start);
				writeLong(temp);
				start = start + 8;
				address = address + 8;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("real"))
	        {
	        	float temp = readFloat();
				seek(start);
				writeFloat(temp);
				start = start + 4;
				address = address + 4;		
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("double"))
	        {
	        	double temp = readDouble();
				seek(start);
				writeDouble(temp);
				start = start + 8;
				address = address + 8;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("datetime"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				address = address + 20;		
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("date"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				address = address + 20;			
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("text"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				address = address + 20;
	        } 
		}
		
		//Make the address of last record as zero
		seek(8 + (recordNum-1)*2);
		writeShort(0);
				
		//Decrementing number of records
		seek(1);
		int updatedNumOfRecords = readByte();
		seek(1);
		writeByte(updatedNumOfRecords-1);
		this.recordNum = updatedNumOfRecords-1;
		
		//Make the last row as zero
		seek(2);
		int add = readShort();
		for (int j = add; j <= (add + recordLength); j++) {
			seek(j);
			writeByte(0);
		}
		
		seek(2);
		int u = readShort() + recordLength;
		seek(2);
		writeShort(u);
	}
	
	/**
	 * Calculate the number of records existing 
	 * @param countPages
	 * @return
	 * @throws IOException
	 */
	public int calculateNumberOfRecords(int countPages) throws IOException {
		int c = 1;
		int numOfRecords = 0;
		while(c <= countPages) {
			if(c == 1) {
				seek(1);
				numOfRecords += readByte();
			} else {
				seek(pageSize + (c - 2)*512  + 1);
				numOfRecords += readByte();
			}
			c++;
		}
		
		return numOfRecords;
	}
	
	
	/**
	 * Helper method to Update record values 
	 * @param lengthUpdate
	 * @param lengthCondition
	 * @param pageToBeProcessed
	 * @param numberOfRecords
	 * @param deciding_col
	 * @param decidingDataType
	 * @param columnToBeUpdatedDataType
	 * @param operator
	 * @param comp_val
	 * @param valueToBeSet
	 * @param rfTbl
	 * @param rfCol
	 * @throws IOException
	 */
	private void helperMethodForUpdate(int lengthUpdate, int lengthCondition, int pageToBeProcessed, int numberOfRecords, String deciding_col, String decidingDataType, String columnToBeUpdatedDataType, String operator,
			String comp_val, String valueToBeSet, RandomAccessFileCreation rfTbl, RandomAccessFileCreation rfCol) throws IOException {
		
		
		int recordStart = 8;
		int startUpdate;
		int startCondition;
		if(pageToBeProcessed == 1) {
			seek(recordStart);
			startUpdate = readShort() + lengthUpdate;
			seek(recordStart);
			startCondition = readShort() + lengthCondition;
		} else {
			seek(pageSize + (pageToBeProcessed - 2)*512 + recordStart);
			startUpdate = readShort() + lengthUpdate;
			seek(pageSize + (pageToBeProcessed - 2)*512 + recordStart);
			startCondition = readShort() + lengthCondition;
		}
		
		for (int j = 0; j < numberOfRecords; j++) {
			if ("<".equals(operator)) 
			{
				seek(startCondition);
				if(decidingDataType.equalsIgnoreCase("int")) 
				{
					if (readInt() < Integer.parseInt(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("byte")) 
				{
					if (readByte() < Byte.parseByte(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("tinyint")) 
				{
					if (readByte() < Byte.parseByte(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("smallint")) 
				{
					if (readShort() < Short.parseShort(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("bigint")) 
				{
					if (readLong() < Long.parseLong(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("float")) 
				{
					if (readFloat() < Float.parseFloat(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("double")) 
				{
					if (readDouble() < Double.parseDouble(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
			} 
			else if (">".equals(operator)) 
			{
				seek(startCondition);
				if(decidingDataType.equalsIgnoreCase("int")) 
				{
					if (readInt() > Integer.parseInt(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("byte")) 
				{
					if (readByte() > Byte.parseByte(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("tinyint")) 
				{
					if (readByte() > Byte.parseByte(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("smallint")) 
				{
					if (readShort() > Short.parseShort(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("bigint")) 
				{
					if (readLong() > Long.parseLong(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("float")) 
				{
					if (readFloat() < Float.parseFloat(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("double")) 
				{
					if (readDouble() < Double.parseDouble(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
			} 
			else 
			{ // = operator
				seek(startCondition);
				if(decidingDataType.equalsIgnoreCase("int")) 
				{
					int x = readInt();
					if (x == Integer.parseInt(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("byte")) 
				{
					if (readByte() == Byte.parseByte(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("tinyint")) 
				{
					if (readByte() == Byte.parseByte(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("smallint")) 
				{
					if (readShort() == Short.parseShort(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("bigint")) 
				{
					if (readLong() == Long.parseLong(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("float")) 
				{
					if (readFloat() < Float.parseFloat(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("double")) 
				{
					if (readDouble() < Double.parseDouble(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("datetime")) 
				{
					if (readLine().substring(0,20).trim().equalsIgnoreCase(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("date")) 
				{
					if (readLine().substring(0,20).trim().equalsIgnoreCase(comp_val)) 
					{
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}
				else if (decidingDataType.equalsIgnoreCase("text")) 
				{
					String stringToBecompared = readLine().substring(0, 20).trim();
					if (stringToBecompared.equals(comp_val)) {
						seek(startUpdate);
						updateValue(columnToBeUpdatedDataType, valueToBeSet, startUpdate);
					}
				}

			}
			startCondition = startCondition - recordLength;
			startUpdate = startUpdate - recordLength;
		}			
		
		
	}

	/**
	 * @param columnToBeUpdatedDataType
	 * @param valueToBeSet
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public void updateValue(String columnToBeUpdatedDataType, String valueToBeSet, int startUpdate) throws NumberFormatException, IOException {
		if(columnToBeUpdatedDataType.equalsIgnoreCase("int"))
		{
			writeInt(Integer.parseInt(valueToBeSet));
		}
		else if(columnToBeUpdatedDataType.equalsIgnoreCase("byte"))
        {	
			writeByte(Byte.parseByte(valueToBeSet));
        }
		else if(columnToBeUpdatedDataType.equalsIgnoreCase("tinyint"))
        {	
			writeByte(Byte.parseByte(valueToBeSet));
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("smallint"))
        {
			writeInt(Short.parseShort(valueToBeSet));
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("bigint"))
        {
			writeLong(Long.parseLong(valueToBeSet));
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("real"))
        {
			writeFloat(Float.parseFloat(valueToBeSet));		
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("double"))
        {
			writeDouble(Double.parseDouble(valueToBeSet));
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("datetime"))
        {
			writeBytes(valueToBeSet);		
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("date"))
        {
			writeBytes(valueToBeSet);			
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("text"))
        {
        	for(int i = startUpdate;i < (startUpdate+20); i++) {
        		writeByte(0);
        		seek(i);
        	}
        	seek(startUpdate);
			writeBytes(valueToBeSet);
        } 
		
	}
	
	/**
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public int[] getNoOfColumnsAndRecordLength(String tableName) throws IOException {
		seek(2);
		int position = readShort() + 1; // this position will give us the table name
		seek(position);
		String referenceTableLine = readLine().substring(0, 20);
		
		//System.out.println("Our table: " + tableName);
		//System.out.println("Reference Table name: " + referenceTableLine);
		
		int k = 1;
		int numberOfColumns = 0;
		int recordLength = 0;
		
		while(!referenceTableLine.contains(tableName)) {
			position = position + 84*(k);
			seek(position);
			referenceTableLine = readLine().substring(0, 20);
		}
		
		while(referenceTableLine.contains(tableName)) { //readLine gives us the table name	
			numberOfColumns++;
			seek(position+40);
			recordLength += help.getRecordLengthForDataType(help.getSerialCodeAsString(readShort()));
			//System.out.println("RecordLength " + recordLength);
			position = position + 84*(k);
			seek(position);
			referenceTableLine = readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + referenceTableLine);
		}
		int values[] = new int[2];
		values[0] = numberOfColumns;
		values[1] = recordLength;
		//System.out.println("Number of col " + numberOfColumns  + " Total Recordlength " + recordLength);
		return values;
		
	}
	
	/**
	 * @param key
	 * @return
	 * @throws IOException
	 */
	private boolean isPrimaryKeyUnique(Integer key) throws IOException {
		int page = 1;
		while(page <= pageCount) {
			if(page == 1) {
				seek(1);
				int numberOfRecords = readByte();
				seek(8);
				int checkStart = 0;
				if(numberOfRecords > 0) {
					checkStart = readShort();
				}
				int numberOfIterations = numberOfRecords;
				while(numberOfIterations != 0) {
					seek(checkStart);
					if(readInt() == key){
						return false;
					}
					numberOfIterations--;
					checkStart = checkStart - recordLength;
				}
				
			} else {
				seek(pageSize + (page - 2)*512 + 1);
				int numberOfRecords = readByte();
				seek(pageSize + (page - 2)*512 + 8);
				int checkStart = 0;
				if(numberOfRecords > 0) {
					checkStart = readShort();
				}
				int numberOfIterations = numberOfRecords;
				while(numberOfIterations != 0) {
					seek(checkStart);
					if(readInt() == key){
						return false;
					}
					numberOfIterations--;
					checkStart = checkStart - recordLength;
				}
				
			}
			page++;
		}
		
		return true;
		
	}
	
	/**
	 * @param columnDetails
	 */
	public void getRecordLength(String columnDetails) {
		columnDetails = columnDetails.substring(0, columnDetails.length()-1);
		String columns[] = columnDetails.split(",");
		noOfColumns = columns.length;
		
		for (int i = 0; i < columns.length; i++) {
			String temp1[] = columns[i].split(" ");
			recordLength += help.getRecordLengthForDataType(temp1[1]);
		}
		//System.out.println("Record Length: " + recordLength);
		
	}

}
