package com.axc166930.davisbase;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;

/**
 * @author avinash
 *
 */
public class Helper {

	/**
	 * @param dataType
	 * @return
	 */
	public int getSerialCode(String dataType) {
		int serialCode = 0;
		if(dataType.equalsIgnoreCase("int"))
        {
            //record_size=record_size+4;
            serialCode=0x06;
        }
        else if(dataType.equalsIgnoreCase("tinyint"))
        {
            //record_size=record_size+1;
            serialCode=0x04;
        }
        else if(dataType.equalsIgnoreCase("smallint"))
        {
            //record_size=record_size+2;
            serialCode=0x05;
        }
        else if(dataType.equalsIgnoreCase("bigint"))
        {
            //record_size=record_size+8;
            serialCode=0x07;
        }
        else if(dataType.equalsIgnoreCase("real"))
        {
            //record_size=record_size+4;
            serialCode=0x08;
        }
        else if(dataType.equalsIgnoreCase("double"))
        {
            //record_size=record_size+8;
            serialCode=0x09;
        }
        else if(dataType.equalsIgnoreCase("datetime"))
        {
            //record_size=record_size+8;
            serialCode=0x0A;
        }
        else if(dataType.equalsIgnoreCase("date"))
        {
            //record_size=record_size+8;
            serialCode=0x0B;
        }
        else if(dataType.equalsIgnoreCase("text"))
        {
            //record_size=record_size+8;
            serialCode=0x0C;
        }
		
		return serialCode;
	}

	/**
	 * @param SerialCode
	 * @return
	 */
	public String getSerialCodeAsString(int SerialCode) {
		String dataType = "";
		if(SerialCode == 6)
        {
            //record_size=record_size+4;
			dataType="int";
        }
        else if(SerialCode == 4)
        {
            //record_size=record_size+1;
        	dataType="tinyint";
        }
        else if(SerialCode == 5)
        {
            //record_size=record_size+2;
        	dataType="smallint";
        }
        else if(SerialCode == 7)
        {
            //record_size=record_size+8;
        	dataType="bigint";
        }
        else if(SerialCode == 8)
        {
            //record_size=record_size+4;
        	dataType="real";
        }
        else if(SerialCode == 9)
        {
            //record_size=record_size+8;
        	dataType= "double";
        }
        else if(SerialCode == 10)
        {
            //record_size=record_size+8;
        	dataType="datetime";
        }
        else if(SerialCode == 11)
        {
            //record_size=record_size+8;
        	dataType="date";
        }
        else if(SerialCode == 12)
        {
            //record_size=record_size+8;
        	dataType="text";
        }
		
		return dataType;
	}
	
	/**
	 * @param data_type
	 * @return
	 */
	public int getRecordLengthForDataType(String data_type) {
		int recordSize = 0;
		if(data_type.equalsIgnoreCase("int"))
        {
            recordSize=recordSize+4;
            //serialCode=0x06;
        }
        else if(data_type.equalsIgnoreCase("tinyint"))
        {
            recordSize=recordSize+1;
            //serialCode=0x04;
        }
        else if(data_type.equalsIgnoreCase("smallint"))
        {
            recordSize=recordSize+2;
            //serialCode=0x05;
        }
        else if(data_type.equalsIgnoreCase("bigint"))
        {
            recordSize=recordSize+8;
            //serialCode=0x07;
        }
        else if(data_type.equalsIgnoreCase("real"))
        {
            recordSize=recordSize+4;
            //serialCode=0x08;
        }
        else if(data_type.equalsIgnoreCase("double"))
        {
            recordSize=recordSize+8;
            //serialCode=0x09;
        }
        else if(data_type.equalsIgnoreCase("datetime"))
        {
            recordSize=recordSize+8;
            //serialCode=0x0A;
        }
        else if(data_type.equalsIgnoreCase("date"))
        {
            recordSize=recordSize+8;
            //serialCode=0x0B;
        }
        else if(data_type.equalsIgnoreCase("text"))
        {
            recordSize=recordSize+20;
            //serialCode=0x0C;
        }
		
		return recordSize;
	}

	
	/**
	 * @param dataType
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public ArrayList getArrayList(String dataType) {
		if(dataType.equalsIgnoreCase("int")){
			return new ArrayList<Integer>();
		} 
		else if(dataType.equalsIgnoreCase("tinyint"))
        {			
			return new ArrayList<Integer>();
        }
        else if(dataType.equalsIgnoreCase("smallint"))
        {
        	return new ArrayList<Integer>();
        }
        else if(dataType.equalsIgnoreCase("bigint"))
        {
        	return new ArrayList<Integer>();
        }
        else if(dataType.equalsIgnoreCase("real"))
        {
        	return new ArrayList<Float>();			
        }
        else if(dataType.equalsIgnoreCase("double"))
        {
        	return new ArrayList<Double>();
        }
        else if(dataType.equalsIgnoreCase("datetime"))
        {
        	return new ArrayList<Long>();			
        }
        else if(dataType.equalsIgnoreCase("date"))
        {
        	return new ArrayList<Long>();			
        }
        else if(dataType.equalsIgnoreCase("text"))
        {
        	return new ArrayList<String>();
        } 
        else {
        	return null;
        }
	}
	
	/**
	 * @param columnValues
	 * @return
	 */
	public long toLong(String columnValues) {
		String dateParams[] = columnValues.split("-");
		ZoneId zoneId = ZoneId.of( "America/Chicago");
		
		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
		
		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
		/* ZonedDateTime toLocalDate() method will display in a simple format */
		System.out.println(zdt.toLocalDate()); 
		
		/* Convert a ZonedDateTime object to epochSeconds
		 * This value can be store 8-byte integer to a binary
		 * file using RandomAccessFile writeLong()
		 */
		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		return epochSeconds;
	}
	
}
