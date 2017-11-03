package com.axc166930.davisbase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * @author avinash
 *
 */
public class DavisBase {
	
	static String prompt = "davisql> ";
	static boolean isExit = false;
	/*
	 * Page size for all files is 512 bytes by default.
	 * You may choose to make it user modifiable
	 */
	static int pageSize = 512;

	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	static RandomAccessFileCreation rafTable;
	
	static RandomAccessFileCreation rafColumn;
	
	static Map<String, BPlusTree> bPlusTreeMap = new HashMap<String, BPlusTree>();
	
	static String version = "v1.01";
	
	static String copyright = "©2017 Avinash Chandrasekharan";
	
	/**
	 * Main method where the initialization of the program starts
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		/* Display the welcome screen */
		splashScreen();

		metadataInitialization();
		
		/* Variable to collect user input from the prompt */
		String userCommand = ""; 
		
		while(!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			userCommandParser(userCommand);
		}
		System.out.println("C:>");

	}
	
	/**
	 *  Displaying the Welcome screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		//System.out.println("DavisBaseLite Version " + getVersion());
		//System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * This method initializes the metadata for the Davis base 
	 * @throws IOException
	 */
	public static void metadataInitialization() throws IOException {
		
		File directory = new File("Data");
		

		 if(!directory.exists()) 
		 {
			 directory.mkdir();
			 
			File directory2 = new File("Data\\catalog");
			directory2.mkdir();
			
			File directory3 = new File("Data\\user_data");
			directory3.mkdir();
			
			File fileTable = new File("Data\\catalog\\davisbase_tables.tbl");			
			rafTable = new RandomAccessFileCreation(fileTable, "rw", 512);
			rafTable.seek(1);
			rafTable.writeShort(13);
			rafTable.setLength(512);
			rafTable.insertIntoDavisBaseTable("davisbase_tables");
			rafTable.insertIntoDavisBaseTable("davisbase_columns");
			
			
			File fileColumn = new File("Data\\catalog\\davisbase_columns.tbl");
			rafColumn = new RandomAccessFileCreation(fileColumn, "rw", 2048);
			rafColumn.seek(1);
			rafColumn.writeShort(13);
			rafColumn.insertIntoDavisBaseColumn("davisbase_tables", "rowid", "BYTE", "PRI", 1, "NO");
			rafColumn.insertIntoDavisBaseColumn("davisbase_tables", "table_name", "TEXT", "", 2, "YES");
			rafColumn.insertIntoDavisBaseColumn("davisbase_columns", "rowid", "BYTE", "PRI", 1, "NO");
			rafColumn.insertIntoDavisBaseColumn("davisbase_columns", "table_name", "TEXT", "", 2, "YES");
			rafColumn.insertIntoDavisBaseColumn("davisbase_columns", "column_name", "TEXT", "", 3, "YES");
			rafColumn.insertIntoDavisBaseColumn("davisbase_columns", "data_type", "SMALLINT", "", 4, "YES");
			rafColumn.insertIntoDavisBaseColumn("davisbase_columns", "column_key", "TEXT", "", 5, "YES");
			rafColumn.insertIntoDavisBaseColumn("davisbase_columns", "ordinal_position", "TINYINT", "", 6, "YES");
			rafColumn.insertIntoDavisBaseColumn("davisbase_columns", "is_nullable", "TEXT", "", 7, "YES");
			
		 }
		 else
		 {
			File fileTbl = new File("Data\\catalog\\davisbase_tables.tbl");			
			rafTable = new RandomAccessFileCreation(fileTbl, "rw", 512);
			rafTable.recordLength = 21;
			rafTable.noOfColumns = 2;
			rafTable.seek(2);
			rafTable.rafTablePosition = rafTable.readShort();
			rafTable.recordNum = rafTable.calculateNumberOfRecords(1);
			rafTable.recordStartPostion = 8 + rafTable.recordNum*2;
					
			File fileCol = new File("Data\\catalog\\davisbase_columns.tbl");
			rafColumn = new RandomAccessFileCreation(fileCol, "rw", 2048);
			System.out.println("Data folder already exists");
			rafColumn.recordLength = 84;
			rafColumn.noOfColumns = 7;
			rafColumn.seek(2);
			rafColumn.rafColumnPosition = rafColumn.readShort();		
			rafColumn.recordNum = rafColumn.calculateNumberOfRecords(1);
			rafColumn.recordStartPostion = 8 + rafColumn.recordNum*2;
			
			rafTable.seek(8);
			int start = rafTable.readShort() + 1;
			rafTable.seek(1);
			int count = rafTable.readByte();
			
			while(count != 0) {
				rafTable.seek(start);
				String tableName = rafTable.readLine().substring(0, 20).trim();
				if(!(tableName.equalsIgnoreCase("davisbase_tables")) && !(tableName.equalsIgnoreCase("davisbase_columns"))) {
					File file = new File("Data\\user_data\\" + tableName + ".tbl");
					RandomAccessFileCreation newTable = new RandomAccessFileCreation(file, "rw", 1024);
					newTable.pageCount = (int) ((newTable.length()/512) - 1); //countPages
					newTable.recordNum = newTable.calculateNumberOfRecords(newTable.pageCount); //NumberOfRecords
					int values[] = rafColumn.getNoOfColumnsAndRecordLength(tableName); //recordLength, numberOfColumns
					newTable.noOfColumns = values[0];
					newTable.recordLength = values[1];
					
					if(newTable.pageCount == 1) {
						newTable.seek(1);
						int numberOfRecords = newTable.readByte();
						
						if(numberOfRecords == 0) {
							newTable.posPointer = 512;
						} else {
							newTable.seek(2);
							newTable.posPointer = newTable.readShort(); //pos
						}
						//newTable.recordStartPostion = 8 + newTable.numberOfRecords*2; //recordStartPosition
						newTable.recordStartPostion = 8 + numberOfRecords*2; //recordStartPosition
					}
					else 
					{
						newTable.seek(1024 + (newTable.pageCount - 2)*512 + 1);
						int numberOfRecords = newTable.readByte();
						if(numberOfRecords == 0) {
							newTable.posPointer = 1024 + (newTable.pageCount-1)*512;
						} else {
							newTable.seek(1024 + (newTable.pageCount - 2)*512 + 2);
							newTable.posPointer = newTable.readShort(); //pos
						}
					}
					
					
					BPlusTree bPlusTree = new BPlusTree(newTable);
					bPlusTreeMap.put(tableName, bPlusTree);
				}
				
				start = start - rafTable.recordLength;
				count--;
			}
			
			
				
		 }	
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	/**
	 * Parse the user Command 
	 * @param userCommand
	 * @throws IOException
	 */
	public static void userCommandParser (String userCommand) throws IOException {
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		
		/*
		*  This switch handles a very small list of hardcoded commands of known syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		switch (commandTokens.get(0)) {
			case "show":
				parseShowString(userCommand);
			break;
			case "create":
				parseCreateString(userCommand);
			break;
			case "insert":
				parseInsertString(userCommand);
			break;
			case "select":
				parseQueryString(userCommand);
			break;
			case "delete":
				parseDeleteString(userCommand);
			break;
			case "drop":
				parseDropString(userCommand);
			break;
			case "update":
				parseUpdateString(userCommand);
			break;
			case "help":
				help();
			break;
			case "version":
				displayVersion();
			break;
			case "quit":
				isExit = true;
			break;
			default:
				System.out.println("Sorry! Improper command: \"" + userCommand + "\"");
			break;
		}
	}

	/**
	 * @param userCommand
	 * @throws IOException
	 */
	private static void parseShowString(String userCommand) throws IOException {
		
		rafTable.processShowTableQuery();
		
	}

	/**
	 * Parse the create command
	 * @param userCommand
	 * @throws IOException
	 */
	private static void parseCreateString(String userCommand) throws IOException {
		String commandElements[] = userCommand.split("\\(");
		String fetchTableName[] = commandElements[0].split(" ");
		String tableName = fetchTableName[2];
		//System.out.println("Table Name: " + tableName);
		
		rafTable.insertIntoDavisBaseTable(tableName);
		rafColumn.insertUserColumsEntry(tableName, commandElements[1]);
		
		//Create a new tbl for file
		File f = new File("Data\\user_data\\" + tableName + ".tbl");
		//RandomAccessFileCreation newTable = new RandomAccessFileCreation("Data\\user_data\\" + tableName + ".tbl", "rw", pageSize);
		RandomAccessFileCreation newTable = new RandomAccessFileCreation(f, "rw", 1024);
		newTable.setLength(1024);
		//Set root pageType
		int rootStart = 1024 - 512;
		newTable.seek(rootStart);
		newTable.writeByte(5);
		
		//Pointing root to first left leaf node
		newTable.seek(rootStart + 4);
		newTable.writeInt(0);
		//Set page type
		newTable.setPageType(13);
		//Initialize the record length for each new table
		newTable.getRecordLength(commandElements[1]);
		//System.out.println(newTable.recordLength);
		
		BPlusTree bPlusTree = new BPlusTree(newTable);

		//tableMap.put(tableName, newTable);
		bPlusTreeMap.put(tableName, bPlusTree);
	}
	
	/**
	 * Parse the insert command
	 * @param userCommand
	 * @throws IOException
	 */
	public static void parseInsertString(String userCommand) throws IOException {
		//INSERT INTO TABLE (column_list) table_name VALUES (value1,value2,value3,…);
		String commandElements[] = userCommand.split(" ");
		String tableName = commandElements[4];
		String columnList[] = commandElements[3].substring(1,commandElements[3].length()-1).split(",");
		String columnValues[] = commandElements[6].substring(1,commandElements[6].length()-1).split(",");
		if(((columnList.length == 1) && (columnList[0].equals(""))) || columnList.length == columnValues.length)  {
			//RandomAccessFileCreation r = tableMap.get(tableName);
			BPlusTree tree = bPlusTreeMap.get(tableName);
			tree.root.insertRecordIntoTable(tableName, columnList, columnValues, rafTable, rafColumn, tree);
		} else {
			System.out.println("Incorrect format");
		}
		
	}
	
	/**
	 * Parse the select command
	 * @param userCommand
	 * @throws IOException
	 */
	private static void parseQueryString(String userCommand) throws IOException {
		/* SELECT *
		FROM table_name
		WHERE column_name operator value;
		*/
		ArrayList<String> queryStringTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		String tableName = queryStringTokens.get(3);
		String wildCard = queryStringTokens.get(1);
		int i =5,j=0;
		String decidingCol[] = new String[(queryStringTokens.size()-5)/3], operator[] = new String[(queryStringTokens.size()-5)/3], compVal[] = new String[(queryStringTokens.size()-5)/3], comparator[] = new String[(queryStringTokens.size()-5)/3], column8 = null, column9 = null, column10 = null, column11 = null;
		if(queryStringTokens.size()>4){
			while(queryStringTokens.size()>=i+3) {
				if(i!=5)
	            	comparator[j] = queryStringTokens.get(i);
	            if(i==5) {
	            	decidingCol[j] = queryStringTokens.get(i);
		            operator[j] = queryStringTokens.get(i+1);
		            compVal[j] = queryStringTokens.get(i+2);
		            i+=3;
	            }
	            else {
	            	decidingCol[j] = queryStringTokens.get(i+1);
		            operator[j] = queryStringTokens.get(i+2);
		            compVal[j] = queryStringTokens.get(i+3);
		            i+=4;
	            }
	            j+=1;
			}
         }
		if(queryStringTokens.size()>8){
            column8 = queryStringTokens.get(8);
            column9 = queryStringTokens.get(9);
            column10 = queryStringTokens.get(10);
            column11 = queryStringTokens.get(11);
         }
		//RandomAccessFileCreation r = tableMap.get(tableName);
		//r.queryFromTable(tableName, wildCard, deciding_col, operator, comp_val, rfTbl, rfCol);
		BPlusTree tree = bPlusTreeMap.get(tableName);
		tree.root.queryFromTable(tableName, wildCard, decidingCol, operator, compVal, comparator, rafTable, rafColumn, column8, column9, column10, column11);
		
		
	}

	/**
	 * To parse the delete command 
	 * @param userCommand
	 * @throws IOException
	 */
	private static void parseDeleteString(String userCommand) throws IOException {
		//DELETE FROM TABLE table_name WHERE row_id = key_value;
		ArrayList<String> queryStringTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		if(queryStringTokens.size() == 4 || queryStringTokens.size() == 8) {
			String tableName = queryStringTokens.get(3);
			String decidingCol = null;
			String operator = null;
			String compVal = null;
			if(queryStringTokens.size() > 4){
	            decidingCol = queryStringTokens.get(5);
	            operator = queryStringTokens.get(6);
	            compVal = queryStringTokens.get(7);
	         }
			
			//RandomAccessFileCreation r = tableMap.get(tableName);
			//r.deleteFromTable(tableName, deciding_col, operator, comp_val, rfTbl, rfCol);
			BPlusTree tree = bPlusTreeMap.get(tableName);
			tree.root.deleteFromTable(tableName, decidingCol, operator, compVal, rafTable, rafColumn);
		} else {
			System.out.println("Invalid command! Seek Help;");
		}
	}

	/**
	 * Method drops the table table_name
	 * @param userCommand
	 * @throws IOException
	 */
	private static void parseDropString(String userCommand) throws IOException {
		//DROP TABLE table_name;
		ArrayList<String> queryStringTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		if(queryStringTokens.size() == 3) {
			String tableName = queryStringTokens.get(2);
			//RandomAccessFileCreation r = tableMap.get(tableName);
			//r.processDropString(tableName, rfTbl, rfCol);
			BPlusTree tree = bPlusTreeMap.get(tableName);
			tree.root.processDropString(tableName, rafTable, rafColumn);
			tree.root.close();
			tree.root.file.delete();
			System.out.println("Drop table success!");
		} else {
			System.out.println("Invalid Command");
		}
		
	}

	/**
	 * Method used to parse the update command
	 * @param userCommand
	 * @throws IOException
	 */
	private static void parseUpdateString(String userCommand) throws IOException {
		// UPDATE table_name SET column_name = value [WHERE condition]
		ArrayList<String> queryStringTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		if(queryStringTokens.size() == 6 || queryStringTokens.size() == 10) {
			String tableName = queryStringTokens.get(1);
			String columnToBeUpdated = queryStringTokens.get(3);	
			String valueToBeSet = queryStringTokens.get(5);
			String decidingCol = null;
			String operator = null;
			String compVal = null;
			if(queryStringTokens.size() > 6){
				decidingCol = queryStringTokens.get(7);
				operator = queryStringTokens.get(8);
				compVal = queryStringTokens.get(9);
				
	         }
			//RandomAccessFileCreation r = tableMap.get(tableName);
			//r.processUpdateString(tableName, columnToBeUpdated, valueToBeSet, deciding_col, operator, comp_val, rfTbl, rfCol);
			BPlusTree tree = bPlusTreeMap.get(tableName);
			tree.root.processUpdateString(tableName, columnToBeUpdated, valueToBeSet, decidingCol, operator, compVal, rafTable, rafColumn);
		} else {
			System.out.println("Invalid");
		}
		
	}
	
	/**
	 *  Help: Display supported commands
	 */
	public static void help() {
		System.out.println(line("*",80));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\tSELECT * FROM table_name;                        					Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  					Display records whose rowid is <id>.");
		System.out.println("\tDROP TABLE table_name;                          					Remove table data and its schema.");
		System.out.println("\tUPDATE table_name SET column_name = value [WHERE condition]       Update table data.\"");
		System.out.println("\tDELETE FROM TABLE table_name;        								Delete all records");
		System.out.println("\tDELETE FROM TABLE table_name WHERE row_id = <value>;       		Delete records whose rowid is <id>.");
		System.out.println("\tSHOW tables;           											Display the table names");
		System.out.println("\tVERSION;                                         					Show the program version.");
		System.out.println("\tHELP;                                            					Show this help information");
		System.out.println("\tQUIT;                                            					Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}

	/**
	 * Display the version of the database
	 */
	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
}
