# Davisbase
The Davisbase application supports the following:

* The root page is available in second 512 bytes(i.e. from 512 bytes) of the table.
* When a Table is created, a leaf page and a root is created.
* Multiple conditions can be given to Select Query like [select * from table where col_name1  = value1 and col_name2 = value2]. This is an additional feature.
* Duplicate primary keys cannot be inserted.
* Checks for is nullable.
* Supported Indexes using the file structure.

While executing the queries, please take care of: 
* The space and punctuations have to be in the format provided in the below queries.
* The queries has to be given using the following syntax.
* Queries execution are case-insensitive.
* For Primary key, represent as "pri". Primary key field can be empty also.
* Always primary key should be first then is nullable field. Is nullable field is mandatory.
* Do not include quotes for String values

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	Query formats: 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

show tables;

create table table_name(col1_name int pri no,col2_name text yes,col3_name text yes,col4_name text yes);      
// "pri" represents primary key(has to be in the first column position only and an integer data type). 
// yes,no are used to represent nullable(yes) and not nullable(no). Has to be given.

insert into table (col1,col2,col3,col4) table_name values (value1,value2,value3,value4);                     
// All columns can be provided in the list and in order of created table columns. All column values have to be given in order. No need of including quotes for Strings.

insert into table () table_name values (value1,value2,value3,value4);
// If not all columns need to be given then only put "()" and the query inserts into in order of created table columns. All column values have to be given in order. No need of including quotes for Strings.

select * from table_name;                                                                                    
// selects all column values

select col1_name,col2_name from table_name;                                                                  
//col1_name,col2_name - can be more than 2 columns

select * from table_name where col1_name = value;                                                             
// operator can be =, <, >

select col1_name,col2_name from table_name where col1_name = value;                                           
//col1_name,col2_name - can be more than 2 columns. operator can be =, <, >

update table_name set col2_name = value;                                                                      
//sets all row values for the given column to the value. No need of including quotes for Strings.

update table_name set col3_name = value where col2_name = value;                                               
//Operator can be =, <, >. No need of including quotes for Strings.

delete from table table_name where col1_name = value;	                                                     
//operator can be =, <, >. No need of including quotes for Strings.

delete from table table_name;                                                                                
//deletes all values. 

drop table table_name;                                                                                       
// table_name is dropped

help;                                                                                                        
// gives overview of query formats. 

version;                                                                                                     
// gives version of the application

quit;                                                                                                        
// to exit the application
