#  SAP/HANA DB example
# 
#  Author: tuttipazzo

import traceback
from hdbcli import dbapi 
import sys
if sys.version_info[0] < 3:
	from collections import OrderedDict

class database:
    def __init__(self, address, port, tName, createStmt, user, passwd, drop=False, saccess=True, debug=False):
	"""
	Initialize SAP/HANA DB and create the table if it does not exist.

	Parameter:
        address - right now only tested on the master node
           port - See below.
      tableName - Have to provide the table name.
     createStmt - The actual SQL statement to create the table.
           user - You want to use 'SYSTEM' if you CSV importing is desired, 
                  otherwise you will have to use a hana DB user which will 
                  add 1 record at a time.  saccess set be set to true is user
                  set to SYSTEM.
         passed - SYSTEM user's password. 
        saccess - If you use SYSTEM user set this to true.
          debug - Do not turn on debug unless you absolutely have to.  
                  It generates a lot fo info.

	According to:
	   https://help.sap.com/viewer/0eec0d68141541d1b07893a39944924e/2.0.02/en-US/d12c86af7cb442d1b9f8520e2aba7758.html

	The instance type & tenant type are part of the port number:
		from hdbcli import dbapi
		conn = dbapi.connect(
		    address="<hostname>",
		    port=3<NN>MM, 
		    user="<username>", 
		    password="<password>"
		)
		For HANA tenant databases, you can use the port number 3<NN>13 (where <NN> is the SAP instance number).
		For HANA system databases in a multitenant system, the port number is 3NN13.
		For HANA single-tenant databases, the port number is 3NN15.
	"""
	self.drop = drop
	self.colNames=[]

	self.sysAccess = saccess
	self.debug = debug

	if( tName == "" ):
	    raise ValueError("Table Name must be provided.")

	self.tableName = tName

	if( address == "" ):
	    raise ValueError("Address/Hostname must be provided.")

	self.address = address

	if( port == "" ):
	    raise ValueError("Port number must be provided.")

	self.port = port

	if( createStmt == "" ):
	    raise ValueError("Table create statement must be provided.")

	if "TEXT" in createStmt.upper():
		print("""
		      ERROR:
		      The TEXT column field data type will break update & delete SQL statements
		      as they do not support WHERE clause against text matches
		      with columns defined as type TEXT.  Please use VARCHAR(<1-5000>)
		      type instead.
		      Exiting....
		      """)
		return

	self.createStmt = createStmt

	if( user == "" ):
	   raise ValueError("User must be provided.")

	self.user = user

	if( passwd == "" ):
	   raise ValueError("Passwd must be provided.")

	self.passwd = passwd

	try:
	    self.conn = dbapi.connect(address=self.address, port=self.port, user=self.user, password=self.passwd)
	except dbapi.Error as e:
	    print("ERROR: connecting to \"{0}:{1}\": {2}\n{3}".format(self.address,self.port,e, traceback.format_exc()))
	finally:
	    if (self.conn):
		if self.drop == True :
			self.dropTable()
		self.__createTable()
		self.__populateColumnInfo()

    def __findTable(self):
	"""
	Search for table.  Returns True if found, False otherwise
	"""
	found = False
	sql = 'SELECT TABLE_NAME FROM TABLES'
	try:
	    cursor = self.conn.cursor()
	    cursor.execute(sql)
	    result_set = cursor.fetchall()
	    cursor.close()
	except dbapi.Error as e:
	    print("ERROR: __findTable() \"{0}\": {1}\n{2}".format(self.tableName,e,traceback.format_exc()))

	for cname in result_set:
		if self.tableName.upper() in cname[0].decode():
			print("Table \'{0}\' exists...".format(self.tableName))
			found = True
	return found

    def __createTable(self):
	"""
	Use user provided SQL to create the table if it does not exist.
	"""
	if self.__findTable() == True:
		return

	try:
	    cursor = self.conn.cursor()
	    cursor.execute(self.createStmt)
	    self.conn.commit()
	    cursor.close()
	except dbapi.Error as e:
	    print("ERROR: createTable() to \"{0}:{1}\" \"{2}\": {3}\n{4}".format(self.address,self.port,self.createStmt,e,traceback.format_exc()))

    def __populateColumnInfo(self):
	"""
	Create a dictionary of columns names and respective types. This class
	needs to know the column field types so it can type the data correctly
        and provide column field names for displays.
	"""
	# Dictionary of column names as key and column type as value
	# Ex: columns['FNAME'] = "VARCHAR"
	#
	# Python versions < 3 do not keep dictionaries ordered so
	# have to use OrderedDict from collections.
	if sys.version_info[0] < 3:
		self.columns=OrderedDict()
	else:
		self.columns={}

	if self.sysAccess == True:
		sql = "select column_name,data_type_name from SYS.COLUMNS where table_name=\'{0}\' ORDER by POSITION".format(self.tableName.upper())
	else:
		sql = "select column_name from SYS.M_CS_COLUMNS where table_name=\'{0}\'".format(self.tableName.upper())
	try:
	    cursor = self.conn.cursor()

	    cursor.execute(sql)
	    data = cursor.fetchall()

	    if self.debug == True:
		print("RAW DB output: {0}".format(data))

	    # The column information is returned as a list of list.
	    # The column indexes matches with the table so all we want
	    # to save is the column name & type
	    for row in data:
		if self.debug == True:
			print("{0}\nPreformated - Name: {1}, {2}".format(row,row[0].decode(),type(row[0])))

		colName = row[0].decode()
		if self.sysAccess == True:
			t = row[1].upper()
			colDataType = t.upper()
		else:
			colDataType = 'VARCHAR'

		self.columns[colName] = colDataType
		if self.debug == True:
			print("Postformated - Name: {0}, {1}".format(colName,type(colDataType)))
			print("\t {0}".format(self.columns))

	    cursor.close()
	except dbapi.Error as e:
	    print("ERROR: connecting to \"{0}:{1}\": {2}\n{3}".format(self.address,self.port,e,traceback.format_exc()))

    def __getColumnNames(self):
        """
        Get the column Names for building sql where clauses
        """
	if self.columns is None:
		print("ERROR: NameError(\"columns\"): \n{0}".format(traceback.format_exc()))
		return []

        return self.columns.keys()

    def getColumnNames(self):
	"""
	Public getter.
	"""
	return self.__getColumnNames()

    def __normalizeColData(self,colData):
	"""
	Required to when adding data so that the proper type can be added
	to each column field.	
	"""
        newColData = []
	if self.debug == True:
		print("Column Data Length: {0}\nColumn Data: {1}".format(len(colData),colData))

        try:
            colNames = self.__getColumnNames()
        except ValueError as e:
            print("ERROR: retrieving column name data: {0}\n{1}", e, traceback.format_exc())
            return

	if self.debug == True:
		print("Column Names Length: {0}\nColumn Names: {1}".format(len(colNames),colNames))

        try:
            for i in range(len(colData)):
		if self.debug == True:
			print("i: {0}\ncolNames[{0}]={1}\ncolumns[colNames[{1}]]={2}".format(i,colNames[i],self.columns[colNames[i]]))
                if "VARCHAR" in self.columns[colNames[i]].upper():
                    newColData.insert(i,str("\'{0}\'".format(colData[i])))
                else:
                    newColData.insert(i,colData[i])
        except ValueError as e:
            print("ERROR: {0}\n{1}".format(e,traceback.format_exc()))
        except IndexError as e:
            print("ERROR: {0}\n{1}".format(e,traceback.format_exc()))

        return newColData

    def add(self, rowData):
        """
        Insert row data into table.
        """
        newData = []

        try:
            cursor = self.conn.cursor()

            # Let's build up the row data statement values to be inserted
            addStm='INSERT INTO {0} VALUES ('.format(self.tableName)
            aSize = len(rowData)
            newData = self.__normalizeColData(rowData)
	    if self.debug == True:
		print("newData: {0}".format(newData))

            for i in range(aSize):
                if( i == (aSize - 1) ):
                    addStm = addStm + '{0})'.format(newData[i])
                else:
                    addStm = addStm + '{0},'.format(newData[i])

	    if self.debug == True:
		print("SQL Add statement: {0}".format(addStm))

            rc = cursor.execute(addStm)
	    if self.debug == True:
		print("RC execute \'ADD\' statement: {0}".format(rc))

            self.conn.commit()
            cursor.close()
        except dbapi.Error as e:
            print("ERROR: connecting/adding RowData to \"{0}:{1}\": {2}\n{3}".format(self.address,self.port,e,traceback.format_exc()))
        except ValueError as e:
            print("ERROR: row data: (\"", rowData, "\"):i {0}\n{1}".format(e,traceback.format_exc()))
        except IndexError as e:
            print("ERROR: indexing row data: (\"", rowData, "\"): {0}\n{1}".format(e,traceback.format_exc()))

    def __csvFileloading(self, disable):
	"""
	Enable/Disable CSV import loading for importing CSV data
	into HANA DB tables.  NOTE: Requires SYSTEM user access.
	"""
        try:
            cursor = self.conn.cursor()

            alterStm='ALTER SYSTEM ALTER CONFIGURATION ( \'indexserver.ini\',\'SYSTEM\' ) ' + \
		     'SET ( \'import_export\',\'enable_csv_import_path_filter\' ) = \'{0}\' ' + \
		     'WITH RECONFIGURE'.format(disable) 

            if self.debug == True:
                print("Alter SQL Statement: {0}".format(alterStm))

            rc = cursor.execute(alterStm)
            if self.debug == True:
                print("RC execute \'ALTER\' statement: {0}".format(rc))

            self.conn.commit()
            cursor.close()
        except dbapi.Error as e:
            print("ERROR: connecting/alter to \"{0}:{1}\": {2}\n{3}".format(self.address,self.port,e,traceback.format_exc()))
        except ValueError as e:
            print("ERROR: Alter: (\"", disable, "\"):i {0}\n{1}".format(e,traceback.format_exc()))

	
    def importFromCSV(self, csvFile, fieldDelimiter=',', recordDelimiter='\n'):
        """
        Import row data from CSV file. By default CSV import loading is disabled 
	(see SAPNOTE https://launchpad.support.sap.com/#/notes/2109565)
	because of security concern.
	
	CSV file contains only data, otherwise header will get inserted a record.

	Default field delimiter is comma (',') 
	Default record delimiter is newline.

	NOTE: Because we are enabling/disabling calling this function
              requires SYSTEM user access.
        """
        try:
	    # Enable CSV file importing
	    self.__csvFileloading(False)

            cursor = self.conn.cursor()

            # Let's build up the row data statement values to be inserted
	    importStm='IMPORT FROM CSV FILE \'{0}\' '.format(csvFile) + \
                  'INTO {0} '.format(self.tableName) + \
                  'WITH RECORD DELIMITED BY \'\\n\' '.format(fieldDelimiter) + \
                  'FIELD DELIMITED BY \',\''.format(recordDelimiter)

	    if self.debug == True:
		print("Import SQL Statement: {0}".format(importStm))

            rc = cursor.execute(importStm)
	    if self.debug == True:
		print("RC execute \'ADD\' statement: {0}".format(rc))

            self.conn.commit()
            cursor.close()
        except dbapi.Error as e:
            print("ERROR: connecting/adding CSV File Data to \"{0}:{1}\": {2}\n{3}".format(self.address,self.port,e,traceback.format_exc()))
        except ValueError as e:
            print("ERROR: CSV File: (\"", csvFile, "\"):i {0}\n{1}".format(e,traceback.format_exc()))
        except IndexError as e:
            print("ERROR: indexing row data: (\"", csvFile, "\"): {0}\n{1}".format(e,traceback.format_exc()))
        except IOError as e:
            print("ERROR: CSB File IO Error: (\"", csvFile, "\"): {0}\n{1}".format(e,traceback.format_exc()))
	finally:
	    # Disable CSV file importing
	    self.__csvFileloading(False)

    def delete(self, rowData):
        """
        Delete the row corresponding to the given rowdata.  This method
        will compare every value in the row in the select statement.
        """
        colNames=""
        try:
            colNames = self.__getColumnNames()
        except ValueError as e:
            print("ERROR: retrieving column name data: {0}\n{1}", e, traceback.format_exc())
            return

	if self.debug == True:
		print("Column Names: {0}".format(colNames))

        try:
            cursor = self.conn.cursor()

            # Let's build up the row data statement values to be inserted
            deleteStm="DELETE FROM {0} WHERE (".format(self.tableName)
            aSize = len(rowData)
            for i in range(aSize):
                if( i == (aSize - 1) ):
                    deleteStm = deleteStm + "{0} = \'{1}\')".format(colNames[i],rowData[i])
                else:
                    deleteStm = deleteStm + "{0} = \'{1}\' AND ".format(colNames[i],rowData[i])

	    if self.debug == True:
		print("SQL DELETE statement: {0}".format(deleteStm))

            rc = cursor.execute(deleteStm)
	    if self.debug == True:
		print("RC execute \'DELETE\' statement: {0}".format(rc))

            self.conn.commit()
            cursor.close()
        except dbapi.Error as e:
            print("ERROR: connecting/deleting row data \"{0}:{1}\": {2}\n{3}".format(self.address,self.port,e,traceback.format_exc()))
        except ValueError as e:
            print("ERROR: deleting row data: (\"", rowData, "\"): {0}\n{1}".format(e,traceback.format_exc()))
        except IndexError as e:
            print("ERROR: indexing row data: (\"", rowData, "\"): {0}\n{1}".format(e,traceback.format_exc()))

    def update(self, curRowData, newRowData):
        """
        Update a row given curRowData with new rowData.
	Requires the current row data for comparision.
        """
        colNames=""
        try:
            colNames = self.__getColumnNames()
        except ValueError as e:
            print("ERROR: retrieving column name data: {0}\n{1}", e, traceback.format_exc())
            return

	if self.debug == True:
		print("Column Names: {0}".format(colNames))

        try:
            cursor = self.conn.cursor()

            # Let's build up the SET part of the Update st
            updateStm="UPDATE {0} SET ".format(self.tableName)
            aSize = len(curRowData)
            for i in range(aSize):            
                if( i == (aSize - 1) ):
                    updateStm = updateStm + "{0} = \'{1}\' WHERE ".format(colNames[i],newRowData[i])
                else:
                    updateStm = updateStm + "{0} = \'{1}\', ".format(colNames[i],newRowData[i])

            # Let's build up the WHERE part of the Update statement
            aSize=len(curRowData)
            for i in range(aSize):
                if( i == (aSize - 1) ):
                    updateStm = updateStm + "\"{0}\" = \'{1}\'".format(colNames[i],curRowData[i])
                else:
                    updateStm = updateStm + "\"{0}\" = \'{1}\' AND ".format(colNames[i],curRowData[i])

	    if self.debug == True:
		print("SQL UPDATE statement: {0}".format(updateStm))

            rc = cursor.execute(updateStm)
	    if self.debug == True:
		print("RC execute \'UPDATE\' statement: {0}".format(rc))

            self.conn.commit()
            cursor.close()
        except dbapi.Error as e:
            print("ERROR: connecting/updating row data \"{0}:{1}\": {2}\n{3}".format(self.address,self.port,e,traceback.format_exc()))
        except ValueError as e:
            print("ERROR: updating row data: {0}\n{1}".format(e,traceback.format_exc()))
        except IndexError as e:
            print("ERROR: indexing row data:i {0}\n{1}".format(e,traceback.format_exc()))

    def getAllRows(self):
        """
        Fetch all rows from the table
        """
        records = ""
        try:
            cursor = self.conn.cursor()
            rc = cursor.execute("SELECT * FROM {0}".format(self.tableName))
	    if self.debug == True:
		print("RC execute \'SELECT *\' statement: {0}".format(rc))

            records = cursor.fetchall()
            cursor.close()
        except dbapi.Error as e:
            print("ERROR: connecting/fetching row data \"{0}:{1}\": {2}\n{3}".format(self.address,self.port,e,traceback.format_exc()))

        return records

    def dropTable(self):
        """
        Drop the table.
	(My favorite SQL statment. ;-))
        """
	if self.__findTable() == False:
		return

        try:
            cursor = self.conn.cursor()

            dropStm="DROP TABLE {0}".format(self.tableName)
	    print("Dropping Table: {0}".format(self.tableName))
	    if self.debug == True:
		print("SQL DROP statement: {0}".format(dropStm))

            rc = cursor.execute(dropStm)
	    if self.debug == True:
		print("RC execute \'DROP TABLE\' statement: {0}".format(rc))

            self.conn.commit()
            cursor.close()
        except dbapi.Error as e:
            print("ERROR: connecting/DROP TABLE \"{0}:{1}\": {2}\n{3}".format(self.address,self.port,e,traceback.format_exc()))

