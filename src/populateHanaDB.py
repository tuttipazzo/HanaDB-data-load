#  What is this script? 
#	This script creates dummy data against HANA DB for testing purposes.
#	It uses the same record for each row.
#
#  Why was this script writen?
#	This script was created to load a database so that HA cluster
#	operations can be tested against a loaded database for customer
#	reproduction issues.
#
#  HOW was this script written?
#	This script is written in 2 parts (or 2 files):
#		1) An abstract HANA DB part (hanaDatabse.py)
#		2) The work part (this file) which knows
#		   what the table is, the column fields, field types, and 
#		   all the logic surrounding what it does. 
#
#  When was this script written? 
#	During hackweek--mostly...
#       Mar/2021
#
#  Who wrote this script?
#	This crazy guy below....
#
#  Author: tuttipazzo
# 
from hanaDatabase import *
import traceback
import string
import random
import sys
import time
import multiprocessing
import os

# SAP/HANA ships its own python version and sets PYTHONPATH for
# sap admin account.  If we want to use python modules not available
# in SAP python version user needs to add system python to sap admin
# PYTHONPATH path
#
try:
	import concurrent.futures 
except ImportError:
	print("Python module \'concurrent.futures\' is not in path.")
	print("Please add system python path to PYTHONPATH  like so:")
	print("\nexport PYTHONPATH=%{$PYTHONPATH}:/usr/lib/python2.7/site-packages")
	print("\nand try again.")
	sys.exit(-1)

workers=multiprocessing.cpu_count()
#workers=1
ONEGIG=1073741824
totalBytes = ONEGIG * 1
recBytes = 0
totalRecs = 0
csvFn='bulkrecords.csv'
db = None 
add1 = []
numCols=8
columnBytes=4096
csvNumRec=columnBytes
tableName = "Contacts"
address = "localhost"
port=30015
user='system'
passwd='<Enter a password here>'

# SAP/HANA DB limit in VARCHAR column data size is 5000.
createStmt = """CREATE TABLE {0} (
                            fName   VARCHAR(5000),
                            mName   VARCHAR(5000),
                            lName   VARCHAR(5000),
                            email   VARCHAR(5000),
                            address VARCHAR(5000),
                            city    VARCHAR(5000),
                            state   VARCHAR(5000),
                            zipCode VARCHAR(5000));""".format(tableName)

def genCSVData(fileName, record, numRec):
    """
    Generate record data in CSV format.  Note the column data only.
    No header is needed.
    """
    with open(fileName, mode='w+') as f:
    	for i in range(numRec):
    		f.write(','.join(record) + '\n')

def doCSVImport():
    """
    Adding 1 record at a time was way too slow.  So this method works
    much better.  Adding 1 gig of data using this method takes around 30 
    second. The same 1 gig test with adding 1 record at a time takes around 
    4 min.

    importFromCSV() function takes 2 additional parameters:
       fieldDelimiter - defaults to comma ','
      recordDelimiter - defaults to newline '\n'

    If you get errors most likely you are NOT using SYSTEM user.
    Note: ImportFromCSV will temporarily enable csv import loading
          and will disable it when it is done.
          This requires SYSTEM level access to alter that table.

    Note2: Be careful when changing either the delimiter or that the column
           data does not contain the delimiter.
    """
    global recBytes, totalRecs, workers, totalBytes, csvFn, csvNumRec

    print("Using {0} threads...".format(workers))
    recs=0
    totalBytesAdded=0.0

    # IMPORT statement requires absolute path to file.
    fileName = os.getcwd() + '/' + csvFn

    while recs < totalRecs:
	db.importFromCSV(fileName)
    	totalBytesAdded = totalBytesAdded + (recBytes * workers * csvNumRec)
	recs = recs + (workers * csvNumRec)
	progress = 'Added %d'%recs + '/%d'%totalRecs + ' records (%d'%totalBytesAdded + \
	           ' bytes/%d'%totalBytes + ' GB)\r'
	sys.stdout.write(progress)
	sys.stdout.flush()

def doOne():
    """
    Insert 1 record at a time.  This is threaded to correspond to
    to the node's number of cores.
    """
    global recBytes, totalRecs, add1, workers, totalBytes
    recList=[]

    # The executor map in the while loop below requires each thread
    # to have it's copy of the record, otherwise you will get threads
    # stomping on each other's memory space.  Weird things happen.
    for i in range(workers):
	recList.append(add1)

    print("Using {0} threads...".format(workers))
    recs=0
    totalBytesAdded=0
    while recs < totalRecs:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            results = executor.map(db.add, recList)  
            executor.shutdown(wait=True) # wait for all complete

    	totalBytesAdded = totalBytesAdded + (recBytes * workers)
	recs = recs + workers
	progress = 'Added %d'%recs + '/%d'%totalRecs + ' records (%d'%totalBytesAdded + \
	           ' bytes/%d'%totalBytes + ' GB)\r'
	sys.stdout.write(progress)
	sys.stdout.flush()

def createRecord():
    """
    This function creates dummy fill data for each column & sets up variables
    needed for the rest of the script.

    """
    global recBytes, totalRecs, add1, doImport, numCols, columnBytes

    colStr = ''.join(random.choice(string.ascii_letters) for _ in range(columnBytes))
    recBytes = len(colStr) * numCols
    print('Record size: {0} bytes'.format(recBytes))
    totalRecs = float(totalBytes) / float(recBytes)

    if doImport:
    	totalRecs = totalRecs + csvNumRec
	overflow = totalRecs % csvNumRec
        totalRecs = totalRecs - overflow

    for i in range(numCols):
	add1.append(colStr)

def doDB():
    """
    Create the database handle.  User needs to supply the SQL statement as the 
    database class knows nothing about the tables or the columns.  It's a 
    simple conduit to the HANA DB so that the DB logic is kept here 
    (i.e., it's resuable). 
    
    Parameter:
	address - right now only tested on the master node
	   port - Figure out who the master is and use the port # 30015
                  (see hanaDatase.py for info port # is calcuated.)
      tableName - Have to provide the table name.
     createStmt - The actual SQL statement to create the table.
           user - You want to use 'SYSTEM' if you CSV importing is desired, 
                  otherwise you will have to a hana DB user which force 
                  adding 1 record at a time.
         passed - SYSTEM user's password.
        saccess - If you use SYSTEM user set this to true.
          debug - Do not turn on debug unless you absolutely have to.  
                  It generates a lot fo info.
    """
    global db

    try:
        db = database(address, port, tableName, createStmt, user, passwd, saccess=True, debug=False)
    except Exception as e:
        print("ERROR: {0}\\n{1}".format(e, traceback.format_exc()))
	exit
   
if __name__ == "__main__":
    doDB()
    doImport = True
    createRecord()
    genCSVData(csvFn, add1, csvNumRec)

    t0 = time.time()

    # TODO: Right now CSV loading is the default as it is 8 times faster
    #       than adding a record at a time.  Need to add script cmdline 
    #       arguments. 
    if doImport:
	workers = 1
	doCSVImport()
    else:
	doOne()

    t1 = time.time() - t0
    print("")
    print("Elapsed time: {0:.2f} sec".format(t1))
