
import json
import os
import psycopg2
import time
import sys
#import multiprocessing as mp
now = time.localtime(time.time())
print "start time:", time.asctime(now)

#connection variables
myHost = "localhost"
myPort = "5432"
myUser = "byrnem"
db = "feomike"
schema = "hmda"
#myTab is used as the unique list to drive which FIs are being run
myTab = "ffiec_ts_2014"
#myLar is used to get the unique set of geographies for this particular FI
myLar = "ffiec_lar_2014"

#this function gets the list of FIs on which to run; it makes a selection of the TS
#then cycles through every respondent ID and agency code in the result and fires of the
#mk_file code; if myLoc = State, then it finds in which states this RID has LARs
def runFIList(myQry, myLoc):
	mySQL = "SELECT respondent_id, agency_code, respondent_name_ts "
	mySQL = mySQL + "FROM " + schema + "." + myTab + " "
	#mySQL = mySQL + " where respondent_id = '7197000003'and agency_code = '7'; "
	#mySQL = mySQL + " where respondent_id = '0000061650'and agency_code = '5'; "
	#mySQL = mySQL + " where respondent_id = '0000004137'; "
	mySQL = mySQL + " where (CAST(coalesce(lar_count,'0') as integer)) > 2000; "
	#execute the SQL string
	theCur.execute(mySQL)
	row_name = []
	#cursor through the return to get the value
	if theCur.rowcount > 0:
		for row in theCur:
			RID = str(row[0])
			myAC = str(row[1])
			myFI = str(row[2]).strip()
			myFI = myFI.replace(",","", 4).replace("'","",4).replace(".","",4).replace("_","",4)
			myFI = "'" + myFI + "'" 
			row_name.insert(len(row_name),row[2].strip().title()  )
			#run the main script
			print "		doing ... " + RID + " for " + myQry + " in " + myLoc
			if myLoc == "All":
				os.system("python mk_file.py " + RID + " " + myAC + " " + myFI + " " + "nationwide " + myQry)
			if myLoc == "State":
				runState(myQry, RID, myAC, myFI)
	return

#find the states they did business in this year
#this is a highly limited process, as true variation would require 
#knowing every state in which the FI did business
def runState(myQry, RID, myAC, myFI):
	stCur = conn.cursor()
	mySQL = "SELECT state_code FROM " + schema + "." + myLar + " WHERE "
	mySQL = mySQL + "respondent_id = '" + RID + "' and agency_code = '" 
	mySQL = mySQL + myAC + "' and state_code <> 'NA' GROUP BY state_code; " # limit 10; "
	#execute the SQL string
	stCur.execute(mySQL)
	#cursor through the return to get the value
	#then pass the value of the return state code on to main process
	#since the state code is only two characters, the main process can distinguish it
	#as a state code. 
	if stCur.rowcount > 0:
		for strow in stCur:
			myLoc = str(strow[0])
			os.system("python mk_file.py " + RID + " " + myAC + " " + myFI + " " + myLoc + " " + myQry)

def writeNamesFile():
	myPath = "../../data/"
	listfiles = os.listdir(myPath)
	FIs = []
	for myFile in listfiles:
		if os.path.isdir(os.path.join(myPath, myFile)):
			FIs.insert(len(FIs),myFile.title())
	name_data = {"financialInstitutionNames":FIs}
	with open(myPath + "/fi.json", 'w') as outfile:
	    json.dump(name_data, outfile)
	
#make database connection - so that we can use cursors to update the table
myConn = "dbname=" + db + " host=" + myHost + " port=" + myPort + " user=" + myUser
conn = psycopg2.connect(myConn)
theCur = conn.cursor()

def runAll(myQry, myLoc):
	#run for all rows (no-rid/agency_cde), at nationwide
	if myLoc == "All":	
		print "		doing ... "  + myQry + " in " + myLoc 
		os.system("python mk_file.py -9 -9 -9 nationwide " + myQry)
	if myLoc == "State":
		states = ["01","02","04","05","06","08","09","10","11","12","13","15",
		"19","16","17",	"18","20","21","22","25","23","27","28","24","26","29",
		"30","38","31","33","37","34","35","32","36","39","40","41","42","44",
		"45","46","47","48","49","51","50","53","55","54","56"]
		for ST in states:
			print "		doing ... "  + myQry + " in " + myLoc + " " + ST
			os.system("python mk_file.py -9 -9 -9 " + ST + " " + myQry)


queries = ["all","single-family","refi","home-improvement","purchased-loan"]
#queries = ["all"]
for qry in queries:
	#runAll(qry, "All")
	#runAll(qry, "State")
	runFIList(qry, "All")
	#runFIList(qry, "State")
writeNamesFile()

theCur.close()
conn.close()

now = time.localtime(time.time())
print "end time:", time.asctime(now)
 
