
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
myUser = "feomike"
db = "feomike"
schema = "hmda"
#myTab is used as the unique list to drive which FIs are being run
myTab = "ffiec_ts_g1_2011"
#myLar is used to get the unique set of geographies for this particular FI
myLar = "ffiec_lar_2014"

def returnFIList(myLoc):
	mySQL = "SELECT respondent_id, agency_code, respondent_mail_name "
	mySQL = mySQL + "FROM " + schema + "." + myTab 	
	#execute the SQL string
	theCur.execute(mySQL)
	#cursor through the return to get the value
	if theCur.rowcount > 0:
		for row in theCur:
			RID = str(row[0])
			myAC = str(row[1])
			myFI = "'" + str(row[2]).strip() + "'" 
			#run the main script
			print " doing ... " + RID
			if myLoc == "All":
				os.system("python mk_file.py " + RID + " " + myAC + " " + myFI + " " + "nationwide")
			if myLoc == "State":
				runState(RID, myAC, myFI)
	return

#find the states they did business in this year
#this is a highly limited process, as true variation would require 
#knowing every state in which the FI did business
def runState(RID, myAC, myFI):
	stCur = conn.cursor()
	mySQL = "SELECT state_code FROM " + schema + "." + myLar + " WHERE "
	mySQL = mySQL + "respondent_id = '" + RID + "' and agency_code = '" 
	mySQL = mySQL + myAC + "' and state_code <> 'NA' GROUP BY state_code; "# limit 2; "	
	#execute the SQL string
	stCur.execute(mySQL)
	#cursor through the return to get the value
	#then pass the value of the return state code on to main process
	#since the state code is only two characters, the main process can distinguish it
	#as a state code. 
	if stCur.rowcount > 0:
		for strow in stCur:
			myLoc = str(strow[0])
			os.system("python mk_file.py " + RID + " " + myAC + " " + myFI + " " + myLoc)

#make database connection - so that we can use cursors to update the table
myConn = "dbname=" + db + " host=" + myHost + " port=" + myPort + " user=" + myUser
conn = psycopg2.connect(myConn)
theCur = conn.cursor()


#open the driver table to read
#if you want to run locations, add that switch
returnFIList("All")
returnFIList("State")
 
