
import json
import os
import psycopg2
import time
import sys
#import multiprocessing as mp
now = time.localtime(time.time())
print "      start time:", time.asctime(now)

#arguments
#rid = "0000061650"
#agency_code = "5"
#theFI = "Golden One Credit Union"
#myLocation = "nationwide"

rid = sys.argv[1]
agency_code = sys.argv[2]
theFI = sys.argv[3]
myLocation = sys.argv[4]

#connection variables
myHost = "localhost"
myPort = "5432"
myUser = "feomike"
db = "feomike"
schema = "hmda"
pre_tab = "ffiec_lar" #ffiec_lar

#dump the json data
def dumpFile(myData, myFile):
	with open(myFile, 'w') as outfile:
	    json.dump(myData, outfile)
	return

#define the queries to run
#this query just returns the count of the rows from that respondent
def qryCount(myYear, myRID, myAC):
	mySQL = "SELECT COUNT(*) from " + schema + "." + pre_tab + "_" + myYear
	mySQL = mySQL + " WHERE respondent_id = '" + myRID + "' AND agency_code = '"
	mySQL = mySQL + myAC + "' " 
	return mySQL

#this query gets the total loan amount for the set of lars
def qryLoanAmount(myYear, myRID, myAC):
	#http://stackoverflow.com/questions/10518258/typecast-string-to-integer-postgres
	mySQL = "SELECT sum(to_number(loan_amount,'99999')) FROM "
	mySQL = mySQL + schema + "." + pre_tab + "_" + myYear + " WHERE respondent_id = '"
	mySQL = mySQL  + myRID + "' AND agency_code = '" + myAC + "' "
#	mySQL = mySQL + "group by respondent_id "	
	return mySQL
	
#this function returns the the value of the query
def returnData(mySQL):
	#execute the SQL string
	theCur.execute(mySQL)
	#cursor through the return to get the value
	myValue = "-9"
	if theCur.rowcount == 1:
		myrow = theCur.fetchone()
		myValue = str(myrow[0])
	if myValue == 'None':
		myValue = '0'
	return int(myValue)

#this function writes the metadata for the json file
def setMeta(myFI):
	#after all the years have run, paste in the header to the json file
	data["bank"] = myFI
	data["title"] = "Financial Institution HMDA filings"
	data["description"] = "Bank Data"
	data["layout"] = "bank-data"
	data["categories"] = "chart"
	data["type"] = "line"
	return

def returnState(myFIPS):
	states = {"01":"Alabama","02":"Alaska","04":"Arizona","05":"Arkansas","06":"California","08":"Colorado","09":"Connecticut",
	   "10":"Delaware","11":"District-of-Columbia","12":"Floria","13":"Georgia","15":"Hawaii","19":"Iowa","16":"Idaho","17":"Illinois",		
	   "18":"Indiana","20":"Kansas","21":"Kentucky","22":"Lousiana","25":"Massachusetts","24":"Maryland","23":"Maine","26":"Michigan",
	   "27":"Minnesota","29":"Missouri","28":"Mississippi","30":"Montana","37":"North-Carolina","38":"North-Dakota","31":"Nebraska","33":"New-Hampshire",
	   "34":"New-Jersey","35":"New-Mexico","32":"Nevada","36":"New-York","39":"Ohio","40":"Oklahoma","41":"Oregon","42":"Pennsylvania",
	   "44":"Rhode-Island","45":"South-Carolia","46":"South-Dakota","47":"Tennessee","48":"Texas","49":"Utah","51":"Virginia",
	   "50":"Vermont","53":"Washington","55":"Wisconsin","54":"West-Virginia","56":"Wyoming"
	   }
	state = states[myFIPS]
	return state

#make database connection - so that we can use cursors to update the table
myConn = "dbname=" + db + " host=" + myHost + " port=" + myPort + " user=" + myUser
conn = psycopg2.connect(myConn)
theCur = conn.cursor()


years = ["1990","1991","1992","1993","1994","1995","1996","1997","1998","1999"]
years = years + ["2000","2001","2002","2003","2004","2005","2006","2007","2008"]
years = years + ["2009","2010","2011","2012","2013","2014"]
queries = ["all","single-family","refi","home-improvement","purchased-loan"]

data = {}
year_data = {}
row_data = []
for qry in queries:
	for year in years:
		#thePath = "../../data/" + rid + "-" + agency_code + "/" + myLocation + "/" + qry
		setMeta(theFI)
		if qry == "all":
			cntSQL = qryCount(year, rid, agency_code)
			amtSQL = qryLoanAmount(year, rid, agency_code)
		if qry == "single-family":
			mySQL = " and action_type = '1' and loan_purpose = '1' "
			cntSQL = qryCount(year, rid, agency_code) + mySQL
			amtSQL = qryLoanAmount(year, rid, agency_code) + mySQL
		if qry == "refi":
			mySQL = " and loan_purpose = '3' "
			cntSQL = qryCount(year, rid, agency_code) + mySQL
			amtSQL = qryLoanAmount(year, rid, agency_code) + mySQL
		if qry == "home-improvement":
			mySQL = " and loan_purpose = '2' "
			cntSQL = qryCount(year, rid, agency_code) + mySQL
			amtSQL = qryLoanAmount(year, rid, agency_code) + mySQL
		if qry == "purchased-loan":
			mySQL = " and purchaser_type <> '0' "
			cntSQL = qryCount(year, rid, agency_code) + mySQL
			amtSQL = qryLoanAmount(year, rid, agency_code) + mySQL
		if len(myLocation) == 2:
			cntSQL = cntSQL + " and state_code = '" + myLocation + "'"
			amtSQL = cntSQL + " and state_code = '" + myLocation + "'"
		count = returnData(cntSQL)
		amount = returnData(amtSQL)
		#after you run the query populate the json
		myRow = {"year":year,"count": count, "loan_amount": amount}
		row_data.insert(len(row_data),myRow)
	#write out the data
	year_data["years"] = row_data
	data["data"] = year_data
	
	if len(myLocation) == 2:
		myST = returnState(myLocation)
		myLocation = "statewide/" + myST
	thePath = "../../data/" + rid + "-" + agency_code + "/" + myLocation + "/" + qry	
	#see if the directory exists, if not, create it
	#if so, overwrite the file
	if not os.path.exists(thePath):
    		os.makedirs(thePath)
	#write out the date file
	dumpFile(data, thePath + "/data.json")

theCur.close()
conn.close()
		
now = time.localtime(time.time())
print "      end time:", time.asctime(now)
	