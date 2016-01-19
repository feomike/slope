
import json
import os
import psycopg2
import sys

RID = sys.argv[1]
AC = sys.argv[2]
theFI = sys.argv[3]
theLoc = sys.argv[4]
theQry = sys.argv[5]

#connection variables
myHost = "localhost"
myPort = "5432"
myUser = "feomike"
db = "feomike"
schema = "hmda"
pre_tab = "ffiec_lar"

#dump the json data
def dumpFile(myData, myFile):
	with open(myFile, 'w') as outfile:
	    json.dump(myData, outfile)
	return

#define the final sql
def finalSQL (myType, myYear):
	#if it is a count function, select count
	if myType == "count":
		mySQL = "SELECT COUNT(*) FROM "
	#if it is an amount function, select amount
	if myType == "amount":
		mySQL = "SELECT sum(CAST(coalesce(loan_amount, '0') AS integer)) FROM "
	#build the from statement
	mySQL = mySQL + schema + "." + pre_tab + "_" + myYear + " "
	
	#if RID.type is a list, then cycle through the list
	
	#if it has an RID, then it is an FI, and needs a who where clause
	if RID <> "-9":
		mySQL = mySQL + "WHERE respondent_id = '" + RID 
		mySQL = mySQL + "' AND agency_code = '" + AC + "' "
	#this could be making it very slow
	elif theQry <> "All":
			mySQL = mySQL + "WHERE state_code is not null "
	
	if theQry == "all":
		mySQL = mySQL
	if theQry == "single-family":
		mySQL = mySQL + " and action_type = '1' and loan_purpose = '1' and occupancy = '1' "
	if theQry == "refi":
		mySQL = mySQL + " and action_type = '1' and loan_purpose = '3' "
	if theQry == "home-improvement":
		mySQL = mySQL + " and action_type = '1' and loan_purpose = '2' "
	if theQry == "purchased-loan":
		mySQL = mySQL + " and purchaser_type <> '0' "
	if theQry == "single-family-white":
		mySQL = mySQL + " and action_type = '1' and loan_purpose = '1' and occupancy = '1' "
		mySQL = mySQL + " and (applicant_race_1 = '5' or co_applicant_race_1 = '5') "
	if theQry == "single-family-black":
		mySQL = mySQL + " and action_type = '1' and loan_purpose = '1' and occupancy = '1' "
		mySQL = mySQL + " and (applicant_race_1 = '3' or co_applicant_race_1 = '3') "
	if theQry == "denials":
		mySQL = mySQL + " and denial_reason_1 in ('1','2','3','4','5','6','7','8','9') "
	if theQry == "denials-white":
		mySQL = mySQL + " and denial_reason_1 in ('1','2','3','4','5','6','7','8','9') "
		mySQL = mySQL + " and (applicant_race_1 = '5' or co_applicant_race_1 = '5') "
	if theQry == "denials-black":
		mySQL = mySQL + " and denial_reason_1 in ('1','2','3','4','5','6','7','8','9') "
		mySQL = mySQL + " and (applicant_race_1 = '3' or co_applicant_race_1 = '3') "	
	#if location
	if len(theLoc) == 2:
		mySQL = mySQL + " and state_code = '" + theLoc + "'"
	#print mySQL
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
	if myFI == "-9":
		myFI = "all"
	data["FinancialInstitution"] = myFI.title()
	data["type"] = "line"
	data["loanType"] = theQry
	data["location"] = theLoc
	return

def writeLocationsFile(myPath):
	listfiles = os.listdir(myPath)
	states = []
	for myFile in listfiles:
		if os.path.isdir(os.path.join(myPath, myFile)):
			states.insert(len(states),myFile)
			#banks.add(myFile)
	name_data = {"states":states}
	with open(myPath + "/locations.json", 'w') as outfile:
	    json.dump(name_data, outfile)

def returnState(myFIPS):
	states = {"01":"al","02":"ak","04":"az","05":"ar","06":"ca","08":"co","09":"ct",
	   "10":"de","11":"dc","12":"fl","13":"ga","15":"hi","19":"ia","16":"id","17":"il",		
	   "18":"in","20":"kn","21":"ky","22":"la","25":"ma","23":"me","27":"mn","28":"ms",
	   "24":"md","26":"mi","29":"mo","30":"mt","38":"nd","31":"ne","33":"nh","37":"nc",
	   "34":"nj","35":"nm","32":"nv","36":"ny","39":"oh","40":"ok","41":"or","42":"pa",
	   "44":"ri","45":"sc","46":"sd","47":"tn","48":"tx","49":"ut","51":"va",
	   "50":"vt","53":"wa","55":"wi","54":"wv","56":"wy","72":"pr","66":"gu"
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

data = {}
year_data = {}
row_data = []
for year in years:
	#print "doing year: " + year
	cntSQL = finalSQL ("count", year)
	count = returnData(cntSQL)
	amtSQL = finalSQL ("amount", year)
	amount = returnData(amtSQL)
	#after you run the query populate the json
	myRow = {"year": year,"count": count, "loan_amount": amount}
	if count > 0:
		row_data.insert(len(row_data),myRow)
#write out the data
year_data["years"] = row_data
data["data"] = year_data
setMeta(theFI)

#if it is a state, then, create a statewide directory	
if len(theLoc) == 2:
	myST = returnState(theLoc)
	theLoc = "statewide/" + myST
if theFI == "-9":
	who = "all"
else:
	who = theFI.replace(" ", "-").lower()
thePath = "../../data/" + who + "/" + theQry + "/" + theLoc
thePath = thePath.lower()
#see if the directory exists, if not, create it
#if so, overwrite the file
if not os.path.exists(thePath):
	os.makedirs(thePath)
#write out the data file
dumpFile(data, thePath + "/data.json")
#bad implementation of writing out the locations.json file
if len(theLoc) == 12:
	writeLocationsFile(thePath[:-2])

theCur.close()
conn.close()

	