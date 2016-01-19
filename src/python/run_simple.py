#run_simple.py


#next challenge is to get the range of similarity index's between expected and current
#expected is:
#	- all all nationwide
#	- all single-family nationwide
#	- all refinance nationwide
#	- all home-improvement nationwide
#	- all sales nationwide

#current is:
#	- all <FI> nationwide
#	- single-family <FI> nationwide
#	- refinance <FI> nationwide
#	- home-improvement <FI> nationwide
#	- sales <FI> nationwide

#go get and load expected
#for each FI
#	go get and load current
#	run simple_similarity
#	write out simple_similarity to a table

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
myTab = "rid_history"

#define the expected data set
def readFile(myWho, myWhat):
	#thePath = "../../data/" #+ who + "/" + theQry + "/" + theLoc
	myFile = thePath + myWho + "/" + myWhat + "/data.json"
	#f = open(myFile, 'r')
	#data = f.read() 
	with open(myFile) as data_file:    
    		data = json.load(data_file)
	return data

#get all of the to be measured (e.g. current) FIs 	
def runFIs(myQry, myWhere, expD):
	#run a qry against the driver table to get an FI name; 
	#use the FI name to read in 
	#appropriate current data.json
	mySQL = "SELECT respondent_id, agency_code, fi_name "
	mySQL = mySQL + "FROM " + schema + "." + myTab + " "
	mySQL = mySQL + "where hist_length > 14 and total > 15000 "
	mySQL = mySQL + "order by total desc; "
	#execute the SQL string
	theCur.execute(mySQL)
	#cursor through the return to get the value
	if theCur.rowcount > 0:
		for row in theCur:
			RID = str(row[0])
			myAC = str(row[1])
			myFI = str(row[2]).strip()
			myFI = myFI.replace(",","", 4).replace("'","",4).replace(".","",4).replace("_","",4)
			myFI = myFI.replace(" ","-",10).lower()
			#run the main script
			print "		doing ... " + myFI + " for " + myQry
			#get data for that file
			data = readFile(myFI, myQry + "/" + myWhere)
			#os.system("python simple_similarity.py '" + expD + "' '" + data + "' ")
			#print data
			num = SimpleSimilarity(expD, data)
			#write number back to DB
			writeRow(RID, myAC, myFI, myQry, myWhere, num)
	return()

#write results back to a table
def writeRow(rid, myac, myfi, myqry, mywhere, num):
	mySQL = "INSERT INTO hmda.simple_results values ( "
	mySQL = mySQL + "'" + rid + "', '" + myac + "', '" + myfi
	mySQL = mySQL + "', '" + myqry + "', '" + mywhere + "', " 
	mySQL = mySQL + str(num) + ") ; commit; "
	myCur = conn.cursor()
	myCur.execute(mySQL)
	myCur.close()
	return()

#this function GrowthIndex or GI returns the difference between this and last year
#a 1 means there was growth, a 0 means there was reduction
def returnGI(myYear, myData):
	cur = myData[myYear]['count']
	prev = myData[myYear - 1]['count']
	#previous might be 0, b/c there was nothing filed that year
	if prev == 0: prev = 1
	myGI = cur / prev
	#all we are determining is growth vs decay; so if growth is 2x or greater, we need
	#to just round down to 1; likewise for significant decline
	if myGI > 1: myGI = 1
	if myGI < 0: myGI = 0
	return myGI

def SimpleSimilarity(myExpD, myCurD):
	numSame = 0
	for year in range (1,25):
		#get expected GI
		expGI = returnGI(year, myExpD['data']['years'])
		#get current GI
		curGI = returnGI(year, myCurD['data']['years'])
		if expGI - curGI == 0: numSame = numSame + 1
	return float(numSame) / float(25) * 100

#make database connection - so that we can use cursors to update the table
myConn = "dbname=" + db + " host=" + myHost + " port=" + myPort + " user=" + myUser
conn = psycopg2.connect(myConn)
theCur = conn.cursor()


expected = '{"type": "line", "data": {"years": [{"count": 6594623, "loan_amount": 643868202, "year": "1990"}, {"count": 7887148, "loan_amount": 732976674, "year": "1991"}, {"count": 12006134, "loan_amount": 1165880617, "year": "1992"}, {"count": 15376391, "loan_amount": 1538611281, "year": "1993"}, {"count": 12194347, "loan_amount": 1086460921, "year": "1994"}, {"count": 11234039, "loan_amount": 940381562, "year": "1995"}, {"count": 14813254, "loan_amount": 1254537873, "year": "1996"}, {"count": 16406627, "loan_amount": 1450216871, "year": "1997"}, {"count": 24661738, "loan_amount": 2534845939, "year": "1998"}, {"count": 22911773, "loan_amount": 2287157245, "year": "1999"}, {"count": 19233166, "loan_amount": 1912800749, "year": "2000"}, {"count": 27578473, "loan_amount": 3472367639, "year": "2001"}, {"count": 31236008, "loan_amount": 4651244479, "year": "2002"}, {"count": 41556856, "loan_amount": 6455511817, "year": "2003"}, {"count": 33607736, "loan_amount": 5460902691, "year": "2004"}, {"count": 36439157, "loan_amount": 6486583582, "year": "2005"}, {"count": 34105441, "loan_amount": 6295214383, "year": "2006"}, {"count": 26605695, "loan_amount": 5279183286, "year": "2007"}, {"count": 17391570, "loan_amount": 3446236491, "year": "2008"}, {"count": 19493491, "loan_amount": 3902210068, "year": "2009"}, {"count": 16717965, "loan_amount": 3449432820, "year": "2010"}, {"count": 14873415, "loan_amount": 3048054347, "year": "2011"}, {"count": 18691551, "loan_amount": 3937513057, "year": "2012"}, {"count": 17016159, "loan_amount": 3855117207, "year": "2013"}, {"count": 11875464, "loan_amount": 2571374930, "year": "2014"}]}, "FinancialInstitution": "All", "loanType": "all", "location": "nationwide"}'
queries = ["all","single-family","refi","home-improvement","purchased-loan"]
queries = ["single-family","refi","home-improvement","purchased-loan"]
#change the next line to "../../data_full/"
thePath = "../../data_full/" #+ who + "/" + theQry + "/" + theLoc
#read in the expected file
for qry in queries:
	#get list of FIs
	expected = readFile("all", qry + "/nationwide")
	runFIs(qry, "nationwide", expected)