#check_ts.py
#mike byrne
#dec 30, 2015
#create a table that checks the transmittal sheets to to see if there are data back 
#time for particular respondent_id and agency_code combinations (e.g. a specific 
#financial institution)


import os
import psycopg2
import time
#import multiprocessing as mp
now = time.localtime(time.time())
print "start time:", time.asctime(now)

#variables
myHost = "localhost"
myPort = "5432"
myUser = "feomike"
db = "feomike"
schema = "hmda"
outTB = "check_ts"

#load the panel data
def create_check_ts():
	#create the working table
	mySQL = "DROP TABLE IF EXISTS " + schema + "." + outTB + "; "
	mySQL = mySQL + "CREATE TABLE " + schema + "." + outTB + " ( "
	mySQL = mySQL + " as_of_date integer, respondent_id character varying(10), "
	mySQL = mySQL + "agency_code character varying(1), "
	mySQL = mySQL + "respondent_mail_name character varying(30), count integer ) "
	mySQL = mySQL + "WITH ( OIDS=TRUE ); COMMIT; "
	theCur.execute(mySQL)
	return

def insert_rows(myRID, myAC, myYr):
	myTB = "ffiec_ts_"
	mySQL = "INSERT INTO " + schema + "." + outTB + " "
	mySQL = mySQL + "SELECT as_of_date, respondent_id, agency_code, "
	mySQL = mySQL + "respondent_mail_name from hmda." + myTB + myYr
	mySQL = mySQL + " where respondent_id = '" + myRID + "' and agency_code = '"
	mySQL = mySQL + myAC + "'; COMMIT; "
	theCur.execute(mySQL)
	return
	
def insert_rows_late(myRID, myAC, myYr):
	myTB = "ffiec_inst_rec_"
	mySQL = "INSERT INTO " + schema + "." + outTB + " "
	mySQL = mySQL + "SELECT activity_year, respondent_id, agency_code, "
	mySQL = mySQL + "respondent_name_ts from hmda." + myTB + myYr
	mySQL = mySQL + " where respondent_id = '" + myRID + "' and agency_code = '"
	mySQL = mySQL + myAC + "'; COMMIT; "
	theCur.execute(mySQL)
	return
	
#make database connection - so that we can use cursors to update the table
myConn = "dbname=" + db + " host=" + myHost + " port=" + myPort + " user=" + myUser
conn = psycopg2.connect(myConn)
theCur = conn.cursor()


#1990 LAR has a utf-8 problem
#the set of data on which to loop

years = ["1990","1991","1992","1993","1994","1995","1996","1997","1998","1999"]
years = years + ["2000","2001","2002","2003","2004","2005","2006","2007","2008"]
years = years + ["2009","2010","2011", "2012", "2013", "2014"]

#OCC - Agency Code 1 + 0000012072 - First National Bank - RSSD - 114260
#FRS - Agency Code 2 - 0003374298 - Virginia Heritage BK - RSSD - 0003374298
#FDIC - Agency Code 3 + 0000001678 - Plains CMRC BK - RSSD - 593052
#NCUA - Agency Code 5 + 0000061650 - Golden 1 CU - RSSD - 959395
#HUD - Agency Code 7 + 7197000003 - Quicken Loans, Inc - RSSD - 3870679
#CFPB - Agency Code 9 - 0004114567 - First Republic Bank - RSSD - 0004114567

#respondent_id + agency_code
FIs = ["0000012072:1","0003374298:2","0000001678:3","0000061650:5","7197000003:7", "0004114567:9"]

create_check_ts()
for FI in FIs:
	print "      doing: " + FI[0:10]
	for year in years:
		try:
			print "       loading year: " + year
			if year == "2012" or year == "2013" or year == "2014":
				insert_rows_late(FI[0:10], FI[-1], year)
			else:
				insert_rows(FI[0:10], FI[-1], year)
		except:
			theCur.close()
			conn.close()
			print "     " + FI[0:10] + "   failed, continuing on ..."
			continue

theCur.close()
conn.close()
		
now = time.localtime(time.time())
print "end time:", time.asctime(now)
