
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
outTB = "rid_history"
myTS = "ffiec_ts_2002"



def mkTB():
	mySQL = "DROP TABLE IF EXISTS " + schema + "." + outTB + ";"
	mySQL = mySQL + "CREATE TABLE " + schema + "." + outTB + "( "
	mySQL = mySQL + "respondent_id character varying(10), "
	mySQL = mySQL + "agency_code character varying(1), "
	mySQL = mySQL + "fi_name character varying(30), "
	mySQL = mySQL + "count_1990 integer, "
	mySQL = mySQL + "count_1991 integer, "
	mySQL = mySQL + "count_1992 integer, "
	mySQL = mySQL + "count_1993 integer, "
	mySQL = mySQL + "count_1994 integer, "
	mySQL = mySQL + "count_1995 integer, "
	mySQL = mySQL + "count_1996 integer, "
	mySQL = mySQL + "count_1997 integer, "
	mySQL = mySQL + "count_1998 integer, "
	mySQL = mySQL + "count_1999 integer, "
	mySQL = mySQL + "count_2000 integer, "
	mySQL = mySQL + "count_2001 integer, "
	mySQL = mySQL + "count_2002 integer, "
	mySQL = mySQL + "count_2003 integer, "
	mySQL = mySQL + "count_2004 integer, "
	mySQL = mySQL + "count_2005 integer, "
	mySQL = mySQL + "count_2006 integer, "
	mySQL = mySQL + "count_2007 integer, "
	mySQL = mySQL + "count_2008 integer, "
	mySQL = mySQL + "count_2009 integer, "
	mySQL = mySQL + "count_2010 integer, "
	mySQL = mySQL + "count_2011 integer, "
	mySQL = mySQL + "count_2012 integer, "
	mySQL = mySQL + "count_2013 integer, "
	mySQL = mySQL + "count_2014 integer ) "
	mySQL = mySQL + "WITH ( OIDS=TRUE ); "
	mySQL = mySQL + "ALTER TABLE " + schema + "." + outTB + " OWNER TO byrnem; "
	mySQL = mySQL + "COMMENT ON TABLE hmda.rid_history "
	mySQL = mySQL + "IS 'created_on_01/11/2016 based of 2014_ts to support time series analysis'; COMMIT; "
	#execute the SQL string
	theCur.execute(mySQL)
	return()

#this function gets the list of FIs on which to run; it makes a selection of the TS
#then cycles through every respondent ID and agency code in the result and fires of the
#mk_file code; if myLoc = State, then it finds in which states this RID has LARs
def runAll():
	mySQL = "SELECT respondent_id, agency_code, respondent_mail_name "
	mySQL = mySQL + "FROM " + schema + "." + myTS + " "
	#mySQL = mySQL + " limit 10; "
	#execute the SQL string
	theCur.execute(mySQL)
	#cursor through the return to get the value
	years = ["2014","2013","2012","2011","2010","2009","2008","2007","2006","2005","2004"]
	years = years + ["2003","2002","2001","2000","1999","1998","1997","1998","1999","1998"]
	years = years + ["1997","1996","1995","1994","1993","1992","1991","1990"]
	if theCur.rowcount > 0:
		for row in theCur:
			RID = str(row[0])
			myAC = str(row[1])
			myFI = str(row[2]).strip()
			myFI = myFI.replace(",","", 4).replace("'","",4).replace(".","",4).replace("_","",4)
			myFI = "'" + myFI + "'" 
			#run the main script
			print "		doing ... " + RID + " for " + myAC + " in " + myFI
			runUpd(RID, myAC, myFI)
			#run an insert and then an update
			for year in years:
				runInsert(RID, myAC, year)

	return

def runUpd(RID, myAC, myFI):
	upCur = conn.cursor()
	#its either a first (insert) or a second (update)
	mySQL = "INSERT INTO " + schema + "." + outTB + " (respondent_id, agency_code, fi_name) "
	mySQL = mySQL + " values ('" + RID + "', '" + myAC + "', " + myFI + "); COMMIT; "
	upCur.execute(mySQL)
	upCur.close()
	return()


def runInsert(RID, myAC, year):
	yrCur = conn.cursor()
	mySQL = "SELECT count(*) from "  + schema + ".ffiec_lar_" + year 
	mySQL = mySQL + " WHERE respondent_id = '" + RID + "' and agency_code = '" + myAC + "' ; " 
	yrCur.execute(mySQL)
	if yrCur.rowcount > 0:
		for row in yrCur:
			cnt = str(row[0])
	else:
		cnt = '0'
	#its either a first (insert) or a second (update)
	mySQL = "UPDATE " + schema + "." + outTB + " set count_" + year + " = " + cnt 
	mySQL = mySQL + " where respondent_id  = '" + RID + "' and agency_code = '" + myAC + "'; COMMIT; "
	yrCur.execute(mySQL)
	yrCur.close()
	#write insert code

	
#make database connection - so that we can use cursors to update the table
myConn = "dbname=" + db + " host=" + myHost + " port=" + myPort + " user=" + myUser
conn = psycopg2.connect(myConn)
theCur = conn.cursor()


mkTB()
runAll()


theCur.close()
conn.close()

now = time.localtime(time.time())
print "end time:", time.asctime(now)
 
