#simple_similarity
#mike byrne
#this measures how similar the signature from an expected curve is to the current curve
#this is a crazy simple measure as all it looks for is a given year close to the current
#year change.  this measure has all kinds of issues w/ it, as we are rounding to an integer
#rather than using floating point, which inherently has problems.  it is only menat as 
#the most simple measures of variance from the expected outcome
#this measure is looking at the count variable in the data

import json
import os
import psycopg2
import sys

expected = sys.argv[1]
current = sys.argv[2]

#all - all - nationwide
#expected = '{"type": "line", "data": {"years": [{"count": 6594623, "loan_amount": 643868202, "year": "1990"}, {"count": 7887148, "loan_amount": 732976674, "year": "1991"}, {"count": 12006134, "loan_amount": 1165880617, "year": "1992"}, {"count": 15376391, "loan_amount": 1538611281, "year": "1993"}, {"count": 12194347, "loan_amount": 1086460921, "year": "1994"}, {"count": 11234039, "loan_amount": 940381562, "year": "1995"}, {"count": 14813254, "loan_amount": 1254537873, "year": "1996"}, {"count": 16406627, "loan_amount": 1450216871, "year": "1997"}, {"count": 24661738, "loan_amount": 2534845939, "year": "1998"}, {"count": 22911773, "loan_amount": 2287157245, "year": "1999"}, {"count": 19233166, "loan_amount": 1912800749, "year": "2000"}, {"count": 27578473, "loan_amount": 3472367639, "year": "2001"}, {"count": 31236008, "loan_amount": 4651244479, "year": "2002"}, {"count": 41556856, "loan_amount": 6455511817, "year": "2003"}, {"count": 33607736, "loan_amount": 5460902691, "year": "2004"}, {"count": 36439157, "loan_amount": 6486583582, "year": "2005"}, {"count": 34105441, "loan_amount": 6295214383, "year": "2006"}, {"count": 26605695, "loan_amount": 5279183286, "year": "2007"}, {"count": 17391570, "loan_amount": 3446236491, "year": "2008"}, {"count": 19493491, "loan_amount": 3902210068, "year": "2009"}, {"count": 16717965, "loan_amount": 3449432820, "year": "2010"}, {"count": 14873415, "loan_amount": 3048054347, "year": "2011"}, {"count": 18691551, "loan_amount": 3937513057, "year": "2012"}, {"count": 17016159, "loan_amount": 3855117207, "year": "2013"}, {"count": 11875464, "loan_amount": 2571374930, "year": "2014"}]}, "FinancialInstitution": "All", "loanType": "all", "location": "nationwide"}'

#BofA  
#current = '{"type": "line", "data": {"years": [{"count": 129055, "loan_amount": 14491102, "year": "1990"}, {"count": 131441, "loan_amount": 15126417, "year": "1991"}, {"count": 158928, "loan_amount": 18177400, "year": "1992"}, {"count": 129117, "loan_amount": 15147325, "year": "1993"}, {"count": 114685, "loan_amount": 13048064, "year": "1994"}, {"count": 92376, "loan_amount": 11134717, "year": "1995"}, {"count": 132280, "loan_amount": 15719606, "year": "1996"}, {"count": 176198, "loan_amount": 17615727, "year": "1997"}, {"count": 125898, "loan_amount": 10051478, "year": "1998"}, {"count": 574024, "loan_amount": 68076071, "year": "1999"}, {"count": 543143, "loan_amount": 67156209, "year": "2000"}, {"count": 851931, "loan_amount": 111936214, "year": "2001"}, {"count": 920189, "loan_amount": 171394265, "year": "2002"}, {"count": 1341468, "loan_amount": 266744101, "year": "2003"}, {"count": 805182, "loan_amount": 165143735, "year": "2004"}, {"count": 860829, "loan_amount": 192544900, "year": "2005"}, {"count": 1017362, "loan_amount": 225085987, "year": "2006"}, {"count": 1169561, "loan_amount": 262798289, "year": "2007"}, {"count": 674341, "loan_amount": 147450416, "year": "2008"}, {"count": 1835930, "loan_amount": 398066838, "year": "2009"}, {"count": 1815792, "loan_amount": 393460342, "year": "2010"}, {"count": 0, "loan_amount": 0, "year": "2011"}, {"count": 0, "loan_amount": 0, "year": "2012"}, {"count": 0, "loan_amount": 0, "year": "2013"}, {"count": 0, "loan_amount": 0, "year": "2014"}]}, "FinancialInstitution": "Bank Of America Na", "loanType": "all", "location": "nationwide"}' 

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

expD = json.loads(expected)
curD = json.loads(current)
numSame = 0
for year in range (1,25):
	#get expected GI
	expGI = returnGI(year, expD['data']['years'])
	#get current GI
	curGI = returnGI(year, curD['data']['years'])
	if expGI - curGI == 0: numSame = numSame + 1
	#print expD['data']['years'][year]['year'] + ": " + str(numSame)

print float(numSame) / float(25) * 100

	
