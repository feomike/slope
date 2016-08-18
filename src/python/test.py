import json

#all - all - nationwide
all = '{"type": "line", "data": {"years": [{"count": 6594623, "loan_amount": 643868202, "year": "1990"}, {"count": 7887148, "loan_amount": 732976674, "year": "1991"}, {"count": 12006134, "loan_amount": 1165880617, "year": "1992"}, {"count": 15376391, "loan_amount": 1538611281, "year": "1993"}, {"count": 12194347, "loan_amount": 1086460921, "year": "1994"}, {"count": 11234039, "loan_amount": 940381562, "year": "1995"}, {"count": 14813254, "loan_amount": 1254537873, "year": "1996"}, {"count": 16406627, "loan_amount": 1450216871, "year": "1997"}, {"count": 24661738, "loan_amount": 2534845939, "year": "1998"}, {"count": 22911773, "loan_amount": 2287157245, "year": "1999"}, {"count": 19233166, "loan_amount": 1912800749, "year": "2000"}, {"count": 27578473, "loan_amount": 3472367639, "year": "2001"}, {"count": 31236008, "loan_amount": 4651244479, "year": "2002"}, {"count": 41556856, "loan_amount": 6455511817, "year": "2003"}, {"count": 33607736, "loan_amount": 5460902691, "year": "2004"}, {"count": 36439157, "loan_amount": 6486583582, "year": "2005"}, {"count": 34105441, "loan_amount": 6295214383, "year": "2006"}, {"count": 26605695, "loan_amount": 5279183286, "year": "2007"}, {"count": 17391570, "loan_amount": 3446236491, "year": "2008"}, {"count": 19493491, "loan_amount": 3902210068, "year": "2009"}, {"count": 16717965, "loan_amount": 3449432820, "year": "2010"}, {"count": 14873415, "loan_amount": 3048054347, "year": "2011"}, {"count": 18691551, "loan_amount": 3937513057, "year": "2012"}, {"count": 17016159, "loan_amount": 3855117207, "year": "2013"}, {"count": 11875464, "loan_amount": 2571374930, "year": "2014"}]}, "FinancialInstitution": "All", "loanType": "all", "location": "nationwide"}'

#BofA - all - nationwide
bofa = '{"type": "line", "data": {"years": [{"count": 129055, "loan_amount": 14491102, "year": "1990"}, {"count": 131441, "loan_amount": 15126417, "year": "1991"}, {"count": 158928, "loan_amount": 18177400, "year": "1992"}, {"count": 129117, "loan_amount": 15147325, "year": "1993"}, {"count": 114685, "loan_amount": 13048064, "year": "1994"}, {"count": 92376, "loan_amount": 11134717, "year": "1995"}, {"count": 132280, "loan_amount": 15719606, "year": "1996"}, {"count": 176198, "loan_amount": 17615727, "year": "1997"}, {"count": 125898, "loan_amount": 10051478, "year": "1998"}, {"count": 574024, "loan_amount": 68076071, "year": "1999"}, {"count": 543143, "loan_amount": 67156209, "year": "2000"}, {"count": 851931, "loan_amount": 111936214, "year": "2001"}, {"count": 920189, "loan_amount": 171394265, "year": "2002"}, {"count": 1341468, "loan_amount": 266744101, "year": "2003"}, {"count": 805182, "loan_amount": 165143735, "year": "2004"}, {"count": 860829, "loan_amount": 192544900, "year": "2005"}, {"count": 1017362, "loan_amount": 225085987, "year": "2006"}, {"count": 1169561, "loan_amount": 262798289, "year": "2007"}, {"count": 674341, "loan_amount": 147450416, "year": "2008"}, {"count": 1835930, "loan_amount": 398066838, "year": "2009"}, {"count": 1815792, "loan_amount": 393460342, "year": "2010"}]}, "FinancialInstitution": "Bank Of America Na", "loanType": "all", "location": "nationwide"}'


#all-single family - ca
ca = '{"type": "line", "data": {"years": [{"count": 278953, "loan_amount": 49145376, "year": "1990"}, {"count": 269109, "loan_amount": 45006677, "year": "1991"}, {"count": 234432, "loan_amount": 40521735, "year": "1992"}, {"count": 275496, "loan_amount": 46841089, "year": "1993"}, {"count": 315238, "loan_amount": 53656286, "year": "1994"}, {"count": 281639, "loan_amount": 46099775, "year": "1995"}, {"count": 345204, "loan_amount": 56534727, "year": "1996"}, {"count": 400309, "loan_amount": 68619081, "year": "1997"}, {"count": 488758, "loan_amount": 85842297, "year": "1998"}, {"count": 544299, "loan_amount": 100460403, "year": "1999"}, {"count": 562646, "loan_amount": 110406611, "year": "2000"}, {"count": 561082, "loan_amount": 113245587, "year": "2001"}, {"count": 636491, "loan_amount": 144580303, "year": "2002"}, {"count": 679471, "loan_amount": 171176190, "year": "2003"}, {"count": 761549, "loan_amount": 218993005, "year": "2004"}, {"count": 804162, "loan_amount": 255616758, "year": "2005"}, {"count": 655715, "loan_amount": 211366378, "year": "2006"}, {"count": 367971, "loan_amount": 135699211, "year": "2007"}, {"count": 277010, "loan_amount": 89432111, "year": "2008"}, {"count": 294003, "loan_amount": 85754746, "year": "2009"}, {"count": 279277, "loan_amount": 86676891, "year": "2010"}, {"count": 245535, "loan_amount": 75055258, "year": "2011"}, {"count": 259033, "loan_amount": 86756854, "year": "2012"}, {"count": 268157, "loan_amount": 103709077, "year": "2013"}, {"count": 270863, "loan_amount": 109899333, "year": "2014"}]}, "FinancialInstitution": "All", "loanType": "single-family", "location": "06"}'
fl = '{"type": "line", "data": {"years": [{"count": 129243, "loan_amount": 11082180, "year": "1990"}, {"count": 117872, "loan_amount": 9878524, "year": "1991"}, {"count": 124214, "loan_amount": 10783025, "year": "1992"}, {"count": 181775, "loan_amount": 16699896, "year": "1993"}, {"count": 203715, "loan_amount": 18688205, "year": "1994"}, {"count": 201060, "loan_amount": 18002346, "year": "1995"}, {"count": 234984, "loan_amount": 22058169, "year": "1996"}, {"count": 245156, "loan_amount": 23847327, "year": "1997"}, {"count": 296774, "loan_amount": 30268260, "year": "1998"}, {"count": 318583, "loan_amount": 34831818, "year": "1999"}, {"count": 321432, "loan_amount": 36433363, "year": "2000"}, {"count": 345254, "loan_amount": 42522058, "year": "2001"}, {"count": 348387, "loan_amount": 48313559, "year": "2002"}, {"count": 386195, "loan_amount": 58393493, "year": "2003"}, {"count": 439960, "loan_amount": 73619454, "year": "2004"}, {"count": 524341, "loan_amount": 97564450, "year": "2005"}, {"count": 458362, "loan_amount": 89097701, "year": "2006"}, {"count": 248200, "loan_amount": 54104417, "year": "2007"}, {"count": 134842, "loan_amount": 27626251, "year": "2008"}, {"count": 123347, "loan_amount": 21436376, "year": "2009"}, {"count": 122804, "loan_amount": 21190431, "year": "2010"}, {"count": 114134, "loan_amount": 20048257, "year": "2011"}, {"count": 127597, "loan_amount": 24512752, "year": "2012"}, {"count": 152379, "loan_amount": 32204714, "year": "2013"}, {"count": 180954, "loan_amount": 38666398, "year": "2014"}]}, "FinancialInstitution": "All", "loanType": "single-family", "location": "12"}'
nd = '{"type": "line", "data": {"years": [{"count": 2544, "loan_amount": 160355, "year": "1990"}, {"count": 2783, "loan_amount": 164296, "year": "1991"}, {"count": 3415, "loan_amount": 211947, "year": "1992"}, {"count": 4835, "loan_amount": 323570, "year": "1993"}, {"count": 4344, "loan_amount": 298385, "year": "1994"}, {"count": 4592, "loan_amount": 316818, "year": "1995"}, {"count": 5511, "loan_amount": 379429, "year": "1996"}, {"count": 5706, "loan_amount": 384036, "year": "1997"}, {"count": 7087, "loan_amount": 513458, "year": "1998"}, {"count": 6605, "loan_amount": 496984, "year": "1999"}, {"count": 5926, "loan_amount": 445977, "year": "2000"}, {"count": 6087, "loan_amount": 494046, "year": "2001"}, {"count": 6717, "loan_amount": 571469, "year": "2002"}, {"count": 7585, "loan_amount": 720409, "year": "2003"}, {"count": 8685, "loan_amount": 845096, "year": "2004"}, {"count": 9564, "loan_amount": 970847, "year": "2005"}, {"count": 9113, "loan_amount": 968207, "year": "2006"}, {"count": 8304, "loan_amount": 957729, "year": "2007"}, {"count": 7267, "loan_amount": 917173, "year": "2008"}, {"count": 7149, "loan_amount": 954307, "year": "2009"}, {"count": 7687, "loan_amount": 1087161, "year": "2010"}, {"count": 6991, "loan_amount": 1052824, "year": "2011"}, {"count": 8649, "loan_amount": 1469092, "year": "2012"}, {"count": 9204, "loan_amount": 1697011, "year": "2013"}, {"count": 9365, "loan_amount": 1816556, "year": "2014"}]}, "FinancialInstitution": "All", "loanType": "single-family", "location": "38"}'


print 'ca'
j = json.loads(ca)
prev = float(j['data']['years'][0]['count'])
myStr = ""
myNum = 0
for year in j['data']['years']:
	#print "year: " + year['year'] + " - " + str(float(year['count']) / prev)
	myStr = myStr + str(round(float(year['count'] / prev),2))+ "|"
	myNum = myNum + year['count'] / prev
	prev = float(year['count'])

print myStr
print myNum
#print j['data']['years'][0]['year']
#all
#1111001110011101000100100
#bofa
#111000110401110111020

#ca, then fl, then nd
#1001101111101111000100111
#1011101111111111000000111
#1111011110011111000010111

#1.0|0.96|0.87|1.18|1.14|0.89|1.23|1.16|1.22|1.11|1.03|1.00|1.13|1.07|1.12|1.06|0.82|0.56|0.75|1.06|0.95|0.88|1.05|1.04|1.01|
#1.0|0.91|1.05|1.46|1.12|0.99|1.17|1.04|1.21|1.07|1.01|1.07|1.01|1.11|1.14|1.19|0.87|0.54|0.54|0.91|1.00|0.93|1.12|1.19|1.19|
#1.0|1.09|1.23|1.42|0.90|1.06|1.20|1.04|1.24|0.93|0.90|1.03|1.10|1.13|1.15|1.10|0.95|0.91|0.88|0.98|1.08|0.91|1.24|1.06|1.02|