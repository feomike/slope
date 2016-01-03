#load_data.py
#loads hmda data from the national archives download files into postgres
#mike byrne
#dec 23, 2015
#

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
tb_pre = "ffiec"
dPath = "/Users/feomike/documents/data/ffiec_hmda/"
myComment = "created_on_12/24/2015_with_import_https://catalog.archives.gov/id/2456161?q=2456161"


#load the panel data
def load_panel_post2011():
	#the panel data source is in a fixed field format, so it has a two step process
	#first it one must load it into a working table as a single field, then the data
	#must be substring'd out into the defined columns
	
	#create the working table
	mySQL = "DROP TABLE IF EXISTS " + schema + ".working; "
	mySQL = mySQL + "CREATE TABLE " + schema + ".working (data text); "
	mySQL = mySQL + "COPY " + schema + ".working "
	mySQL = mySQL + "from '" + dPath + year + "/data/Panel.final." + year + ".dat' ; "
	mySQL = mySQL + "COMMIT; "
	theCur.execute(mySQL)
	
	#create the panel table structure
	mySQL = "DROP TABLE IF EXISTS " + schema + "." + tb_pre + "_panel_" + year + "; "
	mySQL = mySQL + "CREATE TABLE " + schema + "." + tb_pre + "_panel_" + year + " ( "
	mySQL = mySQL + "activity_year integer,  respondent_id character varying(10), "
	mySQL = mySQL + "agency_code integer, parent_respondent_id character varying(10), "
	mySQL = mySQL + "parent_name_panel character varying(30), parent_city_panel character varying(25), "
	mySQL = mySQL + "parent_state_panel character varying(2), region integer, "
	mySQL = mySQL + "assets character varying(10), other_lender_code integer, "
	mySQL = mySQL + "respondent_name_panel character varying(30), respondent_city_panel character varying(25), "
	mySQL = mySQL + "respondent_state_panel character varying(2), top_holder_rssd_id character varying(10), "
	mySQL = mySQL + "top_holder_name character varying(30), top_holder_city character varying(25), "
	mySQL = mySQL + "top_holder_state character varying(2), top_holder_country character varying(40), "
	mySQL = mySQL + "respondent_rssd_id character varying(10),  parent_rssd_id character varying(10), "
	mySQL = mySQL + "respondent_fips_state_number character varying(2) ) WITH ( "
	mySQL = mySQL + "OIDS=TRUE ); "
	mySQL = mySQL + "ALTER TABLE " + schema + "." + tb_pre + "_panel_" + year + " "
	mySQL = mySQL + "OWNER TO " + myUser + "; "
	mySQL = mySQL + "COMMENT ON TABLE " + schema + "." + tb_pre + "_panel_" + year + " "
	mySQL = mySQL + "IS '" + myComment + "'; "
	
#	#substring out the panel rows to each 
	mySQL = mySQL + "INSERT INTO " + schema + "." + tb_pre + "_panel_" + year + "( "
	mySQL = mySQL + "activity_year, respondent_id, agency_code, parent_respondent_id, parent_name_panel, "
	mySQL = mySQL + "parent_city_panel, parent_state_panel, region, assets, other_lender_code, respondent_name_panel, "
	mySQL = mySQL + "respondent_city_panel, respondent_state_panel, "
	mySQL = mySQL + "top_holder_rssd_id, top_holder_name, top_holder_city, "
	mySQL = mySQL + "top_holder_state, top_holder_country, respondent_rssd_id,  parent_rssd_id, "
	mySQL = mySQL + "respondent_fips_state_number ) SELECT  "
	mySQL = mySQL + "to_number(substring(data,1,4),'9999') as activity_year, "
	mySQL = mySQL + "substring(data,5,10) as respondent_id, "
	mySQL = mySQL + "to_number(substring(data,15,1),'9') as agency_code, "
	mySQL = mySQL + "substring(data,16,10) as parent_respondent_id, "
	mySQL = mySQL + "substring(data,26,30) as parent_name, "
	mySQL = mySQL + "substring(data,56,25) as parent_city, "
	mySQL = mySQL + "substring(data,81,2) as parent_state, "
	mySQL = mySQL + "to_number(substring(data,83,2), '99') as region, "
	mySQL = mySQL + "substring(data,85,10) as assets, "
	mySQL = mySQL + "to_number(substring(data,95,1), '9') as other_lender_code, "
	mySQL = mySQL + "substring(data,96,30) as respondent_name, "
	mySQL = mySQL + "substring(data,166, 25 ) as respondent_city, "
	mySQL = mySQL + "substring(data, 191, 2) as respondent_state, "
	mySQL = mySQL + "substring(data,213,10 ) as top_holder_rssd_id, "
	mySQL = mySQL + "substring(data,223, 30) as top_holder_name, "
	mySQL = mySQL + "substring(data,253,25) as top_holder_city, "
	mySQL = mySQL + "substring(data,278,2) as top_holder_state, "
	mySQL = mySQL + "substring(data,280,40) as top_holder_country, "
	mySQL = mySQL + "substring(data,320,10) as respondent_rssd_id,  "
	mySQL = mySQL + "substring(data,330,10) as parent_rssd_id, "
	mySQL = mySQL + "substring(data,340,2) as respondent_fips_state_number "
	mySQL = mySQL + "FROM " + schema + ".working; "
	mySQL = mySQL + "DROP TABLE IF EXISTS " + schema + ".working; "
	theCur.execute(mySQL)

#load the panel data
def load_panel_2004_2011():
	#the panel data source is in a fixed field format, so it has a two step process
	#first it one must load it into a working table as a single field, then the data
	#must be substring'd out into the defined columns
	
	#create the working table
	mySQL = "DROP TABLE IF EXISTS " + schema + ".working; "
	mySQL = mySQL + "CREATE TABLE " + schema + ".working (data text); "
	mySQL = mySQL + "COPY " + schema + ".working "
	mySQL = mySQL + "from '" + dPath + year + "/data/Panel.final." + year + ".dat' ; "
	mySQL = mySQL + "COMMIT; "
	theCur.execute(mySQL)
	
	#create the panel table structure
	mySQL = "DROP TABLE IF EXISTS " + schema + "." + tb_pre + "_panel_" + year + "; "
	mySQL = mySQL + "CREATE TABLE " + schema + "." + tb_pre + "_panel_" + year + " ( "
	mySQL = mySQL + "respondent_id character varying(10), msa_number character varying(5), "
	mySQL = mySQL + "agency_code integer, agency_group_code character varying(2), "
	mySQL = mySQL + "parent_respondent_id character varying(10), respondent_name_panel character varying(30), "
	mySQL = mySQL + "respondent_city_panel character varying(25), respondent_state_panel character varying(2), "
	mySQL = mySQL + "respondent_fips_state_number character varying(2), assets character varying(10), "
	mySQL = mySQL + "other_lender_code integer, parent_rssd_id character varying(10), "
	mySQL = mySQL + "parent_name_panel character varying(30), parent_city_panel character varying(25), "
	mySQL = mySQL + "parent_state_panel character varying(2), activity_year integer, "
	mySQL = mySQL + "respondent_rssd_id character varying(10) ) WITH ( OIDS=TRUE ); "
	mySQL = mySQL + "ALTER TABLE " + schema + "." + tb_pre + "_panel_" + year + " "
	mySQL = mySQL + "OWNER TO " + myUser + "; "
	mySQL = mySQL + "COMMENT ON TABLE " + schema + "." + tb_pre + "_panel_" + year + " "
	mySQL = mySQL + "IS '" + myComment + "'; "
	
#	#substring out the panel rows to each 
	mySQL = mySQL + "INSERT INTO " + schema + "." + tb_pre + "_panel_" + year + "( "
	mySQL = mySQL + "respondent_id, msa_number, agency_code, agency_group_code, "
	mySQL = mySQL + "respondent_name_panel, respondent_city_panel, respondent_state_panel, "
	mySQL = mySQL + "respondent_fips_state_number, assets, other_lender_code, parent_rssd_id, "
	mySQL = mySQL + "parent_name_panel, parent_city_panel, parent_state_panel, activity_year, "
	mySQL = mySQL + "respondent_rssd_id  )  SELECT  "
	mySQL = mySQL + "substring(data,1,10) as respondent_id, substring(data,11,5) as msa_number, "
	mySQL = mySQL + "to_number(substring(data,16,1),'9') as agency_code, substring(data,17,2) as agency_group_code, "
	mySQL = mySQL + "substring(data,19,30) as respondent_name_panel, substring(data,49,25) as respondent_city_panel, "
	mySQL = mySQL + "substring(data,74,2) as respondent_state_panel, substring(data,76,2) as respondent_fips_state_number, "
	mySQL = mySQL + "substring(data,78,10) as assets, to_number(substring(data,88,1), '9') as other_lender_code, "
	mySQL = mySQL + "substring(data,89,10) as parent_rssd_id , substring(data,99,30) as parent_name, "
	mySQL = mySQL + "substring(data,129,25) as parent_city, substring(data,154,2) as parent_state, "
	mySQL = mySQL + "to_number(substring(data,156,4),'9999') as activity_year, substring(data,160,10) as respondent_rssd_id "
	mySQL = mySQL + "FROM hmda.working; "
	mySQL = mySQL + "DROP TABLE IF EXISTS " + schema + ".working; " 
	theCur.execute(mySQL)


#load the panel data
def load_panel_pre_2004():
	#the panel data source is in a fixed field format, so it has a two step process
	#first it one must load it into a working table as a single field, then the data
	#must be substring'd out into the defined columns
	
	#create the working table
	mySQL = "DROP TABLE IF EXISTS " + schema + ".working; "
	mySQL = mySQL + "CREATE TABLE " + schema + ".working (data text); "
	mySQL = mySQL + "COPY " + schema + ".working "
	mySQL = mySQL + "from '" + dPath + year + "/data/Panel.final." + year + ".dat' ; "
	mySQL = mySQL + "COMMIT; "
	theCur.execute(mySQL)
	
	#create the panel table structure
	mySQL = "DROP TABLE IF EXISTS " + schema + "." + tb_pre + "_panel_" + year + "; "
	mySQL = mySQL + "CREATE TABLE " + schema + "." + tb_pre + "_panel_" + year + " ( "
	mySQL = mySQL + "respondent_id character varying(10), msa_number character varying(5), "
	mySQL = mySQL + "agency_code integer, agency_group_code character varying(2), "
	mySQL = mySQL + "parent_respondent_id character varying(10), respondent_name_panel character varying(30), "
	mySQL = mySQL + "respondent_city_panel character varying(25), respondent_state_panel character varying(2), "
	mySQL = mySQL + "respondent_fips_state_number character varying(2), assets character varying(10), "
	mySQL = mySQL + "other_lender_code integer, parent_rssd_id character varying(10), "
	mySQL = mySQL + "parent_name_panel character varying(30), parent_city_panel character varying(25), "
	mySQL = mySQL + "parent_state_panel character varying(2), activity_year integer "
	mySQL = mySQL + " ) WITH ( OIDS=TRUE ); "
	mySQL = mySQL + "ALTER TABLE " + schema + "." + tb_pre + "_panel_" + year + " "
	mySQL = mySQL + "OWNER TO " + myUser + "; "
	mySQL = mySQL + "COMMENT ON TABLE " + schema + "." + tb_pre + "_panel_" + year + " "
	mySQL = mySQL + "IS '" + myComment + "'; "
	
#	#substring out the panel rows to each 
	mySQL = mySQL + "INSERT INTO " + schema + "." + tb_pre + "_panel_" + year + " 	( "
	mySQL = mySQL + "respondent_id, msa_number, agency_code, agency_group_code, "
	mySQL = mySQL + "respondent_name_panel, respondent_city_panel, respondent_state_panel, "
	mySQL = mySQL + "respondent_fips_state_number, assets, other_lender_code, parent_rssd_id, "
	mySQL = mySQL + "parent_name_panel, parent_city_panel, parent_state_panel, activity_year "
	mySQL = mySQL + " )  SELECT  "
	mySQL = mySQL + "substring(data,1,10) as respondent_id, substring(data,11,4) as msa_number, "
	mySQL = mySQL + "to_number(substring(data,15,1),'9') as agency_code, substring(data,16,2) as agency_group_code, "
	mySQL = mySQL + "substring(data,18,30) as respondent_name_panel, substring(data,48,25) as respondent_city_panel, "
	mySQL = mySQL + "substring(data,73,2) as respondent_state_panel, substring(data,75,2) as respondent_fips_state_number, "
	mySQL = mySQL + "substring(data,77,10) as assets, to_number(substring(data,87,1), '9') as other_lender_code, "
	mySQL = mySQL + "substring(data,88,10) as parent_rssd_id , substring(data,98,30) as parent_name, "
	mySQL = mySQL + "substring(data,128,25) as parent_city, substring(data,153,2) as parent_state, "
	mySQL = mySQL + "to_number(substring(data,155,4),'9999') as activity_year "
	mySQL = mySQL + "FROM hmda.working; "
	mySQL = mySQL + "DROP TABLE IF EXISTS " + schema + ".working; " 
	theCur.execute(mySQL)

#load the ts data
def load_ts_2004_2011():
	#the ts data source is in a fixed field format, so it has a two step process
	#first it one must load it into a working table as a single field, then the data
	#must be substring'd out into the defined columns
	
	#create the working table
	mySQL = "DROP TABLE IF EXISTS " + schema + ".working; "
	mySQL = mySQL + "CREATE TABLE " + schema + ".working (data text); "
	mySQL = mySQL + "COPY " + schema + ".working "
	mySQL = mySQL + "from '" + dPath + year + "/data/TS.final." + year + ".dat' ; "
	mySQL = mySQL + "COMMIT; "	
	theCur.execute(mySQL)
			
#	#create the panel table structure
	mySQL = "DROP TABLE IF EXISTS " + schema + "." + tb_pre + "_ts_" + year + "; "
	mySQL = mySQL + "CREATE TABLE " + schema + "." + tb_pre + "_ts_" + year + " ( "
	mySQL = mySQL + "as_of_date integer, agency_code character varying(1), "
	mySQL = mySQL + "respondent_id character varying(10), respondent_mail_name character varying(30), "
	mySQL = mySQL + "respondent_mail_address character varying(40), respondent_mail_city character varying(25), "
	mySQL = mySQL + "respondent_mail_state character varying(2), respondent_mail_zip character varying(10), "
	mySQL = mySQL + "parent_name character varying(30), parent_address character varying(40), "
	mySQL = mySQL + "parent_city character varying(30), "	
	mySQL = mySQL + "parent_state character varying(2), parent_zip character varying(10), "
	mySQL = mySQL + "edit_status character varying(1), tax_id character varying(10) "
	mySQL = mySQL + " ) WITH ( OIDS=TRUE ); "
	mySQL = mySQL + "ALTER TABLE " + schema + "." + tb_pre + "_ts_" + year + " "
	mySQL = mySQL + "OWNER TO " + myUser + "; "
	mySQL = mySQL + "COMMENT ON TABLE " + schema + "." + tb_pre + "_ts_" + year + " "
	mySQL = mySQL + "IS '" + myComment + "'; "

#	#substring out the panel rows to each 
	mySQL = mySQL + "INSERT INTO " + schema + "." + tb_pre + "_ts_" + year + "( "
	mySQL = mySQL + "as_of_date, agency_code, respondent_id, respondent_mail_name, "
	mySQL = mySQL + "respondent_mail_address, respondent_mail_city, respondent_mail_state, "
	mySQL = mySQL + "respondent_mail_zip, parent_name, parent_address, parent_city, "
	mySQL = mySQL + "parent_state, parent_zip, edit_status, tax_id ) SELECT " 
	mySQL = mySQL + "to_number(substring(data,1,4), '9999') as as_of_date, substring(data,5,1) as agency_code, "
	mySQL = mySQL + "substring(data,6,10) as respondent_id, substring(data,16,30) as respondent_mail_city, "
	mySQL = mySQL + "substring(data,46,40) as respondent_mail_address, substring(data,86,25) as respondent_city_panel, "
	mySQL = mySQL + "substring(data,111,2) as respondent_mail_state, substring(data,113,10) as respondent_mail_zip, "
	mySQL = mySQL + "substring(data,123,30) as parent_name, substring(data,153,40) as parent_address, "
	mySQL = mySQL + "substring(data,193,25) as parent_city , substring(data,218,2) as parent_state, "
	mySQL = mySQL + "substring(data,220,10) as parent_zip, substring(data,230,1) as edit_status, "
	mySQL = mySQL + "substring(data,231,10) as tax_id "
	mySQL = mySQL + "FROM hmda.working; "
	mySQL = mySQL + "DROP TABLE IF EXISTS " + schema + ".working; " 
	theCur.execute(mySQL)


#load the ts data
def load_ts_1998_2003():
	#the ts data source is in a fixed field format, so it has a two step process
	#first it one must load it into a working table as a single field, then the data
	#must be substring'd out into the defined columns
	
	#create the working table
	mySQL = "DROP TABLE IF EXISTS " + schema + ".working; "
	mySQL = mySQL + "CREATE TABLE " + schema + ".working (data text); "
	mySQL = mySQL + "COPY " + schema + ".working "
	mySQL = mySQL + "from '" + dPath + year + "/data/TS.final." + year + ".dat' ; "
	mySQL = mySQL + "COMMIT; "
	theCur.execute(mySQL)
	
#	#create the panel table structure
	mySQL = "DROP TABLE IF EXISTS " + schema + "." + tb_pre + "_ts_" + year + "; "
	mySQL = mySQL + "CREATE TABLE " + schema + "." + tb_pre + "_ts_" + year + " ( "
	mySQL = mySQL + "as_of_date integer, agency_code character varying(1), "
	mySQL = mySQL + "respondent_id character varying(10), respondent_mail_name character varying(30), "
	mySQL = mySQL + "respondent_mail_address character varying(40), respondent_mail_city character varying(25), "
	mySQL = mySQL + "respondent_mail_state character varying(2), respondent_mail_zip character varying(10), "
	mySQL = mySQL + "edit_status character varying(1), tax_id character varying(10) "
	mySQL = mySQL + " ) WITH ( OIDS=TRUE ); "
	mySQL = mySQL + "ALTER TABLE " + schema + "." + tb_pre + "_ts_" + year + " "
	mySQL = mySQL + "OWNER TO " + myUser + "; "
	mySQL = mySQL + "COMMENT ON TABLE " + schema + "." + tb_pre + "_ts_" + year + " "
	mySQL = mySQL + "IS '" + myComment + "'; "
	
#	#substring out the panel rows to each 
	mySQL = mySQL + "INSERT INTO " + schema + "." + tb_pre + "_ts_" + year + "( "
	mySQL = mySQL + "as_of_date, agency_code, respondent_id, respondent_mail_name, "
	mySQL = mySQL + "respondent_mail_address, respondent_mail_city, respondent_mail_state, "
	mySQL = mySQL + "respondent_mail_zip, "
	mySQL = mySQL + "edit_status, tax_id ) SELECT " 
	mySQL = mySQL + "to_number(substring(data,1,4), '9999') as as_of_date, substring(data,5,1) as agency_code, "
	mySQL = mySQL + "substring(data,6,10) as respondent_id, substring(data,16,30) as respondent_mail_city, "
	mySQL = mySQL + "substring(data,46,40) as respondent_mail_address, substring(data,86,25) as respondent_city_panel, "
	mySQL = mySQL + "substring(data,111,2) as respondent_mail_state, substring(data,113,10) as respondent_mail_zip, "
	mySQL = mySQL + "substring(data,123,1) as edit_status, "
	mySQL = mySQL + "substring(data,124,10) as tax_id "
	mySQL = mySQL + "FROM hmda.working; "
	mySQL = mySQL + "DROP TABLE IF EXISTS " + schema + ".working; " 
	theCur.execute(mySQL)

#load the ts data
def load_ts_1990_1991():
	#the ts data source is in a fixed field format, so it has a two step process
	#first it one must load it into a working table as a single field, then the data
	#must be substring'd out into the defined columns
	
	#create the working table
	mySQL = "DROP TABLE IF EXISTS " + schema + ".working; "
	mySQL = mySQL + "CREATE TABLE " + schema + ".working (data text); "
	mySQL = mySQL + "COPY " + schema + ".working "
	mySQL = mySQL + "from '" + dPath + year + "/data/TS.final." + year + ".dat' ; "
	mySQL = mySQL + "COMMIT; "
	theCur.execute(mySQL)
	
#	#create the panel table structure
	mySQL = "DROP TABLE IF EXISTS " + schema + "." + tb_pre + "_ts_" + year + "; "
	mySQL = mySQL + "CREATE TABLE " + schema + "." + tb_pre + "_ts_" + year + " ( "
	mySQL = mySQL + "as_of_date integer, agency_code character varying(1), "
	mySQL = mySQL + "respondent_id character varying(10), respondent_mail_name character varying(30), "
	mySQL = mySQL + "respondent_mail_address character varying(40), respondent_mail_city character varying(25), "
	mySQL = mySQL + "respondent_mail_state character varying(2), respondent_mail_zip character varying(10), "
	mySQL = mySQL + "edit_status character varying(1) "
	mySQL = mySQL + " ) WITH ( OIDS=TRUE ); "
	mySQL = mySQL + "ALTER TABLE " + schema + "." + tb_pre + "_ts_" + year + " "
	mySQL = mySQL + "OWNER TO " + myUser + "; "
	mySQL = mySQL + "COMMENT ON TABLE " + schema + "." + tb_pre + "_ts_" + year + " "
	mySQL = mySQL + "IS '" + myComment + "'; "
	
#	#substring out the panel rows to each 
	mySQL = mySQL + "INSERT INTO " + schema + "." + tb_pre + "_ts_" + year + " ( "
	mySQL = mySQL + "as_of_date, agency_code, respondent_id, respondent_mail_name, "
	mySQL = mySQL + "respondent_mail_address, respondent_mail_city, respondent_mail_state, "
	mySQL = mySQL + "respondent_mail_zip, "
	mySQL = mySQL + "edit_status ) SELECT " 
	mySQL = mySQL + "to_number(substring(data,1,4), '9999') as as_of_date, substring(data,5,1) as agency_code, "
	mySQL = mySQL + "substring(data,6,10) as respondent_id, substring(data,16,30) as respondent_mail_city, "
	mySQL = mySQL + "substring(data,46,40) as respondent_mail_address, substring(data,86,25) as respondent_city_panel, "
	mySQL = mySQL + "substring(data,111,2) as respondent_mail_state, substring(data,113,10) as respondent_mail_zip, "
	mySQL = mySQL + "substring(data,123,1) as edit_status "
	mySQL = mySQL + "FROM hmda.working; "
	mySQL = mySQL + "DROP TABLE IF EXISTS " + schema + ".working; " 
	theCur.execute(mySQL)

#load the ts data
def load_lar_2004_2011():
	#the ts data source is in a fixed field format, so it has a two step process
	#first it one must load it into a working table as a single field, then the data
	#must be substring'd out into the defined columns
	
	#create the working table
	mySQL = "DROP TABLE IF EXISTS " + schema + ".working; "
	mySQL = mySQL + "CREATE TABLE " + schema + ".working (data text); "
	mySQL = mySQL + "COPY " + schema + ".working "
	mySQL = mySQL + "from '" + dPath + year + "/data/Lars.final." + year + ".dat' ; "
	mySQL = mySQL + "COMMIT; "
	theCur.execute(mySQL)
	
#	#create the panel table structure
	mySQL = "DROP TABLE IF EXISTS " + schema + "." + tb_pre + "_lar_" + year + "; "
	mySQL = mySQL + "CREATE TABLE " + schema + "." + tb_pre + "_lar_" + year + " ( "
	mySQL = mySQL + "as_of_date integer, respondent_id character varying(10), "
	mySQL = mySQL + "agency_code character varying(1), loan_type integer, "
	mySQL = mySQL + "loan_purpose integer, occupancy integer, loan_amount character varying(5), "
	mySQL = mySQL + "action_type integer, msa_md character varying(5), state_code character varying(2), "
	mySQL = mySQL + "county_code character varying(3), census_tract character varying(7), "
	mySQL = mySQL + "applicant_sex integer, co_applicant_sex integer, applicant_income character varying(4), "
	mySQL = mySQL + "purchaser_type character varying(1), denial_reason_1 character varying(1), "
	mySQL = mySQL + "denial_reason_2 character varying(1), denial_reason_3 character varying(1), "
	mySQL = mySQL + "edit_status character varying(1), property_type character varying(1), "
	mySQL = mySQL + "preapproval character varying(1), applicant_ethnicity character varying(1), "
	mySQL = mySQL + "co_applicant_ethnicity character varying(1), applicant_race_1 character varying(1), "
	mySQL = mySQL + "applicant_race_2 character varying(1), applicant_race_3 character varying(1), "
	mySQL = mySQL + "applicant_race_4 character varying(1), applicant_race_5 character varying(1), "
	mySQL = mySQL + "co_applicant_race_1 character varying(1), co_applicant_race_2 character varying(1), "
	mySQL = mySQL + "co_applicant_race_3 character varying(1), co_applicant_race_4 character varying(1), "
	mySQL = mySQL + "co_applicant_race_5 character varying(1), rate_spread character varying(5), "
	mySQL = mySQL + "hoepa_status character varying(1), lien_status character varying(1), "
	mySQL = mySQL + "sequence_number character varying(7) "
	mySQL = mySQL + " ) WITH ( OIDS=TRUE ); "
	mySQL = mySQL + "ALTER TABLE " + schema + "." + tb_pre + "_lar_" + year + " "
	mySQL = mySQL + "OWNER TO " + myUser + "; "
	mySQL = mySQL + "COMMENT ON TABLE " + schema + "." + tb_pre + "_lar_" + year + " "
	mySQL = mySQL + "IS '" + myComment + "'; "
	
#	#substring out the panel rows to each 
	mySQL = mySQL + "INSERT INTO " + schema + "." + tb_pre + "_lar_" + year + " ( "
	mySQL = mySQL + "as_of_date, respondent_id, "
	mySQL = mySQL + "agency_code, loan_type, "
	mySQL = mySQL + "loan_purpose, occupancy, loan_amount, "
	mySQL = mySQL + "action_type, msa_md, state_code, "
	mySQL = mySQL + "county_code, census_tract, "
	mySQL = mySQL + "applicant_sex, co_applicant_sex, applicant_income, "
	mySQL = mySQL + "purchaser_type, denial_reason_1, "
	mySQL = mySQL + "denial_reason_2, denial_reason_3, "
	mySQL = mySQL + "edit_status, property_type, "
	mySQL = mySQL + "preapproval, applicant_ethnicity, "
	mySQL = mySQL + "co_applicant_ethnicity, applicant_race_1, "
	mySQL = mySQL + "applicant_race_2, applicant_race_3, "
	mySQL = mySQL + "applicant_race_4, applicant_race_5, "
	mySQL = mySQL + "co_applicant_race_1, co_applicant_race_2, "
	mySQL = mySQL + "co_applicant_race_3, co_applicant_race_4, "
	mySQL = mySQL + "co_applicant_race_5, rate_spread, "
	mySQL = mySQL + "hoepa_status, lien_status, "
	mySQL = mySQL + "sequence_number "
	mySQL = mySQL + " ) SELECT " 
	mySQL = mySQL + "to_number(substring(data,1,4), '9999') as as_of_date, substring(data,5,10) respondent_id, "
	mySQL = mySQL + "substring(data,15,1) as agency_code, to_number(substring(data,16,1), '9') as loan_type, "
	mySQL = mySQL + "to_number(substring(data,17,1), '9') as loan_purpose, to_number(substring(data,18,1), '9') as occupancy, "
	mySQL = mySQL + "substring(data,19,5) as loan_amount, "
	mySQL = mySQL + "to_number(substring(data,24,1), '9') action_type, substring(data,25,5) as msa_md, "
	mySQL = mySQL + "substring(data,30,2) as state_code, "
	mySQL = mySQL + "substring(data,32,3) as county_code, substring(data,35,7) as census_tract, "
	mySQL = mySQL + "to_number(substring(data,42,1), '9') as applicant_sex, to_number(substring(data,43,1), '9') as co_applicant_sex, "
	mySQL = mySQL + "substring(data,44,4) as applicant_income, "
	mySQL = mySQL + "substring(data,48,1) as purchaser_type, substring(data,49,1) as denial_reason_1, "
	mySQL = mySQL + "substring(data,50,1) as denial_reason_2, substring(data,51,1) as denial_reason_3, "
	mySQL = mySQL + "substring(data,52,1) as edit_status, substring(data,53,1) as property_type, "
	mySQL = mySQL + "substring(data,54,1) as preapproval, substring(data,55,1) as applicant_ethnicity, "
	mySQL = mySQL + "substring(data,56,1) as co_applicant_ethnicity, substring(data,57,1) as applicant_race_1, "
	mySQL = mySQL + "substring(data,58,1) as applicant_race_2, substring(data,59,1) as applicant_race_3, "
	mySQL = mySQL + "substring(data,60,1) as applicant_race_4, substring(data,61,1) as applicant_race_5, "
	mySQL = mySQL + "substring(data,62,1) as co_applicant_race_1, substring(data,63,1) as co_applicant_race_2, "
	mySQL = mySQL + "substring(data,64,1) as co_applicant_race_3, substring(data,65,1) as co_applicant_race_4, "
	mySQL = mySQL + "substring(data,66,1) as co_applicant_race_5, substring(data,67,5) as rate_spread, "
	mySQL = mySQL + "substring(data,72,1) as hoepa_status, substring(data,73,1) as lien_status, "
	mySQL = mySQL + "substring(data,74,7) as sequence_number "	
	mySQL = mySQL + "FROM hmda.working; "
	mySQL = mySQL + "DROP TABLE IF EXISTS " + schema + ".working; " 
	theCur.execute(mySQL)
	
	#create the index
	mySQL = "DROP INDEX IF EXISTS " + schema + "_" + tb_pre + "_lar_" + year 
	mySQL = mySQL + "_respondent_id_ndx; "
	mySQL = mySQL + "CREATE INDEX " + schema + "_" + tb_pre + "_lar_" + year
	mySQL = mySQL + "_respondent_id_ndx ON " + schema + "." + tb_pre + "_lar_" 
	mySQL = mySQL + year + " USING btree (respondent_id); "
	theCur.execute(mySQL)
	
#load the ts data
def load_lar_1990_2004():
	#the ts data source is in a fixed field format, so it has a two step process
	#first it one must load it into a working table as a single field, then the data
	#must be substring'd out into the defined columns
	
	#create the working table
	mySQL = "DROP TABLE IF EXISTS " + schema + ".working; "
	mySQL = mySQL + "CREATE TABLE " + schema + ".working (data text); "
	mySQL = mySQL + "COPY " + schema + ".working "
	mySQL = mySQL + "from '" + dPath + year + "/data/Lars.final." + year + ".dat' ; "
	mySQL = mySQL + "COMMIT; "
	theCur.execute(mySQL)
	
#	#create the panel table structure
	mySQL = "DROP TABLE IF EXISTS " + schema + "." + tb_pre + "_lar_" + year + "; "
	mySQL = mySQL + "CREATE TABLE " + schema + "." + tb_pre + "_lar_" + year + " ( "
	mySQL = mySQL + "as_of_date integer, respondent_id character varying(10), "
	mySQL = mySQL + "agency_code character varying(1), loan_type character varying(1), "
	mySQL = mySQL + "loan_purpose character varying(1), occupancy character varying(1), "
	mySQL = mySQL + "loan_amount character varying(5), action_type character varying(1), "
	mySQL = mySQL + "msa_md character varying(5), state_code character varying(2), "
	mySQL = mySQL + "county_code character varying(3), census_tract character varying(7), "
	mySQL = mySQL + "applicant_race_1 character varying(1), co_applicant_race_1 character varying(1), "
	mySQL = mySQL + "applicant_sex character varying(1), co_applicant_sex character varying(1), "
	mySQL = mySQL + "applicant_income character varying(4), "
	mySQL = mySQL + "purchaser_type character varying(1), denial_reason_1 character varying(1), "
	mySQL = mySQL + "denial_reason_2 character varying(1), denial_reason_3 character varying(1), "
	mySQL = mySQL + "edit_status character varying(1), sequence_number character varying(7) "
	mySQL = mySQL + " ) WITH ( OIDS=TRUE ); "
	mySQL = mySQL + "ALTER TABLE " + schema + "." + tb_pre + "_lar_" + year + " "
	mySQL = mySQL + "OWNER TO " + myUser + "; "
	mySQL = mySQL + "COMMENT ON TABLE " + schema + "." + tb_pre + "_lar_" + year + " "
	mySQL = mySQL + "IS '" + myComment + "'; "
	theCur.execute(mySQL)
	
#	#substring out the panel rows to each 
	mySQL = mySQL + "INSERT INTO " + schema + "." + tb_pre + "_lar_" + year + " ( "
	mySQL = mySQL + "as_of_date, respondent_id, "
	mySQL = mySQL + "agency_code, loan_type, "
	mySQL = mySQL + "loan_purpose, occupancy, loan_amount, "
	mySQL = mySQL + "action_type, msa_md, state_code, "
	mySQL = mySQL + "county_code, census_tract, "
	mySQL = mySQL + "applicant_race_1, co_applicant_race_1, "
	mySQL = mySQL + "applicant_sex, co_applicant_sex, applicant_income, "
	mySQL = mySQL + "purchaser_type, denial_reason_1, "
	mySQL = mySQL + "denial_reason_2, denial_reason_3, "
	mySQL = mySQL + "edit_status, sequence_number "
	mySQL = mySQL + " ) SELECT " 
	mySQL = mySQL + "to_number(substring(data,1,4), '9999') as as_of_date, substring(data,5,10) respondent_id, "
	mySQL = mySQL + "substring(data,15,1) as agency_code, substring(data,16,1) as loan_type, "
	mySQL = mySQL + "substring(data,17,1) as loan_purpose, substring(data,18,1) as occupancy, "
	mySQL = mySQL + "substring(data,19,5) as loan_amount, "
	mySQL = mySQL + "substring(data,24,1) action_type, substring(data,25,4) as msa_md, "
	mySQL = mySQL + "substring(data,29,2) as state_code, "
	mySQL = mySQL + "substring(data,31,3) as county_code, substring(data,34,7) as census_tract, "
	mySQL = mySQL + "substring(data,41,1) as applicant_race_1, substring(data,42,1) as co_applicant_race_1, "
	mySQL = mySQL + "substring(data,43,1) as applicant_sex, substring(data,44,1) as co_applicant_sex, "
	mySQL = mySQL + "substring(data,45,4) as applicant_income, "
	mySQL = mySQL + "substring(data,49,1) as purchaser_type, substring(data,50,1) as denial_reason_1, "
	mySQL = mySQL + "substring(data,51,1) as denial_reason_2, substring(data,52,1) as denial_reason_3, "
	mySQL = mySQL + "substring(data,53,1) as edit_status, "
	mySQL = mySQL + "substring(data,54,7) as sequence_number "	
	mySQL = mySQL + "FROM hmda.working; "
	mySQL = mySQL + "DROP TABLE IF EXISTS " + schema + ".working; " 
	theCur.execute(mySQL)
	
	#create the index
	mySQL = "DROP INDEX IF EXISTS " + schema + "_" + tb_pre + "_lar_" + year 
	mySQL = mySQL + "_respondent_id_ndx; "
	mySQL = mySQL + "CREATE INDEX " + schema + "_" + tb_pre + "_lar_" + year
	mySQL = mySQL + "_respondent_id_ndx ON " + schema + "." + tb_pre + "_lar_" 
	mySQL = mySQL + year + " USING btree (respondent_id); "
	theCur.execute(mySQL)
	
#make database connection - so that we can use cursors to update the table
myConn = "dbname=" + db + " host=" + myHost + " port=" + myPort + " user=" + myUser
conn = psycopg2.connect(myConn)
theCur = conn.cursor()


#1990 LAR has a utf-8 problem
#the set of data on which to loop
files = ["lar"] #"panel","ts","lar"]
years = ["1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011"]
years = ["2005","2006","2007","2008","2009","2010","2011"] 
for file in files:
	for year in years:
		try:
			print "     loading year: " + year
			if file == "panel":
				if int(year) > 2003 and int(year) < 2012:
					load_panel_2004_2011()
				if int(year) < 2004:
					load_panel_pre_2004()
			if file == "ts":
				if int(year) > 1989 and int(year) < 1992:
					load_ts_1990_1991()
				if int(year) > 1991 and int(year) < 1998:
					load_ts_2004_2011()
				if int(year) > 1997 and int(year) < 2004:
					load_ts_1998_2003()
				if int(year) > 2003 and int(year) < 2012:
					load_ts_2004_2011()
			if file == "lar":
				if int(year) > 2003 and int(year) < 2012:
					load_lar_2004_2011()
				if int(year) > 1989 and int(year) < 2004:
					load_lar_1990_2004()

		except:
			theCur.close()
			conn.close()
			print "     " + file + "   failed, continuing on ..."
			continue

theCur.close()
conn.close()
		
now = time.localtime(time.time())
print "end time:", time.asctime(now)
