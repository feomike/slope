
drop table if exists hmda.ffiec_panel_2012;
CREATE TABLE hmda.ffiec_panel_2012
(
	activity_year integer,
	respondent_id character varying(10),
	agency_code integer,
	parent_respondent_id character varying(10),
	parent_name_panel character varying(30),
	parent_city_panel character varying(25),
	parent_state_panel character varying(2),
	region integer,
	assets character varying(10),
	other_lender_code integer,
	respondent_name_panel character varying(30),
	respondent_city_panel character varying(25),
	respondent_state_panel character varying(2),
	top_holder_rssd_id character varying(10),
	top_holder_name character varying(30),
	top_holder_city character varying(25),
	top_holder_state character varying(2),
	top_holder_country character varying(40),
	respondent_rssd_id character varying(10), 
	parent_rssd_id character varying(10), 
	respondent_fips_state_number character varying(2)
)
WITH (
  OIDS=TRUE
);
ALTER TABLE hmda.ffiec_panel_2012
  OWNER TO feomike;
COMMENT ON TABLE hmda.ffiec_panel_2012
  IS 'created_on_12/15/2015_with_import_http://www.ffiec.gov/hmda/hmdaflat.htm';

drop table if exists hmda.working;
create table hmda.working (data text);
copy hmda.working
	from '/Users/feomike/documents/data/ffiec_hmda/2012/data/2012HMDAReporterPanel.dat';

--	activity_year, respondent_id, agency_code, parent_respondent_id, parent_name,
--	parent_city, parent_state, region, assets, other_lender_code, respondent_name,
--	filler_1, respondent_city, respondent_state, filler_2, filler_3,
--	top_holder_rssd_id, top_holder_name, top_holder_city,
--	top_holder_state, top_holder_country, respondent_rssd_id,  parent_rssd_id, 
--	respondent_fips_state_number

INSERT INTO hmda.ffiec_panel_2012
	(activity_year, respondent_id, agency_code, parent_respondent_id, parent_name_panel,
	parent_city_panel, parent_state_panel, region, assets, other_lender_code, respondent_name_panel,
	respondent_city_panel, respondent_state_panel,
	top_holder_rssd_id, top_holder_name, top_holder_city,
	top_holder_state, top_holder_country, respondent_rssd_id,  parent_rssd_id,
	respondent_fips_state_number
	)
    SELECT 
	to_number(substring(data,1,4),'9999') as activity_year, 
	substring(data,5,10) as respondent_id,
	to_number(substring(data,15,1),'9') as agency_code,
	substring(data,16,10) as parent_respondent_id,
	substring(data,26,30) as parent_name,
	substring(data,56,25) as parent_city,
	substring(data,81,2) as parent_state,
	to_number(substring(data,83,2), '99') as region,
	substring(data,85,10) as assets,
	to_number(substring(data,95,1), '9') as other_lender_code,
	substring(data,96,30) as respondent_name,
	substring(data,166, 25 ) as respondent_city,
	substring(data, 191, 2) as respondent_state,
	substring(data,213,10 ) as top_holder_rssd_id,
	substring(data,223, 30) as top_holder_name,
	substring(data,253,25) as top_holder_city,
	substring(data,278,2) as top_holder_state, 
	substring(data,280,40) as top_holder_country, 
	substring(data,320,10) as respondent_rssd_id,  
	substring(data,330,10) as parent_rssd_id,
	substring(data,340,2) as respondent_fips_state_number
    FROM hmda.working;

drop table if exists hmda.working;


