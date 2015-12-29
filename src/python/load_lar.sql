
drop table if exists hmda.ffiec_lar_2012;
CREATE TABLE hmda.ffiec_lar_2012
(
	as_of_year integer, 
	respondent_id character varying(10),
	agency_code character varying(1),
	loan_type integer,
	property_type character varying(1),
	loan_purpose integer,
	occupancy integer,
	loan_amount integer,
	preapproval character varying(1),
	action_type integer,
	msa_md character varying(5),
	state_code character varying(2),
	county_code character varying(3),
	census_tract character varying(7), 
	applicant_ethnicity character varying(1), 
	co_applicant_ethnicity character varying(1), 
	applicant_race_1 character varying(1), 
	applicant_race_2 character varying(1), 
	applicant_race_3 character varying(1), 
	applicant_race_4 character varying(1), 
	applicant_race_5 character varying(1), 
	co_applicant_race_1 character varying(1), 
	co_applicant_race_2 character varying(1), 
	co_applicant_race_3 character varying(1), 
	co_applicant_race_4 character varying(1), 
	co_applicant_race_5 character varying(1), 
	applicant_sex integer,
	co_applicant_sex integer,
	applicant_income character varying(4), 
	purchaser_type character varying(1), 
	denial_reason_1 character varying(1), 
	denial_reason_2 character varying(1), 
	denial_reason_3 character varying(1), 
	rate_spread character varying(5), 
	hoepa_status character varying(1), 
	lien_status character varying(1), 
	edit_status character varying(1), 
	sequence_number character varying(7), 	
	population character varying(8), 
	minority_population_pct character varying(6), 
	ffiec_median_family_income character varying(8), 
	tract_to_msa_md_income_pct character varying(6), 
	number_of_owner_occupied_units character varying(8), 
	number_of_1_to_4_family_units character varying(8), 
	application_date_indicator character varying(1)
)
WITH (
  OIDS=TRUE
);
ALTER TABLE hmda.ffiec_lar_2012
  OWNER TO feomike;
COMMENT ON TABLE hmda.ffiec_lar_2012
  IS 'created_on_12/14/2015_with_import_http://www.ffiec.gov/hmda/hmdaflat.htm';

copy hmda.ffiec_lar_2012
	from '/Users/feomike/documents/data/ffiec_hmda/2012/data/hmda_lar_2012.csv' csv;

--copy 
--(select sequence_number, count(*)
--	from hmda.ffiec_lar_2012
--	group by sequence_number
--	order by count desc)
--	to '/users/feomike/downloads/2012_seq.csv' 
--	;

--select * from hmda.ffiec_lar_2012 where sequence_number = '0000001'
