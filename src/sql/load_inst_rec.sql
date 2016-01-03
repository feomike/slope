drop table if exists hmda.ffiec_inst_rec_2012;
CREATE TABLE hmda.ffiec_inst_rec_2012
(
	activity_year integer,
	respondent_id character varying(10),
	agency_code character varying(1),
	fed_tax_id character varying(10),
	respondent_name_ts character varying(30),
	respondent_mailing_address character varying(40),
	respondent_city_ts character varying(25),
	respondent_state_ts character varying(2),
	respondent_zip_code  character varying(10),
	parent_name_ts character varying(30),
	parent_address character varying(40),
	parent_city_ts character varying(25),
	parent_state_ts character varying(2),
	parent_zip_code character varying(10),
	respondent_name_panel character varying(30),
	respondent_city_panel character varying(25),
	respondent_state_panel character varying(2),
	assets_panel character varying(10),
	other_lender_code_panel integer,
	region_code_panel character varying(2),
	lar_count character varying(10),
	validity_error character varying(1)
)
WITH (
  OIDS=TRUE
);
ALTER TABLE hmda.ffiec_inst_rec_2012
  OWNER TO feomike;
COMMENT ON TABLE hmda.ffiec_inst_rec_2012
  IS 'created_on_12/15/2015_with_import_http://www.ffiec.gov/hmda/hmdaflat.htm';

copy hmda.ffiec_inst_rec_2012
	from '/Users/feomike/documents/data/ffiec_hmda/2012/data/2012HMDAInstitutionRecords.txt' 
	DELIMITER E'\t';

