create index hmda_ffiec_lar_2012_sequence_numer
	on hmda.ffiec_lar_2012
	USING btree (respondent_id);
	
--2012
select * from hmda.ffiec_inst_rec_2012 limit 1
select *, respondent_id, lar_count from hmda.ffiec_inst_rec_2012 
	where respondent_id = '0000000324'
	limit 5
--"0000000047";"3657" - confirmed
--"0000000056";"2715" - confirmed
--"0000000086";"17" - confirmed
--"0000000182";"207" - confirmed
--"0000000324";"208" - confirmed respondent_id + agency_code is identifier

select sequence_number from hmda.ffiec_lar_2012 
	where respondent_id = '0000000324' and agency_code = '1'
	order by sequence_number desc;

create index hmda_ffiec_lar_2014_sequence_numer
	on hmda.ffiec_lar_2014
	USING btree (respondent_id);

select count(*) from hmda.ffiec_lar_2014
--11,875,464
--11,875,464
select respondent_id || agency_code || sequence_number as myid, count(*)
	from hmda.ffiec_lar_2014
	group by myid
	order by count desc

select respondent_id || agency_code, parent_name_panel 
	from hmda.ffiec_panel_2014 
	where parent_name_panel = '';


select * from hmda.ffiec_panel_2014 
	order by top_holder_name desc
