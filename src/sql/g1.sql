--0000019659; 59-1286880
select respondent_id, fed_tax_id, respondent_name_ts, lar_count

select *
	from hmda.ffiec_inst_rec_2014
	where respondent_id = '0000061650';
--null parent_name_ts = 5325 
--not null parent_name_ts = 1737
select count(*) from hmda.ffiec_inst_rec_2014 where parent_name_ts <> '';

select parent_name_ts, count(*) from hmda.ffiec_inst_rec_2014 group by parent_name_ts order by count



--panel has parent_rssd_id
select *
	from hmda.ffiec_panel_2014 
	where respondent_id = '0000061650';

--very few parents in pane
select agency_code, parent_rssd_id, count(*)
	from hmda.ffiec_panel_2014 
	group by agency_code, parent_rssd_id
	order by agency_code, count

select *
	from hmda.ffiec_inst_rec_2014 
	where respondent_id = '0000061650';

--2012
--"0000061650";"94-0362025";"THE GOLDEN 1 CREDIT UNION";"5489"
--2013
--"0000061650";"94-0362025";"THE GOLDEN 1 CREDIT UNION";"4704"
--2014
--"0000061650";"94-0362025";"THE GOLDEN 1 CREDIT UNION";"3823"


select loan_type, sequence_number 
	from hmda.ffiec_lar_2014
	where respondent_id = '0000061650'
	order by sequence_number