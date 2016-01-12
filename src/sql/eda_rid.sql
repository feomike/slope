SELECT respondent_id, agency_code, respondent_name_ts, lar_count, parent_name_ts 
	FROM hmda.ffiec_ts_2014
	where (CAST(coalesce(lar_count,'0') as integer)) > 2000
	order by (CAST(coalesce(lar_count,'0') as integer)) desc	
	limit 10;