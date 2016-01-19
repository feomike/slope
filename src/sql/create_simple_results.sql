drop table if exists hmda.simple_results;

CREATE TABLE hmda.simple_results
(
  respondent_id character varying(10),
  agency_code character varying(1),
  fi_name character varying(30),
  qry character varying(30),
  mylocation character varying(100),
  simple_num real
)
WITH (
  OIDS=TRUE
);
ALTER TABLE hmda.simple_results
  OWNER TO feomike;
COMMENT ON TABLE hmda.rid_history
  IS 'created_on_01/14/2016 results of simple similarity';

--INSERT INTO hmda.simple_results values ( '0000013044', '1', 'bank-of-america-na', 'all', 'nationwide', 72.0) ;
--truncate hmda.simple_results;
select simple_results.fi_name, simple_results.respondent_id, simple_results.agency_code, qry, simple_num, hist_length, total 
	from hmda.simple_results, hmda.rid_history
	where simple_results.respondent_id = rid_history.respondent_id and 
		simple_results.agency_code = rid_history.agency_code
		and qry = 'purchased-loan' and simple_results.fi_name like '%mariner%'
	order by simple_num desc;
--	and simple_results.fi_name like '%golden%';

select count(*) from hmda.simple_results;
select avg(simple_num), qry
	from hmda.simple_results
	group by qry


