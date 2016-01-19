
--whats compelling?  
--	finding some of enough size with history far back enough?
--	rolling up some?

--for all in 2014, how many had same rid/agency code back to 1991?

drop table if exists hmda.rid_history;
create table hmda.rid_history 
(
   respondent_id character varying(10), 
   agency_code character varying(1),
   count_1990 integer,
   count_1991 integer,
   count_1992 integer,
   count_1993 integer,
   count_1994 integer,
   count_1995 integer,
   count_1996 integer,
   count_1997 integer,
   count_1998 integer,
   count_1999 integer,
   count_2000 integer,
   count_2001 integer,
   count_2002 integer,
   count_2003 integer,
   count_2004 integer,
   count_2005 integer,
   count_2006 integer,
   count_2007 integer,
   count_2008 integer,
   count_2009 integer,
   count_2010 integer,
   count_2011 integer,
   count_2012 integer,
   count_2013 integer,
   count_2014 integer
)
WITH (
  OIDS=TRUE
);
ALTER TABLE hmda.rid_history
  OWNER TO byrnem;
COMMENT ON TABLE hmda.rid_history
  IS 'created_on_01/11/2016 based of 2014_ts to support time series analysis';

SELECT count(*) from hmda.ffiec_lar_2002 WHERE respondent_id = '00-1196704' and agency_code = '1' ; 

insert into hmda.rid_history (respondent_id, agency_code, fi_name) 
	values ('1', '2', 'myName');

select count(*) from hmda.ffiec_ts_2002
select * from hmda.rid_history
	where count_1990 > 0 and count_2014 > 0
	order by count_2014 desc

--alter table add years
--populate years = 24
--for each year, if that year is 0, subtract 1 from years
alter table hmda.rid_history add column hist_length integer;
update hmda.rid_history set hist_length = 25;
update hmda.rid_history set hist_length = hist_length - 1 where count_1990 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_1991 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_1992 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_1993 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_1994 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_1995 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_1996 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_1997 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_1998 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_1999 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2000 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2001 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2002 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2003 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2004 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2005 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2006 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2007 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2008 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2009 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2010 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2011 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2012 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2013 = 0;
update hmda.rid_history set hist_length = hist_length - 1 where count_2014 = 0;


--15*1000 = 15,000
alter table hmda.rid_history add column total integer;
update hmda.rid_history set total = count_1990 + count_1991 + count_1992 + count_1993 + count_1994 + count_1995 + count_1996 + 
	count_1997 + count_1998 + count_1999 + count_2000 + count_2001 + count_2002 + count_2003 + count_2004 +
	count_2005 + count_2006 + count_2007 + count_2008 + count_2009 + count_2010 + count_2011 + count_2012 + 
	count_2013 + count_2014;


