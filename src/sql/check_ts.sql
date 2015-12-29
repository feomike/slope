drop table if exists hmda.check_ts;
create table hmda.check_ts
(
  as_of_date integer,
  respondent_id character varying(10),
  agency_code character varying(1),
  respondent_mail_name character varying(30),
  count integer
  )
WITH (
  OIDS=TRUE
);

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_1990
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_1991
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_1992
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_1993
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_1994
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_1995
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_1996
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_1997
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_1998
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_1999
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_2000
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_2001
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_2002
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_2003
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_2004
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_2005
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_2006
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_2007
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_2008
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_2009
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_2010
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select as_of_date, respondent_id, agency_code, respondent_mail_name
	from hmda.ffiec_ts_2011
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select activity_year, respondent_id, agency_code, respondent_name_ts
	from hmda.ffiec_inst_rec_2012
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select activity_year, respondent_id, agency_code, respondent_name_ts
	from hmda.ffiec_inst_rec_2013
	where respondent_id = '0000061650' and agency_code = '5';

insert into hmda.check_ts
	select activity_year, respondent_id, agency_code, respondent_name_ts
	from hmda.ffiec_inst_rec_2014
	where respondent_id = '0000061650' and agency_code = '5';

select * from hmda.check_ts;
