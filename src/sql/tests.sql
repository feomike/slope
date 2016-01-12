--test sql

drop table if exists hmda.tests;
create table hmda.tests
(
	mywho character varying(30),
	mywhat character varying(200),
	mywhere character varying(25),
	mywhen character varying(4),
	myqry character varying(10),
	myvalue integer
)
WITH (
  OIDS=TRUE
);
ALTER TABLE hmda.tests
  OWNER TO byrnem;
COMMENT ON TABLE hmda.tests
  IS 'contains test results';

--not accurate faster count - SELECT reltuples::bigint AS estimate FROM pg_class where relname='ffiec_lar_2001';
--who is All, or RID+AgencyCode
--what is ["all","single-family","refi","home-improvement","purchased-loan"]
--where is nationwide, or statewide/state_abbrev
--when is a random number between 1990-2014

--16406627
--test #1, All, all, 1997, count
insert into hmda.tests (mywho, mywhat, mywhere, mywhen, myqry, myvalue ) values 
	('All', 'All', 'Nationwide', '1997', 'count', ( select count(*) 
		from hmda.ffiec_lar_1997 where state_code is not null));

--test #2, single-family, all, 1997, count
insert into hmda.tests (mywho, mywhat, mywhere, mywhen, myqry, myvalue ) values 
	('All', 'single-family', 'Nationwide', '1997', 'count', ( select count(*) 
		from hmda.ffiec_lar_1997 where action_type = '1' and loan_purpose = '1' and occupancy = '1'));

--test #3, refi
insert into hmda.tests (mywho, mywhat, mywhere, mywhen, myqry, myvalue ) values 
	('All', 'refi', 'Nationwide', '1997', 'count', ( select count(*) 
		from hmda.ffiec_lar_1997 where action_type = '1' and loan_purpose = '3'));

--test #4, home-improvement
insert into hmda.tests (mywho, mywhat, mywhere, mywhen, myqry, myvalue ) values 
	('All', 'home-improvement', 'Nationwide', '1997', 'count', ( select count(*) 
		from hmda.ffiec_lar_1997 where action_type = '1' and loan_purpose = '2'));

--test #5, purchased-loan
insert into hmda.tests (mywho, mywhat, mywhere, mywhen, myqry, myvalue ) values 
	('All', 'purchased-loan', 'Nationwide', '1997', 'count', ( select count(*) 
		from hmda.ffiec_lar_1997 where purchaser_type <> '0'));








