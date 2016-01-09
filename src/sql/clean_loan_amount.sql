SELECT COUNT(*) FROM hmda.ffiec_lar_1990 
SELECT sum(CAST(coalesce(loan_amount, '0') AS integer)) FROM hmda.ffiec_lar_1990;

select loan_amount, count(*)
	from hmda.ffiec_lar_1992
	group by loan_amount
	order by loan_amount


--deleted rows
delete from hmda.ffiec_lar_1990 where loan_amount = '     ';
delete from hmda.ffiec_lar_1990 where loan_amount like '% %';
delete from hmda.ffiec_lar_1990 where loan_amount like '%!%';
delete from hmda.ffiec_lar_1990 where loan_amount like '%-%';
delete from hmda.ffiec_lar_1990 where loan_amount like '%H%';
delete from hmda.ffiec_lar_1990 where loan_amount like '%A%';

delete from hmda.ffiec_lar_1991 where loan_amount = '0000Q'
delete from hmda.ffiec_lar_1991 where loan_amount = '**105'
delete from hmda.ffiec_lar_1991 where loan_amount like '%J';
delete from hmda.ffiec_lar_1991 where loan_amount like '%-6';
delete from hmda.ffiec_lar_1991 where loan_amount like 'NA%';
delete from hmda.ffiec_lar_1992 where loan_amount like '% %';
delete from hmda.ffiec_lar_1992 where loan_amount like '0006''';
delete from hmda.ffiec_lar_1992 where loan_amount like 'NA%';
delete from hmda.ffiec_lar_1993 where loan_amount like '%NA';
delete from hmda.ffiec_lar_1993 where loan_amount like 'NA%';
delete from hmda.ffiec_lar_1995 where loan_amount like '% %';
delete from hmda.ffiec_lar_1999 where loan_amount like '% %';
delete from hmda.ffiec_lar_2000 where loan_amount like '% %';
delete from hmda.ffiec_lar_2001 where loan_amount like '% %';
delete from hmda.ffiec_lar_2002 where loan_amount like '% %';
delete from hmda.ffiec_lar_2003 where loan_amount like '% %';
