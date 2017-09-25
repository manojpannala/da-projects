--HIVE

CREATE EXTERNAL TABLE HD_hive (status1 string, status2 string, status3 string, status4 string, status5 string, status6 string, status7 string, status8 string, age1 string, age2 string, age3 string, age4 string, age5 string, age6 string, age7 string, age8 string, children_18_or_less string, presence_children string, goc1 string, goc2 string, goc3 string, goc4 string, goc5 string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘|’
LOCATION ‘/user/Jig13302/FinalCaseStudy/pigout2’;

Select * from HD_hive;




--Question 1
CREATE TABLE Q1 as SELECT SUM(CASE WHEN status1 != "" OR status2 != "" OR status3 != "" OR status4 != "" OR status5 != "" OR status6 != "" OR status7 != "" OR status8 != "" THEN 1 ELSE 0 END) AS total FROM HD_hive;


--Question 2
CREATE TABLE Q2 AS SELECT SUM(CASE WHEN status1 != "" THEN 1 ELSE 0 END) + SUM(CASE WHEN status2 != "" THEN 1 ELSE 0 END) + SUM(CASE WHEN status3 != "" THEN 1 ELSE 0 END) + SUM(CASE WHEN status4 != "" THEN 1 ELSE 0 END) + SUM(CASE WHEN status5 != "" THEN 1 ELSE 0 END) + SUM(CASE WHEN status6 != "" THEN 1 ELSE 0 END) + SUM(CASE WHEN status7 != "" THEN 1 ELSE 0 END) + SUM(CASE WHEN status8 != "" THEN 1 ELSE 0 END) AS total FROM HD_hive;

--QUESTION 3

CREATE TABLE Q3 AS SELECT  SUM(CASE WHEN status1 = 'H' THEN 1 ELSE 0 END) + SUM(CASE WHEN status2 = 'H' THEN 1 ELSE 0 END) + SUM(CASE WHEN status3 = 'H' THEN 1 ELSE 0 END) + SUM(CASE WHEN status4 = 'H' THEN 1 ELSE 0 END) + SUM(CASE WHEN status5 = 'H' THEN 1 ELSE 0 END) + SUM(CASE WHEN status6 = 'H' THEN 1 ELSE 0 END) + SUM(CASE WHEN status7 = 'H' THEN 1 ELSE 0 END) + SUM(CASE WHEN status8 = 'H' THEN 1 ELSE 0 END) as Head, SUM(CASE WHEN status1 = 'W' THEN 1 ELSE 0 END) + SUM(CASE WHEN status2 = 'W' THEN 1 ELSE 0 END) + SUM(CASE WHEN status3 = 'W' THEN 1 ELSE 0 END) + SUM(CASE WHEN status4 = 'W' THEN 1 ELSE 0 END) + SUM(CASE WHEN status5 = 'W' THEN 1 ELSE 0 END) + SUM(CASE WHEN status6 = 'W' THEN 1 ELSE 0 END) + SUM(CASE WHEN status7 = 'W' THEN 1 ELSE 0 END) + SUM(CASE WHEN status8 = 'W' THEN 1 ELSE 0 END) as Spouse, SUM(CASE WHEN status1 = 'P' THEN 1 ELSE 0 END) + SUM(CASE WHEN status2 = 'P' THEN 1 ELSE 0 END) +  SUM(CASE WHEN status3 = 'P' THEN 1 ELSE 0 END) + SUM(CASE WHEN status4 = 'P' THEN 1 ELSE 0 END) +
SUM(CASE WHEN status5 = 'P' THEN 1 ELSE 0 END) + SUM(CASE WHEN status6 = 'P' THEN 1 ELSE 0 END) + SUM(CASE WHEN status7 = 'P' THEN 1 ELSE 0 END) + SUM(CASE WHEN status8 = 'P' THEN 1 ELSE 0 END) as Elderely_Parent, SUM(CASE WHEN status1 = 'U' THEN 1 ELSE 0 END) + SUM(CASE WHEN status2 = 'U' THEN 1 ELSE 0 END) +  SUM(CASE WHEN status3 = 'U' THEN 1 ELSE 0 END) + SUM(CASE WHEN status4 = 'U' THEN 1 ELSE 0 END) + SUM(CASE WHEN status5 = 'U' THEN 1 ELSE 0 END) + SUM(CASE WHEN status6 = 'U' THEN 1 ELSE 0 END) + SUM(CASE WHEN status7 = 'U' THEN 1 ELSE 0 END) + SUM(CASE WHEN status8 = 'U' THEN 1 ELSE 0 END) as Other_Adult, SUM(CASE WHEN status1 = 'Y' THEN 1 ELSE 0 END) + SUM(CASE WHEN status2 = 'Y' THEN 1 ELSE 0 END) + SUM(CASE WHEN status3 = 'Y' THEN 1 ELSE 0 END) + SUM(CASE WHEN status4 = 'Y' THEN 1 ELSE 0 END) + SUM(CASE WHEN status5 = 'Y' THEN 1 ELSE 0 END) + SUM(CASE WHEN status6 = 'Y' THEN 1 ELSE 0 END) +  SUM(CASE WHEN status7 = 'Y' THEN 1 ELSE 0 END) + SUM(CASE WHEN status8 = 'Y' THEN 1 ELSE 0 END) as Young_Adult from HD_hive;


--QUESTION 4
CREATE EXTERNAL TABLE housetype (htype string, htypedesc string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/user/Jig13302/FinalCaseStudy/pigout2';
CREATE VIEW vw_hcount AS SELECT h.htypedesc, typecount.hcount FROM
(SELECT t.htype, count(1) hcount FROM HD_hive LATERAL VIEW explode(array(status1, status2, status3, status4, status5, status6, status7, status8)) t AS htype WHERE t.htype != '' GROUP BY t.htype) typecount JOIN housetype h ON typecount.htype = h.htype ORDER BY h.htypedesc;

Create table Q4 as SELECT t1.htypedesc, ROUND(t1.hcount/t2.total, 2) percentoftotal FROM vw_hcount t1 JOIN (SELECT SUM(hcount) total FROM vw_hcount) t2;



--QUESTION 5
CREATE TABLE Q5 AS SELECT SUM(CASE WHEN presence_children !="" THEN 1 ELSE 0 END) AS total FROM HD_hive;





--QUESTION 6
CREATE TABLE Q6 AS SELECT SUM(CASE WHEN presence_children !="" THEN 1 ELSE 0 END) + SUM(CASE WHEN children_18_or_less !="" THEN 1 ELSE 0 END) AS total FROM HD_hive;



--QUESTION 7
create table Q7 as select count(*) as total, sum(case when goc1 = 'M' then 1 else 0 end ) + sum(case when goc2 = 'M' then 1 else 0 end ) + sum(case when goc3 = 'M' then 1 else 0 end )  + sum(case when goc4 = 'M' then 1 else 0 end )  + sum(case when goc5 = 'M' then 1 else 0 end ) AS Male, sum( case when goc1 = 'F' then 1 else 0 end )  + sum( case when goc2 = 'F' then 1 else 0 end )  + sum( case when goc3 = 'F' then 1 else 0 end )  + sum( case when goc4 = 'F' then 1 else 0 end )  + sum( case when goc5 = 'F' then 1 else 0 end ) as Female  from HD_hive;
