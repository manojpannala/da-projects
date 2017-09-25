--HIVE

CREATE DATABASE CTOR;
USE CTOR;

CREATE EXTERNAL TABLE Cust_hive (click string, open string, ethnicity string, gender string, household_status string, salary string, mail_date string, Year string, Month string, Day string, TimeStamp string, AMPM string) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|' 
LOCATION '/user/hduser/finalt/pig/';

select * from Cust_hive;

-- Click to open ratio

Create table ClickToOpenRatio AS SELECT ROUND(SUM(CASE WHEN click = 'Y' THEN 1 ELSE 0 END)/count(*) * 100,2) CTOR FROM Cust_hive WHERE open='Y';

-- CTOR by gender

Create table CTOR_Gender AS SELECT gender, ROUND(SUM(CASE WHEN click = 'Y' THEN 1 ELSE 0 END)/count(*) * 100,2) CTOR FROM Cust_hive WHERE open='Y' GROUP BY gender;

-- CTOR by Time of the day

Create table CTOR_TimeofDay AS SELECT TimeStamp, AMPM, ROUND(SUM(CASE WHEN click = 'Y' THEN 1 ELSE 0 END)/count(*) * 100,2) CTOR FROM Cust_hive WHERE open='Y' GROUP BY TimeStamp,AMPM;

-- CTOR by Day of the Week

Create table CTOR_DayofWeek AS SELECT Day, ROUND(SUM(CASE WHEN click = 'Y' THEN 1 ELSE 0 END)/count(*) * 100,2) CTOR FROM Cust_hive WHERE open='Y' GROUP BY Day;

--CTOR by Month of Year

Create table CTOR_MonthofYear AS SELECT Month, ROUND(SUM(CASE WHEN click = 'Y' THEN 1 ELSE 0 END)/count(*) * 100,2) CTOR FROM Cust_hive WHERE open='Y' GROUP BY Month;

--CTOR by salary

Create table CTOR_Salary AS SELECT salary, ROUND(SUM(CASE WHEN click = 'Y' THEN 1 ELSE 0 END)/count(*) * 100,2) CTOR FROM Cust_hive WHERE open='Y' GROUP BY salary;

-- CTOR by ethnicity

Create table CTOR_Ethnicity AS SELECT ethnicity, ROUND(SUM(CASE WHEN click = 'Y' THEN 1 ELSE 0 END)/count(*) * 100,2) CTOR FROM Cust_hive WHERE open='Y' GROUP BY ethnicity;

--CTOR by household

Create table CTOR_Household AS SELECT household_status, ROUND(SUM(CASE WHEN click = 'Y' THEN 1 ELSE 0 END)/count(*) * 100,2) CTOR FROM Cust_hive WHERE open='Y' GROUP BY household_status;
