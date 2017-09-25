--Registering the piggybank.jar

register /usr/local/pig/lib/piggybank.jar;

--Using CSVLoader to load the data from .csv file

define CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

CustomerData = LOAD '/user/hduser/final/CampaignData.csv' USING CSVLoader;

dump CustomerData;

/user/hduser/final/CampaignData.csv

--FILTER DATA

CD = FOREACH CustomerData GENERATE (chararray) $0 as click, (chararray) $1 as open, (chararray) $4 as ethnicity, (chararray) $60 as gender, (chararray) $61 as household_status, (chararray) $101 as salary, (chararray) $272 as mail_date;

CD2 = FOREACH CD GENERATE  click as click, open as open, (ethnicity == '' ? 'null' : (ethnicity is null ? 'null': ethnicity)) as ethnicity, gender as gender, household_status as household_status, (salary == '' ? 'null' : (salary is null ? 'null': salary)) as salary, mail_date as mail_date, SUBSTRING(mail_date,11,13)  As Year,(SUBSTRING(mail_date,5,7)=='01' ? 'January' : ( SUBSTRING(mail_date,5,7)=='02' ? 'February' : (SUBSTRING(mail_date,5,7)=='03' ? 'March' :(SUBSTRING(mail_date,5,7)=='04' ? 'April' :(SUBSTRING(mail_date,5,7)=='05' ? 'May' : (SUBSTRING(mail_date,5,7)=='06' ? 'June' : (SUBSTRING(mail_date,5,7)=='07' ? 'July' :(SUBSTRING(mail_date,5,7)=='08' ? 'August' :(SUBSTRING(mail_date,5,7)=='09' ? 'September' :(SUBSTRING(mail_date,5,7)=='10' ? 'October' :(SUBSTRING(mail_date,5,7)=='11' ? 'November' :'December'))))))))))) As Month,SUBSTRING(mail_date,0,3) as Day,SUBSTRING(mail_date,14,19) as TimeStamp,SUBSTRING(mail_date,20,22) as AMPM; 

dump CD2;

--Storing the file in order to access the file using hive

STORE CD2  INTO '/user/hduser/finalt/pig'  USING PigStorage('|');
