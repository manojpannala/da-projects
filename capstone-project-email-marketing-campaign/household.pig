--PIG

register /home/data/piggybank.jar;

define CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

CustomerData = LOAD '/user/hduser/final/CampaignData.csv' USING CSVLoader;

--FILTER DATA and using $ and number for selecting desired rows.

HD = FOREACH CustomerData GENERATE (chararray) $125 as status1, (chararray) $126 as status2, (chararray) $127 as status3, (chararray) $128 as status4, (chararray) $129 as status5, (chararray) $130 as status6,
    (chararray) $131 as status7, (chararray) $132 as status8, (chararray) $109 as age1, (chararray) $110 as age2, (chararray) $111 as age3, (chararray) $112 as age4, (chararray) $113 as age5, (chararray) $114 as age6,
	(chararray) $115 as age7, (chararray) $116 as age8, (chararray) $76 as children_18_or_less, (chararray) $92 as presence_children, (chararray) $50 as goc1, (chararray) $51 as goc2, (chararray) $52 as goc3,
	(chararray) $53 as goc4, (chararray) $54 as goc5;



STORE HD INTO ‘/user/Jig13302/FinalCaseStudy/pigout2’ USING PigStorage(‘|’);
