// Data has been loaded into the PIG.

RAW_DATA = LOAD '/home/hduser/pigdata/AirlinesData_sample-1.csv' USING PigStorage(',') AS 
	(year: int, month: int, day: int, dow: int, 
	dtime: int, sdtime: int, arrtime: int, satime: int, 
	carrier: chararray, fn: int, tn: chararray, 
	etime: int, setime: int, airtime: int, 
	adelay: int, ddelay: int, 
	scode: chararray, dcode: chararray, dist: int, 
	tintime: int, touttime: int, 
	cancel: chararray, cancelcode: chararray, diverted: int, 
	cdelay: int, wdelay: int, ndelay: int, sdelay: int, latedelay: int);

// I used the foreach loop for the month and carrier and the Actual arrival time minus the the Scheduled time as the DELAY.

A = FOREACH RAW_DATA GENERATE month AS m, carrier, ddelay, wdelay, (int)(arrtime-satime) AS delay;

// Then I grouped the A into the tuple (m,carrier, ddelay, wdelay) inorder to see the output along with the carrier delay, departure delay & weather delay.


B = GROUP A BY ( m, carrier, ddelay, wdelay) ;


// I used the new relation COUNT_TOTAL where B loops through the bag. FILTER operation is used on the A to the tuple (delay >=15).
//Finally generate the output as GROUP followed by COUNT(A) by tot, COUNT(C) as del and the fraction of COUNT(C)/COUNT(A).

COUNT_TOTAL = FOREACH B {
	C = FILTER A BY (delay >= 15);
GENERATE GROUP, COUNT(A) AS tot, COUNT(C) AS del, (float) COUNT(C)/COUNT(A) AS frac;
}

// Finally dump the values.

dump COUNT_TOTAL
