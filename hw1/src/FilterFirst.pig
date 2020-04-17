REGISTER file:/usr/lib/pig/lib/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

f1_v1 = LOAD 's3://aws-logs-390142268212-us-east-1/elasticmapreduce/data.csv' USING CSVLoader(',');
f1_v2 = foreach f1_v1 generate (datetime)$5 as Date, (chararray)$11 as Origin,
    (chararray)$17 as Dest, (chararray)$41 as Cancelled , (chararray)$43 as Diverted, (int)$35 as Arrtime,
    (double)$37 as Delay;
f1 = FILTER f1_v2 BY  (Origin == 'ORD') AND ( NOT (Dest == 'JFK')) AND (Cancelled == '0.00') AND (Diverted == '0.00')
    AND (Date >= ToDate('2007-06-01','yyyy-MM-dd')) AND (Date <= ToDate('2008-05-31','yyyy-MM-dd'));


f2_v1 = LOAD's3://aws-logs-390142268212-us-east-1/elasticmapreduce/data.csv' USING CSVLoader(',');
f2_v2 = foreach f2_v1 generate (datetime)$5 as Date, (chararray)$11 as Origin,
    (chararray)$17 as Dest, (chararray)$41 as Cancelled , (chararray)$43 as Diverted, (int)$24 as Deptime,
    (double)$37 as Delay;
f2 = FILTER f2_v2 BY  (NOT (Origin == 'ORD')) AND (Dest == 'JFK') AND (Cancelled == '0.00') AND (Diverted == '0.00')
    AND (Date >= ToDate('2007-06-01','yyyy-MM-dd')) AND (Date <= ToDate('2008-05-31','yyyy-MM-dd'));

f3_v1 = JOIN f1 BY Date, f2 BY Date;

f3_v2 = FILTER f3_v1 BY (f1::Arrtime < f2::Deptime);


f3 = foreach f3_v2 GENERATE f1::Delay + f2::Delay as Comb;

f4 = GROUP f3 ALL;

answer = FOREACH f4 GENERATE SUM(f3.Comb)/COUNT(f3);

DUMP answer;