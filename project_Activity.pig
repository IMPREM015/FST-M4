inputepisodeIV = LOAD 'hdfs:///Users/inputs/episodeIV_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
inputepisodeV = LOAD 'hdfs:///Users/inputs/episodeV_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
inputepisodeVI = LOAD 'hdfs:///Users/inputs/episodeVI_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
ranked4= RANK inputepisodeIV;
onlyepisodeIV = FILTER ranked4 BY (rank_inputepisodeIV > 2);
ranked5= RANK inputepisodeV;
onlyepisodeV = FILTER ranked5 BY (rank_inputepisodeV > 2);
ranked6= RANK inputepisodeVI;
onlyepisodeVI = FILTER ranked6 BY (rank_inputepisodeVI > 2);
inputData = UNION onlyepisodeIV, onlyepisodeV, onlyepisodeVI;
groupByName = GROUP inputData BY name;
names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
namesOrdered = ORDER names BY  no_of_lines DESC;
rmf hdfs:///Users/02767W744/Desktop/Backup/Full Stack Testeer/Module 4/Activities/root/outputs;
STORE namesOrdered INTO 'hdfs:///Users/02767W744/Desktop/Backup/Full Stack Testeer/Module 4/Activities/root/outputs' USING PigStorage('\t');

