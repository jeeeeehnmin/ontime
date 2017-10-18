drop table airport;
create table airport(
	iata	string,
	airport	string,
	city	string,
	state	string,
	country string,
	lat	double,
	lng	double
	)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
     LINES TERMINATED BY '\n'
 STORED AS TEXTFILE;
