drop table carrier;
create table carrier (
	code string,
	description string
	)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
     LINES TERMINATED BY '\n'
 STORED AS TEXTFILE;
