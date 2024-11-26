-- NY_CRASHES TABLE (MAIN DATASET) --

-- Create table NY_CRASHES
CREATE OR REPLACE TABLE NY_CRASHES (
    CRASH_DATE DATE NOT NULL,
    CRASH_TIME TIME NOT NULL,
    BOROUGH VARCHAR(100) NOT NULL,
    ZIP_CODE NUMBER(5,0) NOT NULL,
    LATITUDE NUMBER(38,6) NOT NULL,
    LONGITUDE NUMBER(38,6) NOT NULL,
    ON_STREET_NAME VARCHAR(100) DEFAULT 'NA',
    CROSS_STREET_NAME VARCHAR(100) DEFAULT 'NA',
    OFF_STREET_NAME VARCHAR(100) DEFAULT 'NA',
    NUM_PERSONS_INJURED NUMBER(3,0) DEFAULT 0,
    NUM_PERSONS_KILLED NUMBER(3,0) DEFAULT 0,
    NUM_PEDEST_INJURED NUMBER(3,0) DEFAULT 0,
    NUM_PEDEST_KILLED NUMBER(3,0) DEFAULT 0,
    NUM_CYCL_INJURED NUMBER(3,0) DEFAULT 0,
    NUM_CYCL_KILLED NUMBER(3,0) DEFAULT 0,
    NUM_MOTOR_INJURED NUMBER(3,0) DEFAULT 0,
    NUM_MOTOR_KILLED NUMBER(3,0) DEFAULT 0,
    CONT_FACTOR_VEH_1 VARCHAR(100) DEFAULT 'unspecified',
    CONT_FACTOR_VEH_2 VARCHAR(100) DEFAULT 'unspecified',
    CONT_FACTOR_VEH_3 VARCHAR(100) DEFAULT 'unspecified',
    CONT_FACTOR_VEH_4 VARCHAR(100) DEFAULT 'unspecified',
    CONT_FACTOR_VEH_5 VARCHAR(100) DEFAULT 'unspecified',
    COLLISION_ID NUMBER(38,0) UNIQUE NOT NULL,
    VEH_TYPE_CODE_1 VARCHAR(100) DEFAULT 'unspecified',
    VEH_TYPE_CODE_2 VARCHAR(100) DEFAULT 'unspecified',
    VEH_TYPE_CODE_3 VARCHAR(100) DEFAULT 'unspecified',
    VEH_TYPE_CODE_4 VARCHAR(100) DEFAULT 'unspecified',
    VEH_TYPE_CODE_5 VARCHAR(100) DEFAULT 'unspecified'
);

-- Populate data into NY_CRASHES table from compressed csv file in Snowflake STAGE
COPY INTO ny_crashes FROM @my_stage FILE_FORMAT = (TYPE = CSV COMPRESSION = GZIP FIELD_DELIMITER = ',' SKIP_HEADER = 1);



-- FACT & DIMENSTION TABLES --

-- Create dimension table DATE_DIM
create or replace table DATE_DIM (
	DATE_KEY				number(9) PRIMARY KEY,
	DATE					date not null,
	FULL_DATE_DESC			varchar(64) not null,
	DAY_NUM_IN_WEEK			number(1) not null,
	DAY_NUM_IN_MONTH		number(2) not null,
	DAY_NUM_IN_YEAR			number(3) not null,
	DAY_NAME				varchar(10) not null,
	DAY_ABBREV				varchar(3) not null,
	WEEKDAY_IND				varchar(64) not null,
	US_HOLIDAY_IND			varchar(64) not null,
	/*<COMPANYNAME>*/_HOLIDAY_IND varchar(64) not null,
	MONTH_END_IND			varchar(64) not null,
	WEEK_BEGIN_DATE_NKEY		number(9) not null,
	WEEK_BEGIN_DATE			date not null,
	WEEK_END_DATE_NKEY		number(9) not null,
	WEEK_END_DATE			date not null,
	WEEK_NUM_IN_YEAR		number(9) not null,
	MONTH_NAME				varchar(10) not null,
	MONTH_ABBREV			varchar(3) not null,
	MONTH_NUM_IN_YEAR		number(2) not null,
	YEARMONTH				varchar(10) not null,
	QUARTER					number(1) not null,
	YEARQUARTER				varchar(10) not null,
	YEAR					number(5) not null,
	FISCAL_WEEK_NUM			number(2) not null,
	FISCAL_MONTH_NUM		number(2) not null,
	FISCAL_YEARMONTH		varchar(10) not null,
	FISCAL_QUARTER			number(1) not null,
	FISCAL_YEARQUARTER		varchar(10) not null,
	FISCAL_HALFYEAR			number(1) not null,
	FISCAL_YEAR				number(5) not null,
	SQL_TIMESTAMP			timestamp_ntz,
	CURRENT_ROW_IND			char(1) default 'Y',
	EFFECTIVE_DATE			date default to_date(current_timestamp),
	EXPIRATION_DATE			date default To_date('9999-12-31') 
)
comment = 'Type 0 Dimension Table Housing Calendar and Fiscal Year Date Attributes'; 

-- Populate data into DIM_DATE table
insert into DATE_DIM
select DATE_PKEY,
		DATE_COLUMN,
        FULL_DATE_DESC,
		DAY_NUM_IN_WEEK,
		DAY_NUM_IN_MONTH,
		DAY_NUM_IN_YEAR,
		DAY_NAME,
		DAY_ABBREV,
		WEEKDAY_IND,
		US_HOLIDAY_IND,
        COMPANY_HOLIDAY_IND,
		MONTH_END_IND,
		WEEK_BEGIN_DATE_NKEY,
		WEEK_BEGIN_DATE,
		WEEK_END_DATE_NKEY,
		WEEK_END_DATE,
		WEEK_NUM_IN_YEAR,
		MONTH_NAME,
		MONTH_ABBREV,
		MONTH_NUM_IN_YEAR,
		YEARMONTH,
		CURRENT_QUARTER,
		YEARQUARTER,
		CURRENT_YEAR,
		FISCAL_WEEK_NUM,
		FISCAL_MONTH_NUM,
		FISCAL_YEARMONTH,
		FISCAL_QUARTER,
		FISCAL_YEARQUARTER,
		FISCAL_HALFYEAR,
		FISCAL_YEAR,
		SQL_TIMESTAMP,
		CURRENT_ROW_IND,
		EFFECTIVE_DATE,
		EXPIRA_DATE
	from 
	( select  to_date('1999-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') as DD, /*<<Modify date for preferred table start date*/
			seq1() as Sl,row_number() over (order by Sl) as row_numbers,
			dateadd(day,row_numbers,DD) as V_DATE,
			case when date_part(dd, V_DATE) < 10 and date_part(mm, V_DATE) > 9 then
				date_part(year, V_DATE)||date_part(mm, V_DATE)||'0'||date_part(dd, V_DATE)
				 when date_part(dd, V_DATE) < 10 and  date_part(mm, V_DATE) < 10 then 
				 date_part(year, V_DATE)||'0'||date_part(mm, V_DATE)||'0'||date_part(dd, V_DATE)
				 when date_part(dd, V_DATE) > 9 and  date_part(mm, V_DATE) < 10 then
				 date_part(year, V_DATE)||'0'||date_part(mm, V_DATE)||date_part(dd, V_DATE)
				 when date_part(dd, V_DATE) > 9 and  date_part(mm, V_DATE) > 9 then
				 date_part(year, V_DATE)||date_part(mm, V_DATE)||date_part(dd, V_DATE) end as DATE_PKEY,
			V_DATE as DATE_COLUMN,
			dayname(dateadd(day,row_numbers,DD)) as DAY_NAME_1,
			case 
				when dayname(dateadd(day,row_numbers,DD)) = 'Mon' then 'Monday'
				when dayname(dateadd(day,row_numbers,DD)) = 'Tue' then 'Tuesday'
				when dayname(dateadd(day,row_numbers,DD)) = 'Wed' then 'Wednesday'
				when dayname(dateadd(day,row_numbers,DD)) = 'Thu' then 'Thursday'
				when dayname(dateadd(day,row_numbers,DD)) = 'Fri' then 'Friday'
				when dayname(dateadd(day,row_numbers,DD)) = 'Sat' then 'Saturday'
				when dayname(dateadd(day,row_numbers,DD)) = 'Sun' then 'Sunday' end ||', '||
			case when monthname(dateadd(day,row_numbers,DD)) ='Jan' then 'January'
				   when monthname(dateadd(day,row_numbers,DD)) ='Feb' then 'February'
				   when monthname(dateadd(day,row_numbers,DD)) ='Mar' then 'March'
				   when monthname(dateadd(day,row_numbers,DD)) ='Apr' then 'April'
				   when monthname(dateadd(day,row_numbers,DD)) ='May' then 'May'
				   when monthname(dateadd(day,row_numbers,DD)) ='Jun' then 'June'
				   when monthname(dateadd(day,row_numbers,DD)) ='Jul' then 'July'
				   when monthname(dateadd(day,row_numbers,DD)) ='Aug' then 'August'
				   when monthname(dateadd(day,row_numbers,DD)) ='Sep' then 'September'
				   when monthname(dateadd(day,row_numbers,DD)) ='Oct' then 'October'
				   when monthname(dateadd(day,row_numbers,DD)) ='Nov' then 'November'
				   when monthname(dateadd(day,row_numbers,DD)) ='Dec' then 'December' end
				   ||' '|| to_varchar(dateadd(day,row_numbers,DD), ' dd, yyyy') as FULL_DATE_DESC,
			dateadd(day,row_numbers,DD) as V_DATE_1,
			dayofweek(V_DATE_1)+1 as DAY_NUM_IN_WEEK,
			Date_part(dd,V_DATE_1) as DAY_NUM_IN_MONTH,
			dayofyear(V_DATE_1) as DAY_NUM_IN_YEAR,
			case 
				when dayname(V_DATE_1) = 'Mon' then 'Monday'
				when dayname(V_DATE_1) = 'Tue' then 'Tuesday'
				when dayname(V_DATE_1) = 'Wed' then 'Wednesday'
				when dayname(V_DATE_1) = 'Thu' then 'Thursday'
				when dayname(V_DATE_1) = 'Fri' then 'Friday'
				when dayname(V_DATE_1) = 'Sat' then 'Saturday'
				when dayname(V_DATE_1) = 'Sun' then 'Sunday' end as	DAY_NAME,
			dayname(dateadd(day,row_numbers,DD)) as DAY_ABBREV,
			case  
				when dayname(V_DATE_1) = 'Sun' and dayname(V_DATE_1) = 'Sat' then 
                 'Not-Weekday'
				else 'Weekday' end as WEEKDAY_IND,
			 case 
				when (DATE_PKEY = date_part(year, V_DATE)||'0101' or DATE_PKEY = date_part(year, V_DATE)||'0704' or
				DATE_PKEY = date_part(year, V_DATE)||'1225' or DATE_PKEY = date_part(year, V_DATE)||'1226') then  
				'Holiday' 
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Wed' 
				and dateadd(day,-2,last_day(V_DATE_1)) = V_DATE_1  then
				'Holiday'
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Thu' 
				and dateadd(day,-3,last_day(V_DATE_1)) = V_DATE_1  then
				'Holiday'
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Fri' 
				and dateadd(day,-4,last_day(V_DATE_1)) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Sat' 
				and dateadd(day,-5,last_day(V_DATE_1)) = V_DATE_1  then
				'Holiday'
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Sun' 
				and dateadd(day,-6,last_day(V_DATE_1)) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Mon' 
				and last_day(V_DATE_1) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Tue' 
				and dateadd(day,-1 ,last_day(V_DATE_1)) = V_DATE_1  then
				'Holiday'
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Wed' 
				and dateadd(day,5,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Thu' 
				and dateadd(day,4,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Fri' 
				and dateadd(day,3,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Sat' 
				and dateadd(day,2,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Sun' 
				and dateadd(day,1,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Mon' 
				and date_part(year, V_DATE_1)||'-09-01' = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Tue' 
				and dateadd(day,6 ,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Wed' 
				and (dateadd(day,23,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1  or 
					 dateadd(day,22,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Thu' 
				and ( dateadd(day,22,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 or 
					 dateadd(day,21,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Fri' 
				and ( dateadd(day,21,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 or 
					 dateadd(day,20,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				 'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Sat' 
				and ( dateadd(day,27,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 or 
					 dateadd(day,26,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Sun' 
				and ( dateadd(day,26,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 or 
					 dateadd(day,25,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Mon' 
				and (dateadd(day,25,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 or 
					 dateadd(day,24,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Tue' 
				and (dateadd(day,24,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 or 
					 dateadd(day,23,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				 'Holiday'    
				else
				'Not-Holiday' end as US_HOLIDAY_IND,
			/*Modify the following for Company Specific Holidays*/
			case 
				when (DATE_PKEY = date_part(year, V_DATE)||'0101' or DATE_PKEY = date_part(year, V_DATE)||'0219'
				or DATE_PKEY = date_part(year, V_DATE)||'0528' or DATE_PKEY = date_part(year, V_DATE)||'0704' 
				or DATE_PKEY = date_part(year, V_DATE)||'1225' )then 
				'Holiday'               
                when monthname(V_DATE_1) ='Mar' and dayname(last_day(V_DATE_1)) = 'Fri' 
				and last_day(V_DATE_1) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Mar' and dayname(last_day(V_DATE_1)) = 'Sat' 
				and dateadd(day,-1,last_day(V_DATE_1)) = V_DATE_1  then
				'Holiday'
				when monthname(V_DATE_1) ='Mar' and dayname(last_day(V_DATE_1)) = 'Sun' 
				and dateadd(day,-2,last_day(V_DATE_1)) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Tue'
                and dateadd(day,3,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Wed' 
				and dateadd(day,2,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Thu'
                and dateadd(day,1,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Fri' 
				and date_part(year, V_DATE_1)||'-04-01' = V_DATE_1 then
				'Holiday'
                when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Wed' 
				and dateadd(day,5,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Thu' 
				and dateadd(day,4,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Fri' 
				and dateadd(day,3,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Sat' 
				and dateadd(day,2,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Sun' 
				and dateadd(day,1,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Mon' 
                and date_part(year, V_DATE_1)||'-04-01'= V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Tue' 
				and dateadd(day,6 ,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1  then
				'Holiday'   
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Wed' 
				and dateadd(day,5,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Thu' 
				and dateadd(day,4,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Fri' 
				and dateadd(day,3,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Sat' 
				and dateadd(day,2,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Sun' 
				and dateadd(day,1,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Mon' 
                and date_part(year, V_DATE_1)||'-09-01' = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Tue' 
				and dateadd(day,6 ,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Wed' 
				and dateadd(day,23,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Thu' 
				and dateadd(day,22,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Fri' 
				and dateadd(day,21,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1  then
				 'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Sat' 
				and dateadd(day,27,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Sun' 
				and dateadd(day,26,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Mon' 
				and dateadd(day,25,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Tue' 
				and dateadd(day,24,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1  then
				 'Holiday'     
				else
				'Not-Holiday' end as COMPANY_HOLIDAY_IND,
			case                                           
				when last_day(V_DATE_1) = V_DATE_1 then 
				'Month-end'
				else 'Not-Month-end' end as MONTH_END_IND,
					
			case when date_part(mm,date_trunc('week',V_DATE_1)) < 10 and date_part(dd,date_trunc('week',V_DATE_1)) < 10 then
					  date_part(yyyy,date_trunc('week',V_DATE_1))||'0'||
					  date_part(mm,date_trunc('week',V_DATE_1))||'0'||
					  date_part(dd,date_trunc('week',V_DATE_1))  
				 when date_part(mm,date_trunc('week',V_DATE_1)) < 10 and date_part(dd,date_trunc('week',V_DATE_1)) > 9 then
						date_part(yyyy,date_trunc('week',V_DATE_1))||'0'||
						date_part(mm,date_trunc('week',V_DATE_1))||date_part(dd,date_trunc('week',V_DATE_1))    
				 when date_part(mm,date_trunc('week',V_DATE_1)) > 9 and date_part(dd,date_trunc('week',V_DATE_1)) < 10 then
						date_part(yyyy,date_trunc('week',V_DATE_1))||date_part(mm,date_trunc('week',V_DATE_1))||
						'0'||date_part(dd,date_trunc('week',V_DATE_1))    
				when date_part(mm,date_trunc('week',V_DATE_1)) > 9 and date_part(dd,date_trunc('week',V_DATE_1)) > 9 then
						date_part(yyyy,date_trunc('week',V_DATE_1))||
						date_part(mm,date_trunc('week',V_DATE_1))||
						date_part(dd,date_trunc('week',V_DATE_1)) end as WEEK_BEGIN_DATE_NKEY,
			date_trunc('week',V_DATE_1) as WEEK_BEGIN_DATE,

			case when  date_part(mm,last_day(V_DATE_1,'week')) < 10 and date_part(dd,last_day(V_DATE_1,'week')) < 10 then
					  date_part(yyyy,last_day(V_DATE_1,'week'))||'0'||
					  date_part(mm,last_day(V_DATE_1,'week'))||'0'||
					  date_part(dd,last_day(V_DATE_1,'week')) 
				 when  date_part(mm,last_day(V_DATE_1,'week')) < 10 and date_part(dd,last_day(V_DATE_1,'week')) > 9 then
					  date_part(yyyy,last_day(V_DATE_1,'week'))||'0'||
					  date_part(mm,last_day(V_DATE_1,'week'))||date_part(dd,last_day(V_DATE_1,'week'))   
				 when  date_part(mm,last_day(V_DATE_1,'week')) > 9 and date_part(dd,last_day(V_DATE_1,'week')) < 10  then
					  date_part(yyyy,last_day(V_DATE_1,'week'))||date_part(mm,last_day(V_DATE_1,'week'))||'0'||
					  date_part(dd,last_day(V_DATE_1,'week'))   
				 when  date_part(mm,last_day(V_DATE_1,'week')) > 9 and date_part(dd,last_day(V_DATE_1,'week')) > 9 then
					  date_part(yyyy,last_day(V_DATE_1,'week'))||
					  date_part(mm,last_day(V_DATE_1,'week'))||
					  date_part(dd,last_day(V_DATE_1,'week')) end as WEEK_END_DATE_NKEY,
			last_day(V_DATE_1,'week') as WEEK_END_DATE,
			week(V_DATE_1) as WEEK_NUM_IN_YEAR,
			case when monthname(V_DATE_1) ='Jan' then 'January'
				   when monthname(V_DATE_1) ='Feb' then 'February'
				   when monthname(V_DATE_1) ='Mar' then 'March'
				   when monthname(V_DATE_1) ='Apr' then 'April'
				   when monthname(V_DATE_1) ='May' then 'May'
				   when monthname(V_DATE_1) ='Jun' then 'June'
				   when monthname(V_DATE_1) ='Jul' then 'July'
				   when monthname(V_DATE_1) ='Aug' then 'August'
				   when monthname(V_DATE_1) ='Sep' then 'September'
				   when monthname(V_DATE_1) ='Oct' then 'October'
				   when monthname(V_DATE_1) ='Nov' then 'November'
				   when monthname(V_DATE_1) ='Dec' then 'December' end as MONTH_NAME,
			monthname(V_DATE_1) as MONTH_ABBREV,
			month(V_DATE_1) as MONTH_NUM_IN_YEAR,
			case when month(V_DATE_1) < 10 then 
			year(V_DATE_1)||'-0'||month(V_DATE_1)   
			else year(V_DATE_1)||'-'||month(V_DATE_1) end as YEARMONTH,
			quarter(V_DATE_1) as CURRENT_QUARTER,
			year(V_DATE_1)||'-0'||quarter(V_DATE_1) as YEARQUARTER,
			year(V_DATE_1) as CURRENT_YEAR,
			/*Modify the following based on company fiscal year - assumes Jan 01*/
            to_date(year(V_DATE_1)||'-01-01','YYYY-MM-DD') as FISCAL_CUR_YEAR,
            to_date(year(V_DATE_1) -1||'-01-01','YYYY-MM-DD') as FISCAL_PREV_YEAR,
			case when   V_DATE_1 < FISCAL_CUR_YEAR then
			datediff('week', FISCAL_PREV_YEAR,V_DATE_1)
			else 
			datediff('week', FISCAL_CUR_YEAR,V_DATE_1)  end as FISCAL_WEEK_NUM  ,
			decode(datediff('MONTH',FISCAL_CUR_YEAR, V_DATE_1)+1 ,-2,10,-1,11,0,12,
                   datediff('MONTH',FISCAL_CUR_YEAR, V_DATE_1)+1 ) as FISCAL_MONTH_NUM,
			concat( year(FISCAL_CUR_YEAR) 
				   ,case when to_number(FISCAL_MONTH_NUM) = 10 or 
							to_number(FISCAL_MONTH_NUM) = 11 or 
                            to_number(FISCAL_MONTH_NUM) = 12  then
							'-'||FISCAL_MONTH_NUM
					else  concat('-0',FISCAL_MONTH_NUM) end ) as FISCAL_YEARMONTH,
			case when quarter(V_DATE_1) = 4 then 4
				 when quarter(V_DATE_1) = 3 then 3
				 when quarter(V_DATE_1) = 2 then 2
				 when quarter(V_DATE_1) = 1 then 1 end as FISCAL_QUARTER,
			
			case when   V_DATE_1 < FISCAL_CUR_YEAR then
					year(FISCAL_CUR_YEAR)
					else year(FISCAL_CUR_YEAR)+1 end
					||'-0'||case when quarter(V_DATE_1) = 4 then 4
					 when quarter(V_DATE_1) = 3 then 3
					 when quarter(V_DATE_1) = 2 then 2
					 when quarter(V_DATE_1) = 1 then 1 end as FISCAL_YEARQUARTER,
			case when quarter(V_DATE_1) = 4  then 2 when quarter(V_DATE_1) = 3 then 2
				when quarter(V_DATE_1) = 1  then 1 when quarter(V_DATE_1) = 2 then 1
			end as FISCAL_HALFYEAR,
			year(FISCAL_CUR_YEAR) as FISCAL_YEAR,
			to_timestamp_ntz(V_DATE) as SQL_TIMESTAMP,
			'Y' as CURRENT_ROW_IND,
			to_date(current_timestamp) as EFFECTIVE_DATE,
			to_date('9999-12-31') as EXPIRA_DATE
			from table(generator(rowcount => 15000)) /*<< Set to generate 20 years. Modify rowcount to increase or decrease size*/
	)v;

insert into DATE_DIM
select -1,'9999-12-31','NA',-1,-1,-1,'NA','NA','NA','NA','NA','NA',-1,'9999-12-31',-1,'9999-12-31',-1,'NA','NA',-1,'NA',-1,'NA',-1,-1,-1,'NA',-1,'NA',-1,-1,'9999-12-31T00:00:00Z','/','9999-12-31','9999-12-31';

-- Create dimension table TIMEOFDAY_DIM
create or replace TABLE TIMEOFDAY_DIM (
	TIMEOFDAY_KEY NUMBER(38,0) NOT NULL,
	HOUR NUMBER(38,0) NOT NULL,
	AM_PM VARCHAR(2) NOT NULL,
	HOUR_24 NUMBER(2,0) NOT NULL
);

-- Populate data into TIMEOFDAY_DIM table
insert into timeofday_dim Select -1,-1,'NA',-1;
insert into timeofday_dim Select 0,0,'AM',0;
insert into timeofday_dim Select 1,1,'AM',1;
insert into timeofday_dim Select 2,2,'AM',2;
insert into timeofday_dim Select 3,3,'AM',3;
insert into timeofday_dim Select 4,4,'AM',4;
insert into timeofday_dim Select 5,5,'AM',5;
insert into timeofday_dim Select 6,6,'AM',6;
insert into timeofday_dim Select 7,7,'AM',7;
insert into timeofday_dim Select 8,8,'AM',8;
insert into timeofday_dim Select 9,9,'AM',9;
insert into timeofday_dim Select 10,10,'AM',10;
insert into timeofday_dim Select 11,11,'AM',11;
insert into timeofday_dim Select 12,12,'PM',12;
insert into timeofday_dim Select 13,1,'PM',13;
insert into timeofday_dim Select 14,2,'PM',14;
insert into timeofday_dim Select 15,3,'PM',15;
insert into timeofday_dim Select 16,4,'PM',16;
insert into timeofday_dim Select 17,5,'PM',17;
insert into timeofday_dim Select 18,6,'PM',18;
insert into timeofday_dim Select 19,7,'PM',19;
insert into timeofday_dim Select 20,8,'PM',20;
insert into timeofday_dim Select 21,9,'PM',21;
insert into timeofday_dim Select 22,10,'PM',22;
insert into timeofday_dim Select 23,11,'PM',23;

-- Create sequence for dimension table LOCATION_DIM
CREATE OR REPLACE SEQUENCE LOC_DIM;

-- Create dimension table LOCATION_DIM
CREATE OR REPLACE TABLE LOCATION_DIM (
    LOCATION_KEY                NUMBER(9) UNIQUE,
    ZIPCODE                 NUMBER(5,0) NOT NULL,
    BOROUGH                 VARCHAR(50) NOT NULL,
    LATITUDE                NUMBER(38,6) NOT NULL,
    LONGITUDE              NUMBER(38,6) NOT NULL,
    ON_STREET_NAME          VARCHAR(100) NOT NULL DEFAULT 'NA',
    OFF_STREET_NAME         VARCHAR(100) NOT NULL DEFAULT 'NA',
    CROSS_STREET_NAME       VARCHAR(100) NOT NULL DEFAULT 'NA'
);

-- Populate data into LOCATION_DIM table
INSERT INTO LOCATION_DIM (
    LOCATION_KEY,
    ZIPCODE,
    BOROUGH,
    LATITUDE,
    LONGITUDE,
    ON_STREET_NAME,
    OFF_STREET_NAME,
    CROSS_STREET_NAME
)
SELECT
    LOC_DIM.NEXTVAL as LOCATION_KEY,
    t.ZIP_CODE,
    t.BOROUGH,
    t.LATITUDE,
    t.LONGITUDE,
    t.ON_STREET_NAME,
    t.OFF_STREET_NAME,
    t.CROSS_STREET_NAME
FROM (
    SELECT 
        DISTINCT LATITUDE, 
        LONGITUDE, 
        ZIP_CODE, 
        BOROUGH, 
        ON_STREET_NAME, 
        OFF_STREET_NAME, 
        CROSS_STREET_NAME
    FROM NY_CRASHES
) t;

-- Create sequence for dimension table CASUALTY_DIM
CREATE OR REPLACE SEQUENCE CAS_DIM;

-- Create dimension table CASUALTY_DIM
CREATE OR REPLACE TABLE CASUALTY_DIM (
    CASUALTY_KEY number(7) UNIQUE NOT NULL,
    NUMBER_OF_PEDESTRIANS_INJURED number(2) NOT NULL,
    NUMBER_OF_PEDESTRIANS_KILLED number(2) NOT NULL,
    NUMBER_OF_CYCLIST_INJURED number(2) NOT NULL,
    NUMBER_OF_CYCLIST_KILLED number(2) NOT NULL,
    NUMBER_OF_MOTORIST_INJURED number(2) NOT NULL,
    NUMBER_OF_MOTORIST_KILLED number(2) NOT NULL
);

-- Populate data into CASUALTY_DIM table
INSERT INTO CASUALTY_DIM (
    CASUALTY_KEY,
    NUMBER_OF_PEDESTRIANS_INJURED,
    NUMBER_OF_PEDESTRIANS_KILLED,
    NUMBER_OF_CYCLIST_INJURED,
    NUMBER_OF_CYCLIST_KILLED,
    NUMBER_OF_MOTORIST_INJURED,
    NUMBER_OF_MOTORIST_KILLED
)
SELECT
    CAS_DIM.nextval AS CASUALTY_KEY,
    t.NUM_PEDEST_INJURED,
    t.NUM_PEDEST_KILLED,
    t.NUM_CYCL_INJURED,
    t.NUM_CYCL_KILLED,
    t.NUM_MOTOR_INJURED,
    t.NUM_MOTOR_KILLED
FROM 
(
SELECT DISTINCT NUM_PEDEST_INJURED,
    NUM_PEDEST_KILLED,
    NUM_CYCL_INJURED,
    NUM_CYCL_KILLED,
    NUM_MOTOR_INJURED,
    NUM_MOTOR_KILLED
    FROM NY_CRASHES
) t;

-- Create sequence for dimension table CONT_FACTOR_DIM
CREATE OR REPLACE SEQUENCE CF_DIM;

-- Create dimension table CONT_FACTOR_DIM
CREATE OR REPLACE TABLE CONT_FACTOR_DIM (
CONT_FACTOR_KEY NUMBER(3,0) UNIQUE,
CONT_FACTOR VARCHAR(100) NOT NULL
);

-- Populate data into CONT_FACTOR_DIM table
INSERT INTO CONT_FACTOR_DIM (
    CONT_FACTOR_KEY,
    CONT_FACTOR
)
SELECT
    CF_DIM.nextval AS CONT_FACTOR_KEY,
    t.CF_VEH
FROM (
    SELECT CONT_FACTOR_VEH_1 AS CF_VEH FROM NY_CRASHES
    UNION
    SELECT CONT_FACTOR_VEH_2 FROM NY_CRASHES
    UNION
    SELECT CONT_FACTOR_VEH_3 FROM NY_CRASHES
    UNION
    SELECT CONT_FACTOR_VEH_4 FROM NY_CRASHES
    UNION
    SELECT CONT_FACTOR_VEH_5 FROM NY_CRASHES
) t;

-- Create sequence for dimension table VEH_TYPE_DIM
CREATE OR REPLACE SEQUENCE VT_DIM;

-- Create dimension table VEH_TYPE_DIM
CREATE OR REPLACE TABLE VEH_TYPE_DIM (
VEH_TYPE_KEY NUMBER(3,0) NOT NULL,
VEH_TYPE VARCHAR(100) NOT NULL
);

-- Populate data into VEH_TYPE_DIM table
INSERT INTO VEH_TYPE_DIM (
    VEH_TYPE_KEY,
    VEH_TYPE
)
SELECT
    VT_DIM.nextval AS VEH_TYPE_KEY,
    t.VT_CODE
FROM (SELECT VEH_TYPE_CODE_1 AS VT_CODE FROM NY_CRASHES
    UNION
    SELECT VEH_TYPE_CODE_2 FROM NY_CRASHES
    UNION
    SELECT VEH_TYPE_CODE_3 FROM NY_CRASHES
    UNION
    SELECT VEH_TYPE_CODE_4 FROM NY_CRASHES
    UNION
    SELECT VEH_TYPE_CODE_5 FROM NY_CRASHES) t;

-- Create fact table CRASH_FACT
CREATE OR REPLACE TABLE CRASH_FACT (
COLLISION_ID NUMBER(38,0) UNIQUE NOT NULL,
DATE_KEY NUMBER(9,0) NOT NULL,
TIMEOFDAY_KEY NUMBER(38,0) NOT NULL,
LOCATION_KEY NUMBER(38,0) NOT NULL,
CONT_FACTOR_KEY_1 NUMBER(3,0) NOT NULL,
CONT_FACTOR_KEY_2 NUMBER(3,0) NOT NULL,
CONT_FACTOR_KEY_3 NUMBER(3,0) NOT NULL,
CONT_FACTOR_KEY_4 NUMBER(3,0) NOT NULL,
CONT_FACTOR_KEY_5 NUMBER(3,0) NOT NULL,
VEH_TYPE_KEY_1 NUMBER(3,0) NOT NULL,
VEH_TYPE_KEY_2 NUMBER(3,0) NOT NULL,
VEH_TYPE_KEY_3 NUMBER(3,0) NOT NULL, 
VEH_TYPE_KEY_4 NUMBER(3,0) NOT NULL,
VEH_TYPE_KEY_5 NUMBER(3,0) NOT NULL,
CASUALTY_KEY NUMBER(7) NOT NULL,
NUM_PERSONS_INJURED NUMBER(2,0) NOT NULL,
NUM_PERSONS_KILLED NUMBER(2,0) NOT NULL
);

-- Populate data into CRASH_FACT table
INSERT INTO CRASH_FACT (
COLLISION_ID,
DATE_KEY, 
TIMEOFDAY_KEY,
LOCATION_KEY, 
CONT_FACTOR_KEY_1, 
CONT_FACTOR_KEY_2, 
CONT_FACTOR_KEY_3,
CONT_FACTOR_KEY_4,
CONT_FACTOR_KEY_5,
VEH_TYPE_KEY_1,
VEH_TYPE_KEY_2,
VEH_TYPE_KEY_3, 
VEH_TYPE_KEY_4,
VEH_TYPE_KEY_5,
CASUALTY_KEY,
NUM_PERSONS_INJURED,
NUM_PERSONS_KILLED
)

SELECT
ny.collision_id,
d.date_key,
t.timeofday_key,
l.location_key,
c1.cont_factor_key,
c2.cont_factor_key,
c3.cont_factor_key,
c4.cont_factor_key,
c5.cont_factor_key,
v1.veh_type_key,
v2.veh_type_key,
v3.veh_type_key,
v4.veh_type_key,
v5.veh_type_key,
c.casualty_key,
ny.num_persons_injured,
ny.num_persons_killed
FROM NY_CRASHES ny
JOIN DATE_DIM d ON ny.crash_date = d.date
JOIN TIMEOFDAY_DIM t ON HOUR(ny.crash_time) = t.hour_24
JOIN LOCATION_DIM l ON ny.latitude = l.latitude 
AND ny.longitude = l.longitude
AND ny.zip_code = l.zipcode
AND ny.borough = l.borough
AND ny.on_street_name = l.on_street_name
AND ny.off_street_name = l.off_street_name
AND ny.cross_street_name = l.cross_street_name
JOIN CONT_FACTOR_DIM c1 ON ny.cont_factor_veh_1 = c1.cont_factor
JOIN CONT_FACTOR_DIM c2 ON ny.cont_factor_veh_2 = c2.cont_factor
JOIN CONT_FACTOR_DIM c3 ON ny.cont_factor_veh_3 = c3.cont_factor
JOIN CONT_FACTOR_DIM c4 ON ny.cont_factor_veh_4 = c4.cont_factor
JOIN CONT_FACTOR_DIM c5 ON ny.cont_factor_veh_5 = c5.cont_factor
JOIN VEH_TYPE_DIM v1 ON ny.veh_type_code_1 = v1.veh_type
JOIN VEH_TYPE_DIM v2 ON ny.veh_type_code_2 = v2.veh_type
JOIN VEH_TYPE_DIM v3 ON ny.veh_type_code_3 = v3.veh_type
JOIN VEH_TYPE_DIM v4 ON ny.veh_type_code_4 = v4.veh_type
JOIN VEH_TYPE_DIM v5 ON ny.veh_type_code_5 = v5.veh_type
JOIN CASUALTY_DIM c ON ny.num_pedest_injured = c.number_of_pedestrians_injured 
AND ny.num_pedest_killed = c.number_of_pedestrians_killed
AND ny.num_cycl_injured = c.number_of_cyclist_injured
AND ny.num_cycl_killed = c.number_of_cyclist_killed
AND ny.num_motor_injured = c.number_of_motorist_injured
AND ny.num_motor_killed = c.number_of_motorist_killed;