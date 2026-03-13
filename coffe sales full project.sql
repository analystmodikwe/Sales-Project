SELECT * 
FROM coffe_sales.index_1;

SELECT * 
FROM coffe_sales.index_2 ;

-- creating a like table to avoid using original data or table --

create table index_staging1 
like index_1;

create table index_staging2 
like index_2;

-- copying data from original tables --

insert into index_staging1 
select*
from index_1;

insert into index_staging2
select*
from index_2;

-- checking for duplicates --

SELECT * 
FROM coffe_sales.index_1;

select*,
row_number() over(
partition by `date`, `datetime`, "cash_type", "card", "money", "coffee_name") as row_num
from index_staging1;

with duplicates_cte as
(select*,
row_number() over(
partition by `date`, `datetime`, "cash_type", "card", "money", "coffee_name") as row_num
from index_staging1)

select*
from duplicates_cte
where row_num >1;

select*
from index_staging1
where card = 'ANON-0000-0000-0002';


SELECT * 
FROM coffe_sales.index_2;

select*,
row_number() over(
partition by `date`, `datetime`, "cash_type", "money", "coffee_name") as row_num
from index_staging2;

with duplicates_cte as
(select*,
row_number() over(
partition by `date`, `datetime`, "cash_type", "money", "coffee_name") as row_num
from index_staging2)

select*
from duplicates_cte
where row_num >1;

-- deleting duplicates by creating another table/s to delete from (index_staging1_1 and index_staging2_2)  --

CREATE TABLE `index_staging1_1` (
  `date` text,
  `datetime` text,
  `cash_type` text,
  `card` text,
  `money` double DEFAULT NULL,
  `coffee_name` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM index_staging1_1;

insert into index_staging1_1
select*,
row_number() over(
partition by `date`, `datetime`, "cash_type", "card", "money", "coffee_name") as row_num
from index_staging1;

SELECT * 
FROM index_staging1_1
where row_num >1;

delete
FROM index_staging1_1
where row_num >1;


SELECT * 
FROM index_staging2;

CREATE TABLE `index_staging2_2` (
  `date` text,
  `datetime` text,
  `cash_type` text,
  `money` double DEFAULT NULL,
  `coffee_name` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into index_staging2_2
select*,
row_number() over(
partition by `date`, `datetime`, "cash_type", "money", "coffee_name") as row_num
from index_staging2;

SELECT * 
FROM index_staging2_2
where row_num >1;

delete 
FROM index_staging2_2
where row_num >1;


-- standerdizing the data --

SELECT * 
FROM index_staging1_1;
SELECT distinct coffee_name
FROM index_staging1_1
order by 1;

update index_staging1_1
set `date` = str_to_date(`date`, "%m/%d/%Y");

alter table index_staging1_1
modify column `date` date;

update index_staging1_1
set `datetime` = str_to_date (`datetime`, "%m/%d/%Y %H:%i:%s"); 

alter table index_staging1_1
modify column `datetime` datetime;

update index_staging1_1
set money =ROUND(money, 2);

alter table index_staging1_1
modify column money DECIMAL(10,2);


SELECT * 
FROM index_staging2_2;

update index_staging2_2
set `date` = str_to_date (`date`, "%m/%d/%Y");

alter table index_staging2_2
modify column `date` DATE;

update index_staging2_2
set `datetime` = str_to_date (`datetime`, "%m/%d/%Y %H:%i:%s");

alter table index_staging2_2
modify column `datetime` datetime;


update index_staging2_2
set money =ROUND (money, 2);

alter table index_staging2_2
modify column money DECIMAL(10,2); 


-- Removing column --
SELECT * 
FROM index_staging1_1;

alter table index_staging1_1
drop column row_num;

SELECT * 
FROM index_staging2_2;

alter table index_staging2_2
drop column row_num;


-- Using inner joins to combine two tables together to compare the two tables --

SELECT * 
FROM index_staging1_1 
inner join index_staging2_2 
	on coffe_sales.index_staging1_1.date= coffe_sales.index_staging2_2.date;


