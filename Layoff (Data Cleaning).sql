use world_layoffs1;

select * from layoffs;

# Remove Duplicates
#Standardize the data
# Remove null or blanks
# Remove columns

create table layoff_staging
like layoffs;

select * from layoff_staging;

Insert layoff_staging
select * from layoffs;

#REMOVE DUPLICATES

select * ,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoff_staging;

with dupLicate_cte AS
(
select * ,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoff_staging
)
select *
from duplicate_cte
where row_num>1;

select * from layoff_staging
where company = 'Casper';

set sql_safe_updates=0;

with dupLicate_cte AS
(
select * ,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoff_staging
)
Delete 
from duplicate_cte
where row_num>1;

SELECT * from layoff_staging2;

Insert into layoff_staging2
select * ,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoff_staging;

SELECT * from layoff_staging2
where row_num>1;

DELETE from layoff_staging2
where row_num>1;

select * from layoff_staging2
where company = 'Casper';



CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
  
  
  # Standardize the Data
  
  select distinct(company)
  from layoff_staging2;
  
  select company, TRIM(company)
  from layoff_staging2;
  
  set sql_safe_updates=0;
  
  UPDATE layoff_staging2
  set company = trim(company);

select * from layoff_staging2
where industry like 'Crypto%';

UPDATE layoff_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry
from layoff_staging2;

select distinct country
from layoff_staging2
order by 1;

select country
from layoff_staging2
where country like 'United States%'
order by 1;

select distinct country, trim(trailing '.' from country)
from layoff_staging2
order by 1;

update layoff_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select distinct country
from layoff_staging2
order by 1;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoff_staging2;

UPDATE layoff_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select * from layoff_staging2;

alter table layoff_staging2
modify column `date` Date;

select * from layoff_staging2
where country = 'United States';

#Removing Null Values

select * from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoff_staging2
where industry is null
or industry = '' ;

select * from layoff_staging2
where company = 'Airbnb';

update layoff_staging2
set industry = Null
where industry = '' ;

select t1.industry,t2.industry
from layoff_staging2 as t1
join layoff_staging2 as t2
      on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoff_staging2 t1
join layoff_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select * from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

#REMOVE COLUMN
select * from layoff_staging2;

alter table layoff_staging2
drop row_num;


