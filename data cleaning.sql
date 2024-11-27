-- Data cleaning

-- 1. Remove duplicates
-- 2. standardize data (same format and look for mispellings)
-- 3. remove null values or blank values and maybe populate it
-- 4. remove any columns that is irrelivent

CREATE TABLE layoffs_staging LIKE layoffs;

-- new table for cleaning
-- want raw data available if mistake was made with the changes in the staging one

with duplicate_cte as
(
SELECT *, 
ROW_NUMBER() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
# DELETE (cannot do this)
SELECT *
from duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_staging2` (
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

INSERT INTO layoffs_staging2
select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

# always recommends select statement to see what deleting

delete
from layoffs_staging2
where row_num > 1;

select * from layoffs_staging2
where row_num > 1;
# this query should return nothing after the above delete

# standardizing data (finding issues with data)
update layoffs_staging2
set company = trim(company);

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry
from layoffs_staging2;
# after the last update query all 'Crypto%' will be 'Crypto'

update layoffs_staging2
set country = TRIM(TRAILING '.' FROM country)
where country like 'United States%';

Select DISTINCT country
from layoffs_staging2;

Update layoffs_staging2
set `date` = STR_TO_DATE(`date`, '%m/%d/%Y'); #YYYY-MM-DD
# this will only accept MM/DD/YYYY format

Alter Table layoffs_staging2
modify column `date` DATE;
# was text now Date

select *
from layoffs_staging2
where total_laid_off is null # not = null, has to be Is null
and percentage_laid_off is null;

# want to standardize blanks and nulls first
update layoffs_staging2
set industry = null
where industry = '';

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and (t2.industry is not null and t2.industry != '');
# set t1.industry = t2.industry
# where (t1.industry is null OR t1.industry = '') AND t2.industry is NOT NULL;

select * from layoffs_staging2 where industry is null;

# we can calculate and populate data like percentage_laid_off or something if we have other data to calculate with like total_laid_off and total employees

# if there are a lot of nulls in key places, cannot trust data like in layoffs (what is the point)

delete from layoffs_staging2 where total_laid_off is null AND percentage_laid_off is null;

#when we are done with row_num column for our purpose, it will just be confusing
alter table layoffs_staging2 drop column row_num;

