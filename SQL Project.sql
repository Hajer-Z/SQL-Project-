-- Data Cleaning
select * from layoffs;

-- CREATING A STAGING TABLE TO AVOID DELETING COLUMNS FROM RAW DATA
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT * FROM layoffs;

select * from layoffs_staging;

-- 1- Identify Duplicates
WITH duplicate_cte AS
(
SELECT *, 
row_number() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, date , stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
select * from duplicate_cte
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
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from layoffs_staging2;

-- Adding row_num Column
insert into layoffs_staging2
SELECT *, 
row_number() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date` , stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Deleting Dupkicates
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

select * 
from layoffs_staging2
where row_num > 1;

-- Standardizing Data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct(industry)
from layoffs_staging2
order by industry;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';

select distinct(country)
FROM layoffs_staging2
where country like 'united states%';

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United states%';

SELECT `date`, str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

-- Updating Date column datatype
alter table layoffs_staging2
modify column `date` date;

-- Null Values
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';

update layoffs_staging2
set industry = null
where industry = '';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

-- Filling Null values from other Rows with available data

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select *
from layoffs_staging2
where company like 'bally%';

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- Deleting Records with both null values that can not be filled
Delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

select *
from layoffs_staging2;

-- Data Exploration

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

-- Checking companies that laid all employees (shutdown)
select *
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- Companies that laid highest numbers
select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by sum(total_laid_off) desc;

select min(`date`), max(`date`)
from layoffs_staging2;

-- Industries that laid highest numbers
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by sum(total_laid_off) desc;

-- Companies that laid highest numbers
select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by sum(total_laid_off) desc;

-- Comparing layoffs among years
select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by year(`date`);

-- The stage (of company) with higher layoffs
select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

-- I am interested in the progression of layoffs, so I will calculate the rolling sum
with rolling_total as
(
select substring(`date`, 1, 7) as `Month`, sum(total_laid_off) as `sum`
from layoffs_staging2
where `date` is not null
group by `Month`
order by `Month`
)
select `Month`, `sum`,
sum(`sum`) over(order by `Month`)
from rolling_total;

-- Top 5 Companies with the Highest Layoffs by Year
with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
), company_year_rank as
(
select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
)
select * 
from company_year_rank
where ranking <= 5;

-- Top 5 Industries with the Highest Layoffs by Year
with industry_year (industry, years, total_laid_off) as
(
select industry, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by industry, year(`date`)
), industry_year_rank as
(
select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking
from industry_year
where years is not null
)
select * 
from industry_year_rank
where ranking <= 5;

-- Top 5 Companies with the Highest Layoffs by Year
with country_year (country, years, total_laid_off) as
(
select country, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by country, year(`date`)
), country_year_rank as
(
select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking
from country_year
where years is not null
)
select * 
from country_year_rank
where ranking <= 5;