-- Data Cleaning
-- Activating Database and viewing the table
USE world_layoffs;
SELECT * FROM layoffs;


-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove unnecessary columns


-- Creating a copy of the raw data
create table layoffs_staging like layoffs;

insert layoffs_staging
select * from layoffs;

select * from layoffs_staging;

-- 1. Checking for duplicate records using ROW_NUMBER & OVER functions as there is no unique key in the dataset.
select *,
ROW_NUMBER() over(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
ROW_NUMBER() over(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num >1;

select *
from layoffs_staging
WHERE company='Casper';

-- Adding 'row_num' column to a new table 'layoffs_staging2' as the duplicate rows could not be deleted in the CTE
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
-- Inserting data into 'layoffs_staging2' table and adding 'row_num' column to DELETE duplicate records.
select * from layoffs_staging2
WHERE row_num >1;

insert into layoffs_staging2
select *,
ROW_NUMBER() over(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

delete from layoffs_staging2
WHERE row_num >1;

select * from layoffs_staging2;


-- 2. Standardize the data
-- Removing white space in front of the string
SELECT company, trim(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Clubbing 3 different records from the same industry as one record (cryptocurrency, crypto currency to crypto)
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Removing trailing '.' from a duplicate record (United States, United States.) to a single unique record (United States)
SELECT DISTINCT country, trim(trailing '.' from country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = trim(trailing '.' from country)
WHERE country LIKE 'United States%';

-- Converting text to date format
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date` FROM layoffs_staging2;

-- Changing data type to date 
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Eliminating null or blank values
-- Replacing blank values with null
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Cross-checking company within the same industry with different records 
-- (2 records of Airbnb in Travel industry and other record as null)
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Performing a self-join to fetch the 'industry' column which has null values for a company with the same name
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Replacing the null values
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Checking for null values in 'total_laid_off' & 'percentage_laid_off' columns and deleting the same
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 4. Removing column that is not required
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final clean data
SELECT * FROM layoffs_staging2;









