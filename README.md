-- Data Cleaning


SELECT * 
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove any Columns



CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging; 

INSERT layoffs_staging
SELECT*
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
FROM layoffs_staging;

WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, 'date', stage, 
country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, 'date', stage, 
country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE  
FROM duplicate_cte
WHERE row_num > 1;

-- 1. Drop staging2 table if it already exists
DROP TABLE IF EXISTS layoffs;

CREATE TABLE layoffs (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT,
  percentage_laid_off TEXT,
  `date` TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions INT
);

-- 3. Insert data from original (clean) source with row numbers for deduplication
INSERT INTO layoffs_staging2 (
  company, location, industry, total_laid_off,
  percentage_laid_off, `date`, stage, country,
  funds_raised_millions, row_num
)
SELECT 
  company, location, industry, total_laid_off,
  percentage_laid_off, `date`, stage, country,
  funds_raised_millions,
  ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off,
                 percentage_laid_off, `date`, stage, country, funds_raised_millions
  ) AS row_num
FROM layoffs_staging;

-- 4. Remove duplicates (keep only row_num = 1)
DELETE FROM layoffs_staging2
WHERE row_num > 1;


-- ✅ From here on, you can proceed with your data cleaning steps:
-- Trim company names
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Clean up US country names
UPDATE layoffs_staging2 
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Convert date to proper format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Alter column type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT* 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Set blanks to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally5';

-- Fill in missing industries from other rows of same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
  AND t2.industry IS NOT NULL;
SELECT *
FROM layoffs_staging2;


SELECT* 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

SELECT* 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT DISTINCT industry FROM layoffs ORDER BY 1;
SELECT company, industry FROM layoffs WHERE industry = 'Crypto';

UPDATE layoffs
SET industry = 'Transportation'
WHERE company = 'Uber';

UPDATE layoffs
SET industry = 'Retail'
WHERE company = 'Amazon';

UPDATE layoffs
SET industry = 'Finance'
WHERE company = 'Goldman Sachs';
