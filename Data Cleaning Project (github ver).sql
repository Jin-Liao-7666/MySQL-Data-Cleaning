-- Data Cleaning --
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Populate Null Values or Blank Values when necessary
-- 4. Remove unnecessary Columns

SELECT *
FROM layoffs;

-- Create a copy table called layoffs_staging, never make changes on the original table, always make a copy
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Removing Duplicates --


-- Identify Duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Show a table of data with duplicates
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Double Checking if the data from previous line are actually duplicates
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

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

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Essentially adds another row in this table copy that counts up by repetition. 
-- 	If the dataset contains B B, then the rows corresponding to it would be 1 2
INSERT INTO layoffs_staging2 (
  company, location, industry, total_laid_off, percentage_laid_off,
  `date`, stage, country, funds_raised_millions, row_num
)
SELECT company, location, industry, total_laid_off, percentage_laid_off,
  `date`, stage, country, funds_raised_millions,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Remember to turn off Safe Mode on mySQL workbench for this DELETE code to work
-- Deletes the rows where the from the previous table where row_num > 1, so in the example above
-- 	the second B would be deleted
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- Standardizing Data --

-- TRIM takes off whitespace at the end
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- In the table, Crypto, Crypto Currency, and CryptoCurrency could all be one category
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'; 

-- Update data with industry starting with 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Double Check that Crypto has been standardized
SELECT DISTINCT industry
FROM layoffs_staging2;

-- Countries are also not standardized, 'United States' and 'United States.' can be put as one category
SELECT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Double Check that United States has been standardized
SELECT DISTINCT country
FROM layoffs_staging2;

-- Standardizing date format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Changing data type of date from text to date
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Null Values --

-- Trying to populate categories where possible:
-- 	where industry is '' or null:
-- 		if the company has multiple entries and some have null or '' but some have industries, fill it in as those industries

UPDATE layoffs_staging2
SET industry = NULL
WHERE TRIM(industry) = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR TRIM(industry) = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL 
OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Updates the '' or null industries: if the company has multiple entries and some have null or '' but some have industries, fill it in as those industries
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Double Checking if industries have been populated, ex. Airbnb
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Edge Case, Bally's Interactive is still null because there is only one entry

-- Delete data with both total_laid_off and percentage_laid_off to be NULL, be careful when deleting data
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Double Checking Data
SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;