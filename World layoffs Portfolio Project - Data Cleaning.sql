-- Data Cleaning

SELECT *
FROM layoffs;

-- Kopie der Originaldaten anlegen
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- 1. Duplikate entfernen
-- zusätzliche Spalte row_num anlegen zur Untersuchung nach Duplikaten
SELECT *,
ROW_NUMBER() OVER(
				PARTITION BY company,
							industry,
							total_laid_off,
							percentage_laid_off,
							date
				) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
				PARTITION BY company, 
							location, 
                            industry, 
                            total_laid_off, 
                            percentage_laid_off, 
                            date, 
                            stage, 
                            country, 
                            funds_raised_millions
				) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- eine weitere Tabelle zum Bearbeiten anlegen
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
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
				PARTITION BY company, 
							location, 
                            industry, 
                            total_laid_off, 
                            percentage_laid_off, 
                            date, 
                            stage, 
                            country, 
                            funds_raised_millions
				) AS row_num
FROM layoffs_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- 2. Standardisierung
-- Leerzeichen vor den Firmennamen entfernen
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Untersuchung der Spalte industry
SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Unterschiedliche Bezeichnung für identische Branchen angleichen
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Untersuchung Spalte country
SELECT DISTINCT country
FROM layoffs_staging2;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- Entfernen von Satzzeichen 
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Umwandlung der Datumsspalte in DateTime
SELECT date,
STR_TO_DATE(date, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date = STR_TO_DATE(date, '%m/%d/%Y');

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;

-- 3. Untersuchung nach Leerfeldern oder Nullwerten
-- Untersuchung der Tabelle wenn Spalten total_laid_off und percentage_laid_off den Wert Null haben
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Spalte industry nach Null´s untersuchen
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Untersuchung anhand der Firma Airbnb
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Untersuchung ob es für alle Firmen bekannte Werte in company gibt
SELECT t1.company, t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- leere Felder in industry auf Null setzen
UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

-- Nullwerte in industry ersetzen, wenn Wert bekannt
UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Kontrolle
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Kontrolle der Firma Bally´s Interactive
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2;

-- 4. Löschen von Zeilen und Spalten
-- Löschen der Zeilen ohne Mehrwert
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Spalte row_num entfernen
ALTER TABLE layoffs_staging2
DROP row_num;
