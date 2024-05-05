-- EDA

-- allgemeine Erkundung des Datensatzes

SELECT *
FROM layoffs_staging2;

SELECT max(total_laid_off), 
		max(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, 
		SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY sum_laid_off DESC;

SELECT min(date), 
		max(date)
FROM layoffs_staging2;

-- Ermittlung der Anzahl an Kündigungen nach diversen Kategorien

SELECT industry, 
		SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY sum_laid_off DESC;

SELECT country, 
		SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY country
ORDER BY sum_laid_off DESC;

SELECT YEAR(date) AS year, 
		sum(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY year
ORDER BY year DESC;

SELECT stage, 
		SUM(total_laid_off) AS sum_laid_off 
FROM layoffs_staging2
GROUP BY stage
ORDER BY sum_laid_off DESC;

SELECT company, 
		AVG(total_laid_off) AS avg_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY avg_laid_off DESC;

-- Summe der Kündigungen pro Monat

SELECT SUBSTRING(date, 1, 7) AS month,
		SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(date, 1, 7) IS NOT NULL
GROUP BY month
ORDER BY month ASC;

-- Gegenüberstellung der Summe an Kündigungen pro Monat 
-- zu aufsummierter Anzahl an Kündigungen

WITH Rolling_Total AS
(
SELECT SUBSTRING(date, 1, 7) AS month,
		SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(date, 1, 7) IS NOT NULL
GROUP BY month
ORDER BY month ASC
)
SELECT month, 
		total_off,
		SUM(total_off) OVER(ORDER BY month) AS rolling_total
FROM Rolling_Total;

-- Summe der Kündigungen gruppiert nach Firma und Jahr

SELECT company, 
		YEAR(date) AS year,
        SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY company, year
ORDER BY sum_laid_off DESC;

-- Ermitteln der jährlichen Top 5 Firmen nach Anzahl Entlassungen
-- mithilfe von 2 CTE´s

WITH Company_Year (company, years, sum_laid_off) AS
(
SELECT company, 
		YEAR(date),
		sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(date) -- CTE zum gruppieren der Kündigungen nach Firma und Jahr
),
Company_Year_Rank AS
(
SELECT *,
		DENSE_RANK() OVER(PARTITION BY years 
							ORDER BY sum_laid_off DESC+
						) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL -- CTE zum erstellen einer jährlichen Rangordnung
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;

-- Ermitteln der jährlichen Top 3 Sektoren nach Anzahl Entlassungen
-- mithilfe von 2 CTE´s

WITH Sector_Year (sector, year, sum_laid_off) AS
(
SELECT industry, 
		YEAR(date),
		sum(total_laid_off)
FROM layoffs_staging2
GROUP BY industry, YEAR(date) -- CTE zum gruppieren der Kündigungen nach Sektor und Jahr
),
Sector_Year_Rank AS
(
SELECT *,
		DENSE_RANK() OVER(PARTITION BY year 
							ORDER BY sum_laid_off DESC
						) AS Ranking
FROM Sector_Year
WHERE year IS NOT NULL -- CTE zum erstellen einer jährlichen Rangordnung
)
SELECT *
FROM Sector_Year_Rank
WHERE Ranking <= 3;

-- Anzahl der 100%igen Entlassungen nach Jahr und Länder

SELECT YEAR(date) AS year,
        country,
        Count(country) AS amount
FROM layoffs_staging2
WHERE percentage_laid_off = 1
GROUP BY year, country
ORDER BY year, amount ASC;

-- Gesamtinvestitionen nach Jahr

SELECT YEAR(date) AS year,
		SUM(funds_raised_millions) AS total_sum_of_investments_in_millions
FROM layoffs_staging2
WHERE funds_raised_millions IS NOT NULL
GROUP BY year
ORDER BY year;

-- TOP 5 Areas nach Anteil an jährlichen Investitionen

WITH Yearly_Sum_Locations AS
(
SELECT location,
		YEAR(date) AS year,
		SUM(funds_raised_millions) AS sum_of_funds
FROM layoffs_staging2
WHERE funds_raised_millions IS NOT NULL
GROUP BY year, location
),
Yearly_Sum_Funds AS
(
SELECT YEAR(date) AS year,
		SUM(funds_raised_millions) AS sum_of_funds
FROM layoffs_staging2 
GROUP BY year
),
Ranking_Sum_Funds AS
(
SELECT ysl.year,
		ysl.location,
		ysl.sum_of_funds AS sum_of_funds,
        (ysl.sum_of_funds / ysf.sum_of_funds * 100) AS pct_share_of_total_funds,
        DENSE_RANK () OVER(PARTITION BY ysl.year ORDER BY ysl.sum_of_funds DESC) AS Ranking
FROM Yearly_Sum_Locations AS ysl
JOIN Yearly_Sum_Funds AS ysf
	ON ysl.year = ysf.year
ORDER BY ysl.year
)
SELECT *
FROM Ranking_Sum_Funds
WHERE Ranking <= 5
ORDER BY year;

-- Betrachtung der Entwicklung der Arbeitslosenzahlen nach einzelnen Monaten und als gleitender Durchschnitt über 3 Monate

-- Erstellen eines Views mit diversen Kennzahlen
CREATE OR REPLACE VIEW development_of_discharge_figures
AS
(SELECT MONTH(date) AS month,
		YEAR(date) AS year,
        -- Anzahl der Entlassungen pro Monat
        SUM(total_laid_off) AS number_of_redundancies,
        -- absolute Entwicklung der Entlassungen pro Monat 
        SUM(total_laid_off) - LAG(SUM(total_laid_off), 1, NULL) OVER(ORDER BY YEAR(date), MONTH(date)) AS monthly_absolute_development_of_discharge_figures,
        -- relative Entwicklung der Entlassungen pro Monat
        ROUND((SUM(total_laid_off) - LAG(SUM(total_laid_off) , 1, NULL) OVER(ORDER BY YEAR(date), MONTH(date))) / 
				LAG(SUM(total_laid_off), 1, NULL) OVER(ORDER BY YEAR(date), MONTH(date)) * 100,
                2) AS monthly_percentage_development_of_discharge_figures,
		-- gleitender Durchschnitt der Entlassungen
        SUM(SUM(total_laid_off)) OVER three_month_window AS 3_month_moving_average_of_number_of_redundancies
FROM layoffs_staging2
WHERE date IS NOT NULL
GROUP BY MONTH(date), YEAR(date)
WINDOW three_month_window AS (ORDER BY YEAR(date), MONTH(date)
								ROWS 2 PRECEDING)
ORDER BY year, month
);

-- Beispielauslesung der Entwicklung der Entlassungszahlen der letzten 6 Monate
SELECT CONCAT(month, '-', year) AS date,
		number_of_redundancies,
        3_month_moving_average_of_number_of_redundancies
FROM development_of_discharge_figures
ORDER BY year DESC, month DESC
LIMIT 6;
		
