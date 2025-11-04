# Exploratory Data Analysis (EDA)

select *
from world_layoffs.layoffs_staging2;

-- max total_laid_off and percentage_laid_off dalam satu hari
-- max total laid off 12.000
-- max percentage 1/100
select max(total_laid_off), MAX(percentage_laid_off)
from world_layoffs.layoffs_staging2;

-- select untuk ngeliat company yg presentase laid off nya 1 / 100 yg memiliki fund_raised_millionsnya paling besar
-- fund_raised_millions adalah total dana dari investor
select *
from world_layoffs.layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- cari tau datasetnya mulai dikumpulin dari tahun berapa sampai berapa
-- start bulan 3 tahun 2020
-- end bulan 3 tahun 2023
select MIN(`date`), MAX(`date`)
from world_layoffs.layoffs_staging2;



-- cari tau total laid off per company
-- top 5
-- 1. Amazon 18.150
-- 2. Google 12.000
-- 3. Meta 11.000
-- 4. Salesforce 10.090
-- 5. Philips 10.000
select company, SUM(total_laid_off)
from world_layoffs.layoffs_staging2
group by company
order by 2 desc;


-- total laid off berdasarkan industry
-- top 5
-- consumer, retail, other, transportation, finance
select industry , SUM(total_laid_off)
from world_layoffs.layoffs_staging2
group by industry
order by 2 desc;

-- total laid off berdasarkan stage
-- top 5 
-- post-IPO, unknown, acquired, series C, series D
select stage, SUM(total_laid_off)
from world_layoffs.layoffs_staging2
group by stage 
order by 2 desc;


-- total laid off berdasarkan country
-- top 5
-- US, India, Netherlands, Sweden, Brazil
select country, SUM(total_laid_off)
from world_layoffs.layoffs_staging2
group by country 
order by 2 desc;


-- total laid off by datetime
-- by year --> top 2022 (160.661)
-- but tahun 2023 (125.677) juga hampir menyamai 2022 jumlahnya dalam waktu 3 bulan
select YEAR(`date`), SUM(total_laid_off)
from world_layoffs.layoffs_staging2
group by YEAR(`date`)
order by 1 desc;

-- by month --> top 2023-01 (84.714)
select SUBSTR(`date`, 1, 7) as month, SUM(total_laid_off)
from world_layoffs.layoffs_staging2
group by SUBSTR(`date`, 1, 7)
order by 2 desc;

-- rolling total per month with CTEs
with rolling_total as
(
	select SUBSTR(`date`, 1, 7) as `month` , SUM(total_laid_off) as total_off
	from world_layoffs.layoffs_staging2
	where SUBSTR(`date`, 1, 7) is not null
	group by `month`
	order by 1 asc
)
select *, SUM(total_off) over(order by `month`) as rolling_total
from rolling_total;


-- company ranking by year
select company, YEAR(`date`), SUM(total_laid_off)
from world_layoffs.layoffs_staging2
group by company, YEAR(`date`)
order by 3 desc;

with company_year(company, years, total_laid_off) as
(
	select company, YEAR(`date`), SUM(total_laid_off)
	from world_layoffs.layoffs_staging2
	group by company, YEAR(`date`)
), company_year_rank as 
(
	select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking
	from company_year
	where years is not null
	order by ranking asc, years asc
)
select *
from company_year_rank 
where ranking <= 5
order by years, ranking;


select company, avg(percentage_laid_off)
from world_layoffs.layoffs_staging2
group by company
order by 2 desc;


-- industry ranking by year
select industry, YEAR(`date`), SUM(total_laid_off)
from world_layoffs.layoffs_staging2
group by industry, YEAR(`date`)
order by 3 desc;

with industry_year(industry, years, total_laid_off) as
(
	select industry, YEAR(`date`), SUM(total_laid_off)
	from world_layoffs.layoffs_staging2
	group by industry, YEAR(`date`)
), industry_rank as
(
	select *, dense_rank() over (partition by years order by total_laid_off desc) as ranking
	from industry_year
	where industry is not null and years is not null
	order by ranking asc
)
select *
from industry_rank
where ranking <= 5
order by years asc, ranking asc;



-- rank industry in every country by years
select country, industry, YEAR(`date`) as years, SUM(total_laid_off) as total_laid_off
from world_layoffs.layoffs_staging2
group by country, industry, YEAR(`date`)
order by country asc, total_laid_off desc, years asc, total_laid_off desc;

with industry_in_country (country, industry, years, total_laid_off) as
(
	select country, industry, YEAR(`date`), SUM(total_laid_off)
	from world_layoffs.layoffs_staging2
	group by country, industry, YEAR(`date`)
), industry_in_country_rank as
(
	select *, dense_rank() over (partition by country, years order by total_laid_off desc) as ranking
	from industry_in_country
	where years is not null and industry is not null
)
select *
from industry_in_country_rank 
where ranking <= 3;


-- country rank by years
select country, YEAR(`date`) as years, SUM(total_laid_off) as total_laid_off
from world_layoffs.layoffs_staging2
group by country, years
order by country, years, total_laid_off desc;

with country_year (country, years, total_laid_off) as 
(
	select country, YEAR(`date`), SUM(total_laid_off)
	from world_layoffs.layoffs_staging2
	group by country, YEAR(`date`)
), country_rank as 
(
	select *, dense_rank() over (partition by years order by total_laid_off desc) as ranking
	from country_year
	where years is not null and total_laid_off is not null
)
select *
from country_rank
where ranking <= 5;




-- belum baru coba coba
select company, sum(total_laid_off), avg(percentage_laid_off)
from world_layoffs.layoffs_staging2
group by company
order by 3 desc, 2 desc;

with total_emp as
(
select *, round((total_laid_off / percentage_laid_off)) as total_emp
from world_layoffs.layoffs_staging2
)
select *
from total_emp
where total_emp = 400000;










