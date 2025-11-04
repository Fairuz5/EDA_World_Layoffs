select *
from world_layoffs.layoffs;

# CLEANING DATA
-- 1. HAPUS DUPLICATE DATA
-- 2. STANDARISASI DATA
-- 3. OLAH DATA NULL ATAU BLANK
-- 4. REMOVE DATA YANG TIDAK MEMBERIKAN INFORMASI

#
-- 0 buat staging bukan di raw data
create table world_layoffs.layoffs_staging1
like world_layoffs.layoffs;

select *
from world_layoffs.layoffs_staging1;

insert into world_layoffs.layoffs_staging1 
select * from world_layoffs.layoffs; 


-- 1. HAPUS DUPLICATE DATA
-- KARENA NGGAK ADA PRIMARY KEY YG BUAT UNIQUE, KITA BUAT ROW NUMBER DENGAN PARTITION SEMUA DATA/KOLOM

select *, 
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) as row_num
from world_layoffs.layoffs_staging1;


with duplicate_ctes as
(
	select *, 
	row_number() over(
	partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
	) as row_num
	from world_layoffs.layoffs_staging1
)
select *
from duplicate_ctes
where row_num > 1
;

-- karena gabisa di delete atau update di ctes karena ctes itu temporary maka dibuat staging lagi dengan kolom row_num
-- world_layoffs.layoffs_staging1 definition

use `world_layoffs`;
CREATE TABLE `layoffs_staging2` (
  `company` varchar(50) DEFAULT NULL,
  `location` varchar(50) DEFAULT NULL,
  `industry` varchar(50) DEFAULT NULL,
  `total_laid_off` varchar(50) DEFAULT NULL,
  `percentage_laid_off` varchar(50) DEFAULT NULL,
  `date` varchar(50) DEFAULT NULL,
  `stage` varchar(50) DEFAULT NULL,
  `country` varchar(50) DEFAULT NULL,
  `funds_raised_millions` varchar(50) DEFAULT null,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

insert into world_layoffs.layoffs_staging2 
select *, 
	row_number() over(
	partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
	) as row_num
	from world_layoffs.layoffs_staging1
;

delete
from world_layoffs.layoffs_staging2 
where row_num > 1;

select *
from world_layoffs.layoffs_staging2 
where row_num > 1;


-- 2. STANDARISASI DATA
-- standarisasi kolom company
select company, trim(company)
from world_layoffs.layoffs_staging2;

update world_layoffs.layoffs_staging2
set company = TRIM(company);

select *
from world_layoffs.layoffs_staging2;

-- standarisasi kolom industry
select distinct industry
from world_layoffs.layoffs_staging2
order by 1;

select *
from world_layoffs.layoffs_staging2
where industry like 'crypto%';

update world_layoffs.layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- standarisasi kolom country
select distinct country
from world_layoffs.layoffs_staging2
order by 1;

select *
from world_layoffs.layoffs_staging2
where country like 'United States%';

select distinct country, TRIM(trailing '.' from country)
from world_layoffs.layoffs_staging2
order by 1;

update world_layoffs.layoffs_staging2
set country = TRIM(trailing '.' from country)
where country like 'United States%';

-- standarisasi date jadi y/m/d
select `date`, str_to_date(`date`, '%m/%d/%Y')
from world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET `date` = NULL
WHERE `date` = 'NULL';

update world_layoffs.layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table world_layoffs.layoffs_staging2
modify column `date` DATE;


-- 3. OLAH DATA NULL AND BLANK
UPDATE world_layoffs.layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off  = 'NULL';

UPDATE world_layoffs.layoffs_staging2
set percentage_laid_off = null
where percentage_laid_off = 'NULL'

UPDATE world_layoffs.layoffs_staging2
set funds_raised_millions = null
where funds_raised_millions = 'NULL';


-- mengisi nilai null pada kolom industry dengan mencari kesamaan company dan location
UPDATE world_layoffs.layoffs_staging2
set industry = null
where industry = '' or industry = 'null';


select *
from world_layoffs.layoffs_staging2
where industry is null;


select *
from world_layoffs.layoffs_staging2
where company like 'Bally%';


select tb1.company, tb1.location, tb1.industry, tb2.industry 
from world_layoffs.layoffs_staging2 tb1
join world_layoffs.layoffs_staging2 tb2
	on tb1.company = tb2.company
	and tb1.location = tb2.location
where (tb1.industry is null)
and tb2.industry is not null;

update world_layoffs.layoffs_staging2 tb1
join world_layoffs.layoffs_staging2 tb2
	on tb1.company = tb2.company
set tb1.industry = tb2.industry
where tb1.industry is null
and tb2.industry is not null;


-- menghapus data yang total_laid_off dan percentage_laid_off null karena tidak bisa mendapatkan informasi apa apa
select *
from world_layoffs.layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

delete 
from world_layoffs.layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;



-- 4. REMOVE DATA YANG TIDAK MEMBERIKAN INFORMASI
alter table world_layoffs.layoffs_staging2 
drop column row_num;



















