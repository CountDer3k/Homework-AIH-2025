--select a.admission_type, p.gender, a.insurance, p.dod_ssn
--FROM read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/admissions.parquet') as a
--INNER JOIN read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/patients.parquet') as p
--ON a.subject_id = p.subject_id
--WHERE a.admission_type = 'NEWBORN' AND a.insurance = 'Self Pay'
--GROUP BY a.admission_type, p.gender, a.insurance, p.dod_ssn;


select * from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/icustays.parquet') limit 10;
select * from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/admissions.parquet') order by subject_id limit 10;
select * from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/patients.parquet') limit 10;
select * from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/prescriptions.parquet') limit 10;



-- 1)
-- Show every patient who has stayed at the icu more than once. (8755)
select count(*) as 'Repeated ICU Patient Count'
	From( 
	select subject_id, COUNT(subject_id) as stay_count
	from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/icustays.parquet')
	group by subject_id
)
where stay_count > 1
--order by stay_count;



-- 2) Amount of patients who stayed at the ICU more than once (4943 M | 3812 F)
select count(*)
FROM (
	select *
		From( 
		select subject_id, COUNT(subject_id) as stay_count
		from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/icustays.parquet')
		group by subject_id
	)
	where stay_count > 1) as stays
INNER join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/patients.parquet') as ppl
on stays.subject_id = ppl.subject_id
WHERE ppl.gender = 'M'



-- 3) Type of admission plus count
select adm.admission_type, count(adm.admission_type)
FROM (
	select subject_id, COUNT(subject_id) as stay_count
	from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/icustays.parquet')
	group by subject_id
	having count(subject_id) > 1
	) as stays
INNER join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/patients.parquet') as ppl
on stays.subject_id = ppl.subject_id
inner join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/admissions.parquet') as adm
on stays.subject_id = adm.subject_id
group by adm.admission_type
order by adm.admission_type



-- 4) Admission type of people who stayed at the icu more than 1 time. total stays and single person stays
select coalesce(a1.admission_type, a2.admission_type) as admission_type, a1.total_unique_subjects as 'Total ICU Stays', a2.total_unique_subjects as 'Total Unique Patients ICU stays'
FROM (
	select adm.admission_type, count(adm.admission_type) as total_unique_subjects
	FROM (
		select subject_id, COUNT(subject_id) as stay_count
		from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/icustays.parquet')
		group by subject_id
		having count(subject_id) > 1
		) as stays
	INNER join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/patients.parquet') as ppl
	on stays.subject_id = ppl.subject_id
	inner join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/admissions.parquet') as adm
	on stays.subject_id = adm.subject_id
	where ppl.dod_hosp is not null 
	group by adm.admission_type
	) as a1
full outer join( 
		select adm.admission_type, count(DISTINCT stays.subject_id) AS total_unique_subjects
		FROM (
			select subject_id, COUNT(subject_id) as stay_count
			from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/icustays.parquet')
			group by subject_id
			having count(subject_id) > 1
			) as stays
		INNER join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/patients.parquet') as ppl
		on stays.subject_id = ppl.subject_id
		inner join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/admissions.parquet') as adm
		on stays.subject_id = adm.subject_id
		where ppl.dod_hosp is not null
		group by adm.admission_type
	) a2
on a1.admission_type = a2.admission_type
order by admission_type;



-- 5) Amount of patients who passed away during their ICU stay
select count()
from (
	select subject_id, COUNT(subject_id) as stay_count
	from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/icustays.parquet')
	group by subject_id
	having count(subject_id) > 1
	) as stays
INNER join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/patients.parquet') as ppl
on stays.subject_id = ppl.subject_id
inner join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/admissions.parquet') as adm
on stays.subject_id = adm.subject_id
where adm.deathtime is not null



-- 6 ) Amount patients who passed away during their ICU stays for each admission type
select adm.admission_type, count() as 'Number of patients'
from (
	select subject_id, COUNT(subject_id) as stay_count
	from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/icustays.parquet')
	group by subject_id
	having count(subject_id) > 1
	) as stays
INNER join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/patients.parquet') as ppl
on stays.subject_id = ppl.subject_id
inner join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/admissions.parquet') as adm
on stays.subject_id = adm.subject_id
where adm.deathtime is not null
group by adm.admission_type



-- 7) Amount of patients who passed away druing their ICU stays for each admission type for gender
select adm.admission_type,
	sum(case when ppl.gender = 'M' then 1 else 0 end) as 'Male Patient count',
	sum(case when ppl.gender = 'F' then 1 else 0 end) as 'Female Patient count',
from (
	select subject_id, COUNT(subject_id) as stay_count
	from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/icustays.parquet')
	group by subject_id
	having count(subject_id) > 1
	) as stays
INNER join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/patients.parquet') as ppl
on stays.subject_id = ppl.subject_id
inner join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/admissions.parquet') as adm
on stays.subject_id = adm.subject_id
group by adm.admission_type
order by adm.admission_type



-- 8) Amount of patients who passed away druing their ICU stays for each admission type for gender who were prescribed a drug
select adm.admission_type,
	sum(case when ppl.gender = 'M' then 1 else 0 end) as 'Male Patient count',
	sum(case when ppl.gender = 'F' then 1 else 0 end) as 'Female Patient count',
from (
	select subject_id, COUNT(subject_id) as stay_count
	from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/icustays.parquet')
	group by subject_id
	having count(subject_id) > 1
	) as stays
INNER join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/patients.parquet') as ppl
on stays.subject_id = ppl.subject_id
inner join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/admissions.parquet') as adm
on stays.subject_id = adm.subject_id
inner join (
	-- when unique subject_id so that we only get patients once even if they are prescribed multiple drugs.
	select subject_id 
	from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/prescriptions.parquet')
	group by subject_id
) drugs
on stays.subject_id = drugs.subject_id
group by adm.admission_type
order by adm.admission_type



-- 9) Amount of patients who passed away druing their ICU stays for each admission type for gender who were prescribed more than 1 drug
select adm.admission_type,
	sum(case when ppl.gender = 'M' then 1 else 0 end) as 'Male Patient count',
	sum(case when ppl.gender = 'F' then 1 else 0 end) as 'Female Patient count',
from (
	select subject_id, COUNT(subject_id) as stay_count
	from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/icustays.parquet')
	group by subject_id
	having count(subject_id) > 1
	) as stays
INNER join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/patients.parquet') as ppl
on stays.subject_id = ppl.subject_id
inner join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/admissions.parquet') as adm
on stays.subject_id = adm.subject_id
inner join (
	select subject_id, count(subject_id) as 'Unique Drugs Prescribed'
	from ( 
		-- Gets rid of any duplicate drug pateint prescription (unique only)
		select subject_id, drug
		from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/prescriptions.parquet')
		group by subject_id, drug
	)
	group by subject_id
	having count(subject_id) > 1
) drugs
on stays.subject_id = drugs.subject_id
group by adm.admission_type
order by adm.admission_type



-- 10) Count of patients who were taking more than 1 drug during the death at the ICU
select count()
from(
	select icu.subject_id
	from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/icustays.parquet') icu
	inner join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/admissions.parquet') adm
	on icu.subject_id = adm.subject_id
	inner join(
		select drugs.subject_id, count() as num
		from read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/prescriptions.parquet') drugs
		inner join read_parquet('/Users/derekburrola/Documents/Programming/University of Texas MSAI/2025 - Spring/AIH/Module 1/Homework-AIH-2025/data/parquet/admissions.parquet') adm
		on drugs.subject_id = adm.subject_id
		where adm.deathtime between drugs.startdate and drugs.enddate -- who were taking drugs during their death
		group by drugs.subject_id, drugs.drug
		having count(drugs.subject_id) > 1 -- were taking more than 1 drug
	) drugs
	on drugs.subject_id = icu.subject_id
	where adm.deathtime between icu.intime and icu.outtime -- death at the icu
	group by icu.subject_id
	)



-- Develop 10 SQL queries. For full credit, these must include:

-- 1 JOIN table query
-- 1 GROUP BY query
-- 1 nested query (e.g., select is embedded in another select query) using the MIMIC dataset

-- Please make your SQL queries them interesting and meaningful. 
-- Consider the kind of statistics that might be useful to someone who is doing healthcare research or working in quality improvement. 

-- Once you have built and executed your queries:

	-- create a slide deck with:
		-- description
		-- screenshots
		-- an explanation of the results of each one. 

-- If you can align your 10 queries into one cohesive story, you can get one bonus point. 
-- For example, telling a story of the descriptive statistics of a certain disease: 
	-- the distribution of patients in gender, insurance, top medication prescribed, their length of stay range, or readmission percentage, major lab tests and so on.  
