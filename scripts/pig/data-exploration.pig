-- Data Exploration Script with Apache Pig
-- Author: Student Project
-- Purpose: Exploratory Data Analysis

-- Register MongoDB connector
REGISTER /opt/hadoop/share/hadoop/common/lib/mongo-hadoop-core-2.0.2.jar;
REGISTER /opt/hadoop/share/hadoop/common/lib/mongodb-driver-3.12.11.jar;

-- Load data from HDFS (CSV format)
raw_data = LOAD '/data/input/sample_data.csv' 
    USING PigStorage(',') 
    AS (id:int, name:chararray, age:int, city:chararray, salary:double, department:chararray);

-- Basic data exploration
DESCRIBE raw_data;

-- Count total records
total_count = FOREACH (GROUP raw_data ALL) GENERATE COUNT(raw_data) as total_records;
DUMP total_count;

-- Age statistics
age_stats = FOREACH (GROUP raw_data ALL) GENERATE 
    MIN(raw_data.age) as min_age,
    MAX(raw_data.age) as max_age,
    AVG(raw_data.age) as avg_age;
DUMP age_stats;

-- Salary statistics by department
dept_salary = GROUP raw_data BY department;
dept_salary_stats = FOREACH dept_salary GENERATE 
    group as department,
    COUNT(raw_data) as employee_count,
    AVG(raw_data.salary) as avg_salary,
    MIN(raw_data.salary) as min_salary,
    MAX(raw_data.salary) as max_salary;
DUMP dept_salary_stats;

-- City distribution
city_dist = GROUP raw_data BY city;
city_counts = FOREACH city_dist GENERATE 
    group as city,
    COUNT(raw_data) as count;
city_sorted = ORDER city_counts BY count DESC;
DUMP city_sorted;

-- Age groups analysis
age_grouped = FOREACH raw_data GENERATE 
    *,
    (age < 30 ? 'Young' : (age < 50 ? 'Middle' : 'Senior')) as age_group;

age_group_analysis = GROUP age_grouped BY age_group;
age_group_stats = FOREACH age_group_analysis GENERATE 
    group as age_group,
    COUNT(age_grouped) as count,
    AVG(age_grouped.salary) as avg_salary;
DUMP age_group_stats;

-- Store results to HDFS
STORE dept_salary_stats INTO '/data/output/department_analysis' USING PigStorage(',');
STORE city_sorted INTO '/data/output/city_distribution' USING PigStorage(',');
STORE age_group_stats INTO '/data/output/age_group_analysis' USING PigStorage(',');