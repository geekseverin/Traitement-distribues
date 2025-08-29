-- Remplacez le contenu de scripts/pig/data-exploration.pig

-- Charger les vraies données CSV depuis data/raw/sample_data.csv
raw_data = LOAD '/data/input/sample_data.csv' 
    USING PigStorage(',') 
    AS (id:int, name:chararray, age:int, city:chararray, salary:double, department:chararray);

-- Filtrer la ligne d'en-tête si elle existe
filtered_data = FILTER raw_data BY id IS NOT NULL AND id > 0;

-- Afficher le schéma
DESCRIBE filtered_data;

-- Compter le total d'enregistrements
total_count = FOREACH (GROUP filtered_data ALL) GENERATE COUNT(filtered_data) as total_records;
DUMP total_count;

-- Statistiques par département (données réelles)
dept_salary = GROUP filtered_data BY department;
dept_salary_stats = FOREACH dept_salary GENERATE 
    group as department,
    COUNT(filtered_data) as employee_count,
    AVG(filtered_data.salary) as avg_salary,
    MIN(filtered_data.salary) as min_salary,
    MAX(filtered_data.salary) as max_salary;

-- Afficher et sauvegarder les résultats
DUMP dept_salary_stats;
STORE dept_salary_stats INTO '/data/output/department_analysis' USING PigStorage(',');

-- Distribution par ville (données réelles)
city_dist = GROUP filtered_data BY city;
city_counts = FOREACH city_dist GENERATE 
    group as city,
    COUNT(filtered_data) as count;
city_sorted = ORDER city_counts BY count DESC;

DUMP city_sorted;
STORE city_sorted INTO '/data/output/city_distribution' USING PigStorage(',');

-- Analyse par groupes d'âge avec les vraies données
age_grouped = FOREACH filtered_data GENERATE 
    *,
    (age < 30 ? 'Young' : (age < 50 ? 'Middle' : 'Senior')) as age_group;

age_group_analysis = GROUP age_grouped BY age_group;
age_group_stats = FOREACH age_group_analysis GENERATE 
    group as age_group,
    COUNT(age_grouped) as count,
    AVG(age_grouped.salary) as avg_salary;

DUMP age_group_stats;
STORE age_group_stats INTO '/data/output/age_group_analysis' USING PigStorage(',');