-- MongoDB Connection and Data Reading Script
-- Purpose: Read data from MongoDB using Hadoop connector

-- Register MongoDB Hadoop connector
REGISTER /opt/hadoop/share/hadoop/common/lib/mongo-hadoop-core-2.0.2.jar;
REGISTER /opt/hadoop/share/hadoop/common/lib/mongodb-driver-3.12.11.jar;

-- Set MongoDB input URI
%default MONGO_INPUT 'mongodb://mongodb:27017/bigdata.employees'
%default MONGO_OUTPUT 'mongodb://mongodb:27017/bigdata.results'

-- Load data from MongoDB
mongo_data = LOAD '$MONGO_INPUT' 
    USING com.mongodb.hadoop.pig.MongoLoader('id, name, age, department, salary', 'id');

-- Display schema
DESCRIBE mongo_data;

-- Basic analysis
total_employees = FOREACH (GROUP mongo_data ALL) GENERATE COUNT(mongo_data) as total;
DUMP total_employees;

-- Department-wise analysis
dept_group = GROUP mongo_data BY department;
dept_analysis = FOREACH dept_group GENERATE 
    group as department,
    COUNT(mongo_data) as emp_count,
    AVG(mongo_data.salary) as avg_salary;

-- Display results
DUMP dept_analysis;

-- Filter high salary employees (> 50000)
high_salary = FILTER mongo_data BY salary > 50000;
high_salary_count = FOREACH (GROUP high_salary ALL) GENERATE COUNT(high_salary) as count;
DUMP high_salary_count;

-- Store results back to MongoDB
STORE dept_analysis INTO '$MONGO_OUTPUT' 
    USING com.mongodb.hadoop.pig.MongoInsertStorage();

-- Also store to HDFS for further processing
STORE dept_analysis INTO '/data/output/mongodb_analysis' USING PigStorage(',');