# HadoopHiveHue
Hadoop , Hive, Hue setup pseudo distributed  environment  using docker compose

Overview

This project involves analyzing employee and department data using Apache Hive. The tasks include loading data into Hive tables, performing various analytical queries, and generating output files.


Steps for Execution

1. Create External Table for Employees (Temporary Table)

CREATE EXTERNAL TABLE employees_temp (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary FLOAT,
    project STRING,
    join_date STRING,
    department STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION '/user/hive/warehouse/employees_raw';

2. Create External Table for Departments

CREATE EXTERNAL TABLE departments (
    dept_id INT,
    department_name STRING,
    location STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION '/user/hive/warehouse/departments';

3. Create Partitioned Table for Employees

CREATE TABLE employees_partitioned (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary FLOAT,
    project STRING,
    join_date STRING
)
PARTITIONED BY (department STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS PARQUET;

4. Load Data into Partitioned Table

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO TABLE employees_partitioned PARTITION(department)
SELECT emp_id, name, age, job_role, salary, project, join_date, department 
FROM employees_temp;

5. Queries Execution

a) Retrieve Employees Who Joined After 2015

SELECT * FROM employees_partitioned WHERE year(join_date) > 2015;

b) Find Average Salary Per Department

SELECT department, AVG(salary) AS avg_salary 
FROM employees_partitioned 
GROUP BY department;

c) Identify Employees in the 'Alpha' Project

SELECT * FROM employees_partitioned WHERE project = 'Alpha';

d) Count Employees in Each Job Role

SELECT job_role, COUNT(*) AS num_employees 
FROM employees_partitioned 
GROUP BY job_role;

e) Retrieve Employees with Salary Above Department Average

SELECT e1.* 
FROM employees_partitioned e1
JOIN (
    SELECT department, AVG(salary) AS avg_salary 
    FROM employees_partitioned 
    GROUP BY department
) e2
ON e1.department = e2.department
WHERE e1.salary > e2.avg_salary;

f) Find Department with Highest Number of Employees

SELECT department, COUNT(*) AS num_employees 
FROM employees_partitioned 
GROUP BY department
ORDER BY num_employees DESC
LIMIT 1;

g) Exclude Employees with Null Values

SELECT * FROM employees_partitioned 
WHERE emp_id IS NOT NULL 
AND name IS NOT NULL 
AND age IS NOT NULL 
AND job_role IS NOT NULL 
AND salary IS NOT NULL 
AND project IS NOT NULL 
AND join_date IS NOT NULL 
AND department IS NOT NULL;

h) Join Employees with Departments for Location Details

SELECT e.emp_id, e.name, e.job_role, e.salary, d.department_name, d.location 
FROM employees_partitioned e 
JOIN departments d 
ON e.department = d.department_name;

i) Rank Employees Within Each Department Based on Salary

SELECT emp_id, name, department, salary, 
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank 
FROM employees_partitioned;

j) Find Top 3 Highest-Paid Employees in Each Department

SELECT * FROM (
    SELECT emp_id, name, department, salary,
           RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank 
    FROM employees_partitioned
) ranked 
WHERE rank <= 3;
