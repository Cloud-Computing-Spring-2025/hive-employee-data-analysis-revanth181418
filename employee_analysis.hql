-- Create external table for employees (temporary table)
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

-- Create external table for departments
CREATE EXTERNAL TABLE departments (
    dept_id INT,
    department_name STRING,
    location STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION '/user/hive/warehouse/departments';

-- Create partitioned table for employees
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

-- Load data into partitioned table
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO TABLE employees_partitioned PARTITION(department)
SELECT emp_id, name, age, job_role, salary, project, join_date, department 
FROM employees_temp;

-- Retrieve employees who joined after 2015
SELECT * FROM employees_partitioned WHERE year(join_date) > 2015;

-- Find average salary per department
SELECT department, AVG(salary) AS avg_salary 
FROM employees_partitioned 
GROUP BY department;

-- Identify employees in the 'Alpha' project
SELECT * FROM employees_partitioned WHERE project = 'Alpha';

-- Count employees in each job role
SELECT job_role, COUNT(*) AS num_employees 
FROM employees_partitioned 
GROUP BY job_role;

-- Retrieve employees with salary above department average
SELECT e1.* 
FROM employees_partitioned e1
JOIN (
    SELECT department, AVG(salary) AS avg_salary 
    FROM employees_partitioned 
    GROUP BY department
) e2
ON e1.department = e2.department
WHERE e1.salary > e2.avg_salary;

-- Find department with highest number of employees
SELECT department, COUNT(*) AS num_employees 
FROM employees_partitioned 
GROUP BY department
ORDER BY num_employees DESC
LIMIT 1;

-- Exclude employees with null values
SELECT * FROM employees_partitioned 
WHERE emp_id IS NOT NULL 
AND name IS NOT NULL 
AND age IS NOT NULL 
AND job_role IS NOT NULL 
AND salary IS NOT NULL 
AND project IS NOT NULL 
AND join_date IS NOT NULL 
AND department IS NOT NULL;

-- Join employees with departments for location details
SELECT e.emp_id, e.name, e.job_role, e.salary, d.department_name, d.location 
FROM employees_partitioned e 
JOIN departments d 
ON e.department = d.department_name;

-- Rank employees within each department based on salary
SELECT emp_id, name, department, salary, 
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank 
FROM employees_partitioned;

-- Find top 3 highest-paid employees in each department
SELECT * FROM (
    SELECT emp_id, name, department, salary,
           RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank 
    FROM employees_partitioned
) ranked 
WHERE rank <= 3;
