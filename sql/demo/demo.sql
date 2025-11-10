CREATE DATABASE demo_db;
USE demo_db;
-- 1. 创建表，包含主键
CREATE TABLE students (
    student_id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT,
    gpa DECIMAL(3,2)
);
SHOW TABLES;

-- 2. 创建索引
CREATE INDEX idx_students_id ON students(student_id);

-- 3. 插入10条数据
INSERT INTO students VALUES (1, 'Alice', 20, 3.80);
INSERT INTO students VALUES (2, 'Bob', 21, 3.50);
INSERT INTO students VALUES (3, 'Charlie', 22, 3.20);
INSERT INTO students VALUES (4, 'David', 23, 3.90);
INSERT INTO students VALUES (5, 'Eve', 20, 3.60);
INSERT INTO students VALUES (6, 'Frank', 21, 3.10);
INSERT INTO students VALUES (7, 'Grace', 22, 3.70);
INSERT INTO students VALUES (8, 'Heidi', 23, 3.40);
INSERT INTO students VALUES (9, 'Ivan', 24, 3.30);
INSERT INTO students VALUES (10, 'Judy', 25, 3.80);
SELECT * FROM students;
SELECT * FROM students WHERE student_id = 1;



-- 4. 更新2条数据
UPDATE students SET gpa = 3.95 WHERE student_id = 4;
UPDATE students SET name = 'Alice Smith' WHERE student_id = 1;
SELECT * FROM students;

-- 5. 删除2条数据
DELETE FROM students WHERE student_id = 6;
DELETE FROM students WHERE student_id = 9;
SELECT * FROM students;

-- 6. 删除表前，创建新表并进行复杂查询
CREATE TABLE courses (
    course_id INT PRIMARY KEY,
    course_name VARCHAR(100),
    credits INT
);
INSERT INTO courses VALUES (101, 'Database Systems', 3);
INSERT INTO courses VALUES (102, 'Operating Systems', 4);
INSERT INTO courses VALUES (103, 'Computer Networks', 3);
SHOW TABLES;

-- 复杂查询1：查询所有GPA大于3.5的学生及其信息
SELECT * FROM students WHERE gpa > 3.5;

-- 复杂查询2：统计每个学分数的课程数量
SELECT credits, COUNT(*) AS course_count FROM courses GROUP BY credits;

-- 复杂查询3：子查询，查找所有GPA高于平均值的学生
SELECT * FROM students WHERE gpa > (SELECT AVG(gpa) FROM students);


-- 复杂查询4：聚合与排序
SELECT AVG(gpa) AS avg_gpa FROM students;
SELECT * FROM students ORDER BY gpa DESC LIMIT 5;

-- 6. 删除表
DROP TABLE students;
DROP TABLE courses;