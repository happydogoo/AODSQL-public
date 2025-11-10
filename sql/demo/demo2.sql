CREATE DATABASE demo_db2;
USE demo_db2;
-- 1. 创建表，包含主键
CREATE TABLE students (
    student_id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT,
    gpa DECIMAL(3,2)
);
SHOW TABLES;
CREATE INDEX idx_students_id ON students(student_id);


INSERT INTO students VALUES (1, 'Alice', 21, 2.1);
INSERT INTO students VALUES (2, 'Alice', 21, 2.0);
INSERT INTO students VALUES (3, 'Alice', 19, 2.1);
INSERT INTO students VALUES (4, 'Alice', 22, 3.1);
INSERT INTO students VALUES (5, 'Alice', 25, 2.2);
INSERT INTO students VALUES (6, 'Alice', 24, 2.5);
INSERT INTO students VALUES (7, 'Alice', 20, 3.0);
INSERT INTO students VALUES (8, 'Alice', 25, 3.9);
INSERT INTO students VALUES (9, 'Alice', 22, 2.4);
INSERT INTO students VALUES (10, 'Alice', 22, 3.3);
INSERT INTO students VALUES (11, 'Alice', 23, 3.9);
INSERT INTO students VALUES (12, 'Alice', 22, 3.6);
INSERT INTO students VALUES (13, 'Alice', 21, 3.4);
INSERT INTO students VALUES (14, 'Alice', 24, 2.5);
INSERT INTO students VALUES (15, 'Alice', 22, 2.9);
INSERT INTO students VALUES (16, 'Alice', 20, 3.2);
INSERT INTO students VALUES (17, 'Alice', 19, 3.1);
INSERT INTO students VALUES (18, 'Alice', 23, 2.9);
INSERT INTO students VALUES (19, 'Alice', 19, 3.1);
INSERT INTO students VALUES (20, 'Alice', 23, 3.3);
INSERT INTO students VALUES (21, 'Alice', 19, 3.4);
INSERT INTO students VALUES (22, 'Alice', 19, 2.3);
INSERT INTO students VALUES (23, 'Alice', 19, 3.5);
INSERT INTO students VALUES (24, 'Alice', 25, 4.0);
INSERT INTO students VALUES (25, 'Alice', 19, 3.9);
INSERT INTO students VALUES (26, 'Alice', 19, 2.1);
INSERT INTO students VALUES (27, 'Alice', 21, 3.2);
INSERT INTO students VALUES (28, 'Alice', 20, 3.5);
INSERT INTO students VALUES (29, 'Alice', 22, 2.7);
INSERT INTO students VALUES (30, 'Alice', 19, 2.4);
INSERT INTO students VALUES (31, 'Alice', 24, 2.1);
INSERT INTO students VALUES (32, 'Alice', 20, 2.7);
INSERT INTO students VALUES (33, 'Alice', 22, 3.2);
INSERT INTO students VALUES (34, 'Alice', 19, 3.1);
INSERT INTO students VALUES (35, 'Alice', 18, 2.2);
INSERT INTO students VALUES (36, 'Alice', 18, 3.3);
INSERT INTO students VALUES (37, 'Alice', 22, 2.2);
INSERT INTO students VALUES (38, 'Alice', 21, 2.9);
INSERT INTO students VALUES (39, 'Alice', 20, 3.8);
INSERT INTO students VALUES (40, 'Alice', 18, 2.4);
INSERT INTO students VALUES (41, 'Alice', 23, 3.7);
INSERT INTO students VALUES (42, 'Alice', 25, 3.9);
INSERT INTO students VALUES (43, 'Alice', 18, 3.4);
INSERT INTO students VALUES (44, 'Alice', 22, 4.0);
INSERT INTO students VALUES (45, 'Alice', 21, 3.0);
INSERT INTO students VALUES (46, 'Alice', 24, 3.9);
INSERT INTO students VALUES (47, 'Alice', 25, 2.3);
INSERT INTO students VALUES (48, 'Alice', 22, 2.8);
INSERT INTO students VALUES (49, 'Alice', 23, 2.5);
INSERT INTO students VALUES (50, 'Alice', 19, 2.1);
INSERT INTO students VALUES (51, 'Alice', 22, 2.2);
INSERT INTO students VALUES (52, 'Alice', 24, 3.4);
INSERT INTO students VALUES (53, 'Alice', 22, 3.6);
INSERT INTO students VALUES (54, 'Alice', 23, 2.5);
INSERT INTO students VALUES (55, 'Alice', 19, 2.6);
INSERT INTO students VALUES (56, 'Alice', 19, 2.8);
INSERT INTO students VALUES (57, 'Alice', 21, 2.3);
INSERT INTO students VALUES (58, 'Alice', 19, 2.4);
INSERT INTO students VALUES (59, 'Alice', 22, 2.1);
INSERT INTO students VALUES (60, 'Alice', 24, 3.1);

SELECT * FROM students WHERE student_id = 60;