-- 事务管理功能测试

-- 1. 开始事务
BEGIN TRANSACTION;

-- 2. 在事务中执行操作
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (9, 'David Wilson', 'david.wilson@student.edu', 23, 1, '2023-09-01', 3.60, 'active');
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (10, 'Eve Brown', 'eve.brown@student.edu', 24, 2, '2023-09-01', 3.80, 'active');

-- 3. 提交事务
COMMIT;

-- 4. 开始新事务
BEGIN TRANSACTION;

-- 5. 在事务中执行操作
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (11, 'Frank Davis', 'frank.davis@student.edu', 25, 1, '2023-09-01', 3.40, 'active');
UPDATE students SET age = 26 WHERE name = 'Frank Davis';

-- 6. 回滚事务
ROLLBACK;

-- 7. 验证回滚结果
SELECT * FROM students WHERE name = 'Frank Davis';
