-- 错误处理测试

-- 1. 语法错误 - 缺少FROM子句
SELECT name students WHERE student_id = 1;

-- 2. 语义错误 - 不存在的列
SELECT non_exist_column FROM students;

-- 3. 语义错误 - 不存在的表
SELECT * FROM non_exist_table;

-- 4. 约束错误 - 重复主键
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (1, 'Duplicate', 'duplicate@student.edu', 30, 1, '2023-09-01', 3.0, 'active');

-- 5. 数据类型错误 - 字符串插入到数字列
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (12, 'Test', 'test@student.edu', 'not_a_number', 1, '2023-09-01', 3.0, 'active');

-- 6. 语法错误 - 缺少关键字
CREATE TABLE test_table (id INT, name VARCHAR);

-- 7. 语义错误 - 删除不存在的索引
DROP INDEX non_exist_index ON students;

-- 8. 约束错误 - 违反CHECK约束
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (13, 'Invalid Age', 'invalid@student.edu', 15, 1, '2023-09-01', 3.0, 'active');

-- 9. 约束错误 - 违反GPA约束
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (14, 'Invalid GPA', 'invalid@student.edu', 20, 1, '2023-09-01', 5.0, 'active');

-- 10. 外键约束错误 - 引用不存在的部门
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (15, 'Invalid Dept', 'invalid@student.edu', 20, 999, '2023-09-01', 3.0, 'active');
