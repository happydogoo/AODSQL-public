-- 系统命令功能测试

-- 1. 显示所有表
SHOW TABLES;

-- 2. 显示表结构
SHOW COLUMNS FROM students;
SHOW COLUMNS FROM courses;
SHOW COLUMNS FROM teachers;
SHOW COLUMNS FROM departments;
SHOW COLUMNS FROM enrollments;
SHOW COLUMNS FROM grades;

-- 3. 显示索引
SHOW INDEX FROM students;
SHOW INDEX FROM courses;
SHOW INDEX FROM teachers;

-- 4. 显示触发器
SHOW TRIGGERS;

-- 5. 显示视图
SHOW VIEWS;

-- 6. 解释查询计划
EXPLAIN SELECT * FROM students WHERE age > 20;
EXPLAIN SELECT s.name, c.course_name FROM students s JOIN enrollments e ON s.student_id = e.student_id JOIN courses c ON e.course_id = c.course_id;
EXPLAIN SELECT d.dept_name, COUNT(s.student_id) FROM departments d LEFT JOIN students s ON d.dept_id = s.dept_id GROUP BY d.dept_id;
