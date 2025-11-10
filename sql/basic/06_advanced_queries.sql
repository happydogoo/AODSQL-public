-- 高级查询功能测试
-- 包括JOIN、聚合函数、GROUP BY、ORDER BY等

-- 1. 内连接查询 - 学生选课信息
SELECT s.name, c.course_name, t.name as teacher_name
FROM students s
INNER JOIN enrollments e ON s.student_id = e.student_id
INNER JOIN courses c ON e.course_id = c.course_id
INNER JOIN teachers t ON c.teacher_id = t.teacher_id;

-- 2. 左连接查询 - 所有学生及其选课情况
SELECT s.name, c.course_name, e.grade_letter
FROM students s
LEFT JOIN enrollments e ON s.student_id = e.student_id
LEFT JOIN courses c ON e.course_id = c.course_id;

-- 3. 聚合函数查询
SELECT COUNT(*) as total_students FROM students;
SELECT AVG(age) as avg_age FROM students;
SELECT MAX(gpa) as max_gpa, MIN(gpa) as min_gpa FROM students;
SELECT AVG(salary) as avg_teacher_salary FROM teachers;

-- 4. GROUP BY查询 - 按部门统计学生数量
SELECT d.dept_name, COUNT(s.student_id) as student_count
FROM departments d
LEFT JOIN students s ON d.dept_id = s.dept_id
GROUP BY d.dept_id, d.dept_name;

-- 5. GROUP BY查询 - 按教师统计课程数量
SELECT t.name, COUNT(c.course_id) as course_count
FROM teachers t
LEFT JOIN courses c ON t.teacher_id = c.teacher_id
GROUP BY t.teacher_id, t.name;

-- 6. ORDER BY查询
SELECT name, gpa FROM students ORDER BY gpa DESC;
SELECT name, age FROM students ORDER BY name ASC;

-- 7. 复杂查询组合 - 高GPA学生的选课情况
SELECT s.name, s.gpa, c.course_name, e.grade_letter
FROM students s
JOIN enrollments e ON s.student_id = e.student_id
JOIN courses c ON e.course_id = c.course_id
WHERE s.gpa > 3.7
ORDER BY s.gpa DESC, c.course_name;

-- 8. 子查询 - 查询选修了数据库课程的学生
SELECT s.name, s.email
FROM students s
WHERE s.student_id IN (
    SELECT e.student_id 
    FROM enrollments e 
    JOIN courses c ON e.course_id = c.course_id 
    WHERE c.course_name LIKE '%Database%'
);

-- 9. 多表连接 - 完整的选课信息
SELECT s.name as student_name, 
       c.course_name, 
       t.name as teacher_name,
       d.dept_name as department,
       e.grade_letter
FROM students s
JOIN enrollments e ON s.student_id = e.student_id
JOIN courses c ON e.course_id = c.course_id
JOIN teachers t ON c.teacher_id = t.teacher_id
JOIN departments d ON s.dept_id = d.dept_id
ORDER BY s.name, c.course_name;
