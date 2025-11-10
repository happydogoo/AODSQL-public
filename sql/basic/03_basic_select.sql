-- 基础查询示例
-- 1. 查询所有学生信息
SELECT * FROM students;

-- 2. 查询计算机科学系的学生
SELECT student_id, name, email, gpa 
FROM students 
WHERE dept_id = 1;

-- 3. 查询GPA大于3.5的学生
SELECT name, gpa, status 
FROM students 
WHERE gpa > 3.5 
ORDER BY gpa DESC;

-- 4. 查询所有课程信息
SELECT course_id, course_name, course_code, credits 
FROM courses;

-- 5. 查询数据库相关课程
SELECT course_name, course_code, credits 
FROM courses 
WHERE course_name LIKE '%Database%' OR course_name LIKE '%Data%';  