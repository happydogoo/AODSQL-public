-- 视图管理功能测试

-- 1. 创建简单视图
CREATE VIEW student_courses AS
SELECT s.name, s.age, c.course_name, t.name as teacher_name
FROM students s
JOIN enrollments e ON s.student_id = e.student_id
JOIN courses c ON e.course_id = c.course_id
JOIN teachers t ON c.teacher_id = t.teacher_id;

-- 2. 创建聚合视图
CREATE VIEW student_stats AS
SELECT 
    COUNT(*) as total_students,
    AVG(age) as average_age,
    MAX(age) as max_age,
    MIN(age) as min_age,
    AVG(gpa) as average_gpa
FROM students;

-- 3. 查询视图
SELECT * FROM student_courses;
SELECT * FROM student_stats;

-- 4. 显示视图
SHOW VIEWS;

-- 5. 修改视图
ALTER VIEW student_courses AS
SELECT s.name, s.age, c.course_name
FROM students s
JOIN enrollments e ON s.student_id = e.student_id
JOIN courses c ON e.course_id = c.course_id;

-- 6. 删除视图
DROP VIEW student_stats;
DROP VIEW student_courses;
