-- 游标操作功能测试

-- 1. 声明游标
DECLARE student_cursor CURSOR FOR
SELECT student_id, name, age, gpa FROM students ORDER BY name;

-- 2. 打开游标
OPEN student_cursor;

-- 3. 获取游标数据
FETCH student_cursor;

-- 4. 再次获取
FETCH student_cursor;

-- 5. 关闭游标
CLOSE student_cursor;

-- 6. 声明另一个游标
DECLARE course_cursor CURSOR FOR
SELECT course_name, course_code, credits FROM courses;

-- 7. 打开并获取数据
OPEN course_cursor;
FETCH course_cursor;
CLOSE course_cursor;

-- 8. 声明复杂查询游标
DECLARE enrollment_cursor CURSOR FOR
SELECT s.name, c.course_name, e.grade_letter
FROM students s
JOIN enrollments e ON s.student_id = e.student_id
JOIN courses c ON e.course_id = c.course_id;

-- 9. 使用复杂游标
OPEN enrollment_cursor;
FETCH enrollment_cursor;
CLOSE enrollment_cursor;
