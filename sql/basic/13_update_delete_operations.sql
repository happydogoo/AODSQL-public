-- UPDATE和DELETE操作测试

-- 1. 更新单行数据 - 更新学生年龄
UPDATE students SET age = 25 WHERE name = 'Alice Johnson';

-- 2. 更新多行数据 - 所有活跃学生的GPA增加0.1
UPDATE students SET gpa = gpa + 0.1 WHERE status = 'active' AND gpa < 4.0;

-- 3. 更新学生状态
UPDATE students SET status = 'graduated' WHERE student_id = 6;

-- 4. 更新教师薪资
UPDATE teachers SET salary = salary * 1.05 WHERE dept_id = 1;

-- 5. 更新课程学分
UPDATE courses SET credits = 4 WHERE course_name LIKE '%Database%';

-- 6. 更新选课成绩
UPDATE enrollments SET grade_letter = 'A+' WHERE student_id = 4 AND course_id = 101;

-- 7. 条件删除 - 删除非活跃学生
DELETE FROM students WHERE status = 'inactive';

-- 8. 删除特定记录 - 删除特定选课记录
DELETE FROM enrollments WHERE student_id = 8;

-- 9. 删除成绩记录
DELETE FROM grades WHERE score < 70;

-- 10. 验证更新和删除结果
SELECT student_id, name, age, gpa, status FROM students ORDER BY student_id;
SELECT course_id, course_name, credits FROM courses ORDER BY course_id;
SELECT enrollment_id, student_id, course_id, grade_letter FROM enrollments ORDER BY enrollment_id;
