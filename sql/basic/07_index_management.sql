-- 索引管理功能测试

-- 1. 创建索引
CREATE INDEX idx_students_name ON students(name);
CREATE INDEX idx_students_gpa ON students(gpa);
CREATE INDEX idx_teachers_name ON teachers(name);

-- 2. 显示索引
SHOW INDEX FROM students;
SHOW INDEX FROM teachers;

-- 3. 使用索引的查询（优化器应该选择索引扫描）
SELECT * FROM students WHERE name = 'Alice Johnson';
SELECT * FROM students WHERE gpa > 3.5;
SELECT * FROM teachers WHERE name = 'Dr. Zhang Wei';

-- 4. 删除索引
DROP INDEX idx_students_name ON students;
DROP INDEX idx_teachers_name ON teachers;
