-- 触发器管理功能测试

-- 1. 创建BEFORE INSERT触发器
CREATE TRIGGER before_insert_student BEFORE INSERT ON students FOR EACH ROW WHEN NEW.age < 16 BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Age must be at least 16' END

-- 2. 创建AFTER INSERT触发器
CREATE TRIGGER after_insert_student AFTER INSERT ON students FOR EACH ROW BEGIN SELECT 'Student inserted: ' || NEW.name END

-- 3. 创建BEFORE UPDATE触发器
CREATE TRIGGER before_update_student BEFORE UPDATE ON students FOR EACH ROW WHEN NEW.gpa < 0 OR NEW.gpa > 4.0 BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'GPA must be between 0.0 and 4.0' END

-- 4. 创建AFTER UPDATE触发器
CREATE TRIGGER after_update_student AFTER UPDATE ON students FOR EACH ROW WHEN NEW.gpa != OLD.gpa BEGIN SELECT 'Student GPA updated: ' || NEW.name || ' - New GPA: ' || NEW.gpa END

-- 5. 显示触发器
SHOW TRIGGERS;

-- 6. 删除触发器
DROP TRIGGER before_insert_student;
DROP TRIGGER after_insert_student;
DROP TRIGGER before_update_student;
DROP TRIGGER after_update_student;
