-- 删除表（按依赖关系顺序删除）
DROP TABLE IF EXISTS enrollments;
DROP TABLE IF EXISTS grades;
DROP TABLE IF EXISTS students;
DROP TABLE IF EXISTS courses;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS teachers;

-- 创建部门表
CREATE TABLE departments (
    dept_id INT PRIMARY KEY,
    dept_name VARCHAR(50) NOT NULL,
    location VARCHAR(100),
    budget DECIMAL(12,2)
);

-- 创建教师表
CREATE TABLE teachers (
    teacher_id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    dept_id INT,
    salary DECIMAL(10,2),
    hire_date DATE,
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

-- 创建学生表
CREATE TABLE students (
    student_id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    age INT CHECK (age >= 16 AND age <= 100),
    dept_id INT,
    enrollment_date DATE,
    gpa DECIMAL(3,2) CHECK (gpa >= 0.0 AND gpa <= 4.0),
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'graduated')),
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

-- 创建课程表
CREATE TABLE courses (
    course_id INT PRIMARY KEY,
    course_name VARCHAR(100) NOT NULL,
    course_code VARCHAR(20) UNIQUE,
    credits INT CHECK (credits > 0 AND credits <= 6),
    teacher_id INT,
    dept_id INT,
    description TEXT,
    FOREIGN KEY (teacher_id) REFERENCES teachers(teacher_id),
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

-- 创建选课表（学生-课程多对多关系）
CREATE TABLE enrollments (
    enrollment_id INT PRIMARY KEY,
    student_id INT,
    course_id INT,
    semester VARCHAR(20),
    enrollment_date DATE,
    grade_letter VARCHAR(2),
    UNIQUE(student_id, course_id, semester),
    FOREIGN KEY (student_id) REFERENCES students(student_id),
    FOREIGN KEY (course_id) REFERENCES courses(course_id)
);

-- 创建成绩表
CREATE TABLE grades (
    grade_id INT PRIMARY KEY,
    student_id INT,
    course_id INT,
    assignment_name VARCHAR(100),
    score DECIMAL(5,2) CHECK (score >= 0 AND score <= 100),
    max_score DECIMAL(5,2) DEFAULT 100,
    grade_date DATE,
    FOREIGN KEY (student_id) REFERENCES students(student_id),
    FOREIGN KEY (course_id) REFERENCES courses(course_id)
);

-- 创建索引
CREATE INDEX idx_students_dept ON students(dept_id);
CREATE INDEX idx_students_email ON students(email);
CREATE INDEX idx_courses_teacher ON courses(teacher_id);
CREATE INDEX idx_courses_dept ON courses(dept_id);
CREATE INDEX idx_enrollments_student ON enrollments(student_id);
CREATE INDEX idx_enrollments_course ON enrollments(course_id);
CREATE INDEX idx_grades_student ON grades(student_id);
CREATE INDEX idx_grades_course ON grades(course_id); 