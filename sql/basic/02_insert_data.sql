-- 插入部门数据
INSERT INTO departments (dept_id, dept_name, location, budget) VALUES (1, 'Computer Science', 'Building A', 500000.00);
INSERT INTO departments (dept_id, dept_name, location, budget) VALUES (2, 'Mathematics', 'Building B', 300000.00);
INSERT INTO departments (dept_id, dept_name, location, budget) VALUES (3, 'Physics', 'Building C', 400000.00);
INSERT INTO departments (dept_id, dept_name, location, budget) VALUES (4, 'English', 'Building D', 200000.00);

-- 插入教师数据
INSERT INTO teachers (teacher_id, name, email, dept_id, salary, hire_date) VALUES (1, 'Dr. Zhang Wei', 'zhang.wei@university.edu', 1, 80000.00, '2020-01-15');
INSERT INTO teachers (teacher_id, name, email, dept_id, salary, hire_date) VALUES (2, 'Dr. Li Ming', 'li.ming@university.edu', 1, 75000.00, '2019-09-01');
INSERT INTO teachers (teacher_id, name, email, dept_id, salary, hire_date) VALUES (3, 'Dr. Wang Fang', 'wang.fang@university.edu', 2, 70000.00, '2021-03-10');
INSERT INTO teachers (teacher_id, name, email, dept_id, salary, hire_date) VALUES (4, 'Dr. Chen Lei', 'chen.lei@university.edu', 3, 72000.00, '2020-08-20');
INSERT INTO teachers (teacher_id, name, email, dept_id, salary, hire_date) VALUES (5, 'Dr. Liu Xia', 'liu.xia@university.edu', 4, 65000.00, '2022-01-05');

-- 插入学生数据
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (1, 'Alice Johnson', 'alice.johnson@student.edu', 20, 1, '2023-09-01', 3.75, 'active');
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (2, 'Bob Smith', 'bob.smith@student.edu', 21, 1, '2022-09-01', 3.50, 'active');
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (3, 'Charlie Brown', 'charlie.brown@student.edu', 22, 2, '2021-09-01', 3.90, 'active');
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (4, 'Diana Prince', 'diana.prince@student.edu', 19, 1, '2023-09-01', 3.85, 'active');
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (5, 'Eve Wilson', 'eve.wilson@student.edu', 20, 3, '2023-09-01', 3.60, 'active');
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (6, 'Frank Miller', 'frank.miller@student.edu', 23, 2, '2020-09-01', 3.40, 'graduated');
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (7, 'Grace Lee', 'grace.lee@student.edu', 21, 4, '2022-09-01', 3.95, 'active');
INSERT INTO students (student_id, name, email, age, dept_id, enrollment_date, gpa, status) VALUES (8, 'Henry Davis', 'henry.davis@student.edu', 22, 1, '2021-09-01', 3.20, 'inactive');

-- 插入课程数据
INSERT INTO courses (course_id, course_name, course_code, credits, teacher_id, dept_id, description) VALUES (101, 'Database Systems', 'CS301', 3, 1, 1, 'Introduction to database design and management');
INSERT INTO courses (course_id, course_name, course_code, credits, teacher_id, dept_id, description) VALUES (102, 'Data Structures', 'CS201', 4, 2, 1, 'Fundamental data structures and algorithms');
INSERT INTO courses (course_id, course_name, course_code, credits, teacher_id, dept_id, description) VALUES (103, 'Calculus I', 'MATH101', 4, 3, 2, 'Differential and integral calculus');
INSERT INTO courses (course_id, course_name, course_code, credits, teacher_id, dept_id, description) VALUES (104, 'Physics I', 'PHYS101', 4, 4, 3, 'Mechanics and thermodynamics');
INSERT INTO courses (course_id, course_name, course_code, credits, teacher_id, dept_id, description) VALUES (105, 'English Literature', 'ENG201', 3, 5, 4, 'Survey of English literature');
INSERT INTO courses (course_id, course_name, course_code, credits, teacher_id, dept_id, description) VALUES (106, 'Software Engineering', 'CS401', 3, 1, 1, 'Software development methodologies');
INSERT INTO courses (course_id, course_name, course_code, credits, teacher_id, dept_id, description) VALUES (107, 'Linear Algebra', 'MATH201', 3, 3, 2, 'Vector spaces and linear transformations');

-- 插入选课数据
INSERT INTO enrollments (enrollment_id, student_id, course_id, semester, enrollment_date, grade_letter) VALUES (1, 1, 101, 'Fall2023', '2023-09-01', 'A');
INSERT INTO enrollments (enrollment_id, student_id, course_id, semester, enrollment_date, grade_letter) VALUES (2, 1, 102, 'Fall2023', '2023-09-01', 'B+');
INSERT INTO enrollments (enrollment_id, student_id, course_id, semester, enrollment_date, grade_letter) VALUES (3, 2, 101, 'Fall2023', '2023-09-01', 'A-');
INSERT INTO enrollments (enrollment_id, student_id, course_id, semester, enrollment_date, grade_letter) VALUES (4, 2, 106, 'Fall2023', '2023-09-01', 'B');
INSERT INTO enrollments (enrollment_id, student_id, course_id, semester, enrollment_date, grade_letter) VALUES (5, 3, 103, 'Fall2023', '2023-09-01', 'A');
INSERT INTO enrollments (enrollment_id, student_id, course_id, semester, enrollment_date, grade_letter) VALUES (6, 3, 107, 'Fall2023', '2023-09-01', 'A-');
INSERT INTO enrollments (enrollment_id, student_id, course_id, semester, enrollment_date, grade_letter) VALUES (7, 4, 101, 'Fall2023', '2023-09-01', 'A+');
INSERT INTO enrollments (enrollment_id, student_id, course_id, semester, enrollment_date, grade_letter) VALUES (8, 4, 102, 'Fall2023', '2023-09-01', 'A');
INSERT INTO enrollments (enrollment_id, student_id, course_id, semester, enrollment_date, grade_letter) VALUES (9, 5, 104, 'Fall2023', '2023-09-01', 'B+');
INSERT INTO enrollments (enrollment_id, student_id, course_id, semester, enrollment_date, grade_letter) VALUES (10, 7, 105, 'Fall2023', '2023-09-01', 'A');

-- 插入成绩数据
INSERT INTO grades (grade_id, student_id, course_id, assignment_name, score, max_score, grade_date) VALUES (1, 1, 101, 'Midterm Exam', 92.5, 100, '2023-10-15');
INSERT INTO grades (grade_id, student_id, course_id, assignment_name, score, max_score, grade_date) VALUES (2, 1, 101, 'Final Exam', 88.0, 100, '2023-12-10');
INSERT INTO grades (grade_id, student_id, course_id, assignment_name, score, max_score, grade_date) VALUES (3, 1, 102, 'Project 1', 95.0, 100, '2023-10-20');
INSERT INTO grades (grade_id, student_id, course_id, assignment_name, score, max_score, grade_date) VALUES (4, 2, 101, 'Midterm Exam', 85.0, 100, '2023-10-15');
INSERT INTO grades (grade_id, student_id, course_id, assignment_name, score, max_score, grade_date) VALUES (5, 2, 101, 'Final Exam', 90.0, 100, '2023-12-10');
INSERT INTO grades (grade_id, student_id, course_id, assignment_name, score, max_score, grade_date) VALUES (6, 3, 103, 'Quiz 1', 98.0, 100, '2023-09-20');
INSERT INTO grades (grade_id, student_id, course_id, assignment_name, score, max_score, grade_date) VALUES (7, 3, 103, 'Quiz 2', 96.0, 100, '2023-10-10');
INSERT INTO grades (grade_id, student_id, course_id, assignment_name, score, max_score, grade_date) VALUES (8, 4, 101, 'Midterm Exam', 100.0, 100, '2023-10-15');
INSERT INTO grades (grade_id, student_id, course_id, assignment_name, score, max_score, grade_date) VALUES (9, 4, 101, 'Final Exam', 97.0, 100, '2023-12-10');
INSERT INTO grades (grade_id, student_id, course_id, assignment_name, score, max_score, grade_date) VALUES (10, 5, 104, 'Lab Report 1', 88.5, 100, '2023-09-25'); 