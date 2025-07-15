-- Create Table Problem

CREATE TABLE IF NOT EXISTS problems (
    problem_id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    problem_type_id INT,
    source VARCHAR(100),
    file_path VARCHAR(500) UNIQUE,
    language enum('python','sql','pyspark','notebook') NOT NULL,
    description TEXT,
    estimated_time int,
    created_at datetime DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (problem_type_id) REFERENCES problem_types(problem_type_id)
);

-- Create problem_types TABLE
CREATE TABLE IF NOT EXISTS problem_types(
    problem_type_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR (100) UNIQUE
)