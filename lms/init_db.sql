CREATE TABLE problems (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    difficulty VARCHAR(50),
    problem_type VARCHAR(50),
    description TEXT,
    sample_data TEXT,
    expected_output TEXT,
    script_path VARCHAR(255)
);

CREATE TABLE progress (
    user_id VARCHAR(50),
    problem_id INT,
    status VARCHAR(50),
    last_attempted DATETIME,
    solution_code TEXT,
    PRIMARY KEY (user_id, problem_id),
    FOREIGN KEY (problem_id) REFERENCES problems(id)
);

CREATE TABLE revision_schedule (
    problem_id INT,
    next_review_date DATE,
    PRIMARY KEY (problem_id),
    FOREIGN KEY (problem_id) REFERENCES problems(id)
);