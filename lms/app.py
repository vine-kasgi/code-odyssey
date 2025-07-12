import streamlit as st
import mysql.connector
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast,List, Dict, Any
import sys

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

try:
    from lms.config import DB_CONFIG
except ModuleNotFoundError:
    st.error("Error: Could not find lms/config.py. Ensure it exists in the lms/ directory with correct DB_CONFIG settings.")
    sys.exit(1)

# Database connection
def get_db_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as e:
        st.error(f"Error connecting to MySQL: {e}")
        sys.exit(1)

# Load problems
try:
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute('SELECT id, title, difficulty, problem_type FROM problems')
    problems = cast(List[Dict[str, Any]], cursor.fetchall())
    cursor.close()
    conn.close()
except mysql.connector.Error as e:
    st.error(f"Error fetching problems from database: {e}")
    sys.exit(1)

st.title("Data Engineering LMS 😎")

# Problem selection
if problems:
    problem_id = st.selectbox(
        "Select Problem",
        [p['id'] for p in problems],
        key="problem_select",
        format_func=lambda x: next(f"{p['title'] or 'Untitled'} ({p['problem_type'] or 'Unknown'})" for p in problems if p['id'] == x)
    )

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute('SELECT title, difficulty, problem_type, description, sample_data, expected_output, script_path FROM problems WHERE id = %s', (problem_id,))
    problem = cast(Dict[str, Any], cursor.fetchone())
    cursor.close()
    conn.close()

    if problem:
        st.write(f"### {problem['title'] or 'Untitled'}")
        st.write(f"**Difficulty**: {problem['difficulty'] or 'Unknown'}")
        st.write(f"**Problem Type**: {problem['problem_type'] or 'Unknown'}")
        st.write(problem['description'] or "No description available.")
        st.write("**Sample Data:**")
        st.code(problem['sample_data'] or "No sample data available.", language='text')
        st.write("**Expected Output:**")
        st.code(problem['expected_output'] or "No expected output available.", language='text')

        # Code submission
        code = st.text_area("Write your solution", height=300, key="code_input")
        if st.button("Run Code", key="run_code"):
            try:
                with open('lms/temp_solution.py', 'w', encoding='utf-8') as f:
                    f.write(code)

                # SAFER: Removed shell=True
                result = subprocess.run(
                    ['python', 'lms/temp_solution.py'],
                    capture_output=True,
                    text=True
                )

                st.write("**Output:**")
                st.code(result.stdout)

                if result.stderr:
                    st.error(f"Execution error:\n{result.stderr}")
                elif result.stdout.strip() == (problem['expected_output'].strip() if problem['expected_output'] else ""):
                    st.success("✅ Correct!")

                    # Log progress
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute(
                        'INSERT INTO progress (user_id, problem_id, status, last_attempted, solution_code) VALUES (%s, %s, %s, %s, %s)',
                        ('user1', problem_id, 'completed', datetime.now(), code)
                    )
                    # Schedule revision
                    next_review_date = (datetime.now() + timedelta(days=7)).date()
                    cursor.execute(
                        'INSERT INTO revision_schedule (problem_id, next_review_date) VALUES (%s, %s) '
                        'ON DUPLICATE KEY UPDATE next_review_date = %s',
                        (problem_id, next_review_date, next_review_date)
                    )
                    conn.commit()
                    cursor.close()
                    conn.close()
                else:
                    st.error("❌ Incorrect output. Try again!")

                    # Log attempt
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute(
                        'INSERT INTO progress (user_id, problem_id, status, last_attempted, solution_code) '
                        'VALUES (%s, %s, %s, %s, %s) '
                        'ON DUPLICATE KEY UPDATE status = %s, last_attempted = %s, solution_code = %s',
                        ('user1', problem_id, 'attempted', datetime.now(), code, 'attempted', datetime.now(), code)
                    )
                    conn.commit()
                    cursor.close()
                    conn.close()
            except Exception as e:
                st.error(f"Error running code: {e}")
    else:
        st.error("Selected problem not found in the database.")
else:
    st.warning("No problems found in the database. Run lms/parse_problems.py to populate problems.")

# Progress visualization
st.write("## Progress Dashboard")

conn = get_db_connection()
cursor = conn.cursor(dictionary=True)
cursor.execute(
    'SELECT p.title, p.problem_type, pr.status, pr.last_attempted '
    'FROM progress pr JOIN problems p ON pr.problem_id = p.id '
    'WHERE pr.user_id = %s',
    ('user1',)
)
progress_data = cursor.fetchall()
cursor.close()
conn.close()

if progress_data:
    df = pd.DataFrame(progress_data)
    st.dataframe(df)

    # Plot completion status by problem type
    status_by_type = df.groupby(['problem_type', 'status']).size().unstack(fill_value=0)
    fig, ax = plt.subplots()
    status_by_type.plot(kind='bar', stacked=True, ax=ax)
    ax.set_title('Problem Completion Status by Type')
    ax.set_xlabel('Problem Type')
    ax.set_ylabel('Count')
    st.pyplot(fig)
else:
    st.info("No progress yet. Solve some problems!")
