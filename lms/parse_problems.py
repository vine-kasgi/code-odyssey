import os
import re
import mysql.connector
from pathlib import Path
import sys

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from lms.config import DB_CONFIG

def parse_problem_metadata(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

    metadata = {
        'title': re.search(r'Title: (.*?)\n', content),
        'difficulty': re.search(r'Difficulty: (.*?)\n', content),
        'problem_type': re.search(r'Problem Type: (.*?)\n', content),
        'description': re.search(r'Problem Statement:\n(.*?)Sample Data:', content, re.DOTALL),
        'sample_data': re.search(r'Sample Data:\n(.*?)Expected Output:', content, re.DOTALL),
        'expected_output': re.search(r'Expected Output:\n(.*?)(?:\n|$)', content, re.DOTALL)
    }
    # Process regex fields separately
    processed_metadata = {k: v.group(1).strip() if v else None for k, v in metadata.items()}
    # Add script_path separately
    processed_metadata['script_path'] = file_path
    
    # Infer problem_type if not in metadata
    if not processed_metadata['problem_type']:
        if 'leetcode_sql' in file_path.replace('\\', '/'):
            processed_metadata['problem_type'] = 'MySQL'
        elif any(dir in file_path.replace('\\', '/') for dir in ['pyspark', 'leetcode_pyspark', 'yt_pyspark']):
            processed_metadata['problem_type'] = 'PySpark'
        elif file_path.endswith('.py'):
            processed_metadata['problem_type'] = 'Python'
        else:
            processed_metadata['problem_type'] = 'Unknown'
    
    return processed_metadata

# Connect to MySQL
try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
except mysql.connector.Error as e:
    print(f"Error connecting to MySQL: {e}")
    sys.exit(1)

# Parse files and insert into database
problem_data = []
for root, _, files in os.walk(project_root):
    for file in files:
        if file.endswith(('.py', '.sql')) and any(dir in root.replace('\\', '/') for dir in ['pyspark', 'leetcode_sql', 'leetcode_pyspark', 'yt_pyspark', 'dsa']):
            metadata = parse_problem_metadata(os.path.join(root, file))
            if metadata:
                problem_data.append(metadata)
            else:
                print(f"Skipping file {file} due to parsing error")

for data in problem_data:
    try:
        cursor.execute("""
            INSERT INTO problems (title, difficulty, problem_type, description, sample_data, expected_output, script_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (data['title'], data['difficulty'], data['problem_type'], data['description'], data['sample_data'], data['expected_output'], data['script_path']))
    except mysql.connector.Error as e:
        print(f"Error inserting data for {data.get('script_path', 'unknown file')}: {e}")

conn.commit()
cursor.close()
conn.close()