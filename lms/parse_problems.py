import os
import re
import mysql.connector
from pathlib import Path
import sys
from typing import Optional, Dict, Any
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from lms.config import DB_CONFIG

def parse_problem_metadata(file_path:str) -> Optional[Dict[str, Optional[str]]]:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None
    
    
    metadata = {
        'title': re.search(r'title: (.*?)\n', content),
        'difficulty': re.search(r'difficulty: (.*?)\n', content),
        'problem_type': re.search(r'problem_type: (.*?)\n', content),
        'source': re.search(r'source: (.*?)\n',content),
        'topic': re.search(r'topic: (.*?)\n',content),
        'estimated_time' : re.search(r'estimated_time: (.*?)\n',content),
        'file_path': re.search(r'file_path: (.*?)\n',content),
        'description': re.search(r'Problem Statement:\n(.*?)🧾 Sample Input', content, re.DOTALL),
        'sample_input': re.search(r'Sample Input:\n(.*?)✅ Expected Output:', content, re.DOTALL),
        'expected_output': re.search(r'Expected Output:\n(.*?)(?:\n|$)', content, re.DOTALL)
    }
    # Process regex fields separately
    processed_metadata = {k: v.group(1).strip() if v else None for k, v in metadata.items()}

    # Add tags seperately
    tag_match = re.search(r'tags: (.*?)\n', content)
    if tag_match:
        tags_list = [tag.strip() for tag in tag_match.group(1).split(',')]
        tags = ','.join(tags_list)
    else:
        tags = None

    processed_metadata['tags']  = tags 

    # Add language separately
    lang_ext = {
        'py' : 'python',
        'sql' : 'sql',
        'ipynb' : 'notebook'
    }
    processed_metadata['language'] = lang_ext.get(file_path.split('.')[-1].lower(),None)

    # This is not required now
    """ 
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
    """
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
        if file.endswith(('.py','.ipynb','.sql')) and file.startswith('11.High') and any(dir in root.replace('\\', '/') for dir in ['pyspark', 'leetcode_sql', 'leetcode_pyspark', 'yt_pyspark', 'dsa']):
            metadata = parse_problem_metadata(os.path.join(root, file))
            if metadata:
                problem_data.append(metadata)
            else:
                print(f"Skipping file {file} due to parsing error")

for data in problem_data:
    try:
        data: Dict[str, Any] = data 
        # check whether problem_type exist in the db
        problem_type = data.get('problem_type')
        problem_type_id = None

        if problem_type:
            cursor.execute("SELECT problem_type_id from problem_types where name = %s", (problem_type,))
            row = cursor.fetchone()

            if row and isinstance(row, tuple):
                problem_type_id = row[0]
            else:
                cursor.execute("INSERT into problem_types (name) values (%s)", (problem_type,))
                problem_type_id = cursor.lastrowid
        

        # insert into problems table
        # params_ = (
        #     data['title'], 
        #     problem_type_id, 
        #     data['source'], 
        #     data['file_path'], 
        #     data['language'], 
        #     data['description'], 
        #     data['estimated_time'], 
        #     datetime.now()
        # )
        params = (
            data['title'] if data['title'] is not None else '', 
            int(float(problem_type_id)) if problem_type_id is not None and isinstance(problem_type_id, (int, float, str)) else None,
            data['source'] if data['source'] is not None else '', 
            data['file_path'] if data['file_path'] is not None else '', 
            data['language'] if data['language'] is not None else '', 
            data['description'] if data['description'] is not None else '', 
            data['estimated_time'] if data['estimated_time'] is not None else '', 
            datetime.now()
        )
        cursor.execute("""
            INSERT INTO problems (title, problem_type_id, source, file_path, language, description, estimated_time, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, params)
    except mysql.connector.Error as e:
        print(f"Error inserting data for {data.get('script_path', 'unknown file')}: {e}")

conn.commit()
cursor.close()
conn.close()