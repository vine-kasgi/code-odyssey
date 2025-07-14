import sys
import os
import tempfile

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from parse_problems import parse_problem_metadata

def test_parse_basic_metadata():
    sample_content = """Title: Sample Problem
Difficulty: Easy
Problem Type: Python
source: LMS
topic: Strings
estimated_time: 5 min
file_path: sample_problem.py
tags: strings, beginner

Problem Statement:
Reverse a string.

🧾 Sample Input:
hello

✅ Expected Output:
olleh
"""
    # Create a temporary file for testing
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.py', delete=False, encoding='utf-8') as tmp:
        tmp.write(sample_content)
        tmp_path = tmp.name

    result = parse_problem_metadata(tmp_path)
    assert result is not None, "Parsing returned None, expected dict"

    assert result['title'] == "Sample Problem"
    assert result['difficulty'] == "Easy"
    assert result['problem_type'] == "Python"
    assert result['description'].startswith("Reverse a string")
    assert result['sample_input'].strip() == "hello"
    assert result['expected_output'].strip() == "olleh"
    assert result['language'] == "python"
    assert result['tags'] == ["strings", "beginner"]
