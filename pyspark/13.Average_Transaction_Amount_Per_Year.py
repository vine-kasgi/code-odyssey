# ---
# title: Average Yearly Transaction Amount (2018–2022)
# difficulty: Medium
# problem_type: pyspark
# source: 
# topic: aggregation
# tags: pyspark, avg, groupby, date functions
# estimated_time: 15
# file_path: pyspark/13.Average_Transaction_Amount_Per_Year.py
# ---

"""
Problem Statement:

Calculate the average transaction amount per year for each user for the years 2018 to 2022.

📘 Table: transactions

| Column Name        | Type    |
|--------------------|---------|
| transaction_id     | INT     |
| user_id            | INT     |
| transaction_date   | DATE    |
| transaction_amount | DECIMAL(10, 2) |

Each row represents a financial transaction made by a user on a specific date.

🧾 Sample Data:

| transaction_id | user_id | transaction_date | transaction_amount |
|----------------|---------|------------------|--------------------|
| 1              | 269     | 2018-08-15       | 500.00             |
| 2              | 478     | 2018-11-25       | 400.00             |
| 3              | 269     | 2019-01-05       | 1000.00            |
| 4              | 123     | 2020-10-20       | 600.00             |
| 5              | 478     | 2021-07-05       | 700.00             |
| 6              | 123     | 2022-03-05       | 900.00             |

✅ Expected Output:

| user_id | transaction_year | avg_transaction_amount |
|---------|------------------|-------------------------|
| 123     | 2020             | 600.00                  |
| 123     | 2022             | 900.00                  |
| 269     | 2018             | 500.00                  |
| 269     | 2019             | 1000.00                 |
| 478     | 2018             | 400.00                  |
| 478     | 2021             | 700.00                  |

Notes:
- Consider only transactions from years **2018 to 2022**.
- Use the `transaction_date` to extract the year.
- Round averages to 2 decimal places if necessary.
"""
