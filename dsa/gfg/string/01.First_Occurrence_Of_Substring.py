# ---
# title: First Occurrence of Substring Without Built-in Function
# difficulty: Easy
# problem_type: algorithms
# source: 
# topic: pattern searching
# tags: string, pattern searching, substring, implementation
# estimated_time: 10
# file_path: dsa/gfg/string/01.First_Occurrence_Of_Substring.py
# ---

"""
Problem Statement:

Find the first occurrence of the string `pat` in the string `txt`. The function should return an integer denoting the **first index** (0-based) where `pat` occurs in `txt`.

🚫 Restrictions:
- Do **not** use any inbuilt string functions such as `find`, `index`, or similar.

📥 Input:
- A string `txt` representing the main string.
- A string `pat` representing the pattern to search for.

📤 Output:
- An integer representing the first index where `pat` is found in `txt`. If `pat` is not found, return `-1`.

🧪 Examples:

Input:
txt = "GeeksForGeeks"
pat = "Fr"

Output:
-1  
Explanation: "Fr" is not a substring of "GeeksForGeeks".

---

Input:
txt = "GeeksForGeeks"
pat = "For"

Output:
5  
Explanation: "For" occurs starting at index 5 in "GeeksForGeeks".

---

Input:
txt = "GeeksForGeeks"
pat = "gr"

Output:
-1  
Explanation: "gr" is not a substring of "GeeksForGeeks".

📝 Notes:
- The function must simulate the substring search manually using loops or similar logic.
- The matching should be case-sensitive.
"""

txt = "GeeksForGeeks"
pat = "For"

def firstOccurence(pat,txt):
    for i in range(0, len(txt) - len(pat) + 1):
        j = 0
        while  j < len(pat) and txt[i+j] == pat[j]:
            j += 1

        if j == len(pat):
            return i

    return -1

print(firstOccurence(pat,txt))