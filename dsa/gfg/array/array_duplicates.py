"""
    Array Duplicates
        Given an array arr of integers, find all the elements that occur more than once in the array. If no element repeats, return an empty array.

    Examples:

        Input: arr[] = [2, 3, 1, 2, 3]
        Output: [2, 3] 
        Explanation: 2 and 3 occur more than once in the given array.
        Input: arr[] = [0, 3, 1, 2] 
        Output: [] 
        Explanation: There is no repeating element in the array, so the output is empty.
        Input: arr[] = [2]
        Output: [] 
        Explanation: There is single element in the array. Therefore output is empty.
"""

class Solution:
    def findDuplicates(self, arr):
        # code here
        counter = {}
        result = []
        
        for i in arr:
            counter[i] = counter.get(i, 0) + 1

        for k,v in counter.items():
            if v > 1:
                result.append(k)

        return result
    
