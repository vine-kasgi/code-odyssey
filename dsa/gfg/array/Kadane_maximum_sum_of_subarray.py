"""
    Given an integer array arr[]. You need to find the maximum sum of a subarray.


    Examples:
        Input: arr[] = [2, 3, -8, 7, -1, 2, 3]
        Output: 11
        Explanation: The subarray {7, -1, 2, 3} has the largest sum 11.

        Input: arr[] = [-2, -4]
        Output: -2
        Explanation: The subarray {-2} has the largest sum -2.

        Input: arr[] = [5, 4, 1, 7, 8]
        Output: 25
        Explanation: The subarray {5, 4, 1, 7, 8} has the largest sum 25.

        
    Constraints:
        1 ≤ arr.size() ≤ 105
        -109 ≤ arr[i] ≤ 104

"""

## Naive/Brute Force approach
# IDEA -> Find all subarrays -> compare sum of all subarrays -> return max sum of subarray
# STEP 1 Calulating all the subarrays present
class Solution:
    def printAllSubarrays(self,arr):

        for i in range(len(arr)):
            curr_subarray = []
            for j in range(i,len(arr)):
                curr_subarray.append(arr[j])
                print (curr_subarray)

# STEP 2 Compare sum of all the subarrays present
class Solution:
    def sumAllSubarrays(self,arr):
        n = len(arr)
        res = float('-inf')

        for i in range(n):
            curr_subarray = []
            for j in range(i,n):
                curr_subarray.append(arr[j])
                print(f'{curr_subarray} ----> {sum(curr_subarray)}')
                res = max(res,sum(curr_subarray))
                print(f'Max between current subarray --> {res}')


# Step 3 Return Max value, skip finding the subarray 
class Solution:
    def maxSumSubarray(self,arr):
        n = len(arr)
        res = float('-inf')

        for i in range(n):
            curr_sum = 0

            for j in range(i,n):
                curr_sum += arr[j]
                res = max(res,curr_sum)

        return res


# naive approach
# Time : O(n^2)
# Space : O(1)


## Kadane Algorithm

class Solution:
    def maxSumSubarrayV1(self,arr):

        max_sum = float('-inf')
        curr_sum = 0

        for i in range(len(arr)):
            
            curr_sum += arr[i]
            
            max_sum = max(curr_sum ,max_sum)

            if curr_sum < 0:
                curr_sum = 0
            

        return max_sum
# V1
# Time : O(n)
# Space : O(1)
