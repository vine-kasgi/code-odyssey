"""
    You are given an array arr of positive integers. Your task is to find all the leaders in the array. 
    An element is considered a leader if it is greater than or equal to all elements to its right. 
    The rightmost element is always a leader.

    
    Examples:
        Input: arr = [16, 17, 4, 3, 5, 2]
        Output: [17, 5, 2]
        Explanation: Note that there is nothing greater on the right side of 17, 5 and, 2.

        Input: arr = [10, 4, 2, 4, 1]
        Output: [10, 4, 4, 1]
        Explanation: Note that both of the 4s are in output, as to be a leader an equal element is also allowed on the right. side

        Input: arr = [5, 10, 20, 40]
        Output: [40]
        Explanation: When an array is sorted in increasing order, only the rightmost element is leader.

        Input: arr = [30, 10, 10, 5]
        Output: [30, 10, 10, 5]
        Explanation: When an array is sorted in non-increasing order, all elements are leaders.

        
    Constraints:
        1 <= arr.size() <= 106
        0 <= arr[i] <= 106

"""

## Naive/Brute Force Approach 
# Idea : Two loops, for 1st value outer check for numbers that are greater than 1st in the inner loop, If found 1st value is
# not the leader, If not found first value is the leader. -> move to 2nd value outer and check the same

class Solution:
    def leadersV1(self,arr):
        n = len(arr)
        result=[]
        
        for i in range(n):
            leader_flag = True
            for j in range(i+1,n):
                if arr[j] > arr[i]:
                    leader_flag = False
                    break

            if leader_flag:
                result.append(arr[i])

        return result
    
# naive approach
# Time : O(n^2)
# Space : O(1)

## V2   
# Idea : start from end -> check second last elemnt greater if yes append it too else move towards start
class Solution:
    def leadersV2(self,arr):
        n = len(arr)
        i = n-1
        result = [arr[i]]
        max_element = arr[i]

        while i > 0:
            if arr[i-1] > max_element:
                max_element = arr[i-1]
                result.append(arr[i-1])

            i -= 1
        result.reverse()
        return result
    
# V2
# Time : O(n)
# Space : O(n)

s1 = Solution()
res = s1.leadersV2(arr=[16, 17, 4, 3, 5, 2])
print(res)