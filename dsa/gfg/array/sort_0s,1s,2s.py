"""
    Given an array arr[] containing only 0s, 1s, and 2s. Sort the array in ascending order.

    You need to solve this problem without utilizing the built-in sort function.

    Examples:

        Input: arr[] = [0, 1, 2, 0, 1, 2]
        Output: [0, 0, 1, 1, 2, 2]
        Explanation: 0s 1s and 2s are segregated into ascending order.
        Input: arr[] = [0, 1, 1, 0, 1, 2, 1, 2, 0, 0, 0, 1]
        Output: [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2]
        Explanation: 0s 1s and 2s are segregated into ascending order.

    Follow up: Could you come up with a one-pass algorithm using only constant extra space?
    
    Constraints:
        1 <= arr.size() <= 106
        0 <= arr[i] <= 2

"""

## Naive/Brute Force Approach 
# IDEA : Using .sort() method

class Solution:
    def sort012(self, arr : list[int]):

        arr.sort()

        return arr

# naive approach
# Time : O(nlogn)
# Space : O(1)

## V1
# IDEA : Use 3 variables(for 0 , 1 and 2) and set their counts from the arr
#        Iterate the arr and update the values of the list


    def sort012V1(self, arr : list[int]):

        c0 = 0
        c1 = 0
        c2 = 0

        for item in arr:
            if item == 0:
                c0 += 1
            elif item == 1:
                c1 += 1
            else:
                c2 += 1

        idx = 0

        for i in range(c0):
            arr[idx] = 0
            idx += 1
        for i in range(c1):
            arr[idx] = 1
            idx += 1

        for i in range(c2):
            arr[idx] = 2                 
            idx += 1

        return arr

# V1 approach
# Time : O(n)
# Space : O(1)

## V2
# IDEA : Using DNF algorithm (Dutch National Flag)
s = Solution()
print(s.sort012([2,0,2,1,1,0]))

print(s.sort012V1([2,0,2,1,1,0]))


## V2