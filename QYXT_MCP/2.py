# -*- coding: utf-8 -*-
'''
@File    : 2.py
@IDE     : PyCharm
@Author  : shihongyu
@Date    : 2026/04/28
@Describe: 
'''
from ast import main


class Solution:
    def divide(self, dividend: int, divisor: int) -> int:
        if divisor == 0:
            return 2**31-1
        max_ =  2**31-1
        sign = 1
        if (dividend < 0 and divisor > 0) or (dividend > 0 and divisor < 0):
            sign = -1
        dividend = abs(dividend)
        divisor = abs(divisor)
        if divisor > dividend:
            return 0

        result = 0

        while dividend >= divisor:
            temp = divisor
            multiple = 1
            while dividend >= temp + temp:
                temp = temp + temp
                multiple = multiple + multiple
            dividend -= temp
            result += multiple
        result = result * sign
        return result if result < max_ else max_

if __name__ == '__main__':
    c = Solution()
    print(c.divide(-2147483648, -1))