# -*- coding: utf-8 -*-
"""
Function that for rows where value of sum_revenue column is smaller than threshold_revenue changes  
originalCol value to 'minor_segment', otherwise changes nothing
"""
def convertMinority(threshold_revenue,
                    originalCol,
                    sum_revenue):
    if sum_revenue > threshold_revenue:
        return originalCol
    else:
        return 'minor_segment'