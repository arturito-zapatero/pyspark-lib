"""
Function that for rows where value of sum_revenue column is smaller than threshold_revenue changes  
originalCol value to 'minor_segment', otherwise changes nothing
"""


def convertMinority(
    threshold_revenue: [int, float],
    originalCol: str,
    sum_revenue: [int, float]
):
    if sum_revenue > threshold_revenue:
        return originalCol
    else:
        return 'minor_segment'
