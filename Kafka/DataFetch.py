# -*- coding: utf-8 -*-
"""
Created on 2025-12-01 12:57:59

@author: Kate Yang
@description: This script is used for ...
"""

import akshare as ak

stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="601127", period="daily", start_date="20250301", end_date='20251129', adjust="")
print(stock_zh_a_hist_df)

