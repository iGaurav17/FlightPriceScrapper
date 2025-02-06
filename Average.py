#  Created on 07-Jan-2025.
 
#   ####################################################################
#   Copyright (c) 2025 Optiontown, Inc. All Rights Reserved.
 
#   This software without any limitations is strictly confidential and
#   proprietary to Optiontown, Inc. Any unlawful copying, disclosure
#   or use shall be vigorously prosecuted.
#   ####################################################################
#   Changes (from 07-Jan-2025)
#   ####################################################################
  
#   05-Feb-2025 Gaurav Bhardwaj   : Bug 30535 - Google Flight Price Scrapper
                                    # New file created to calculate the average fares zone wise


from db_dao import export_avg_fare_to_excel
from constants import *
avg_airline_id = 150
try:
    export_avg_fare_to_excel("average_fares.xlsx",avg_airline_id)
except Exception as e:
    print(f"Error exporting average fare to Excel: {e}")
    exit()