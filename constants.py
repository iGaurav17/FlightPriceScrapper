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
                                    # Added new constants for Auto_url file
                                    # Removed Airline_Name constant

from datetime import date, datetime
from db_dao import fetch_airline_name

def format_date_for_oracle(dt):
    """Convert datetime/date objects to Oracle-compatible format."""
    if isinstance(dt, date):
        dt = datetime.combine(dt, datetime.min.time())
    return dt.strftime('%Y-%m-%d %H:%M:%S')

days=350     #No of days for which data is to be scrapped
Concurrent_urls=5          #No of concurrent urls to be scrapped

# Airline-related constants
Airline_id = 148

urls_file = 'urls_to_parse.csv'

min_stops=0               # Mark it zero for nonstop zones.
max_stops=0               # Mark it zero for nonstop zones.
url_results = []    # Lists to store results 
failed_zones = []  # Lists to store failed attempts

dept_date="Nov 6"
arrv_date ="Nov 23"

# Default values (can be updated dynamically)
Depart_Arpt_ID = ""
Arrv_Arpt_Id = ""
Sample_DateTime = format_date_for_oracle(date.today())
Search_By_Cabin_Class = ""
Dept_DateTime = format_date_for_oracle(date.today())
Arrv_DateTime = format_date_for_oracle(date.today())
Fare_Class = "2301"
Fare = 0
Aircraft = ""
Code="USD"

# Flight number and stops
Flight_Number1 = 1
Flight_Number2 = 0
Flight_Number3 = 0
Flight_Number4 = 0
Stops1 = 0
Stops2 = 0
Stops3 = 0
Stops4 = 0

# Operating airlines
Operating_Airline1 =fetch_airline_name(Airline_id)
Operating_Airline2 = ""
Operating_Airline3 = ""
Operating_Airline4 = ""

# Connecting airports
Connecting_Arpt1 = 0
Connecting_Arpt2 = 0
Connecting_Arpt3 = 0

# Availability and fare details
Availability_Comment = ""
Economy_Seat_Available = 0
Economy_Fare = ""
EXECUTIVE_SEAT_AVAILABLE = 0
EXECUTIVE_FARE = 0
INTL_ECONOMY_LOWEST_AVAIL = 1
INTL_ECONOMY_FLEXIBLE_AVAIL = 0
INTL_EXECUTIVE_LOWEST_AVAIL = 0
INTL_EXECUTIVE_FLEXIBLE_AVAIL = 0
Economy_Fare1 = 0
Economy_Fare2 = 0
Economy_Fare3 = 0
Economy_Fare4 = 0
Economy_Fare5 = 0
Executive_Fare1 = 0
Executive_Fare2 = 0

#new columns in flight_availability table
currency_id = 1                # 1 for usd
zone_id = 15090136             
cabin_id=1



# COLUMN_NAMES for storing data into files
COLUMN_NAMES = [
    "id", "Airline_id", "Dept_Arpt_ID", "Arrv_Arpt_Id", "Sample_DateTime",
    "Search_By_Cabin_Class", "Dept_DateTime", "Arrv_DateTime", "Fare_Class",
    "price", "Aircraft", "Flight_Number1", "Flight_Number2", "Flight_Number3", "Flight_Number4",
    "Operating_Airline1", "Operating_Airline2", "Operating_Airline3", "Operating_Airline4",
    "Connecting_Arpt1", "Connecting_Arpt2", "Connecting_Arpt3",
    "Stops1", "Stops2", "Stops3", "Stops4", "Availability_Comment", "Economy_Seat_Available",
    "ECONOMY_FARE", "EXECUTIVE_SEAT_AVAILABLE", "EXECUTIVE_FARE",
    "INTL_ECONOMY_LOWEST_AVAIL", "INTL_ECONOMY_FLEXIBLE_AVAIL",
    "INTL_EXECUTIVE_LOWEST_AVAIL", "INTL_EXECUTIVE_FLEXIBLE_AVAIL",
    "Economy_Fare1", "Economy_Fare2", "Economy_Fare3", "Economy_Fare4", "Economy_Fare5",
    "Executive_Fare1", "Executive_Fare2", "Airline_Name", "Currency_Id","Zone_Id", "Cabin_id"
]

# list for managing list of those zones for which scrapping failed
failed_zones =[]


# list for managing list of those zones for which scrapping was successful
passed_zones =[]
all_prices = []

