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
                                    # New file created to generate URLs automatically


from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
import urllib.parse
from selenium.webdriver.common.keys import Keys
from constants import *
from db_dao import fetch_zone_list, fetch_airline_name
import csv

Airline_Name= fetch_airline_name(Airline_id)

if not Airline_Name:
    print(f"Error fetching airline name for airline ID {Airline_id}: {Airline_Name}")
    exit()

print(f"Fetched Airline Name: {Airline_Name}")


def create_test_csv(Airline_ID, min_stops, max_stops):
    zone_list = fetch_zone_list(Airline_ID, min_stops, max_stops)
    
    if not zone_list:
        print("No zones found, exiting.")
        return False

    with open('test.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['origin', 'destination'])  # Header
        writer.writerows(zone_list)  # Writing data

    print("test.csv created successfully.")
    return True

# Function to generate the Google Flights URL
def create_google_flights_url(origin, destination):
    base_url = "https://www.google.com/travel/flights"
    params = {
        'q': f"{origin} to {destination}",
        'curr': 'USD'
    }
    return f"{base_url}?{urllib.parse.urlencode(params)}"
# First, create test.csv
if not create_test_csv(Airline_id, min_stops, max_stops):
    exit()
    

# Load data from CSV
data = pd.read_csv('test.csv')
    
def click_airline_filter(driver,  failed_zones, origin, destination):
    """
    Attempts to click on the airline filter in Google Flights.
    """
    try:
        filter_elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "nCOOmf"))
        )
        if len(filter_elements) > 1:
            airline_filter = filter_elements[1]
            airline_filter.click()
            return True
        else:
            raise Exception("Airline filter not found in the list of elements.")
    except Exception as e:
        print(f"Airline filter not found: {e}")
        failed_zones.append({'origin': origin, 'destination': destination})
        return False
    
def click_toggle_button(driver, failed_zones, origin, destination):
    try:
        toggle_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "xdLGIc"))
        )
        toggle_button.click()
        time.sleep(2)
        return True
    except Exception as e:
        print(f"Toggle button click failed: {e}")
        failed_zones.append({'origin': origin, 'destination': destination})
        return False

def selecting_desired_airline(driver, failed_zones, origin, destination, Airline_Name):
    try:
        airline_options = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "XfSiE"))
        )
        for option in airline_options:
            if Airline_Name in option.text:
                option.click()
                print(f"Selected the airline: {Airline_Name} for zone: {it}")
                return True
            else:
                raise Exception(f"Airline '{Airline_Name}' not found in the dropdown options.")
    except Exception as e:
        print(f"Failed to select the airline: {e}")
        failed_zones.append({'origin': origin, 'destination': destination})
        return False   

def input_date(driver, failed_zones, origin, destination, date, field_type):
    try:
        depart_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, f"//input[@aria-label='{field_type}']"))
        )
        driver.execute_script("arguments[0].value = '';", depart_element)
        depart_element.send_keys(date)
        depart_element.send_keys(Keys.RETURN)
        time.sleep(2)
        return True
    except Exception as e:
        print(f"Failed to enter the departure date: {e}")
        failed_zones.append({'origin': origin, 'destination': destination})
        return False

# Setup WebDriver options and driver
options = Options()
options.headless = False  # Set to True for headless mode
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

it = 0

# Loop through the data and generate URLs, then open and search each flight URL
for index, row in data.iterrows():
    it += 1
    origin = row['origin']
    destination = row['destination']
    cabin = 'Business'
       
    url = create_google_flights_url(origin, destination)
    driver.get(url)

    time.sleep(5)  # Adjust sleep time if necessary

    try:
        # #Selecting the cabin
        # if not click_the_cabin_button(driver, failed_zones, origin, destination):
        #     continue
        # if not selecting_desired_cabin(driver, failed_zones, origin, destination, cabin):
        #     continue
        
        # Clicking the airline filter
        if not click_airline_filter(driver, failed_zones, origin, destination):
            continue
        time.sleep(2)
            
        # Clicking the toggle button
        if not click_toggle_button(driver, failed_zones, origin, destination):
            continue

        # Selecting the desired airline name
        # if not selecting_desired_airline(driver, failed_zones, origin, destination, Airline_Name):
        #     continue
        try:
            airline_options = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "XfSiE"))
            )
            for option in airline_options:
                if Airline_Name in option.text:
                    option.click()
                    print(f"Selected the airline: {Airline_Name} for zone: {it}")
                    break
            else:
                raise Exception(f"Airline '{Airline_Name}' not found in the dropdown options.")
        except Exception as e:
            print(f"Failed to select the airline: {e}")
            failed_zones.append({'origin': origin, 'destination': destination})
            continue
        
        # Entering departure date
        if not input_date(driver, failed_zones, origin, destination, dept_date, "Departure"):
            continue
        
        # Entering return date
        if not input_date(driver, failed_zones, origin, destination, arrv_date, "Return"):
            continue
        
        # Get the updated URL
        updated_url = driver.current_url
        if cabin == 'Business':
            # Replace 'SAF' with 'SAN' if found in the URL
            if "SAF" in updated_url:
                updated_url = updated_url.replace("SAF", "SAN")
        
        print(f"URL generated for zone {it}: {updated_url}")
        url_results.append({
            'url': updated_url,
            'depart': origin,
            'arrive': destination,
            'cabin': cabin
        })

    except Exception as e:
        print(f"Failed to process zone {it}: {e}")
        failed_zones.append({'origin': origin, 'destination': destination})

# Save results and failed zones to CSV files
output_file = urls_file
failed_file = 'failed_zones_1.csv'

results_df = pd.DataFrame(url_results)
results_df.to_csv(output_file, index=False)

failed_zones_df = pd.DataFrame(failed_zones)
failed_zones_df.to_csv(failed_file, index=False)

print(f"Saved results to {output_file}")
print(f"Saved failed zones to {failed_file}")

# Close the browser after processing
driver.quit()