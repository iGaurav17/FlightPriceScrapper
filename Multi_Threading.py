#  Created on 07-Jan-2025.
 
#   ####################################################################
#   Copyright (c) 2025 Optiontown, Inc. All Rights Reserved.
 
#   This software without any limitations is strictly confidential and
#   proprietary to Optiontown, Inc. Any unlawful copying, disclosure
#   or use shall be vigorously prosecuted.
#   ####################################################################
#   Changes (from 07-Jan-2025)
#   ####################################################################
  
#   07-Jan-2025 Gaurav Bhardwaj   : Bug 30535 - Google Flight Price Scrapper
                                    # Added separate db_dao.py
                                    # Added seperate db_Properties.py
                                    # Added separate constants.py
                                    # Added Requirements.txt
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import datetime, date, timedelta
import pandas as pd
import oracledb
from openpyxl import load_workbook, Workbook
import threading
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from db_dao import *
from db_Properties import DB_CONFIG
from constants import *

connection, cursor = connect_to_db(DB_CONFIG)

urls_df = pd.read_csv('urls1.csv')

def Save_data_To_Excel(prices, filename):
    # Save the data to an Excel file
    df = pd.DataFrame(prices, columns=COLUMN_NAMES)        
    df.to_excel(filename, index=False)
    
def Save_data_To_CSV(prices, filename):
    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(prices, columns=COLUMN_NAMES)
    df.to_csv(filename, index=False, encoding="utf-8")
    
def wait_for_price_update(driver, initial_price, timeout=30):
    """
    Waits for the price element to update from the initial price.
    
    Args:
        driver: WebDriver instance.
        initial_price (str): The initial price displayed on the page.
        timeout (int): Maximum time to wait for the price to update (in seconds).
    
    Returns:
        str: Updated price or the initial price if timeout occurs.
    """
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.find_element(By.CLASS_NAME, "hXU5Ud").get_attribute("aria-label") != initial_price
        )
        # Fetch the updated price
        updated_price_element = driver.find_element(By.CLASS_NAME, "hXU5Ud")
        updated_price = updated_price_element.get_attribute("aria-label")
        return updated_price
    except Exception as e:
        print(f"Timeout waiting for price update")
        return initial_price

def Select_The_Cheapest_Button(driver):
    try:   
        # Wait for the div to become clickable
        clickable_div = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "M7sBEb"))
        )
        clickable_div.click()  # Click the div
        print("Cheapest Button (div) clicked successfully!")
    except Exception as e:
        print(f"Cheapest Button click failed: {e}")
   
def click_date_navigation_button(driver,class_name, timeout=10):
    try:
        # Wait for the button to become clickable
        button = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.CLASS_NAME, class_name))
            )
        button.click()  
        return True
    except Exception as e:
        print(f"Button click failed: {e}")
        return False
    
def extract_departure_date(driver,last_fetched_date, temp_date, class_name="GYgkab", retry_limit=2, timeout=10):
    retry_counter = 0
    while retry_counter <= retry_limit:
        try:
            # Wait until the element contains a valid, non-empty value
            WebDriverWait(driver, timeout).until(
                lambda d: (element := d.find_element(By.CLASS_NAME, class_name)).get_attribute("data-value").strip() != ""
            )
            current_date = driver.find_element(By.CLASS_NAME, class_name).get_attribute("data-value")
            
            
            if current_date!=last_fetched_date: 
                return current_date, current_date, True
        except (TimeoutException, NoSuchElementException) as e:
            print(f"Error extracting date: {e}. Retrying ({retry_counter + 1}/{retry_limit})...")
        retry_counter += 1
    
    if retry_counter > retry_limit:
        print(f"Failed to extract date, falling back to temp_date: {temp_date}")
        temp_date = (datetime.strptime(temp_date, "%d-%m-%Y") - timedelta(days=1)).strftime("%d-%m-%Y")
        return temp_date, temp_date, False

def extract_price(driver, price_class="hXU5Ud", timeout=10):
    try:
        # Fetch initial price
        
        price_element = WebDriverWait(driver, timeout).until(
            
            EC.presence_of_element_located((By.CLASS_NAME, price_class))
        )
        initial_price = price_element.get_attribute("aria-label")
        initial_price = int(float(initial_price.replace("US dollars", "").strip()))
        
        # Wait for price update
        updated_price = wait_for_price_update(driver, initial_price, timeout=timeout)
        updated_price = int(float(updated_price.replace("US dollars", "").strip()))
        
        print(f"Price updated from {initial_price} to {updated_price}")
        
        # Return the minimum of the two prices
        price = min(initial_price, updated_price)
        print(f"Considered Price: {price}")
        return price, True
    except Exception as e:
        print(f"Error extracting price")
        return 0, False

    
id_lock = Lock()
        
def Scrapping_Data_For_Each_URL(shared_id, index, url, Depart_Arpt_code, Arrv_Arpt_code, Search_By_Cabin_Class):
    global id_lock
    global failed_zones
    print(f"Starting process for URL {index+ 1}: {Depart_Arpt_code}-{Arrv_Arpt_code}\n")
    
    try:
        
        #Fetching Depart and Arrive airport id from Airport table
        try:
            Dept_Arpt_ID = fetch_airport_id(DB_CONFIG,cursor, Depart_Arpt_code)
            Arrv_Arpt_Id = fetch_airport_id(DB_CONFIG,cursor, Arrv_Arpt_code)
            print(f"Fetched Depart Airport ID: {Dept_Arpt_ID}, Arrive Airport ID: {Arrv_Arpt_Id}")
            if not Arrv_Arpt_Id or not Dept_Arpt_ID:
                raise ValueError("Failed to fetch airport ID")
            else:
                passed_zones.append({"Depart_Arpt_code": Depart_Arpt_code, "Arrv_Arpt_code": Arrv_Arpt_code})
        except Exception as e:
            print(f"Error fetching airport IDs for {Depart_Arpt_code}-{Arrv_Arpt_code}: {e}")
            failed_zones.append({"Depart_Arpt_code": Depart_Arpt_code, "Arrv_Arpt_code": Arrv_Arpt_code})
            return  # Exit early since we cannot proceed without airport IDs
        
        
        #Fetching cabin_ID from cabin table
        try:
            cabin_id = fetch_cabin_id(DB_CONFIG, cursor, Search_By_Cabin_Class)
            print(f"Fetched Cabin ID: {cabin_id}")
            if not cabin_id:
                raise ValueError("Failed to fetch cabin ID")
        except Exception as e:
            print(f"Error fetching cabin ID for {Depart_Arpt_code}-{Arrv_Arpt_code}: {e}")
            return  # Exit early since we cannot proceed without cabin ID
        
        #Fetching Currency ID from currency table
        try:
            currency_ID = fetch_currency_id(DB_CONFIG, cursor, "USD")
            print(f"Fetched Currency ID: {currency_ID}")
            if not currency_ID:
                raise ValueError("Failed to fetch currency ID")
        except Exception as e:
            print(f"Error fetching currency ID for {Depart_Arpt_code}-{Arrv_Arpt_code}: {e}")
            return  # Exit early since we cannot proceed without currency ID
        
        #Fetching Zone id from from zone airport
        try:
            zone_id = fetch_zone_id(DB_CONFIG, cursor, Depart_Arpt_code, Arrv_Arpt_code)
            print(f"Fetched Zone ID: {zone_id}")
            if not zone_id:
                raise ValueError("Failed to fetch zone ID")
        except Exception as e:
            print(f"Error fetching zone ID for {Depart_Arpt_code}-{Arrv_Arpt_code}: {e}")
            return  # Exit early since we cannot proceed without zone ID
        
        # Initialize Chrome WebDriver
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920x1080")
        driver = webdriver.Chrome(options=chrome_options)
        options = Options()
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        # Automatically manage ChromeDriver installation
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # Navigate to the URL
        driver.get(url)

        # Allow the page to load
        time.sleep(3)
    
        prices = []  # Initialize an empty list to store prices and dates
        
        #Clicking the cheapest button
        Select_The_Cheapest_Button(driver)
        last_fetched_date = None
        temp_date=None
        failure_counter = 0
        for i in range(days):  # Adjust the range for desired iterations
            print(f"Iteration {i + 1} for {Depart_Arpt_code}-{Arrv_Arpt_code}")
            
            if failure_counter >= 3:
                print("Button click failed 3 times. Ending scraping for this URL.")
                break
            
            with id_lock:
                unique_id = shared_id[0]
                shared_id[0] += 1
            
            #Clicking the button to change 
            if not click_date_navigation_button(driver, "Tmm8n", timeout=10):
                failure_counter += 1
                continue
            else:
                failure_counter = 0
            
            # Extract Date of Departure
                current_Date, temp_date, success = extract_departure_date(driver, last_fetched_date, temp_date)

                if not success:
                    print(f"Using fallback date: {current_Date} for iteration {i + 1}")
                    i-=1
                    continue
                else:
                    print(f"Extracted new date: {current_Date}")
            
            # Extract price
                price, success = extract_price(driver)

                if not success:
                    print(f"Price not available for: {current_Date}")
                else:
                    pass
                    
            time.sleep(5)
            driver.refresh()

            flight_date=current_Date
            Dept_DateTime= current_Date         
            Arrv_DateTime= current_Date
        
            prices.append((unique_id, Airline_id, Dept_Arpt_ID, Arrv_Arpt_Id, Sample_DateTime,    #5
                           Search_By_Cabin_Class, Dept_DateTime, Arrv_DateTime, Fare_Class, price,  #10 
                           Aircraft, Flight_Number1, Flight_Number2, Flight_Number3, Flight_Number4, #15
                           Operating_Airline1, Operating_Airline2, Operating_Airline3, Operating_Airline4, Connecting_Arpt1, #20 
                           Connecting_Arpt2, Connecting_Arpt3, Stops1, Stops2, Stops3, #25
                           Stops4, Availability_Comment, Economy_Seat_Available, Economy_Fare, EXECUTIVE_SEAT_AVAILABLE, #30
                           EXECUTIVE_FARE,INTL_ECONOMY_LOWEST_AVAIL, INTL_ECONOMY_FLEXIBLE_AVAIL, INTL_EXECUTIVE_LOWEST_AVAIL,INTL_EXECUTIVE_FLEXIBLE_AVAIL, #35
                           Economy_Fare1, Economy_Fare2, Economy_Fare3, Economy_Fare4, Economy_Fare5, #40
                           Executive_Fare1, Executive_Fare2, Airline_Name,currency_id,zone_id, cabin_id #46
                        ))
            print(f"Stored: Zone: {Depart_Arpt_code}-{Arrv_Arpt_code}, Stored: Date: {flight_date}, Price: {price}")
            
        all_prices.extend(prices)
        rows_inserted=0
        for price_data in prices:
            try:
                if price_data[6] == "1900-01-01":  # Assuming the 6th index in price_data corresponds to Dept_DateTime
                    print(f"Skipping record with invalid date: {price_data[6]}")
                    continue
                if insert_into_flight_availability(DB_CONFIG,cursor, *price_data):
                    rows_inserted+=1
            except Exception as e:
                pass
        print(f"{rows_inserted} rows inserted into flight availability table for zone: {Depart_Arpt_code}-{Arrv_Arpt_code}\n")

    except Exception as e:
        print(f"Error during the process for URL {index+1}: {e}\n")

    finally:
        # Close the browser
        driver.quit()
        print("Browser closed.\n")

#Main function
def main():
    current_max_id = fetch_max_id(DB_CONFIG,cursor)
    shared_id = [current_max_id + 1]
    # Create a thread pool to manage the concurrent scraping tasks
    with ThreadPoolExecutor(max_workers=Concurrent_urls)as executor:
        for index, row in urls_df.iterrows():
            url = row['url']                          #Fetched from the url.csv
            Depart_Arpt_code = row['depart']          #Fetched from the url.csv
            Arrv_Arpt_code = row['arrive']            #Fetched from the url.csv
            Search_By_Cabin_Class = row['cabin']      #Fetched from the url.csv
        # Submit the scraping function as a task to be executed by a thread
            executor.submit(Scrapping_Data_For_Each_URL, shared_id, index, url, Depart_Arpt_code, Arrv_Arpt_code, Search_By_Cabin_Class)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")        
    #csv_fileName=f"flight_prices_{timestamp}.csv"
    #Save_data_To_CSV(all_prices, csv_fileName)
    excel_filename = f"flight_prices_{timestamp}.xlsx"
    Save_data_To_Excel(all_prices, excel_filename)
    print(f"All data saved to {excel_filename}\n")
    
    # Print failed zones
    if failed_zones:
        print("Scraping failed for the following zones:")
        for zone in failed_zones:
            print(f"Depart: {zone['Depart_Arpt_code']}, Arrive: {zone['Arrv_Arpt_code']}")
    if passed_zones:
        print("Scraping completed for the following zones:")
        for zone in passed_zones:
            print(f"Depart: {zone['Depart_Arpt_code']}, Arrive: {zone['Arrv_Arpt_code']}")
    
    if not failed_zones:
        print("Scraping completed for all zones successfully!")

    cursor.close()
    connection.close()
    
if __name__ == "__main__":
    main()
    
