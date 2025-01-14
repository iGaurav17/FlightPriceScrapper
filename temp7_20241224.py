from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import datetime, date, timedelta
import pandas as pd
#import cx_Oracle
import oracledb



# Making connection to the oracle database
#connection = cx_Oracle.connect('GAURAV_BHARDWAJ/XZFAauJ1ZTvhaa@192.168.64.137:1523/betadb')

#USING oracledb
connection = oracledb.connect(
    user="GAURAV_BHARDWAJ",
    password="XZFAauJ1ZTvhaa",
    dsn="192.168.64.137:1523/betadb"
)
# cursor object used to execute the SQL query
cursor = connection.cursor()



def format_date_for_oracle(dt):
    """Convert datetime/date objects to Oracle-compatible format"""
    if isinstance(dt, date):
        dt = datetime.combine(dt, datetime.min.time())
    return dt.strftime('%Y-%m-%d %H:%M:%S')


Airline_id = 750
Depart_Arpt_ID=""                #Need tp fetch it from DB
Arrv_Arpt_Id=""                  #Need to fetch it from DB
Sample_DateTime=format_date_for_oracle(date.today())               #date of the scrapping
Search_By_Cabin_Class=""         #Search option selected for the Cabin/Class of travel 
Dept_DateTime=format_date_for_oracle(date.today())                   #Date of departure 
Arrv_DateTime=format_date_for_oracle(date.today())                   #Date of landing  
Fare_Class="2301"                    
Fare=0                 #Price of the 
Aircraft=""
Flight_Number1=1                 # 1 since flight is non stop                             #NOT NULL        
Flight_Number2=0
Flight_Number3=0
Flight_Number4=0                            
Operating_Airline1="Oman Air" 
Operating_Airline2=""
Operating_Airline3=""
Operating_Airline4=""             
Connecting_Arpt1=0   
Connecting_Arpt2=0 
Connecting_Arpt3=0                   
Stops1=0                          #zero since flight is non stop
Stops2=0  
Stops3=0  
Stops4=0  
Availability_Comment=""           
Economy_Seat_Available=0
Economy_Fare=""
EXECUTIVE_SEAT_AVAILABLE=0    
EXECUTIVE_FARE=0          
INTL_ECONOMY_LOWEST_AVAIL=1
INTL_ECONOMY_FLEXIBLE_AVAIL=0                
INTL_EXECUTIVE_LOWEST_AVAIL=0                
INTL_EXECUTIVE_FLEXIBLE_AVAIL=0
Economy_Fare1=0
Economy_Fare2=0
Economy_Fare3=0
Economy_Fare4=0
Economy_Fare5=0
Executive_Fare1=0
Executive_Fare2=0
Airline_Name="Oman Air"


# List of URLs to process
# urls = [
#     {
#         "url": "https://www.google.com/travel/flights/search?tfs=CBwQAhoiEgoyMDI1LTEwLTAxMgJXWWoHCAESA01DVHIHCAESA0ZSQRoiEgoyMDI1LTEwLTA0MgJXWWoHCAESA0ZSQXIHCAESA01DVEABSAFwAYIBCwj___________8BmAEB&hl=en&gl=IN&curr=USD",
#         "depart": "MCT",
#         "arrive": "FRA"
#     },
#     {
#         "url": "https://www.google.com/travel/flights/search?tfs=CBwQAhoiEgoyMDI1LTEwLTAxMgJXWWoHCAESA01DVHIHCAESA1NMTBoiEgoyMDI1LTEwLTA0MgJXWWoHCAESA1NMTHIHCAESA01DVEABSAFwAYIBCwj___________8BmAEB&hl=en&gl=IN&curr=USD",
#         "depart": "MCT",
#         "arrive": "SLL"
#     }
# ]

urls_df = pd.read_csv('urls.csv')

def fetch_max_id(cursor):
    """
    Fetch the maximum ID currently in the Flight_Availability table.
    """
    try:
        query = "SELECT MAX(ID) FROM Flight_Availability"
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result[0] is not None else 0
    except Exception as e:
        print(f"Error fetching max ID: {e}")
        return 0

def fetch_airport_id(cursor, Depart_Arpt_code):
    """
    Fetch the airport ID from the database for a given airport code.
    
    Args:
        cursor: Database cursor object.
        airport_code (str): The code of the airport to fetch the ID for.

    Returns:
        int: The ID of the airport if found, otherwise None.
    """
    try:
        query = "SELECT ID FROM airport WHERE code = :code"
        cursor.execute(query, {"code": Depart_Arpt_code})
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            print(f"No airport found with code: {Depart_Arpt_code}")
            return None
    except Exception as e:
        print(f"Error fetching airport ID for code {Depart_Arpt_code}: {e}")
        return None


def Save_data_To_Excel(prices, filename):
    # Save the data to an Excel file
    df = pd.DataFrame(prices, columns=["id", "Airline_id", "Dept_Arpt_ID", "Arrv_Arpt_Id", "Sample_DateTime", 
                                        "Search_By_Cabin_Class", "Dept_DateTime", "Arrv_DateTime", "Fare_Class", 
                                       "price", "Aircraft", "Flight_Number1", "Flight_Number2", "Flight_Number3", "Flight_Number4",
                                       "Operating_Airline1", "Operating_Airline2", "Operating_Airline3", "Operating_Airline4",
                                       "Connecting_Arpt1", "Connecting_Arpt2", "Connecting_Arpt3",
                                       "Stops1", "Stops2", "Stops3", "Stops4", "Availability_Comment", "Economy_Seat_Available",
                                       "ECONOMY_FARE", "EXECUTIVE_SEAT_AVAILABLE", "EXECUTIVE_FARE", 
                                       "INTL_ECONOMY_LOWEST_AVAIL", "INTL_ECONOMY_FLEXIBLE_AVAIL", 
                                       "INTL_EXECUTIVE_LOWEST_AVAIL", "INTL_EXECUTIVE_FLEXIBLE_AVAIL",
                                       "Economy_Fare1", "Economy_Fare2", "Economy_Fare3", "Economy_Fare4", "Economy_Fare5",
                                       "Executive_Fare1", "Executive_Fare2", "Airline_Name"])
        
    df.to_excel(filename, index=False)


# Fetch the max ID before starting the process
current_max_id = fetch_max_id(cursor)
print(f"Max id: {current_max_id}")
id = current_max_id + 1  # Start with the next available ID
#print(f"Sample Date: {Sample_DateTime}\n")


all_prices = []
#Main function
# Iterate over each URL
for index, row in urls_df.iterrows():
    url = row['url']                          #Fetched from the url.csv
    Depart_Arpt_code = row['depart']          #Fetched from the url.csv
    Arrv_Arpt_code = row['arrive']            #Fetched from the url.csv
    Search_By_Cabin_Class = row['cabin']      #Fetched from the url.csv 
    
    print(f"Starting process for URL {index+ 1}: {Depart_Arpt_code}-{Arrv_Arpt_code}\n")
    
    try:
        # Configure WebDriver with WebDriver Manager
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

        for i in range(280):  # Adjust the range for desired iterations
            id+1
            print(f"Iteration {i + 1}: Clicking the button...")

            try:
                # Wait for the button to become clickable
                button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "Tmm8n"))
                )
                button.click()  # Click the button
                print("Button clicked successfully!")
            except Exception as e:
                print(f"Button click failed: {e}")
            
            # Extract Date of Departure
            try:
                Date_element = WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "GYgkab"))
                )
                current_Date = Date_element.get_attribute("data-value")
                
                print(f"{current_Date}")
            except Exception as e: 
                print(f"Error extracting date on iteration {i + 1}: {e}")

            # Optional: Pause to let the page update
            time.sleep(4)

            # Extract price
            try:
                price_element = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "hXU5Ud"))
                )
                price = price_element.get_attribute("aria-label")
                price = price.replace("US dollars", "").strip()
            except Exception as e: 
                price = "0"  # Default value if price is not found
                print(f"Price not available for: {current_Date}")
                #print(f"Error extracting price on iteration {i + 1}: {e}")

            
            # Storing the zone, date and price
            #flight_date = current_date.strftime("%Y-%m-%d")
            flight_date=current_Date
            Dept_DateTime= flight_date         
            Arrv_DateTime= flight_date
            

            Dept_Arpt_ID = fetch_airport_id(cursor, Depart_Arpt_code)
            Arrv_Arpt_Id = fetch_airport_id(cursor, Arrv_Arpt_code)
            

            prices.append((id, Airline_id, Dept_Arpt_ID, Arrv_Arpt_Id, Sample_DateTime,    #5
                           Search_By_Cabin_Class, Dept_DateTime, Arrv_DateTime, Fare_Class, price,  #10 
                           Aircraft, Flight_Number1, Flight_Number2, Flight_Number3, Flight_Number4, #15
                           Operating_Airline1, Operating_Airline2, Operating_Airline3, Operating_Airline4, Connecting_Arpt1, #20 
                           Connecting_Arpt2, Connecting_Arpt3, Stops1, Stops2, Stops3, #25
                           Stops4, Availability_Comment, Economy_Seat_Available, Economy_Fare, EXECUTIVE_SEAT_AVAILABLE, #30
                           EXECUTIVE_FARE,INTL_ECONOMY_LOWEST_AVAIL, INTL_ECONOMY_FLEXIBLE_AVAIL, INTL_EXECUTIVE_LOWEST_AVAIL,INTL_EXECUTIVE_FLEXIBLE_AVAIL, #35
                           Economy_Fare1, Economy_Fare2, Economy_Fare3, Economy_Fare4, Economy_Fare5, #40
                           Executive_Fare1, Executive_Fare2, Airline_Name #43
                        ))
            
            # for price_data in prices:
            #     insert_into_flight_availability(cursor, *price_data)

            #prices.append((zone, flight_date, price))
            print(f"Stored: Zone: {Depart_Arpt_code}-{Arrv_Arpt_code}, Stored: Date: {flight_date}, Price: {price}")

        
        all_prices.extend(prices)

    except Exception as e:
        print(f"Error during the process for URL {index+1}: {e}\n")

    finally:
        # Close the browser
        driver.quit()
        print("Browser closed.\n")
        
        
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")   
filename = f"flight_prices_{timestamp}.xlsx"    
Save_data_To_Excel(all_prices, filename)
print(f"All data saved to {filename}\n")

cursor.close()
connection.close()


