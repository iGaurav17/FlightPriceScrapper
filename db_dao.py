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
                                    # Add new functions to fetch Airline Name
                                    # Add new functions to fetch zone_list and calculate average fare respective to zones
import oracledb
from db_Properties import DB_CONFIG
import pandas as pd

def connect_to_db(config):
    try:
        connection = oracledb.connect(**config)
        cursor = connection.cursor()
        return connection, cursor
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

def release_connection(connection, cursor):

    try:
        # Check if the cursor is open before closing it
        if cursor:
            cursor.close()
        else:
            print("Cursor was already closed or never opened.")
        if connection:
            connection.close()
            connection =None
        else:
            print("Connection was already closed.")

    except Exception as e:
        print(f"Error releasing database connection: {e}")
        
def fetch_max_id(config, cursor):
    """
    Fetch the maximum ID from the Flight_Availability table.

    Args:
        cursor: Database cursor.

    Returns:
        int: Maximum ID or 0 if the table is empty.
    """
    try:
        connection, cursor = connect_to_db(config)
        cursor.execute("SELECT MAX(ID) FROM Flight_Availability")
        result = cursor.fetchone()
        max_id= result[0] if result[0] is not None else 0
        release_connection(connection, cursor)
        return max_id
    except Exception as e:
        print(f"Error fetching max ID: {e}")
        return 0

def fetch_cabin_id(config,cursor, cabin):
    try:
        connection, cursor =connect_to_db(config)
        cursor.execute("SELECT ID FROM Cabin WHERE Name = :cabin", {"cabin": cabin})
        result = cursor.fetchone()
        cabin_id = result[0] if result else None
        release_connection(connection, cursor)
        return cabin_id
    except Exception as e:
        print(f"Error fetching cabin ID: {e}")
        return None
        
def fetch_currency_id(config, cursor, code):
    try:
        connection, cursor = connect_to_db(config)
        cursor.execute("SELECT ID FROM Currency WHERE Code = :code", {"code": code})
        result = cursor.fetchone()
        currency_id = result[0] if result else None
        release_connection(connection, cursor)
        return currency_id
    except Exception as e:
        print(f"Error fetching currency ID: {e}")
        return None
    

def fetch_airport_id(config, cursor, airport_code):
    """
    Fetch the airport ID from the database for a given airport code.

    Args:
        cursor: Database cursor.
        airport_code (str): Airport code to look up.

    Returns:
        int: Airport ID if found, None otherwise.
    """
    try:
        connection, cursor = connect_to_db(config)
        cursor.execute("SELECT ID FROM airport WHERE code = :code", {"code": airport_code})
        result = cursor.fetchone()
        airport_id= result[0] if result else None
        release_connection(connection, cursor)
        return airport_id
    except Exception as e:
        print(f"Error fetching airport ID for code {airport_code}: {e}")
        return None


def fetch_zone_id(config, cursor, Depart_Arpt_code, arrv_arpt_code):
    """
    Fetch the zone ID from the database for a given departure and arrival airport codes.
    """
    try:
        connection, cursor = connect_to_db(config)
        cursor.execute("""
            SELECT ZONE_ID
            FROM Zone_Airport
            WHERE Depart_Airport_Set = :dept_arpt_code
            AND ARRIVE_AIRPORT_SET = :arrv_arpt_code
        """, {"dept_arpt_code": Depart_Arpt_code, "arrv_arpt_code": arrv_arpt_code})
        result = cursor.fetchone()
        zone_id = result[0] if result else None
        release_connection(connection, cursor)
        return zone_id
    except Exception as e:
        print(f"Error fetching zone ID for departure {Depart_Arpt_code} and arrival {arrv_arpt_code}: {e}")
        return None

def fetch_airline_name(Airline_id):
    """
    Fetch the airline name from the database for a given airline ID.

    """
    try:
        connection, cursor = connect_to_db(DB_CONFIG)
        
        cursor.execute("SELECT Name FROM Airline WHERE ID = :id", {"id": Airline_id})
        row = cursor.fetchone()
        airline_name = row[0] if row else None
        release_connection(connection, cursor)
        return airline_name
    except Exception as e:
        print(f"Error fetching airline name: {e}")
        raise e

def insert_into_flight_availability(config, cursor, *data):
    """        
    Insert data into the Flight_Availability table.

    Args:
        cursor: Database cursor.
        data: Tuple containing the row values to insert.
    """
    check_query = """
    SELECT COUNT(*)
    FROM Flight_Availability
    WHERE Dept_DateTime = TO_DATE(:1, 'YYYY-MM-DD HH24:MI:SS')
      AND Dept_Arpt_ID = :2
      AND Arrv_Arpt_ID = :3
      AND CABIN_ID = :4
    """
    query = """
    INSERT INTO Flight_Availability (
        ID, Airline_id, Dept_Arpt_ID, Arrv_Arpt_Id, Sample_DateTime, Search_By_Cabin_Class, Dept_DateTime,
        Arrv_DateTime, Fare_Class, fare, Aircraft, Flight_Number1, Flight_Number2, Flight_Number3,
        Flight_Number4, Operating_Airline1, Operating_Airline2, Operating_Airline3, Operating_Airline4,
        Connecting_Arpt1, Connecting_Arpt2, Connecting_Arpt3, Stops1, Stops2, Stops3, Stops4,
        Availability_Comment, Economy_Seat_Available, Economy_Fare, EXECUTIVE_SEAT_AVAILABLE, EXECUTIVE_FARE,
        INTL_ECONOMY_LOWEST_AVAIL, INTL_ECONOMY_FLEXIBLE_AVAIL, INTL_EXECUTIVE_LOWEST_AVAIL,
        INTL_EXECUTIVE_FLEXIBLE_AVAIL, Economy_Fare1, Economy_Fare2, Economy_Fare3, Economy_Fare4,
        Economy_Fare5, Executive_Fare1, Executive_Fare2, Airline_Name,currency_id, zone_id, cabin_id
    ) VALUES (
        :1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD HH24:MI:SS'), :6, TO_DATE(:7, 'YYYY-MM-DD HH24:MI:SS'),
        TO_DATE(:8, 'YYYY-MM-DD HH24:MI:SS'), :9, :10, :11, :12, :13, :14, 
        :15, :16, :17, :18, :19,
        :20, :21, :22, :23, :24, :25, :26, 
        :27, :28, :29, :30, :31, 
        :32, :33, :34, 
        :35, :36, :37, :38, :39,
        :40, :41, :42, :43, :44, :45 , :46   )
    """
        
    try:
        connection, cursor = connect_to_db(config)
        # Check for existing Dept_DateTime
        try:
            cursor.execute(check_query, [data[6], data[2], data[3], data[45]])  # Index 6 is the position of Dept_DateTime in the data tuple
            print("Check query executing successfully")
        except Exception as e:
            print(f"Error executing check query: {e}")
            release_connection(connection, cursor)
            return False
        
        result = cursor.fetchone()
        if result[0] > 0:
            print(f"Record with Dept_DateTime {data[6]} already exists. Skipping insertion.")
            release_connection(connection, cursor)
            return False
        cursor.execute(query, data)
        cursor.connection.commit()
        print(f"Data inserted successfully")
        release_connection(connection, cursor)
        return True
    except Exception as e:
        print(f"Error inserting data: {e}")
        release_connection(connection, cursor)
        return False
    
    
def fetch_zone_list(Airline_ID, min_stops, max_stops):
    connection, cursor = connect_to_db(DB_CONFIG)
    try:
        query= """
            SELECT ZA.Depart_Airport_Set, ZA.Arrive_Airport_Set
            FROM Zone_Airport ZA, Zone Z, Zone_Cabin ZC
            WHERE ZA.Zone_ID = ZC.Zone_ID 
            AND ZA.Airline_ID = :1
            AND ZC.Cabin_ID = 1 AND ZC.is_Valid = 1
            AND Z.Id = ZA.Zone_ID
            AND ZA.Min_Stops = :2
            AND ZA.Max_Stops = :3
            AND Z.Zone_Category_ID IN (1, 2)
            AND Z.Id NOT IN (
                SELECT Zone_ID 
                FROM Flight_Availability 
                GROUP BY Zone_ID 
                HAVING COUNT(ID) > 1000
            )
        """
        cursor.execute(query, (Airline_ID, min_stops, max_stops))
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        return rows
    except Exception as e:
        print(f"Error fetching zone list: {e}")
        raise
    finally:
        release_connection(connection, cursor)
    


def export_avg_fare_to_excel(output_file, Airline_id):
    """
    Fetch the average fare for each unique (airline_id, dept_arpt_id, arrv_arpt_id, cabin_id)
    and export it to an Excel file.
    
    Args:
        config: Database connection config.
        output_file (str): Name of the output Excel file.
    """
    query = """
    SELECT 
        Airline_ID, Dept_Arpt_ID, Arrv_Arpt_ID, Cabin_ID, 
        ROUND(AVG(Fare), 2) AS Avg_Fare
    FROM Flight_Availability
    WHERE Fare > 50 AND Fare < 18000 AND Airline_ID = :1
    GROUP BY Airline_ID, Dept_Arpt_ID, Arrv_Arpt_ID, Cabin_ID
    ORDER BY Airline_ID, Dept_Arpt_ID, Arrv_Arpt_ID, Cabin_ID
    """
    connection, cursor = connect_to_db(DB_CONFIG)
    try:
        cursor.execute(query,(Airline_id,))
        rows = cursor.fetchall()
        if rows:
            df = pd.DataFrame(rows, columns=['Airline_ID', 'Dept_Arpt_ID', 'Arrv_Arpt_ID', 'Cabin_ID', 'Avg_Fare'])
            df.to_excel(output_file, index=False)
            print(f"Excel file saved: {output_file}")
        else:
            print("No data found to export.")
    except Exception as e:
        print(f"Error fetching and exporting average fares: {e}")
    finally:
        release_connection(connection, cursor)
    
    