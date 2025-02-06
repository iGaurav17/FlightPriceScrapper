import pandas as pd
import numpy as np

def analyze_zone_data(prices_df, dept_arpt_id, arrv_arpt_id):
    # Filter data for the specific zone based on dept_arpt_id and arrv_arpt_id
    zone_data = prices_df[(prices_df['Dept_Arpt_ID'] == dept_arpt_id) & 
                          (prices_df['Arrv_Arpt_Id'] == arrv_arpt_id)]
    
    if zone_data.empty:
        print(f"No data found for zone: {dept_arpt_id}-{arrv_arpt_id}")
        return None
    
    # Convert Dept_DateTime to datetime
    zone_data['Dept_DateTime'] = pd.to_datetime(zone_data['Dept_DateTime'], format='%d-%m-%Y')
    
    # Sort by date to make sure the data is in chronological order
    zone_data = zone_data.sort_values(by='Dept_DateTime')

    # Calculate average price excluding blackout period
    avg_price = zone_data['Fare_Class'].mean()

    # Identifying blackout period as 20% higher than the average price
    blackout_threshold = avg_price * 1.2
    zone_data['is_blackout'] = zone_data['Fare_Class'] > blackout_threshold

    # Find the blackout period (continuous period with prices above threshold)
    blackout_periods = []
    blackout_start = None
    for i, row in zone_data.iterrows():
        if row['is_blackout']:
            if blackout_start is None:
                blackout_start = row['Dept_DateTime']
        else:
            if blackout_start is not None:
                blackout_end = zone_data.loc[i - 1, 'Dept_DateTime']
                blackout_periods.append((blackout_start, blackout_end))
                blackout_start = None
    if blackout_start is not None:
        blackout_periods.append((blackout_start, zone_data.iloc[-1]['Dept_DateTime']))

    # Peak price during the blackout period
    peak_price = zone_data[zone_data['is_blackout']]['Fare_Class'].max()

    # Calculate the average price excluding the blackout period
    avg_price_excluding_blackout = zone_data[~zone_data['is_blackout']]['Fare_Class'].mean()

    return {
        'blackout_periods': blackout_periods,
        'peak_price': peak_price,
        'avg_price_excluding_blackout': avg_price_excluding_blackout
    }
