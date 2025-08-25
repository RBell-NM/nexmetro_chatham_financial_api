import requests
from datetime import datetime, timedelta
import pandas as pd
from nexmetro_chatham_financial_api_credentials import chatham_api

# Get the API token from the nexmetro_chatham_financial_api_credentials file
api_token = chatham_api["api_token"]

# Define the headers for the API request
headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': f"Bearer {api_token}"
}

# Define RunDate (you can set this to your desired date)
RunDate = datetime.now().date()  # Or set to a specific date like datetime(2025, 3, 10).date()

# Calculate fromdate = RunDate
fromdate = RunDate

# Calculate todate = 5 years in the future from RunDate
todate = RunDate.replace(year=RunDate.year + 5)

# Calculate asofdate = previous business date
asofdate = pd.bdate_range(end=RunDate, periods=2)[0].date()

# Define the data payload with dynamic dates
data = {
    "fromdate": fromdate.strftime("%Y-%m-%d"), 
    "todate": todate.strftime("%Y-%m-%d"), 
    "asofdate": asofdate.strftime("%Y-%m-%d"), 
    "datagroupings": 31
}

def jobid():
    # Send a POST request to the Chatham API
    response = requests.post('https://api.chathamdirect.com/report/portfolio', headers=headers, json=data)
    
    # Check if the response is successful
    if response.status_code == 200:
        # Parse the JSON response
        response_data = response.json()

        # Extract and return the JobId value
        job_id = response_data.get('JobId')
        
        if job_id is not None:
            return job_id  # Return the JobId instead of printing
        else:
            print("JobId not found in the response.")
            return None
    else:
        print(f"Failed to get a response. Status code: {response.status_code}")
        print(response.text)
        return None