import requests
import pandas as pd
import xml.etree.ElementTree as ET
import time  # To use sleep for waiting
from datetime import datetime  # Added for RunDate
##import openpyxl

##from openpyxl import Workbook

from nexmetro_chatham_financial_api_credentials import chatham_api
from queuing_report import jobid  

# Get the API token from the nexmetro_chatham_financial_api_credentials file
api_token = chatham_api["api_token"]

# Define the headers for the API request
headers = {
    'Accept': 'application/xml',
    'Authorization': f"Bearer {api_token}"
}

# Get the jobid from the jobid function
job_id = jobid()

if job_id:  # Ensure the jobid was retrieved
    url = f'https://api.chathamdirect.com/report/{job_id}'

    # Set the number of retries and the delay between attempts
    max_retries = 5  # Max number of retries
    retries = 0
    success = False

    while retries < max_retries and not success:
        print(f"Attempt {retries + 1}/{max_retries}...")
        
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            success = True
            # Parse the XML response
            tree = ET.ElementTree(ET.fromstring(response.text))
            root = tree.getroot()

            # Define the XML namespaces (to handle the XML properly)
            namespaces = {'ns': 'http://schemas.datacontract.org/2009/11/Chatham.FMS.Data'}

            # Function to flatten the XML into a dictionary (key = tag name, value = text)
            def flatten_xml(element, parent_tag=''):
                data = {}

                for child in element:
                    tag = child.tag.split('}')[1] if '}' in child.tag else child.tag  # Handle namespace

                    # If the element has children, recurse
                    if len(child):
                        data.update(flatten_xml(child, tag))
                    else:
                        data[tag] = child.text

                return data

            # Extracting data from the XML
            data_list = []
            for loan in root.findall('.//ns:Instruments/ns:Loan', namespaces=namespaces):
                loan_data = flatten_xml(loan, parent_tag='Loan')  # Flatten all child elements of the 'Loan' tag
                data_list.append(loan_data)

            # Convert the list of dictionaries to a pandas DataFrame
            df = pd.DataFrame(data_list)

            # Add RunDate column as the first column
            df.insert(0, 'RunDate', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            # Set location to save the Debt_Report.xlsx
            file_path = "./Generated Files/"
            
            # Ensure the directory exists (optional, but useful for robustness)
            import os
            if not os.path.exists(file_path):
                os.makedirs(file_path)
            
            # Save the DataFrame to an Excel file
            df.to_excel(f"{file_path}Debt_Report.xlsx", index=False)
            print("Excel file Debt_Report.xlsx created successfully in Generated Files folder.")
        else:
            retries += 1
            print(f"Error: {response.status_code}. Retrying in 30 seconds...")
            time.sleep(30)  # Wait for 30 seconds before retrying

    if not success:
        print("Failed to retrieve the data after multiple attempts.")
else:
    print("Failed to retrieve jobid.")