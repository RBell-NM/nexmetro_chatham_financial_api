import requests
import pandas as pd
import xml.etree.ElementTree as ET
import time
from datetime import datetime
import os

from nexmetro_chatham_financial_api_credentials import chatham_api
from .queuing_report import jobid  # Fixed: Added relative import


def generate_debt_report():
    """Generate debt report and return result status"""
    start_time = time.time()
    try:
        # Get the API token
        api_token = chatham_api["api_token"]

        # Define headers
        headers = {
            'Accept': 'application/xml',
            'Authorization': f"Bearer {api_token}"
        }

        # Get the jobid
        job_id = jobid()
        if not job_id:
            return {
                "success": False,
                "error": "Failed to retrieve jobid",
                "file_path": None
            }

        url = f'https://api.chathamdirect.com/report/{job_id}'

        # Retry logic
        max_retries = 5
        retries = 0
        success = False
        df = None
        output_path = "./Generated Files/Debt_Report.xlsx"

        while retries < max_retries and not success:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                success = True
                # Parse XML
                tree = ET.ElementTree(ET.fromstring(response.text))
                root = tree.getroot()

                namespaces = {'ns': 'http://schemas.datacontract.org/2009/11/Chatham.FMS.Data'}

                def flatten_xml(element, parent_tag=''):
                    data = {}
                    for child in element:
                        tag = child.tag.split('}')[1] if '}' in child.tag else child.tag
                        if len(child):
                            data.update(flatten_xml(child, tag))
                        else:
                            data[tag] = child.text
                    return data

                # Extract loan data
                data_list = []
                for loan in root.findall('.//ns:Instruments/ns:Loan', namespaces=namespaces):
                    loan_data = flatten_xml(loan, parent_tag='Loan')
                    data_list.append(loan_data)

                df = pd.DataFrame(data_list)
                df.insert(0, 'RunDate', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                # Ensure output dir exists
                if not os.path.exists(os.path.dirname(output_path)):
                    os.makedirs(os.path.dirname(output_path))

                # Save Excel
                df.to_excel(output_path, index=False)

            else:
                retries += 1
                time.sleep(30)  # Retry delay

        if success and os.path.exists(output_path):
            duration = round(time.time() - start_time, 2)
            return {
                "success": True,
                "file_path": os.path.abspath(output_path),
                "file_size": os.path.getsize(output_path),
                "records": len(df) if df is not None else "Unknown",
                "duration": duration
            }
        else:
            return {
                "success": False,
                "error": "Failed to retrieve data after multiple attempts",
                "file_path": None
            }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "file_path": None
        }