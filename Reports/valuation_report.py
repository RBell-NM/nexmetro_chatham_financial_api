# Combined Valuation Template Creation and Report Export - UPDATED

import os
import time
import requests
import pandas as pd
import json
from datetime import datetime, timedelta
from tqdm import tqdm
from nexmetro_chatham_financial_api_credentials import chatham_api  # Auth token source

# ==== CONFIGURATION ====
API_TOKEN = f"Bearer {chatham_api['api_token']}"
BASE_URL = chatham_api["api_endpoint"].rstrip('/')
TEMPLATE_ID = "valuation_caps_only"
OUTPUT_FOLDER = "Generated Files"
FILENAME = "Valuation_Report.xlsx"

headers = {
    'Authorization': API_TOKEN,
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

# ==== STEP 1: Create/Update Template ====
def create_valuation_template(template_id):
    """Create or update the valuation reporting template."""
    url = f'{BASE_URL}/reporting/templates/{template_id}'
    
    payload = {
        "Id": template_id,
        "Type": "Transaction",
        "Fields": [
            "ChathamReferenceNumber",
            "ClientLegalEntityName",
            "Portfolio1",
            "HedgedItem",
            "ProductType",
            "OriginalStrike",
            "NotionalCurrency",
            "NotionalAmountDescription",
            "CurrentNotional",
            "IndexDescription",
            "CounterpartyLegalEntityName",
            "TradeDateTime",
            "EffectiveDate",
            "MaturityDate",
            "ValuationDateTimeForReportEnd",
            "ValuationCurrency",
            "IntrinsicValueAsOfReportEnd",
            "TimeValueForReportEnd",
            "AccruedInterestAsOfReportEnd",
            "CleanPriceForReportEnd",
            "CleanPricePlusAccruedInterestForReportEnd"
        ]
    }
    
    response = requests.put(url, headers=headers, data=json.dumps(payload))
    return response.status_code == 200

# ==== STEP 2: Request report ====
def get_transaction_report(template_id):
    """Request a transaction report using the specified template."""
    active_from = datetime.today()
    active_to = active_from + timedelta(days=5*365)

    params = {
        "ActiveFromDate": active_from.strftime("%Y-%m-%d"),
        "ActiveToDate": active_to.strftime("%Y-%m-%d")
    }

    url = f"{BASE_URL}/reporting/reports/transactions/{template_id}"
    report_headers = {
        'Authorization': API_TOKEN,
        'Accept': 'application/json'
    }
    
    response = requests.get(url, headers=report_headers, params=params)

    if response.status_code == 200:
        json_resp = response.json()
        job_id = json_resp.get("JobId")
        if job_id:
            return {"job_id": job_id}
        else:
            return {"data": json_resp}
    else:
        raise Exception(f"Failed to start report job. Status {response.status_code}: {response.text}")

# ==== STEP 3: Poll for report readiness ====
def poll_report_status(job_id, retries=30, delay=5):
    """Poll the report status until it's ready or timeout."""
    url = f"{BASE_URL}/report/{job_id}"
    poll_headers = {
        'Authorization': API_TOKEN,
        'Accept': 'application/json'
    }

    for _ in tqdm(range(retries), desc="Polling report status", unit="try"):
        response = requests.get(url, headers=poll_headers)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 202:
            time.sleep(delay)
        else:
            raise Exception(f"Unexpected status: {response.status_code}, {response.text}")

    raise TimeoutError("Report generation timed out.")

# ==== STEP 4: Export to Excel ====
def export_to_excel(data, filename):
    """Export the report data to Excel format."""
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    file_path = os.path.join(OUTPUT_FOLDER, filename)

    items = data.get("Items", [])
    if not items:
        return None, 0

    df = pd.DataFrame(items)
    run_date = datetime.now()
    df.insert(0, "RunDate", run_date)

    df.to_excel(file_path, index=False)
    return file_path, len(df)


# ==== NEW MAIN ENTRYPOINT ====
def generate_valuation_report():
    """Generate valuation report and return result status"""
    start_time = time.time()
    try:
        # Step 1: Create/update template
        if not create_valuation_template(TEMPLATE_ID):
            return {
                "success": False,
                "error": "Template creation failed",
                "file_path": None
            }
        
        # Step 2: Request report
        result = get_transaction_report(TEMPLATE_ID)
        if "job_id" in result:
            report_data = poll_report_status(result["job_id"])
        else:
            report_data = result["data"]
        
        # Step 3: Export to Excel
        file_path, record_count = export_to_excel(report_data, FILENAME)

        if file_path and os.path.exists(file_path):
            duration = round(time.time() - start_time, 2)
            return {
                "success": True,
                "file_path": os.path.abspath(file_path),
                "file_size": os.path.getsize(file_path),
                "records": record_count,
                "duration": duration
            }
        else:
            return {
                "success": False,
                "error": "Report file was not created or contains no data",
                "file_path": None
            }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "file_path": None
        }
