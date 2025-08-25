# Combined Valuation Template Creation and Report Export - WORKING

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
    
    print(f"🔧 Creating/updating template: {template_id}")
    response = requests.put(url, headers=headers, data=json.dumps(payload))
    
    if response.status_code == 200:
        print("✅ Template created/updated successfully.")
        return True
    else:
        print(f"❌ Failed to create/update template. Status Code: {response.status_code}")
        print(response.text)
        return False

# ==== STEP 2: Request report ====
def get_transaction_report(template_id):
    """Request a transaction report using the specified template."""
    active_from = datetime.today()
    active_to = active_from + timedelta(days=5*365)  # 5 years forward

    params = {
        "ActiveFromDate": active_from.strftime("%Y-%m-%d"),
        "ActiveToDate": active_to.strftime("%Y-%m-%d")
    }

    url = f"{BASE_URL}/reporting/reports/transactions/{template_id}"
    print(f"📡 Requesting report for template: {template_id}")
    print(f"📅 Date range: {active_from.strftime('%Y-%m-%d')} to {active_to.strftime('%Y-%m-%d')} (5 years forward)")
    
    # Update headers for report request (remove Content-Type)
    report_headers = {
        'Authorization': API_TOKEN,
        'Accept': 'application/json'
    }
    
    response = requests.get(url, headers=report_headers, params=params)

    if response.status_code == 200:
        json_resp = response.json()
        job_id = json_resp.get("JobId")
        if job_id:
            print(f"⏳ Report job queued. Job ID: {job_id}")
            return {"job_id": job_id}
        else:
            print("✅ Report data received immediately (no job).")
            return {"data": json_resp}
    else:
        raise Exception(f"❌ Failed to start report job. Status {response.status_code}: {response.text}")

# ==== STEP 3: Poll for report readiness with tqdm progress bar ====
def poll_report_status(job_id, retries=30, delay=5):
    """Poll the report status until it's ready or timeout."""
    url = f"{BASE_URL}/report/{job_id}"
    print(f"🔁 Polling report status for Job ID: {job_id}")

    # Update headers for polling (remove Content-Type)
    poll_headers = {
        'Authorization': API_TOKEN,
        'Accept': 'application/json'
    }

    for _ in tqdm(range(retries), desc="Polling report status", unit="try"):
        response = requests.get(url, headers=poll_headers)

        if response.status_code == 200:
            print("✅ Report is ready.")
            return response.json()
        elif response.status_code == 202:
            time.sleep(delay)
        else:
            raise Exception(f"❌ Unexpected status: {response.status_code}, {response.text}")

    raise TimeoutError("⏰ Report generation timed out.")

# ==== STEP 4: Export to Excel with RunDate column ====
def export_to_excel(data, filename):
    """Export the report data to Excel format."""
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    file_path = os.path.join(OUTPUT_FOLDER, filename)

    items = data.get("Items", [])
    if not items:
        print("⚠️ No data items found to export.")
        return

    df = pd.DataFrame(items)
    run_date = datetime.now()
    df.insert(0, "RunDate", run_date)

    df.to_excel(file_path, index=False)
    print(f"💾 Data exported successfully to {file_path}")
    return file_path

# ==== MAIN LOGIC ====
def main():
    """Main execution function that orchestrates the entire process."""
    try:
        # Step 1: Create/update the template
        template_success = create_valuation_template(TEMPLATE_ID)
        if not template_success:
            print("❌ Template creation failed. Aborting.")
            return
        
        print()  # Add spacing for readability
        
        # Step 2: Generate the report
        result = get_transaction_report(TEMPLATE_ID)
        if "job_id" in result:
            report_data = poll_report_status(result["job_id"])
        else:
            report_data = result["data"]
        
        # Step 3: Export to Excel
        file_path = export_to_excel(report_data, FILENAME)
        
        print(f"\n🎉 Process completed successfully!")
        if file_path:
            print(f"📄 Report saved to: {file_path}")
            
    except Exception as e:
        print(f"💥 Error: {e}")

if __name__ == "__main__":
    main()