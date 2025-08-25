# Payment Template Creation and Report Generation - COMPLETE

import requests
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from nexmetro_chatham_financial_api_credentials import chatham_api  # Auth token source

# ==== CONFIGURATION ====
API_TOKEN = f"Bearer {chatham_api['api_token']}"
BASE_URL = chatham_api["api_endpoint"].rstrip('/')
TEMPLATE_ID = 'custom_payment_template'
API_URL = f'{BASE_URL}/reporting/templates/{TEMPLATE_ID}'
OUTPUT_FOLDER = "Generated Files"
FILENAME = "Payment_Report.xlsx"

headers = {
    'Authorization': API_TOKEN,
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

# ==== PAYLOAD ====
payload = {
    "Id": TEMPLATE_ID,
    "Type": "Payment",
    "Fields": [
        "PaymentDateForThisPeriod",
        "ChathamReferenceNumber",
        "Portfolio1",
        "ProductType",
        "CounterpartyLegalEntityName",
        "ClientLegalEntityName",
        "Description",
        "TradeDateTime",
        "EffectiveDate",
        "MaturityDate",
        "NotionalCurrency",
        "NotionalAmountDescription",
        "Leg1StrikeRateForThisPeriod",
        "IndexDescription",
        "Leg1SpreadOverIndexForThisPeriod",
        "Leg1IndexRateForThisPeriod",
        "ReportingPeriodPaymentType",
        "NetPaymentAmountForThisPeriod"
    ]
}

# ==== STEP 1: CREATE TEMPLATE (YOUR WORKING CODE) ====
print("📡 Creating payment report template...")
response = requests.put(API_URL, headers=headers, data=json.dumps(payload))

if response.status_code == 200:
    print("✅ Payment template created/updated successfully.")
    print(json.dumps(response.json(), indent=2))
    
    # ==== STEP 2: GENERATE PAYMENT REPORT ====
    print("\n📊 Generating payment report...")
    
    # Set date range (last 365 days)
    active_to = datetime.today()
    active_from = active_to - timedelta(days=365)
    
    # Parameters based on API documentation (100 record limit per call)
    params = {
        "transactionactivefromdate": active_from.strftime("%Y-%m-%d"),
        "transactionactivetodate": active_to.strftime("%Y-%m-%d"),
        "offset": 0,
        "limit": 100  # API maximum per call
    }
    
    # Request payment report
    report_url = f"{BASE_URL}/reporting/reports/payments/{TEMPLATE_ID}"
    print(f"📅 Date range: {active_from.strftime('%Y-%m-%d')} to {active_to.strftime('%Y-%m-%d')}")
    print(f"🔗 URL: {report_url}")
    
    report_response = requests.get(report_url, headers=headers, params=params)
    
    if report_response.status_code == 200:
        print("✅ Payment report data received successfully!")
        report_data = report_response.json()
        
        # Show basic info about the response
        if "Items" in report_data:
            print(f"📋 Found {len(report_data['Items'])} payment records")
            
            if "Paging" in report_data:
                paging = report_data["Paging"]
                print(f"📄 Pagination: Showing {paging.get('Offset', 0)}-{paging.get('Limit', 0)} of {paging.get('TotalRecords', 0)} total records")
        
        # ==== STEP 3: HANDLE PAGINATION (if needed) ====
        all_items = []
        if "Items" in report_data:
            all_items.extend(report_data["Items"])
            
            # Get all pages if there are more records
            if "Paging" in report_data:
                paging = report_data["Paging"]
                total_records = paging.get("TotalRecords", 0)
                limit = 100  # API maximum per call
                
                while len(all_items) < total_records:
                    current_offset = len(all_items)
                    print(f"📄 Fetching more records (offset: {current_offset})...")
                    
                    page_params = params.copy()
                    page_params["offset"] = current_offset
                    page_params["limit"] = 100
                    
                    page_response = requests.get(report_url, headers=headers, params=page_params)
                    
                    if page_response.status_code == 200:
                        page_data = page_response.json()
                        if "Items" in page_data and page_data["Items"]:
                            all_items.extend(page_data["Items"])
                            print(f"📊 Total records collected: {len(all_items)}")
                        else:
                            break
                    else:
                        print(f"❌ Failed to get additional records: {page_response.status_code}")
                        break
        
        # ==== STEP 4: EXPORT TO EXCEL ====
        print(f"\n💾 Exporting {len(all_items)} records to Excel...")
        
        # Create output directory
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        file_path = os.path.join(OUTPUT_FOLDER, FILENAME)
        abs_path = os.path.abspath(file_path)
        
        if all_items:
            # Create DataFrame
            df = pd.DataFrame(all_items)
            
            # Add RunDate as first column
            run_date = datetime.now()
            df.insert(0, "RunDate", run_date)
            
            # Save to Excel
            df.to_excel(file_path, index=False)
            
            # Verify file was created
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                print(f"✅ Report exported successfully!")
                print(f"📁 Location: {abs_path}")
                print(f"📊 File size: {file_size:,} bytes")
                print(f"📋 Rows: {len(df):,}, Columns: {len(df.columns)}")
                print(f"📅 Run date: {run_date.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print(f"❌ File was not created at {abs_path}")
        else:
            print("⚠️ No payment data found for the specified date range")
            # Create empty file with headers
            df = pd.DataFrame([{"RunDate": datetime.now(), "Message": "No data found"}])
            df.to_excel(file_path, index=False)
            print(f"📄 Empty report created at: {abs_path}")
            
    else:
        print(f"❌ Failed to generate payment report. Status Code: {report_response.status_code}")
        print(f"Response: {report_response.text}")
        
else:
    print(f"❌ Failed to create/update payment template. Status Code: {response.status_code}")
    print(response.text)
    print("❌ Cannot proceed with report generation.")

print(f"\n🏁 Process completed!")
print(f"📍 Working directory: {os.getcwd()}")
print(f"📁 Output folder: {os.path.abspath(OUTPUT_FOLDER)}")