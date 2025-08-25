# Payment Template Creation and Report Generation - UPDATED

import requests
import json
import os
import pandas as pd
import time
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


def generate_payment_report():
    """Generate payment report and return result status"""
    start_time = time.time()
    try:
        # Step 1: Create/update template
        response = requests.put(API_URL, headers=headers, data=json.dumps(payload))
        if response.status_code != 200:
            return {
                "success": False,
                "error": f"Failed to create/update payment template. Status: {response.status_code}",
                "file_path": None
            }

        # Step 2: Generate payment report
        active_to = datetime.today()
        active_from = active_to - timedelta(days=365)

        params = {
            "transactionactivefromdate": active_from.strftime("%Y-%m-%d"),
            "transactionactivetodate": active_to.strftime("%Y-%m-%d"),
            "offset": 0,
            "limit": 100
        }

        report_url = f"{BASE_URL}/reporting/reports/payments/{TEMPLATE_ID}"
        report_response = requests.get(report_url, headers=headers, params=params)

        if report_response.status_code != 200:
            return {
                "success": False,
                "error": f"Failed to generate payment report. Status: {report_response.status_code}",
                "file_path": None
            }

        report_data = report_response.json()
        all_items = []

        # Collect first page
        if "Items" in report_data:
            all_items.extend(report_data["Items"])

            # Handle pagination if needed
            if "Paging" in report_data:
                paging = report_data["Paging"]
                total_records = paging.get("TotalRecords", 0)
                limit = 100

                while len(all_items) < total_records:
                    current_offset = len(all_items)
                    page_params = params.copy()
                    page_params["offset"] = current_offset
                    page_params["limit"] = limit

                    page_response = requests.get(report_url, headers=headers, params=page_params)
                    if page_response.status_code == 200:
                        page_data = page_response.json()
                        if "Items" in page_data and page_data["Items"]:
                            all_items.extend(page_data["Items"])
                        else:
                            break
                    else:
                        break

        # Step 3: Export to Excel
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        file_path = os.path.join(OUTPUT_FOLDER, FILENAME)

        if all_items:
            df = pd.DataFrame(all_items)
            run_date = datetime.now()
            df.insert(0, "RunDate", run_date)
            df.to_excel(file_path, index=False)
        else:
            # Write empty file if no data
            df = pd.DataFrame([{"RunDate": datetime.now(), "Message": "No data found"}])
            df.to_excel(file_path, index=False)

        # Verify file
        if os.path.exists(file_path):
            duration = round(time.time() - start_time, 2)
            return {
                "success": True,
                "file_path": os.path.abspath(file_path),
                "file_size": os.path.getsize(file_path),
                "records": len(df),
                "duration": duration
            }
        else:
            return {
                "success": False,
                "error": "Report file was not created",
                "file_path": None
            }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "file_path": None
        }
