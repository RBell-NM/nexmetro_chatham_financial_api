import os
import sys
import time
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure project root is on sys.path so imports work from anywhere
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import logging utilities from the renamed file
from utils.logger import setup_logger, LoggerContext

# Import your report functions - note the capital R in Reports
from Reports.payment_report import generate_payment_report
from Reports.Debt_report import generate_debt_report
from Reports.valuation_report import generate_valuation_report

# Configuration - set to "1" for sequential, "2" for parallel
DEFAULT_EXECUTION_METHOD = "1"

# Set up main logger
logger = setup_logger('main')

def run_reports_sequentially():
    """Run all reports one after another"""
    with LoggerContext(logger, "run_reports_sequentially"):
        reports = [
            {
                "name": "Payment Report",
                "function": generate_payment_report,
                "kwargs": {}  # No parameters needed
            },
            {
                "name": "Debt Report", 
                "function": generate_debt_report,
                "kwargs": {}  # No parameters needed
            },
            {
                "name": "Valuation Report",
                "function": generate_valuation_report,
                "kwargs": {}  # No parameters needed
            }
        ]
        results = []

        for report in reports:
            logger.info(f"Starting {report['name']}...")
            report_start = time.time()

            try:
                result = report["function"](**report["kwargs"])
                report_time = time.time() - report_start

                if result.get("success", False):
                    logger.info(f"{report['name']} completed in {report_time:.2f}s")
                else:
                    error_msg = result.get("error", "Unknown error")
                    logger.error(f"{report['name']} failed: {error_msg}")

                results.append({
                    "name": report["name"],
                    "result": result,
                    "duration": report_time
                })

            except Exception as e:
                report_time = time.time() - report_start
                logger.exception(f"{report['name']} crashed after {report_time:.2f}s")
                results.append({
                    "name": report["name"],
                    "result": {"success": False, "error": str(e)},
                    "duration": report_time
                })

        return results

def run_reports_parallel(max_workers=3):
    """Run reports in parallel"""
    with LoggerContext(logger, "run_reports_parallel", max_workers=max_workers):
        reports = [
            {
                "name": "Payment Report",
                "function": generate_payment_report,
                "kwargs": {}  # No parameters needed
            },
            {
                "name": "Debt Report",
                "function": generate_debt_report, 
                "kwargs": {}  # No parameters needed
            },
            {
                "name": "Valuation Report",
                "function": generate_valuation_report,
                "kwargs": {}  # No parameters needed
            }
        ]
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_report = {
                executor.submit(report["function"], **report["kwargs"]): report
                for report in reports
            }

            for future in as_completed(future_to_report):
                report = future_to_report[future]

                try:
                    result = future.result()
                    results.append({
                        "name": report["name"],
                        "result": result
                    })
                except Exception as e:
                    logger.exception(f"{report['name']} crashed")
                    results.append({
                        "name": report["name"],
                        "result": {"success": False, "error": str(e)}
                    })

        return results

def print_summary(results):
    """Print a summary of all report results"""
    logger.info("="*50)
    logger.info("REPORT GENERATION SUMMARY")
    logger.info("="*50)

    successful = 0
    failed = 0
    total_records = 0
    total_time = 0

    for item in results:
        name = item["name"]
        result = item["result"]
        duration = item.get("duration", 0)
        total_time += duration if isinstance(duration, (int, float)) else 0

        if result.get("success", False):
            successful += 1
            records = result.get("records", 0)
            total_records += records
            duration_str = f" ({duration:.2f}s)" if isinstance(duration, (int, float)) else ""
            file_path = result.get("file_path", "Unknown location")
            file_size = result.get("file_size", 0)
            
            logger.info(f"{name}: {records:,} records{duration_str}")
            logger.info(f"  File: {file_path} ({file_size:,} bytes)")
        else:
            failed += 1
            error_msg = result.get("error", "Unknown error")
            logger.error(f"{name}: {error_msg}")

    logger.info("-" * 50)
    logger.info(f"Total: {successful} successful, {failed} failed")
    logger.info(f"Total records: {total_records:,}")
    logger.info(f"Total time: {total_time:.2f}s")
    logger.info(f"Output folder: {os.path.abspath('Generated Files')}")
    logger.info(f"Log folder: {os.path.abspath('logs')}")

    return successful, failed, total_records

if __name__ == "__main__":
    try:
        logger.info("Nexmetro Chatham Financial Report Generator Starting")
        logger.info(f"Working directory: {os.getcwd()}")
        logger.info(f"Output directory: {os.path.abspath('Generated Files')}")
        logger.info(f"Log directory: {os.path.abspath('logs')}")

        choice = DEFAULT_EXECUTION_METHOD
        logger.info(f"Using execution method: {choice} ({'Sequential' if choice == '1' else 'Parallel'})")

        start_time = time.time()
        if choice == "2":
            results = run_reports_parallel(max_workers=2)
        else:
            results = run_reports_sequentially()
        total_time = time.time() - start_time

        successful, failed, total_records = print_summary(results)
        logger.info(f"All operations completed in {total_time:.2f}s")

        if failed == 0:
            logger.info("All reports generated successfully!")
        else:
            logger.warning(f"{failed} report(s) failed. Check logs for details.")

    except KeyboardInterrupt:
        logger.warning("Report generation interrupted by user")
    except Exception as e:
        logger.exception("Fatal error occurred")
        print(f"Fatal error: {e}")