#!/usr/bin/env python3
# parsing_worker.py

import os
import subprocess
import datetime
from pymongo import MongoClient

MONGO_URI   = "mongodb://localhost:27017/"
DB_NAME     = "judgmentsV2"
REQUESTS    = "parsingRequests"

# --- adjust these to the real paths in your project ---
STE_SCRAPER       = "/path/to/legal_crawlers/ste_scrapper/scrapper.py"
AREIOS_SCRAPY_DIR = "/path/to/legal_crawlers"
COUNCIL_XML       = "/path/to/judgmentsV2/createCouncilOfStateJudgmentsAkn.py"
AREIOS_XML        = "/path/to/judgmentsV2/createAreiosPagosJudgmentsAkn.py"
INGESTION_SCRIPT  = "/path/to/judgmentsV2/insertToDb.py"
# ---------------------------------------------------------

def connect_requests():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][REQUESTS]

def run_or_die(cmd, cwd=None):
    """Run a shell command; raise if it fails."""
    print(f">>> {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=cwd)
    if r.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")

def process_request(req):
    year = req["year"]
    print(f"=== Processing year {year} ===")
    # 1) run the STE selenium scraper
    run_or_die(["python", STE_SCRAPER, year])

    # 2) run the Areios Pagos scrapy crawler
    run_or_die(["scrapy", "crawl", "CyLaw", "-a", f"year={year}"], cwd=AREIOS_SCRAPY_DIR)

    # 3) build XMLs from text for Council of State
    run_or_die(["python", COUNCIL_XML, "-year", year])

    # 4) build XMLs from text for Areios Pagos
    run_or_die(["python", AREIOS_XML, "-year", year])

    # 5) ingest all XMLs into Mongo
    run_or_die(["python", INGESTION_SCRIPT])

    # 6) mark this request as done
    col = connect_requests()
    col.update_one(
        { "_id": req["_id"] },
        { "$set": {
            "status":       "done",
            "processed_at": datetime.datetime.utcnow()
        }}
    )
    print(f"=== Done year {year} ===\n")

def main():
    col = connect_requests()
    # pick up all pending requests, in order
    pending = list(col.find({"status":"pending"}).sort("requested_at",1))
    if not pending:
        print("No pending requests.")
        return

    for req in pending:
        try:
            process_request(req)
        except Exception as e:
            print(f"❌ Failed processing year {req['year']}: {e}")
            # you might want to mark it as "error" or leave it pending for retry
            col.update_one(
                {"_id": req["_id"]},
                {"$set": {
                    "status": "error",
                    "error":   str(e),
                    "failed_at": datetime.datetime.utcnow()
                }}
            )

if __name__ == "__main__":
    main()