# -*- coding: utf-8 -*-
import os
import errno
import time
import math
import re
import sys

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from pymongo import MongoClient


MONGO_URI  = "mongodb://localhost:27017/"
DB_NAME    = "judgmentsV2"
client     = MongoClient(MONGO_URI)
decisions_col = client[DB_NAME]["courtDecisions"]

def init_driver():
    driver = webdriver.Firefox()
    driver.wait = WebDriverWait(driver, 10)
    return driver

def lookup(driver, year):
    outdir = os.path.join(os.pardir, "data", "ste", year)
    try:
        os.makedirs(outdir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    driver.get("http://www.adjustice.gr/webcenter/portal/ste/ypiresies/nomologies")
    try:
        # fill in the year
        box    = driver.wait.until(EC.presence_of_element_located((By.ID, "dec_year")))
        submit = driver.wait.until(EC.element_to_be_clickable((By.ID, "form1submit")))
        box.clear()
        box.send_keys(year)
        submit.click()
        time.sleep(2)

        info = driver.wait.until(EC.presence_of_element_located((By.ID, "cldResultTable_info"))).text
        m = re.search(r'από ([\d,]+) αποτελέσματα', info)
        if not m:
            print(f"❌ Could not parse number of results from: “{info}”")
            return

        total = int(m.group(1).replace(',', ''))
        per_page = 10
        pages = math.ceil(total / per_page)

        for page in range(1, pages + 1):
            # open each result on this page
            links = driver.find_elements(By.CLASS_NAME, "doc_opener")
            for link in links:
                # first extract the decision number without clicking
                # (they all share the same CSS class, but we can grab the text)
                full_number = link.text.strip()  # e.g. "Α3535/2017"
                decision_no = full_number.split("/")[0]

                # skip if already in our Mongo collection:
                if decisions_col.find_one({
                    "header.docNumber": decision_no,
                    "court": "Council of State"
                }):
                    print(f"→ {decision_no} already in DB, skipping")
                    continue

                link.click()
                # wait for the full text to load
                dec_div = driver.wait.until(EC.presence_of_element_located((By.ID, "full_display_dec_text")))
                if len(dec_div.text) > 100:
                    # build filenames
                    full_number = driver.find_element(By.ID, "display_dec_number").text  # e.g. "Α3535/2017"
                    decision_no = full_number.split("/")[0]
                    out_fn      = os.path.join(outdir, f"{decision_no}.txt")
                    tmp_fn      = out_fn + ".tmp"

                    # write atomically to .tmp
                    with open(tmp_fn, "w", encoding="utf-8") as f:
                        f.write(full_number + "\n")
                        f.write(driver.find_element(By.ID, "display_chamber").text + "\n")
                        f.write(driver.find_element(By.ID, "display_dec_category").text + "\n")
                        f.write(driver.find_element(By.ID, "display_dec_date").text + "\n")
                        f.write(driver.find_element(By.ID, "display_init_category").text + "\n")
                        f.write(driver.find_element(By.ID, "display_init_number").text + "\n")
                        f.write(driver.find_element(By.ID, "display_composition").text + "\n")
                        f.write(driver.find_element(By.ID, "ecli").text + "\n\n")
                        f.write(dec_div.text)

                    # replace old file with new, atomically
                    os.replace(tmp_fn, out_fn)

                # go back to list
                back = driver.wait.until(EC.element_to_be_clickable((By.ID, "cld-single-back")))
                back.click()
                time.sleep(0.5)

            # move to next page (if any)
            if page < pages:
                print(f"{year}: page {page}/{pages}")
                next_link = driver.wait.until(EC.element_to_be_clickable((By.LINK_TEXT, str(page+1))))
                next_link.click()
                time.sleep(1)

    except TimeoutException:
        print("Timeout while loading results.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scrapper.py <YEAR>")
        sys.exit(1)

    year = sys.argv[1]
    driver = init_driver()
    try:
        lookup(driver, year)
    finally:
        time.sleep(1)
        driver.quit()