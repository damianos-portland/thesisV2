#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import time
import math
import errno
import random

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
)

# Mongo (προαιρετικά: αν αποτύχει η σύνδεση, συνεχίζουμε χωρίς skip)
from pymongo import MongoClient
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME   = "judgmentsV2"
try:
    _mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=800)
    _ = _mongo.server_info()
    decisions_col = _mongo[DB_NAME]["courtDecisions"]
except Exception:
    decisions_col = None


# --------------------------
# WebDriver helpers
# --------------------------

def init_driver(headless=False):
    opts = FxOptions()
    if headless:
        opts.add_argument("-headless")
    driver = webdriver.Firefox(options=opts)
    driver.wait = WebDriverWait(driver, 12)
    return driver

def wait_for_table_to_load(driver, timeout=20):
    """Περίμενε να εμφανιστούν είτε σειρές του πίνακα είτε μη-κενό info κείμενο."""
    wait = WebDriverWait(driver, timeout)
    def _loaded(d):
        try:
            rows = d.find_elements(By.CSS_SELECTOR, "#cldResultTable tbody tr")
            if any(r.is_displayed() for r in rows):
                return True
        except Exception:
            pass
        try:
            info = d.find_element(By.ID, "cldResultTable_info").text.strip()
            if info:
                return True
        except Exception:
            pass
        return False
    wait.until(_loaded)

def wait_datatable_idle(driver, timeout=10):
    """Περίμενε να μην είναι ορατό το DataTables processing overlay."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.invisibility_of_element_located((By.ID, "cldResultTable_processing"))
        )
    except TimeoutException:
        # αν δεν υπάρχει overlay, απλώς προχώρα
        pass

def parse_total_from_info(text):
    """Προσπάθησε να βγάλεις 'total' από διάφορες παραλλαγές κειμένου info."""
    text = (text or "").strip()
    if not text:
        return None
    pats = [
        r'από\s+([\d\.,]+)\s+αποτελέσματα',
        r'από\s+([\d\.,]+)\s+εγγραφ',
        r'of\s+([\d\.,]+)\s+entries',
    ]
    for p in pats:
        m = re.search(p, text, re.I)
        if m:
            return int(m.group(1).replace('.', '').replace(',', ''))
    return None

def click_doc_opener_by_index(driver, row_index, max_retries=6):
    """
    Κάνε click στο .doc_opener της N-οστής (0-based) ορατής γραμμής.
    Σε κάθε προσπάθεια γίνεται re-query + scroll + click.
    """
    css = f"#cldResultTable tbody tr:nth-child({row_index + 1}) .doc_opener"
    for attempt in range(max_retries):
        try:
            wait_datatable_idle(driver, timeout=10)
            el = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, css))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            driver.execute_script("arguments[0].click();", el)
            return True
        except (StaleElementReferenceException, ElementClickInterceptedException):
            time.sleep(0.25 + random.random() * 0.25)
        except TimeoutException:
            return False
    return False


# --------------------------
# Scraper helpers
# --------------------------

def safe_text(driver, elem_id, wait):
    try:
        el = wait.until(EC.visibility_of_element_located((By.ID, elem_id)))
        txt = (el.text or "").strip()
        if not txt:
            txt = el.get_attribute("textContent") or ""
        return txt.strip()
    except TimeoutException:
        return ""

def write_decision(out_fn, header_lines, body_text):
    tmp_fn = out_fn + ".tmp"
    with open(tmp_fn, "w", encoding="utf-8") as f:
        for line in header_lines:
            f.write((line or "").strip() + "\n")
        f.write("\n")
        f.write((body_text or "").strip())
    os.replace(tmp_fn, out_fn)


# --------------------------
# Main scraping routine
# --------------------------

def lookup(driver, year, headless=False):
    outdir = os.path.join(os.pardir, "data", "ste", year)
    os.makedirs(outdir, exist_ok=True)

    driver.get("http://www.adjustice.gr/webcenter/portal/ste/ypiresies/nomologies")
    try:
        # Συμπλήρωσε έτος και υποβολή
        box    = driver.wait.until(EC.presence_of_element_located((By.ID, "dec_year")))
        submit = driver.wait.until(EC.element_to_be_clickable((By.ID, "form1submit")))
        box.clear()
        box.send_keys(year)
        submit.click()

        # Περίμενε φόρτωμα πίνακα (rows ή info)
        wait_for_table_to_load(driver, timeout=25)

        # Προαιρετικά: κατέγραψε σύνολο από info (δεν είναι blocking)
        total = None
        try:
            info_txt = driver.find_element(By.ID, "cldResultTable_info").text.strip()
            total = parse_total_from_info(info_txt)
            if total:
                per_page = 10
                print(f"{year}: detected ~{total} results (~{math.ceil(total/per_page)} pages)")
            else:
                print(f"{year}: dynamic paging (no reliable info text)")
        except Exception:
            print(f"{year}: dynamic paging (info element not found)")

        page = 1
        while True:
            # Φρόντισε να είναι idle ο πίνακας
            wait_datatable_idle(driver, timeout=15)

            # Πόσοι clickable σύνδεσμοι υπάρχουν τώρα;
            links_now = driver.find_elements(By.CSS_SELECTOR, "#cldResultTable tbody tr .doc_opener")
            row_count = len(links_now)

            row_idx = 0
            while row_idx < row_count:
                # Click με re-query by index
                ok = click_doc_opener_by_index(driver, row_idx)
                if not ok:
                    # αν δεν καταφέραμε click, συνέχισε στην επόμενη γραμμή
                    row_idx += 1
                    continue

                # Περίμενε να ανοίξει το modal
                try:
                    driver.wait.until(EC.visibility_of_element_located((By.ID, "display_dec_number")))
                except TimeoutException:
                    # modal δεν άνοιξε σωστά -> προχώρα στην επόμενη γραμμή
                    row_idx += 1
                    wait_datatable_idle(driver, timeout=10)
                    continue

                # Διάβασε πεδία με fallbacks
                full_number = safe_text(driver, "display_dec_number", driver.wait)
                decision_no = full_number.split("/")[0] if "/" in full_number else full_number

                chamber      = safe_text(driver, "display_chamber", driver.wait)
                dec_cat      = safe_text(driver, "display_dec_category", driver.wait)
                dec_date     = safe_text(driver, "display_dec_date", driver.wait)
                init_cat     = safe_text(driver, "display_init_category", driver.wait)
                init_number  = safe_text(driver, "display_init_number", driver.wait)
                composition  = safe_text(driver, "display_composition", driver.wait)
                ecli         = safe_text(driver, "ecli", driver.wait)

                # Σώμα απόφασης
                try:
                    body_el = driver.wait.until(EC.visibility_of_element_located((By.ID, "full_display_dec_text")))
                    body_txt = body_el.text.strip() or (body_el.get_attribute("textContent") or "").strip()
                except TimeoutException:
                    body_txt = ""

                # Skip μέσω Mongo (αν είναι διαθέσιμη)
                if decisions_col is not None:
                    exists = decisions_col.find_one({
                        "header.docNumber": decision_no,
                        "court": "Council of State"
                    })
                    if exists:
                        print(f"-> {decision_no} already in DB, skipping")
                    else:
                        out_fn = os.path.join(outdir, f"{decision_no}.txt")
                        header_lines = [full_number, chamber, dec_cat, dec_date, init_cat, init_number, composition, ecli]
                        write_decision(out_fn, header_lines, body_txt)
                        print(f"saved {decision_no} (len={len(body_txt)})")
                else:
                    # Χωρίς Mongo, σώσε πάντα
                    out_fn = os.path.join(outdir, f"{decision_no}.txt")
                    header_lines = [full_number, chamber, dec_cat, dec_date, init_cat, init_number, composition, ecli]
                    write_decision(out_fn, header_lines, body_txt)
                    print(f"saved {decision_no} (len={len(body_txt)})")

                # Κλείσιμο modal με το back κουμπί (όχι driver.back())
                try:
                    back = driver.wait.until(EC.element_to_be_clickable((By.ID, "cld-single-back")))
                    driver.execute_script("arguments[0].click();", back)
                except TimeoutException:
                    driver.back()

                # Περίμενε να σταθεροποιηθεί ξανά ο πίνακας πριν συνεχίσεις
                wait_datatable_idle(driver, timeout=15)
                time.sleep(0.2 + random.random() * 0.2)
                row_idx += 1

            # Μετάβαση στην επόμενη σελίδα (αριθμητικός σύνδεσμος)
            try:
                page += 1
                next_link = driver.find_element(By.LINK_TEXT, str(page))
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_link)
                driver.execute_script("arguments[0].click();", next_link)
                wait_for_table_to_load(driver, timeout=20)
                wait_datatable_idle(driver, timeout=15)
                print(f"{year}: page {page}")
                time.sleep(0.3)
            except NoSuchElementException:
                # Δοκίμασε fallback "Επόμενο" αν υπάρχει
                try:
                    nxt = driver.find_element(By.LINK_TEXT, "Επόμενο")
                    if "disabled" in (nxt.get_attribute("class") or ""):
                        break
                    driver.execute_script("arguments[0].click();", nxt)
                    wait_for_table_to_load(driver, timeout=20)
                    wait_datatable_idle(driver, timeout=15)
                    print(f"{year}: page {page} (next)")
                    time.sleep(0.3)
                except NoSuchElementException:
                    break

    except TimeoutException:
        print("Timeout while loading results.")


# --------------------------
# Entrypoint
# --------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scrapper.py <YEAR> [--headless]")
        sys.exit(1)

    year = sys.argv[1]
    headless = ("--headless" in sys.argv)

    driver = init_driver(headless=headless)
    try:
        lookup(driver, year, headless=headless)
    finally:
        time.sleep(1)
        driver.quit()
