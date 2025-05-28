#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import datetime
from pymongo import MongoClient
from lxml import etree

# ------------------------------------------------------------------------
# Configuration: adjust as needed
# ------------------------------------------------------------------------
MONGO_URI        = "mongodb://localhost:27017/"
DB_NAME          = "judgmentsV2"
COLLECTION_NAME  = "courtDecisions"
XML_DIR          = "XML"  # top-level folder containing subfolders

# AKN namespace
NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"}

# ------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------
def clean_text(text):
    """Collapse whitespace and strip."""
    return re.sub(r'\s+', ' ', text).strip() if text else ""

def detect_court(folder_path):
    """Infer court name by subfolder."""
    p = folder_path.lower()
    if "ste" in p:
        return "Council of State"
    if "legal opinions"   in p:
        return "Legal Opinions"
    if "areios_pagos" in p:
        return "Areios Pagos"
    return "Unknown Court"

def connect_to_mongo():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLLECTION_NAME]

# ------------------------------------------------------------------------
# Parse <meta> element into dict
# ------------------------------------------------------------------------
def parse_meta(meta_elem):
    meta = {}
    if meta_elem is None:
        return meta

    # identification
    idn = meta_elem.find("akn:identification", NS)
    if idn is not None:
        ident = {"source": idn.get("source")}
        for tag in ("FRBRWork","FRBRExpression","FRBRManifestation"):
            t = idn.find(f"akn:{tag}", NS)
            if t is not None:
                sub = {}
                for child in t:
                    name = etree.QName(child).localname
                    sub[name] = child.attrib
                ident[tag] = sub
        meta["identification"] = ident

    # lifecycle
    lc = meta_elem.find("akn:lifecycle", NS)
    if lc is not None:
        lifecycle = {"source": lc.get("source")}
        ev = lc.find("akn:eventRef", NS)
        if ev is not None:
            lifecycle["eventRef"] = ev.attrib
        meta["lifecycle"] = lifecycle

    # workflow
    wf = meta_elem.find("akn:workflow", NS)
    if wf is not None:
        wfd = {"source": wf.get("source")}
        wfd["steps"] = [step.attrib for step in wf.findall("akn:step", NS)]
        meta["workflow"] = wfd

    # references
    refs = meta_elem.find("akn:references", NS)
    if refs is not None:
        rd = {"source": refs.get("source")}
        orig = refs.find("akn:original", NS)
        if orig is not None:
            rd["original"] = orig.attrib
        rd["TLCEvents"] = [e.attrib for e in refs.findall("akn:TLCEvent", NS)]
        meta["references"] = rd

    return meta

# ------------------------------------------------------------------------
# Parse a single XML into our document dict
# ------------------------------------------------------------------------
def parse_akn_xml(path):
    try:
        tree = etree.parse(path)
        root = tree.getroot()

        # meta
        meta_elem = root.find("akn:judgment/akn:meta", NS)
        meta_dict = parse_meta(meta_elem)

        # header
        header_elem = root.find("akn:judgment/akn:header", NS)
        docNumber      = clean_text(header_elem.xpath("string(akn:p[1]/akn:docNumber)", namespaces=NS))
        docProponent   = clean_text(header_elem.xpath("string(akn:p[2]/akn:docProponent)", namespaces=NS))
        subDepartment  = clean_text(header_elem.xpath("string(akn:p[3])", namespaces=NS))
        # remaining <p> become headerDetails
        hdr_ps = header_elem.findall("akn:p", NS)[3:]
        headerDetails = " ".join(clean_text(p.xpath("string(.)", namespaces=NS)) for p in hdr_ps)

        # publicHearingDate (in header)
        publicHearingDate = None
        for d in header_elem.findall(".//akn:date", NS):
            if d.get("refersTo") == "publicHearingDate":
                publicHearingDate = d.get("date")
                break

        header_obj = {
            "docNumber":     docNumber,
            "docProponent":  docProponent,
            "subDepartment": subDepartment,
            "headerDetails": headerDetails
        }

        # judgmentBody → introduction, motivation, decision
        intro = clean_text(root.xpath("string(//akn:judgment/akn:judgmentBody/akn:introduction)", namespaces=NS))
        motiv = clean_text(root.xpath("string(//akn:judgment/akn:judgmentBody/akn:motivation)",     namespaces=NS))

        # decision outcome + details
        dec_elem = root.find("akn:judgment/akn:judgmentBody/akn:decision", NS)
        outcome, details = "", ""
        if dec_elem is not None:
            ps = dec_elem.findall("akn:p", NS)
            if len(ps) >= 2:
                outcome = clean_text(ps[1].xpath("string(.)", namespaces=NS))
                details = " ".join(clean_text(p.xpath("string(.)", namespaces=NS)) for p in ps[2:])
            else:
                outcome = clean_text(dec_elem.xpath("string(.)", namespaces=NS))
        decision_obj = {"outcome": outcome, "decisionDetails": details}

        judgmentBody = {
            "introduction": intro,
            "motivation":   motiv,
            "decision":     decision_obj
        }

        # conclusions text + dates
        concl_elem = root.find("akn:judgment/akn:conclusions", NS)
        conclusions = ""
        courtConferenceDate    = None
        decisionPublicationDate = None

        if concl_elem is not None:
            ps = [clean_text(p.xpath("string(.)", namespaces=NS)) for p in concl_elem.findall("akn:p", NS)]
            conclusions = " ".join(ps)
            for d in concl_elem.findall(".//akn:date", NS):
                r = d.get("refersTo")
                if r == "courtConferenceDate":
                    courtConferenceDate = d.get("date")
                elif r == "decisionPublicationDate":
                    decisionPublicationDate = d.get("date")

        # build final dict
        doc = {
            "document_type":           "judgment",
            "file_name":               os.path.basename(path),
            "inserted_at":             datetime.datetime.utcnow().isoformat() + "Z",
            "meta":                    meta_dict,
            "header":                  header_obj,
            "judgmentBody":            judgmentBody,
            "conclusions":             conclusions,
            "publicHearingDate":       publicHearingDate,
            "courtConferenceDate":     courtConferenceDate,
            "decisionPublicationDate": decisionPublicationDate
        }

        return doc

    except Exception as e:
        print(f"[ERROR] parsing {path}: {e}")
        return None

# ------------------------------------------------------------------------
# Walk folder & upsert into Mongo
# ------------------------------------------------------------------------
def insert_all_judgments():
    coll = connect_to_mongo()
    for root, _, files in os.walk(XML_DIR):
        for fn in files:
            if not fn.lower().endswith(".xml"):
                continue
            full = os.path.join(root, fn)
            print("→ processing", full)
            doc = parse_akn_xml(full)
            if not doc:
                continue
            doc["court"] = detect_court(root)
            result = coll.replace_one(
                {"file_name": doc["file_name"]},
                doc,
                upsert=True
            )
            if result.matched_count:
                print("  replaced", fn)
            else:
                print("  inserted", fn)

if __name__ == "__main__":
    insert_all_judgments()
    print("✅ All XML files ingested into MongoDB.")