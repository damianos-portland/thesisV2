#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
text_to_json.py — Build the 'intermediate' JSON from raw TXT decisions.

Usage:
  python text_to_json.py --court ste --year 2024 [--root legal_texts] [--outroot JSON] [--dry-run]

What it does:
  * Reads *.txt from <root>/<court>/<year>
  * Extracts: court, header.docNumber, header.docProponent, header.subDepartment
  * Dates: publicHearingDate, courtConferenceDate, decisionPublicationDate (ISO 8601)
  * Light segmentation into judgmentBody.{introduction, motivation, decision{outcome, decisionDetails}, conclusions}
  * Writes to <outroot>/<court>/<year>/<basename>.json
  * On parsing failure, writes <basename>.parse-error.json
"""

import argparse
import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, Optional, Tuple

# -----------------------------
# Utilities
# -----------------------------

MONTHS = {
    # canonical (allow a few common variants without diacritics)
    "ιανουαρίου": "01", "ιουανουαριου": "01", "ιανουαριου": "01",
    "φεβρουαρίου": "02", "φεβρουαριου": "02",
    "μαρτίου": "03", "μαρτιου": "03",
    "απριλίου": "04", "απριλιου": "04",
    "μαΐου": "05", "μαιου": "05", "μάϊου": "05", "μαϊου": "05",
    "ιουνίου": "06", "ιουνιου": "06",
    "ιουλίου": "07", "ιουλιου": "07",
    "αυγούστου": "08", "αυγουστου": "08",
    "σεπτεμβρίου": "09", "σεπτεμβριου": "09",
    "οκτωβρίου": "10", "οκτωβριου": "10",
    "νοεμβρίου": "11", "νοεμβριου": "11",
    "δεκεμβρίου": "12", "δεκεμβριου": "12",
}

OUTCOME_VERBS = [
    "Απορρίπτει", "Αναιρεί", "Δέχεται", "Παραπέμπει",
    "Κηρύσσει", "Καταδικάζει", "Επιβάλλει", "Διατάσσει",
]

def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def to_iso_date(day: str, month_token: str, year: str) -> Optional[str]:
    try:
        d = int(re.sub(r"[^\d]", "", day))  # "1η" -> 1, "31ης" -> 31
        mkey = nfc(month_token).lower()
        mkey = re.sub(r"[^\wάέήίόύώϊΐΰϋ]", "", mkey)  # strip punctuation
        m = MONTHS.get(mkey)
        if not m:
            return None
        return f"{int(year):04d}-{m}-{d:02d}"
    except Exception:
        return None

def spaced_regex(phrase: str) -> re.Pattern:
    """
    Build a regex that matches the phrase allowing arbitrary non-letter chars between letters.
    Example: "Σκέφθηκε κατά τον Νόμο" -> matches "Σ κ έ φ θ η κ ε κ α τ ά τ ο ν Ν ό μ ο"
    """
    chars = [re.escape(ch) for ch in phrase]
    pattern = r"\W*".join(chars)
    return re.compile(pattern, flags=re.IGNORECASE | re.UNICODE)

# Markers (very tolerant)
MOTIVATION_MARKERS = [
    spaced_regex("Σκέφθηκε κατά τον Νόμο"),
    re.compile(r"Σκέφθηκε\s+κατά\s+τον\s+Νόμο", re.IGNORECASE),
]

DECISION_MARKERS = [
    spaced_regex("Διατάυτα"),              # ανορθογραφικές παραλλαγές
    spaced_regex("Διατά τ α"),             # υπερ-χαλαρό
    spaced_regex("Δι ά τ α ύ τ α"),
    re.compile(r"Διατάυτα", re.IGNORECASE),
    re.compile(r"ΓΙΑ\s+ΤΟΥΣ\s+ΛΟΓΟΥΣ\s+ΑΥΤΟΥΣ", re.IGNORECASE),
]

CONCLUSIONS_MARKERS = [
    re.compile(r"Η\s+διάσκεψη\s+έγινε", re.IGNORECASE),
    re.compile(r"Κρίθηκε\s+και\s+αποφασίσθηκε", re.IGNORECASE),
]

# Date-extractors near certain keywords
DATE_RE = re.compile(
    r"(?P<d>\d{1,2}(?:η|ης|ος|ου)?)\s+"
    r"(?P<m>Ιανουαρίου|Φεβρουαρίου|Μαρτίου|Απριλίου|Μαΐου|Μαίου|Ιουνίου|Ιουλίου|Αυγούστου|Σεπτεμβρίου|Οκτωβρίου|Νοεμβρίου|Δεκεμβρίου)\s+"
    r"(?P<y>\d{4})",
    re.IGNORECASE | re.UNICODE
)

def find_first(text: str, patterns) -> int:
    for p in patterns:
        m = p.search(text)
        if m:
            return m.start()
    return -1

def find_keyword_date(text: str, kw_patterns) -> Optional[str]:
    """
    Find a date that appears after (or within the same line as) one of the keywords.
    """
    for p in kw_patterns:
        m = p.search(text)
        if not m:
            continue
        tail = text[m.start(): m.start() + 1000]  # local window
        mdate = DATE_RE.search(tail)
        if mdate:
            return to_iso_date(mdate.group("d"), mdate.group("m"), mdate.group("y"))
    return None

def extract_doc_number(text: str) -> Optional[str]:
    m = re.search(r"Αριθμός\s+(\d{1,4}\s*/\s*\d{4})", text, re.IGNORECASE)
    if m:
        return m.group(1).replace(" ", "")
    return None

def extract_court_and_titles(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns (court_code, docProponent, subDepartment)
    """
    # docProponent
    proponent = None
    if re.search(r"ΤΟ\s+ΣΥΜΒΟΥΛΙΟ\s+ΤΗΣ\s+ΕΠΙΚΡΑΤΕΙΑΣ", text):
        proponent = "ΤΟ ΣΥΜΒΟΥΛΙΟ ΤΗΣ ΕΠΙΚΡΑΤΕΙΑΣ"
        court = "COS"
    elif re.search(r"ΤΟ\s+ΔΙΚΑΣΤΗΡΙΟ\s+ΤΟΥ\s+ΑΡΕΙΟΥ\s+ΠΑΓΟΥ", text):
        proponent = "ΤΟ ΔΙΚΑΣΤΗΡΙΟ ΤΟΥ ΑΡΕΙΟΥ ΠΑΓΟΥ"
        court = "AP"
    else:
        court = None

    # subDepartment
    sub = None
    m = re.search(r"ΤΜΗΜΑ[^ \n]*[^\n]*", text, re.IGNORECASE)
    if m:
        sub = m.group(0).strip()

    return court, proponent, sub

def segment_text(text: str) -> Dict[str, str]:
    """
    Return segments: introduction, motivation, decision_text, conclusions
    """
    i_mot = find_first(text, MOTIVATION_MARKERS)
    i_dec = find_first(text, DECISION_MARKERS)
    i_conc = find_first(text, CONCLUSIONS_MARKERS)

    n = len(text)

    if i_mot == -1: i_mot = n
    if i_dec == -1: i_dec = n
    if i_conc == -1: i_conc = n

    intro = text[:min(i_mot, i_dec, i_conc)].strip()
    mot_start = i_mot
    mot_end = min(i_dec, i_conc, n)
    motivation = text[mot_start:mot_end].strip() if mot_start < n else ""

    dec_start = i_dec
    dec_end = min(i_conc, n)
    decision_text = text[dec_start:dec_end].strip() if dec_start < n else ""

    conclusions = text[i_conc:].strip() if i_conc < n else ""

    return {
        "introduction": intro,
        "motivation": motivation,
        "decision_text": decision_text,
        "conclusions": conclusions,
    }

def extract_outcome(decision_text: str) -> Optional[str]:
    # Look for the first strong verb (Απορρίπτει/Αναιρεί/Δέχεται/…)
    if not decision_text:
        return None
    for verb in OUTCOME_VERBS:
        m = re.search(rf"{verb}[^.\n]*[.\n]", decision_text)
        if m:
            return m.group(0).strip().rstrip("\n")
    # fallback: first non-empty line
    for line in decision_text.splitlines():
        ln = line.strip()
        if ln:
            return ln
    return None

def build_intermediate(text: str, src_path: Path) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Returns (json_dict or None, error_message or None)
    """
    text = nfc(text)

    doc_number = extract_doc_number(text)
    court, docProponent, subDepartment = extract_court_and_titles(text)

    # Dates
    publicHearingDate = find_keyword_date(text, [
        re.compile(r"Συνεδρίασε\s+δημόσια.*?στις", re.IGNORECASE),
        re.compile(r"Συνήλθε\s+σε\s+δημόσια\s+συνεδρίαση.*?(?:την|στις)", re.IGNORECASE),
    ])
    courtConferenceDate = find_keyword_date(text, [
        re.compile(r"Η\s+διάσκεψη\s+έγινε.*?(?:την|στις)", re.IGNORECASE),
        re.compile(r"Κρίθηκε\s+και\s+αποφασίσθηκε.*?(?:την|στις)", re.IGNORECASE),
    ])
    decisionPublicationDate = find_keyword_date(text, [
        re.compile(r"δημοσιεύθηκε.*?(?:την|της|στις)", re.IGNORECASE),
    ])

    seg = segment_text(text)
    outcome = extract_outcome(seg["decision_text"])

    # Minimal acceptance
    missing = []
    if not doc_number:
        missing.append("docNumber")
    if not court:
        missing.append("court")
    if missing:
        return None, f"Missing required fields: {', '.join(missing)}"

    intermediate = {
        "court": court,
        "source": str(src_path),
        "checksum": sha256_text(text),
        "header": {
            "docNumber": doc_number,
            "docProponent": docProponent,
            "subDepartment": subDepartment,
            "headerDetails": None,
        },
        "publicHearingDate": publicHearingDate,
        "courtConferenceDate": courtConferenceDate,
        "decisionPublicationDate": decisionPublicationDate,
        "judgmentBody": {
            "introduction": seg["introduction"],
            "motivation": seg["motivation"],
            "decision": {
                "outcome": outcome,
                "decisionDetails": seg["decision_text"],
            },
            "conclusions": seg["conclusions"],
        },
    }
    return intermediate, None

# -----------------------------
# CLI
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Build intermediate JSON from raw TXT decisions.")
    ap.add_argument("--court", required=True, help="Court folder under root (e.g., areios_pagos, ste).")
    ap.add_argument("--year", required=True, help="Year folder (e.g., 2024).")
    ap.add_argument("--root", default="legal_texts", help="Root folder containing <court>/<year> (default: legal_texts).")
    ap.add_argument("--outroot", default="JSON", help="Root for output JSON (default: JSON).")
    ap.add_argument("--dry-run", action="store_true", help="Parse/validate without writing files.")
    args = ap.parse_args()

    base = Path(__file__).resolve().parent
    in_dir = (base / args.root / args.court / args.year).resolve()
    out_dir = (base / args.outroot / args.court / args.year).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not in_dir.exists():
        raise SystemExit(f"Input directory not found: {in_dir}")

    txt_files = sorted(in_dir.glob("*.txt"))
    if not txt_files:
        raise SystemExit(f"No TXT files found in: {in_dir}")

    print(f"[INFO] Parsing {len(txt_files)} TXT files from {in_dir}")
    print(f"[INFO] Output directory: {out_dir}")

    summary = {"ok": 0, "errors": 0, "items": []}

    for i, tf in enumerate(txt_files, 1):
        try:
            text = tf.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            # Retry with latin-1 as last resort
            text = tf.read_text(encoding="latin-1")
            text = text.encode("latin-1").decode("utf-8", errors="ignore")

        data, err = build_intermediate(text, tf)
        if err:
            summary["errors"] += 1
            item = {"file": str(tf), "ok": False, "error": err}
            summary["items"].append(item)
            err_path = (out_dir / tf.name).with_suffix(".parse-error.json")
            if not args.dry_run:
                err_path.write_text(json.dumps(item, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"  [{i}/{len(txt_files)}] ERR — {tf.name}: {err}")
            continue

        # Write JSON
        out_path = (out_dir / tf.name).with_suffix(".json")
        js = json.dumps(data, ensure_ascii=False, indent=2)
        if not args.dry_run:
            out_path.write_text(js, encoding="utf-8")

        summary["ok"] += 1
        summary["items"].append({"file": str(tf), "ok": True, "json": str(out_path)})
        print(f"  [{i}/{len(txt_files)}] OK  — {tf.name} -> {out_path.name}")

    # Batch report
    report = out_dir / "_text2json_batch_report.json"
    report.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[INFO] Done. OK={summary['ok']}  ERRORS={summary['errors']}")
    print(f"[INFO] Batch report: {report}")

if __name__ == "__main__":
    main()
