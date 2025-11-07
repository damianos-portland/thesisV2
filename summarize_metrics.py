#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
summarize_metrics.py — Collect end-to-end KPIs from outputs + logs and emit LaTeX macros.

Usage:
  python summarize_metrics.py --courts ste areios_pagos --years 2024 2018 \
                              [--root .] [--logs logs] [--reportdir reports]

No external deps (standard library only).
"""

import argparse, json, re, statistics as stats
from pathlib import Path
from datetime import datetime

def glob_bytes(paths):
    total = 0
    for p in paths:
        try: total += p.stat().st_size
        except: pass
    return total

def list_files(d: Path, pattern: str):
    if not d.exists(): return []
    return sorted(d.glob(pattern))

def to_kb(b): return round(b / 1024.0, 1)

def percent(n, d):
    try: return round(100.0 * n / d, 1)
    except: return 0.0

def parse_times_from_logs(logdir: Path):
    """
    Parse parsing durations from any *.log under logs/.
    Heuristics for patterns like:
      - 'parse ... took 123 ms' / 'duration=0.123 s' / 'elapsed_ms=123'
    Returns list of ms (float).
    """
    times = []
    if not logdir.exists(): return times
    rx = [
        re.compile(r'(?:parse|parsing|AKN|xml).*?(?:took|elapsed|duration)\s*=?\s*(\d+(?:\.\d+)?)\s*(ms|msec|s|sec)', re.I),
        re.compile(r'elapsed_ms\s*=\s*(\d+)', re.I),
        re.compile(r'duration_ms\s*=\s*(\d+)', re.I),
    ]
    for lf in logdir.glob('**/*.log'):
        try:
            for line in lf.read_text(encoding='utf-8', errors='ignore').splitlines():
                for r in rx:
                    m = r.search(line)
                    if not m: continue
                    if len(m.groups()) == 2:
                        v, unit = m.group(1), m.group(2).lower()
                        val = float(v)
                        if unit in ('s','sec'): val *= 1000.0
                        times.append(val)
                    else:
                        times.append(float(m.group(1)))
        except: pass
    return times

def parse_error_counts(root: Path):
    """
    Count common error categories from *.parse-error.json and *.export-error.json.
    Keys produced: schema, wellformed, metadata, parsing, ingest, duplicates, deadletter, invalid, warn.
    """
    cats = dict(schema=0, wellformed=0, metadata=0, parsing=0, ingest=0, duplicates=0, deadletter=0, invalid=0, warn=0)
    # scan JSON files that end with those suffixes
    for jf in root.glob('**/*.parse-error.json'):
        try:
            j = json.loads(jf.read_text(encoding='utf-8'))
            msg = (j.get('error') or '').lower()
            if 'well' in msg and 'form' in msg: cats['wellformed'] += 1
            elif 'schema' in msg or 'xsd' in msg or 'akn skeleton' in msg: cats['schema'] += 1
            elif 'metadata' in msg or 'docnumber' in msg or 'date' in msg: cats['metadata'] += 1
            elif 'parse' in msg: cats['parsing'] += 1
            elif 'ingest' in msg or 'download' in msg: cats['ingest'] += 1
            elif 'duplicate' in msg: cats['duplicates'] += 1
            elif 'dead-letter' in msg: cats['deadletter'] += 1
            elif 'invalid' in msg: cats['invalid'] += 1
            elif 'warn' in msg: cats['warn'] += 1
        except: pass
    for jf in root.glob('**/*.export-error.json'):
        try:
            j = json.loads(jf.read_text(encoding='utf-8'))
            msgs = " ".join(j.get('errors', []))
            msg = msgs.lower()
            if 'well' in msg and 'form' in msg: cats['wellformed'] += 1
            elif 'schema' in msg or 'xsd' in msg: cats['schema'] += 1
            elif 'metadata' in msg or 'title' in msg or 'issued' in msg: cats['metadata'] += 1
            elif 'serialize' in msg or 'validation' in msg: cats['parsing'] += 1
        except: pass
    return cats

def load_batch_reports(root: Path):
    """
    Consult known batch reports if present.
    """
    res = {}
    for jf in root.glob('**/_text2json_batch_report.json'):
        try: res['text2json'] = json.loads(jf.read_text(encoding='utf-8'))
        except: pass
    for jf in root.glob('**/_rdf_export_batch_report.json'):
        try: res['rdf_export'] = json.loads(jf.read_text(encoding='utf-8'))
        except: pass
    return res

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', default='.', help='Repository root')
    ap.add_argument('--logs', default='logs', help='Logs directory')
    ap.add_argument('--reportdir', default='reports', help='Where to write metrics.{json,tex}')
    ap.add_argument('--courts', nargs='+', default=['ste','areios_pagos'])
    ap.add_argument('--years', nargs='+', required=True)
    args = ap.parse_args()

    root = Path(args.root).resolve()
    logs = (root / args.logs).resolve()
    repodir = (root / args.reportdir).resolve()
    repodir.mkdir(parents=True, exist_ok=True)

    # Aggregate counts
    txt_files, json_files, xml_files, ttl_files, jsonld_files = [], [], [], [], []
    for court in args.courts:
        for year in args.years:
            txt_files  += list_files(root / 'legal_texts' / court / year, '*.txt')
            json_files += list_files(root / 'JSON'        / court / year, '*.json')
            xml_files  += list_files(root / 'XML'         / court / year, '*.xml')
            ttl_files  += list_files(root / 'RDF'         / court / year, '*.ttl')
            jsonld_files += list_files(root / 'RDF'       / court / year, '*.jsonld')

    # Parse durations from logs
    times_ms = parse_times_from_logs(logs)
    times_ms = [t for t in times_ms if t > 0]
    avg_ms = round(stats.mean(times_ms), 1) if times_ms else 0.0
    p50_ms = round(stats.median(times_ms), 1) if times_ms else 0.0
    p95_ms = round(stats.quantiles(times_ms, n=100)[94], 1) if len(times_ms) >= 20 else (avg_ms or 0.0)

    # Sizes
    xml_sizes = [f.stat().st_size for f in xml_files if f.exists()]
    ttl_sizes = [f.stat().st_size for f in ttl_files if f.exists()]
    jsonld_sizes = [f.stat().st_size for f in jsonld_files if f.exists()]

    avg_xml_kb = round(to_kb(stats.mean(xml_sizes)), 1) if xml_sizes else 0.0
    p95_xml_kb = round(to_kb(stats.quantiles(xml_sizes, n=100)[94]), 1) if len(xml_sizes) >= 20 else avg_xml_kb
    avg_ttl_kb = round(to_kb(stats.mean(ttl_sizes)), 1) if ttl_sizes else 0.0
    avg_jsonld_kb = round(to_kb(stats.mean(jsonld_sizes)), 1) if jsonld_sizes else 0.0

    # Error categories
    cats = parse_error_counts(root)

    # Batch reports (if any)
    batches = load_batch_reports(root)

    # Time window (approx: use newest mtime among outputs)
    mtimes = [f.stat().st_mtime for f in (xml_files + ttl_files + jsonld_files) if f.exists()]
    if mtimes:
        end_ts = max(mtimes)
        start_ts = min(mtimes)
        start_iso = datetime.fromtimestamp(start_ts).isoformat(timespec='seconds')
        end_iso = datetime.fromtimestamp(end_ts).isoformat(timespec='seconds')
        duration_min = round((end_ts - start_ts)/60.0, 1)
    else:
        start_iso = end_iso = ''
        duration_min = 0.0

    data = {
        "courts": args.courts,
        "years": args.years,
        "counts": {
            "txt": len(txt_files),
            "json": len(json_files),
            "xml": len(xml_files),
            "ttl": len(ttl_files),
            "jsonld": len(jsonld_files),
        },
        "sizes_kb": {
            "xml_avg": avg_xml_kb,
            "xml_p95": p95_xml_kb,
            "ttl_avg": avg_ttl_kb,
            "jsonld_avg": avg_jsonld_kb,
        },
        "timings_ms": {
            "avg": avg_ms,
            "p50": p50_ms,
            "p95": p95_ms,
            "samples": len(times_ms),
        },
        "errors": cats,
        "batches": batches,
        "time_window": {
            "start": start_iso,
            "end": end_iso,
            "duration_minutes": duration_min
        }
    }

    # Heuristics for OK/Invalid/Warn:
    ok = len(xml_files)            # treat as successful AKN produced
    invalid = cats.get('schema',0) + cats.get('wellformed',0) + cats.get('metadata',0) + cats.get('parsing',0)
    warn = cats.get('warn', 0)

    # Write JSON report
    (repodir / 'metrics.json').write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')

    # Produce LaTeX macros
    courts_years = ", ".join(args.courts) + " — " + ", ".join(args.years)
    macros = f"""% Auto-generated by summarize_metrics.py
\\newcommand{{\\MetricCourtList}}{{{", ".join(args.courts).upper()}}}
\\newcommand{{\\MetricPeriod}}{{{", ".join(args.years)}}}
\\newcommand{{\\MetricTotalTxt}}{{{len(txt_files)}}}
\\newcommand{{\\MetricTotalJson}}{{{len(json_files)}}}
\\newcommand{{\\MetricTotalXml}}{{{len(xml_files)}}}
\\newcommand{{\\MetricTotalTtl}}{{{len(ttl_files)}}}
\\newcommand{{\\MetricTotalJsonld}}{{{len(jsonld_files)}}}
\\newcommand{{\\MetricOk}}{{{ok}}}
\\newcommand{{\\MetricInvalid}}{{{invalid}}}
\\newcommand{{\\MetricWarn}}{{{warn}}}
\\newcommand{{\\MetricDuplicates}}{{{cats.get('duplicates',0)}}}
\\newcommand{{\\MetricDeadLetter}}{{{cats.get('deadletter',0)}}}
\\newcommand{{\\MetricAvgParseMs}}{{{avg_ms}}}
\\newcommand{{\\MetricP50ParseMs}}{{{p50_ms}}}
\\newcommand{{\\MetricP95ParseMs}}{{{p95_ms}}}
\\newcommand{{\\MetricAvgXmlKB}}{{{avg_xml_kb}}}
\\newcommand{{\\MetricP95XmlKB}}{{{p95_xml_kb}}}
\\newcommand{{\\MetricAvgTtlKB}}{{{avg_ttl_kb}}}
\\newcommand{{\\MetricAvgJsonldKB}}{{{avg_jsonld_kb}}}
\\newcommand{{\\MetricErrWellformed}}{{{cats.get('wellformed',0)}}}
\\newcommand{{\\MetricErrSchema}}{{{cats.get('schema',0)}}}
\\newcommand{{\\MetricErrMetadata}}{{{cats.get('metadata',0)}}}
\\newcommand{{\\MetricErrParsing}}{{{cats.get('parsing',0)}}}
\\newcommand{{\\MetricErrIngest}}{{{cats.get('ingest',0)}}}
\\newcommand{{\\MetricStartTs}}{{{start_iso}}}
\\newcommand{{\\MetricEndTs}}{{{end_iso}}}
\\newcommand{{\\MetricDurationMinutes}}{{{duration_min}}}
"""
    (repodir / 'metrics.tex').write_text(macros, encoding='utf-8')

    print(f"[INFO] Wrote {repodir/'metrics.json'} and {repodir/'metrics.tex'}")
    print(f"[INFO] TXT={len(txt_files)} JSON={len(json_files)} XML={len(xml_files)} TTL={len(ttl_files)} JSONLD={len(jsonld_files)}")
    print(f"[INFO] parse_ms: avg={avg_ms} p50={p50_ms} p95={p95_ms} samples={len(times_ms)}")
    print(f"[INFO] errors: {cats}")
    print(f"[INFO] time window: {start_iso} — {end_iso} ({duration_min} min)")
if __name__ == "__main__":
    main()
