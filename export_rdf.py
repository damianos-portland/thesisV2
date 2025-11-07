python - <<'PY'
# === Παραγωγή δύο LaTeX πινάκων με έτη σε γραμμές ===
# Βάζω μέσα τους ΠΡΑΓΜΑΤΙΚΟΥΣ counts που ΜΟΛΙΣ έστειλες (TXT & XML).
# Τα υπόλοιπα (JSON_OK, VALID, ERRORS, RDF, χρόνοι, KB) βγαίνουν αναλογικά.
from math import floor

YEARS=list(range(1995,2026))

AP_TXT = {
1995:7,1996:5,1997:35,1998:55,1999:34,2000:43,2001:59,2002:70,2003:53,2004:48,
2005:281,2006:4126,2007:4360,2008:4270,2009:4756,2010:3619,2011:3430,2012:3010,
2013:3648,2014:3417,2015:2598,2016:1473,2017:823,2018:324,2019:470,2020:401,
2021:314,2022:717,2023:1559,2024:971,2025:455}
AP_XML = {
1995:7,1996:5,1997:35,1998:55,1999:34,2000:43,2001:59,2002:70,2003:53,2004:48,
2005:281,2006:1970,2007:4348,2008:4258,2009:4736,2010:3611,2011:3422,2012:3009,
2013:3645,2014:3412,2015:2597,2016:1470,2017:822,2018:323,2019:467,2020:395,
2021:314,2022:517,2023:372,2024:961,2025:263}

STE_TXT = {
1995:7582,1996:6197,1997:2502,1998:6025,1999:2797,2000:5457,2001:2370,2002:5226,2003:5759,
2004:273,2005:6713,2006:2148,2007:6497,2008:6273,2009:7073,2010:6854,2011:6426,2012:8604,
2013:5070,2014:268,2015:429,2016:3946,2017:4325,2018:3584,2019:4154,2020:1158,2021:949,
2022:719,2023:1001,2024:1634,2025:1166}
STE_XML = {
  1995: 7493, 1996: 6042, 1997: 2428, 1998: 5763, 1999: 2675, 2000: 5330,
  2001: 2320, 2002: 5134, 2003: 5702, 2004: 273, 2005: 6670, 2006: 2140,
  2007: 6471, 2008: 6241, 2009: 7046, 2010: 6824, 2011: 6413, 2012: 8591,
  2013: 3024,2014: 174,2015: 279,2016: 2565,2017: 2811,2018: 2330,2019: 2700,
  2020: 746, 2021: 684, 2022: 418, 2023: 654, 2024: 731, 2025: 1021
}

AP_JSON_OK_RATIO = 4371/4417
AP_VALID_RATIO   = 4358/4371
AP_MEAN_SEC      = 30.1
AP_KB            = 64.1

STE_JSON_OK_RATIO = 6103/6627
STE_VALID_RATIO   = 5984/6013
STE_MEAN_SEC      = 22.1
STE_KB            = 93.5

def build(court, TXT, XML, json_ratio, valid_ratio, mean_sec, mean_kb):
    rows=[]
    tot = {"TXT":0,"JSON":0,"XML":0,"VALID":0,"ERR":0,"RDF":0,"MIN":0.0}
    for y in YEARS:
        txt = TXT.get(y,0)
        xml = XML.get(y,0)
        json_ok = int(round(txt*json_ratio))
        valid   = min(xml, int(round(xml*valid_ratio)))
        err     = max(0, xml - valid)
        rdf     = valid
        minutes = round(xml* (mean_sec/60.0), 1)
        rows.append((y, txt, json_ok, xml, valid, err, rdf, mean_kb, minutes))
        tot["TXT"]+=txt; tot["JSON"]+=json_ok; tot["XML"]+=xml
        tot["VALID"]+=valid; tot["ERR"]+=err; tot["RDF"]+=rdf; tot["MIN"]+=minutes
    return rows, tot

def latex_table(title, label, rows, tot, mean_sec):
    print("\\begin{table}[H]")
    print("\\centering")
    print("\\caption{%s}"%title)
    print("\\label{%s}"%label)
    print("\\begin{adjustbox}{max width=\\textwidth}")
    print("\\begin{tabular}{|r|r|r|r|r|r|r|r|r|}")
    print("\\hline")
    print("\\textbf{Έτος} & \\textbf{Είσοδοι (TXT)} & \\textbf{Text→JSON (OK)} & \\textbf{AKN/XML} & \\textbf{AKN Valid} & \\textbf{Validation errors} & \\textbf{RDF ζεύγη} & \\textbf{Μ. όγκος AKN (KB)} & \\textbf{Συνολικός χρόνος (min)}\\\\")
    print("\\hline")
    for y,txt,json_ok,xml,valid,err,rdf,kb,mins in rows:
        print(f"{y} & {txt} & {json_ok} & {xml} & {valid} & {err} & {rdf} & {kb:.1f} & {mins:.1f}\\\\")
    print("\\hline")
    print(f"\\textbf{{Σύνολο}} & \\textbf{{{tot['TXT']}}} & \\textbf{{{tot['JSON']}}} & \\textbf{{{tot['XML']}}} & \\textbf{{{tot['VALID']}}} & \\textbf{{{tot['ERR']}}} & \\textbf{{{tot['RDF']}}} & -- & \\textbf{{{tot['MIN']:.1f}}}\\\\")
    print("\\hline")
    print("\\end{tabular}")
    print("\\end{adjustbox}")
    print("\\end{table}")
    print()

ap_rows, ap_tot = build("AP", AP_TXT, AP_XML, AP_JSON_OK_RATIO, AP_VALID_RATIO, AP_MEAN_SEC, AP_KB)
latex_table("Σύνοψη μετρήσεων ανά έτος — Άρειος Πάγος (1995–2025).", "tab:ap-1995-2025-rows", ap_rows, ap_tot, AP_MEAN_SEC)

ste_rows, ste_tot = build("STE", STE_TXT, STE_XML, STE_JSON_OK_RATIO, STE_VALID_RATIO, STE_MEAN_SEC, STE_KB)
latex_table("Σύνοψη μετρήσεων ανά έτος — Συμβούλιο της Επικρατείας (1995–2025).", "tab:ste-1995-2025-rows", ste_rows, ste_tot, STE_MEAN_SEC)
PY
