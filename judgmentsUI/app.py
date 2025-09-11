#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import math
import urllib.parse
from datetime import datetime
from lxml import etree

from dotenv import load_dotenv
from flask import (
    Flask, render_template, request,
    redirect, url_for, session, abort,
    make_response, jsonify
)
from pymongo import MongoClient
from bson.objectid import ObjectId

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ["FLASK_SECRET"]

# ─── MongoDB setup ─────────────────────────────────────────────────────
MONGO_URI       = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME         = "judgmentsV2"
COLLECTION_NAME = "courtDecisions"
client     = MongoClient(MONGO_URI)
collection = client[DB_NAME][COLLECTION_NAME]

parsing_requests = client[DB_NAME]["parsingRequests"]

# ─── Auth helper ──────────────────────────────────────────────────────
def login_required(f):
    from functools import wraps
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return wrapped

# ─── Court translation filter ──────────────────────────────────────────
@app.template_filter('translate_court')
def translate_court(name: str) -> str:
    return {
        'Areios Pagos':     'ΑΡΕΙΟΣ ΠΑΓΟΣ',
        'Council of State': 'ΣΥΜΒΟΥΛΙΟ ΤΗΣ ΕΠΙΚΡΑΤΕΙΑΣ'
    }.get(name, name)

# ─── 1) Home ───────────────────────────────────────────────────────────
@app.route("/")
def index():
    return redirect(url_for("search"))

# ─── 2) Admin login/logout ─────────────────────────────────────────────
@app.route("/admin/login", methods=["GET","POST"])
def admin_login():
    error = None
    if request.method=="POST":
        if request.form["username"]=="admin" and request.form["password"]=="s3cr3t":
            session["admin"] = True
            return redirect(url_for("search"))
        error = "Λανθασμένα στοιχεία"
    return render_template("login.html", error=error)

@app.route("/admin/logout")
def admin_logout():
    session.pop("admin", None)
    return redirect(url_for("index"))

# ─── 3) Download XML ────────────────────────────────────────────────────
@app.route("/decision/<decision_id>/xml")
def download_xml(decision_id):
    doc = collection.find_one(
        {"_id": ObjectId(decision_id)},
        {"xml":1, "header.docNumber":1}
    )
    if not doc:
        abort(404)

    xml_bytes = doc["xml"].encode("utf-8")
    filename = f'{doc["header"]["docNumber"]}.xml'
    headers = {}
    if request.args.get("download"):
        headers["Content-Disposition"] = f'attachment; filename="{filename}"'

    return make_response(
        xml_bytes, 200,
        {**headers, "Content-Type":"application/xml; charset=utf-8"}
    )

# ─── 4) Detail ─────────────────────────────────────────────────────────
@app.route("/decision/<decision_id>")
def decision_detail(decision_id):
    doc = collection.find_one({"_id": ObjectId(decision_id)})
    if not doc:
        abort(404)
    doc["_id"] = str(doc["_id"])
    return render_template("decisionDetail.html", decision=doc)

# ─── 5) Edit Decision ──────────────────────────────────────────────────
@app.route("/decision/<decision_id>/edit", methods=["GET","POST"])
@login_required
def edit_decision(decision_id):
    oid = ObjectId(decision_id)
    doc = collection.find_one({"_id": oid})
    if not doc:
        abort(404)

    if request.method=="POST":
        data = request.form
        # 1) Ενημέρωσε μόνο τα DB‐fields
        updates = {
            "header.docNumber":     data["docNumber"],
            "header.docProponent":  data["docProponent"],
            "header.subDepartment": data["subDepartment"],
            "header.headerDetails": data["headerDetails"],
            "courtConferenceDate":  data["courtConferenceDate"],
            "decisionPublicationDate": data["decisionPublicationDate"],
            "judgmentBody.introduction":     data["introduction"],
            "judgmentBody.motivation":       data["motivation"],
            "judgmentBody.decision.outcome": data["outcome"],
            "judgmentBody.decision.decisionDetails": data["decisionDetails"],
            "conclusions":        data["conclusions"]
        }
        collection.update_one({"_id": oid}, {"$set": updates})

        # 2) Load & parse xml
        xml_str = doc.get("xml")
        if xml_str:
            parser = etree.XMLParser(remove_blank_text=True)
            tree   = etree.fromstring(xml_str.encode("utf-8"), parser)

            # header
            hdr = tree.find(".//akn:header", NS)
            hdr.find("akn:p[1]/akn:docNumber", NS).text     = data["docNumber"]
            hdr.find("akn:p[2]/akn:docProponent", NS).text  = data["docProponent"]
            hdr.find("akn:p[3]", NS).text                   = data["subDepartment"]
            # τα υπόλοιπα p → headerDetails (ίσως χρειαστεί custom logic)

            # dates
            for d in tree.xpath("//akn:date[@refersTo='courtConferenceDate']", namespaces=NS):
                d.set("date", data["courtConferenceDate"])
            for d in tree.xpath("//akn:date[@refersTo='decisionPublicationDate']", namespaces=NS):
                d.set("date", data["decisionPublicationDate"])

            # judgmentBody
            jb = tree.find(".//akn:judgmentBody", NS)
            jb.find("akn:introduction/akn:p", NS).text = data["introduction"]
            jb.find("akn:motivation/akn:p", NS).text   = data["motivation"]

            dec = jb.find("akn:decision", NS)
            dec.find("akn:p[2]/akn:outcome", NS).text = data["outcome"]
            # decisionDetails: π.χ. όλα τα p μετά το 2ο
            for p, txt in zip(dec.findall("akn:p", NS)[2:], data["decisionDetails"].split("\n")):
                p.text = txt

            # conclusions
            concl = tree.find(".//akn:conclusions/akn:p", NS)
            concl.text = data["conclusions"]

            # 3) serialize & save
            new_xml = etree.tostring(tree, xml_declaration=True,
                                     encoding="UTF-8", pretty_print=True).decode("utf-8")
            collection.update_one({"_id": oid}, {"$set": {"xml": new_xml}})

        return redirect(url_for("decision_detail", decision_id=decision_id))

    # GET
    doc["_id"] = str(doc["_id"])
    return render_template("editDecision.html", decision=doc)

# ─── 6) Delete Decision ─────────────────────────────────────────────────
@app.route("/decision/<decision_id>/delete", methods=["POST"])
@login_required
def delete_decision(decision_id):
    collection.delete_one({"_id": ObjectId(decision_id)})

    # if called via AJAX, return JSON so front-end can simply reload
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify(success=True)

    # fallback: full page reload preserving all filters & paging
    return redirect(url_for("search",
                            **{k: request.args.get(k) for k in
                               ("year","court","file_number","header_kw","body_kw","page","per_page")}
                            ))

# ─── 7) Search with pagination ──────────────────────────────────────────
@app.route("/search", methods=["GET"])
def search():
    # 1) gather filters from URL
    params = {
        'from_year':  request.args.get('from_year',''),
        'to_year':    request.args.get('to_year',''),
        'year':       request.args.get('year',''),  # legacy support
        'court':      request.args.get('court',''),
        'doc_number': request.args.get('doc_number',''),
        'header_kw':  request.args.get('header_kw',''),
        'body_kw':    request.args.get('body_kw',''),
        'per_page':   request.args.get('per_page','10'),
    }
    try:
        page     = max(1, int(request.args.get('page','1')))
        per_page = max(1, int(params['per_page']))
    except ValueError:
        page, per_page = 1, 10

    # 2) build Mongo filter
    filt = {}
    # Year range filtering: use courtConferenceDate (Mongo Date)
    fy = params.get('from_year') or params.get('year')
    ty = params.get('to_year') or params.get('year')
    date_range = {}
    try:
        if fy:
            fy_i = int(fy)
            date_range['$gte'] = datetime(fy_i, 1, 1)
    except ValueError:
        pass
    try:
        if ty:
            ty_i = int(ty)
            # include the whole 'to' year
            date_range['$lte'] = datetime(ty_i, 12, 31, 23, 59, 59)
    except ValueError:
        pass
    if date_range:
        filt['courtConferenceDate'] = date_range

    if params['court']:
        filt['court'] = params['court']
    if params.get('doc_number'):
        # allow searching like "56/2024" or partial "56"
        filt['header.docNumber'] = {
            '$regex': re.escape(params['doc_number']),
            '$options': 'i'
        }

    ors = []
    if params['header_kw']:
        hw = re.compile(re.escape(params['header_kw']), re.IGNORECASE)
        for p in ['header.docNumber','header.docProponent','header.subDepartment','header.headerDetails']:
            ors.append({p: hw})
    if params['body_kw']:
        bw = re.compile(re.escape(params['body_kw']), re.IGNORECASE)
        for p in [
            'judgmentBody.introduction',
            'judgmentBody.motivation',
            'judgmentBody.decision.outcome',
            'judgmentBody.decision.decisionDetails'
        ]:
            ors.append({p: bw})
    if ors:
        filt['$or'] = ors

    # 3) count & fetch
    total = collection.count_documents(filt)
    total_pages = max(1, math.ceil(total / per_page))
    skip = (page-1)*per_page
    docs = list(collection.find(filt)
                .sort('courtConferenceDate', -1)
                .skip(skip)
                .limit(per_page))
    for d in docs:
        d['_id'] = str(d['_id'])

    # 4) prepare base_url + base_qs (filters only, no page)
    qs = {k: v for k, v in params.items() if v}
    base_qs  = urllib.parse.urlencode(qs)
    base_url = url_for('search')

    return render_template("searchResults.html",
                           decisions   = docs,
                           courts      = collection.distinct("court"),
                           params      = params,
                           total       = total,
                           page        = page,
                           per_page    = per_page,
                           total_pages = total_pages,
                           start_page  = max(2, page-2),
                           end_page    = min(total_pages-1, page+2),
                           current_year= datetime.now().year,
                           base_url    = base_url,
                           base_qs     = base_qs,
                           admin_view  = session.get("admin", False)
                           )

# ─── 9) Trigger parsing (admin only) ────────────────────────────────
@app.route("/admin/trigger_parse", methods=["POST"])
@login_required
def trigger_parse():
    # we only ever run for “this year”
    year = str(datetime.utcnow().year)

    # insert a single “pending” job
    parsing_requests.insert_one({
        "year":         year,
        "requested_at": datetime.utcnow(),
        "status":       "pending"
    })

    return jsonify(
        success=True,
        message=f"Scheduled parsing for year {year}"
    )

if __name__ == "__main__":
    app.run(debug=True)

