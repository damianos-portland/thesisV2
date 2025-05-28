# -*- coding: UTF-8 -*-
"""
This crawler extracts judgment decisions of the Greek Supreme Civil and
Criminal court "Areios Pagos". It's architecture is based on the DOM
of the Pancyprian Bar Association website (see: www.cylaw.org).
"""
import os
import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
from datetime import datetime
from pymongo import MongoClient

# default to current year if none supplied
CURRENT_YEAR = datetime.now().year


class CyLawSpider(scrapy.Spider):
    name = "CyLaw"
    allowed_domains = ["cylaw.org"]

    # ─── add these lines ─────────────────────────────────────
    MONGO_URI = "mongodb://localhost:27017/"
    DB_NAME = "judgmentsV2"

    def __init__(self, year=f"{CURRENT_YEAR},{CURRENT_YEAR}", *args, **kwargs):
        # set up Mongo once
        client = MongoClient(self.MONGO_URI)
        self.decisions_col = client[self.DB_NAME]["courtDecisions"]
        super().__init__(*args, **kwargs)

        """
        year: "YYYY" or "YYYY,YYYY" (inclusive range)
        """  # Parse range
        parts = year.split(',')
        try:
            start = int(parts[0])
            end = int(parts[1]) if len(parts) > 1 else start
        except ValueError:
            raise ValueError("year must be YYYY or YYYY,YYYY")
        # build index pages for each year in [start..end]
        self.start_urls = [
            f"http://www.cylaw.org/areiospagos/index_{y}.html"
            for y in range(start, end + 1)
        ]
        # store folder (top‑level)
        self.STORE_DIR = os.path.join("data", "areios_pagos")
        os.makedirs(self.STORE_DIR, exist_ok=True)

    def parse(self, response):
        sel = Selector(response)
        # every <li><a href="..."> on the index page
        for rel in sel.xpath('//li/a/@href').getall():
            yield response.follow(rel, self.parse_decision)

    def parse_decision(self, response):
        sel = Selector(response)

        # extract and sanitize title → filename
        title = sel.xpath('//title/text()').get(default="judgment").strip()
        filename = title.replace('/', '_').strip()

        # extract year from URL:
        #  .../areiospagos/1/2017/2017_1_0004.html
        parts = response.url.split('/')
        if len(parts) >= 8 and parts[7].isdigit():
            year = parts[7]
        else:
            year = str(CURRENT_YEAR)

        # build per‑year subfolder
        outdir = os.path.join(self.STORE_DIR, year)
        os.makedirs(outdir, exist_ok=True)

        # gather all <p> text
        paras = sel.xpath('//p//text()').getall()
        body = '\n'.join(p.strip() for p in paras if p.strip())

        # final path (opening with 'w' *always* overwrites any existing file)
        path = os.path.join(outdir, f"{filename}.txt")
        # skip if already in Mongo
        if self.decisions_col.find_one({
            "header.docNumber": filename,
            "court": "Areios Pagos"
        }):
            self.logger.info(f"→ {filename} already in DB, skipping")
            return
        with open(path, 'w', encoding='utf-8') as f:
            # write header + blank line, then the body
            f.write(title + "\n\n")
            f.write(body + "\n")

        # log so you know we've just overwritten or created this file
        self.logger.info(f"✅ Saved (and overwritten if existed): {path}")
