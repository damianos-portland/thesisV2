# -*- coding: utf-8 -*-
import argparse
import fnmatch
import os
import sys
import codecs
import time

from functions import (
    clean_areios_pagos_text,
    clean_ste_text,
    clean_nsk_text,
    delete_summaries,
    pdf_to_text,
    copy_files,
    GrToLat,
)
from variables import (
    TXT_EXT,
    PDF_EXT,
    STE,
    NSK,
    NSK_TMP,
    AREIOS_PAGOS,
    STE_METADATA,
    NSK_METADATA,
    LEGAL_CRAWLERS,
    DATA,
    LEGAL_TEXTS,
)

program_description = (
    "A Command Line Interface for implementing the "
    "pre-processing steps for judgments and legal opinions "
    "of the three major legal authorities of Greece. "
    "Pre-processing steps include garbage removal,"
    "escaping XML invalid characters and metadata "
    "storage management (if available)"
)

parser = argparse.ArgumentParser(
    description=program_description,
    epilog="Enjoy the program!",
)

parser.add_argument(
    "legal_authority",
    metavar="legal_authority",
    choices=[AREIOS_PAGOS, STE, NSK],
    help="run pre-processing steps for a specific legal authority",
)

year_help = (
    "choose a specific year for pre-processing (redundant for nsk). "
    "if absent all years will be included."
)
parser.add_argument("-year", help=year_help)
parser.add_argument(
    "-fn", metavar="FILENAME", help="choose a specific file for pre-processing"
)

args = parser.parse_args()

if __name__ == "__main__":

    if args.fn is not None and args.year is None and args.legal_authority != NSK:
        parser.error("You must provide -year parameter to process a specific file")

    file_pattern = "*" + (args.fn or TXT_EXT)

    # build source_path
    source_path = os.path.join(
        os.getcwd(),
        LEGAL_CRAWLERS,
        DATA,
        args.legal_authority,
    )
    if args.year is not None and args.legal_authority != NSK:
        source_path = os.path.join(source_path, args.year)

    # build dest_path
    dest_path = os.path.join(
        os.getcwd(),
        LEGAL_TEXTS,
        args.legal_authority,
    )
    if args.year is not None and args.legal_authority != NSK:
        dest_path = os.path.join(dest_path, args.year)

    # create the top‑level dest directory (and any subdirs) if needed
    os.makedirs(dest_path, exist_ok=True)

    if args.legal_authority == AREIOS_PAGOS:
        print("Start cleaning data...")
        time.sleep(1)
        clean_areios_pagos_text(source_path, dest_path, file_pattern)

        print("Creating latin names for file(s)...")
        time.sleep(1)
        GrToLat(dest_path)

        print("Start searching for summaries...")
        time.sleep(1)
        delete_summaries(dest_path)

    elif args.legal_authority == STE:
        print("Start cleaning data...")
        time.sleep(1)
        clean_ste_text(source_path, dest_path, file_pattern)

        print("Creating latin names for file(s)...")
        time.sleep(1)
        GrToLat(dest_path)

        # metadata folder sits alongside STE
        meta_dest = dest_path.replace(STE, STE_METADATA)
        os.makedirs(meta_dest, exist_ok=True)
        print("Creating latin names for metadata file(s)...")
        time.sleep(1)
        GrToLat(meta_dest)

        print("Start searching for summaries...")
        time.sleep(1)
        delete_summaries(dest_path, meta_dest)

    else:  # NSK
        # PDF → text
        print("Converting PDF file(s) to text...")
        time.sleep(1)
        tmp_dest = dest_path.replace(NSK, NSK_TMP)
        os.makedirs(tmp_dest, exist_ok=True)
        pdf_to_text(source_path, tmp_dest, file_pattern.replace(TXT_EXT, PDF_EXT))

        # clean NSK text
        print("Start cleaning data...")
        time.sleep(1)
        clean_nsk_text(tmp_dest, dest_path, file_pattern)

        # copy metadata files
        print("Copying metadata file(s) to dest...")
        time.sleep(1)
        meta_nsk_dest = dest_path.replace(NSK, NSK_METADATA)
        os.makedirs(meta_nsk_dest, exist_ok=True)
        copy_files(source_path, meta_nsk_dest, file_pattern)

        # create latin names
        print("Creating latin names for file(s)...")
        time.sleep(1)
        GrToLat(tmp_dest)
        GrToLat(dest_path)

        print("Creating latin names for metadata file(s)...")
        time.sleep(1)
        GrToLat(meta_nsk_dest)

        print("\n")
        print(
            "To enrich Akoma Ntoso metadata nodes, "
            "you may now run extractLegalOpinionsCstmMetadata.py"
        )
        print("\n")