# -*- coding: utf-8 -*-
import os
import re
import datetime
import sys
import fnmatch
import time
import argparse
import traceback
import logging

from antlr4 import FileStream, CommonTokenStream, InputStream, ParseTreeWalker
from lxml import etree

from AknJudgementClass import AknJudgementXML
from AknLegalReferencesClass import AknLegalReferences
from functions import (
    validateXML,
    findDatesOfInterest,
    setupLogger,
    fixStringXML,
    CheckXMLvalidity
)
from variables import (
    LEGAL_TEXTS, STE, LOGS, XML, NER, STE_METADATA,
    TXT_EXT, XML_EXT,
    publicHearingDatePattern,
    courtConferenceDatePattern,
    decisionPublicationDatePattern,
    paragraphPattern
)

from grammars.gen.CouncilOfStateLexer import CouncilOfStateLexer
from grammars.gen.CouncilOfStateParser import CouncilOfStateParser
from grammars.gen.Legal_refLexer import Legal_refLexer
from grammars.gen.Legal_refParser import Legal_refParser
from antlr4.error.ErrorListener import ErrorListener


class SilentErrorListener(ErrorListener):
    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        # swallow ANTLR errors
        pass


def safe_to_str(data):
    """Convert bytes to UTF-8 string, leave str unchanged."""
    if isinstance(data, bytes):
        return data.decode('utf-8')
    if isinstance(data, str):
        return data
    raise TypeError(f"Unexpected type: {type(data)}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Transform Council of State texts into Akoma Ntoso XML"
    )
    parser.add_argument('-year', help='Year to process (e.g. 2017)')
    parser.add_argument('-fn', metavar='FILENAME',
                        help='Specific file to process (requires -year)')
    args = parser.parse_args()

    if args.fn and not args.year:
        parser.error("When using -fn, you must also specify -year")

    file_pattern = '*' + (args.fn if args.fn else TXT_EXT)
    source_base  = os.path.join(os.getcwd(), LEGAL_TEXTS, STE)
    if args.year:
        source_base = os.path.join(source_base, args.year)

    # compile regexes
    publicHearingDateObj       = re.compile(publicHearingDatePattern)
    courtConferenceDateObj     = re.compile(courtConferenceDatePattern)
    decisionPublicationDateObj = re.compile(decisionPublicationDatePattern)
    paragraphPatternObj        = re.compile(paragraphPattern)

    for root, dirs, files in os.walk(source_base):
        logs_path       = root.replace(
            os.path.join(os.getcwd(), LEGAL_TEXTS),
            os.path.join(os.getcwd(), LOGS)
        )
        xml_path        = root.replace(
            os.path.join(os.getcwd(), LEGAL_TEXTS),
            os.path.join(os.getcwd(), XML)
        )
        ner_path        = root.replace(
            os.path.join(os.getcwd(), LEGAL_TEXTS),
            os.path.join(os.getcwd(), NER)
        )
        ste_meta_path   = root.replace(
            os.path.join(os.getcwd(), LEGAL_TEXTS),
            os.path.join(os.getcwd(), STE_METADATA)
        )
        os.makedirs(logs_path, exist_ok=True)
        os.makedirs(xml_path,  exist_ok=True)

        for name in files:
            if not fnmatch.fnmatch(name, file_pattern):
                continue

            print(f"▷ Processing {name}")
            start_time = time.perf_counter()
            log_file    = os.path.join(logs_path, name)
            xml_file    = os.path.join(xml_path, name.rsplit('.',1)[0] + XML_EXT)
            txt_file    = os.path.join(root, name)
            gate_xml    = os.path.join(ner_path, name + XML_EXT)

            Akn_LOGGER = setupLogger('Akn_LOGGER', log_file)
            Akn_LOGGER.info(f"Converting {name}")

            try:
                # ─── METADATA ────────────────────────────────────────────────
                meta = {'textType':"judgment", 'author':"#COS", 'foreas':"COS"}
                year_part = name.rsplit('.',1)[0].split('_')[-1]
                meta_file = os.path.join(
                    ste_meta_path,
                    name.rsplit('.',1)[0] + '_meta' + TXT_EXT
                )
                if os.path.isfile(meta_file):
                    lines = open(meta_file, encoding='utf-8').read().splitlines()
                    num, yr = lines[0].split('/')
                    meta.update(
                        decisionNumber=num + "/" + yr,
                        issueYear=yr,
                        ECLI=None if lines[7].strip() in ('','-') else lines[7].strip()
                    )
                    try:
                        d = datetime.datetime.strptime(lines[3].strip(), '%d/%m/%Y').date()
                    except ValueError:
                        d = datetime.date(int(lines[3].strip()), 1, 1)
                    meta['publicationDate'] = str(d)
                else:
                    meta.update(
                        decisionNumber=name.split('_')[0],
                        issueYear=year_part,
                        ECLI=None,
                        publicationDate=None
                    )
                # ─────────────────────────────────────────────────────────────


                # ─── READ RAW TEXT FOR HEADER & INTRO ────────────────────────
                raw = open(txt_file, encoding='utf-8').read().splitlines()
                # find "Αριθμός X/Y"
                idx_num = next(i for i,ln in enumerate(raw) if ln.startswith("Αριθμός"))
                docNumber = raw[idx_num].split(" ",1)[1].strip()

                # skip blank lines
                idx = idx_num + 1
                while idx < len(raw) and not raw[idx].strip():
                    idx += 1
                docProponent = raw[idx].strip()

                idx += 1
                while idx < len(raw) and not raw[idx].strip():
                    idx += 1
                subDepartment = raw[idx].strip()

                # headerDetails until "Για να δικάσει" line
                hd_start = idx + 1
                intro_idx = next(i for i,ln in enumerate(raw)
                                 if ln.strip().startswith("Για να δικάσει"))
                headerDetails = " ".join(
                    ln.strip() for ln in raw[hd_start:intro_idx] if ln.strip()
                )
                introduction_block = "\n\n".join(raw[intro_idx:]).strip()
                # ─────────────────────────────────────────────────────────────

                # build our judgment object
                judgmentObj = AknJudgementXML(
                    textType       = meta['textType'],
                    author         = meta['author'],
                    foreas         = meta['foreas'],
                    issueYear      = meta['issueYear'],
                    decisionNumber = meta['decisionNumber'],
                    ECLI           = meta['ECLI'],
                    publicationDate= meta['publicationDate']
                )
                metaElem = judgmentObj.createMeta()
                if os.path.isfile(gate_xml):
                    refs = metaElem.find('references')
                    if refs is not None:
                        idx0 = list(metaElem).index(refs)
                        newr = judgmentObj.modifyReferencesFromGateXml(gate_xml, refs)
                        metaElem.remove(refs)
                        metaElem.insert(idx0, newr)

                # ─── LEGAL REFERENCES ───────────────────────────────────────
                stream1 = CommonTokenStream(Legal_refLexer(FileStream(txt_file, encoding='utf-8')))
                tree1   = Legal_refParser(stream1).legal_text()
                answer  = AknLegalReferences().visit(tree1)
                # ─────────────────────────────────────────────────────────────

                # ─── STRUCTURE PARSING ─────────────────────────────────────
                Akn_LOGGER.info("Parsing judgment structure")
                stream2 = CommonTokenStream(CouncilOfStateLexer(InputStream(answer)))
                parser2 = CouncilOfStateParser(stream2)
                tree2   = parser2.judgment()
                walker  = ParseTreeWalker()
                walker.walk(judgmentObj, tree2)
                # ─────────────────────────────────────────────────────────────

                # inline named entities if present
                if os.path.isfile(gate_xml):
                    judgmentObj.text = judgmentObj.createNamedEntitiesInText(gate_xml, judgmentObj.text)

                # build AkomaNtoso root
                ak = judgmentObj.createAkomaNtosoRoot()
                judgmentElem = judgmentObj.XML()
                ak.insert(0, judgmentElem)
                jud_node = ak.find('judgment')
                jud_node.insert(0, metaElem)

                # ─── OVERRIDE HEADER ───────────────────────────────────────
                hdr_node = ak.xpath("/akomaNtoso/judgment/header")[0]
                # clear existing children
                for c in list(hdr_node):
                    hdr_node.remove(c)
                # docNumber
                p1 = etree.SubElement(hdr_node, 'p')
                etree.SubElement(p1, 'docNumber').text = docNumber
                # docProponent
                p2 = etree.SubElement(hdr_node, 'p')
                etree.SubElement(p2, 'docProponent').text = docProponent
                # subDepartment
                p3 = etree.SubElement(hdr_node, 'p')
                p3.text = subDepartment
                # headerDetails
                if headerDetails:
                    p4 = etree.SubElement(hdr_node, 'p')
                    p4.text = headerDetails
                # ─────────────────────────────────────────────────────────────

                # ─── OVERRIDE INTRODUCTION ─────────────────────────────────
                intro_node = ak.xpath("/akomaNtoso/judgment/judgmentBody/introduction")[0]
                for c in list(intro_node):
                    intro_node.remove(c)
                for para in filter(bool, introduction_block.split('\n\n')):
                    p = etree.SubElement(intro_node, 'p')
                    p.text = para.strip()
                # ─────────────────────────────────────────────────────────────

                # ─── DATES OF INTEREST ─────────────────────────────────────
                wf_node   = ak.xpath("/akomaNtoso/judgment/meta/workflow")[0]
                refs_node = metaElem.find('references')
                frbrW     = ak.xpath("/akomaNtoso/judgment/meta/identification/FRBRWork/FRBRdate")[0]
                frbrE     = ak.xpath("/akomaNtoso/judgment/meta/identification/FRBRExpression/FRBRdate")[0]

                def add_date(node, regex, name):
                    res = findDatesOfInterest(node, regex, name, meta['author'])
                    if not res:
                        return
                    _, step, tlc = res
                    wf_node.insert(0, step)
                    if refs_node is not None:
                        refs_node.append(tlc)
                    frbrW.set('date', step.get('date')); frbrW.set('name', name)
                    frbrE.set('date', step.get('date')); frbrE.set('name', name)

                # publicHearingDate in header
                add_date(hdr_node, publicHearingDateObj, 'publicHearingDate')
                # courtConferenceDate in conclusions
                concl_node = ak.xpath("/akomaNtoso/judgment/conclusions")[0]
                add_date(concl_node, courtConferenceDateObj, 'courtConferenceDate')
                # decisionPublicationDate
                add_date(concl_node, decisionPublicationDateObj, 'decisionPublicationDate')
                # ─────────────────────────────────────────────────────────────

                # ─── SERIALIZE & VALIDATE ───────────────────────────────────
                tree = etree.ElementTree(ak)
                xml_bytes = etree.tostring(
                    tree,
                    pretty_print=True,
                    encoding='UTF-8',
                    xml_declaration=True
                )
                xml_str = safe_to_str(xml_bytes).replace('&gt;', '>')
                with open(xml_file, 'w', encoding='utf-8') as fout:
                    fout.write(xml_str)
                validateXML('akomantoso30.xsd', xml_file, log_file)
                Akn_LOGGER.info(f"Wrote XML → {xml_file}")

            except KeyboardInterrupt:
                raise
            except Exception:
                tb = traceback.format_exc()
                print(f"Error processing {name}:\n{tb}")
                Akn_LOGGER.error(f"❌ Failed {name}\n{tb}")
            finally:
                elapsed = round(time.perf_counter() - start_time, 2)
                Akn_LOGGER.info(f"Finished {name} in {elapsed}s")
                logging.shutdown()