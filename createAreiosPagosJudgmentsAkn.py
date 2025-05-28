# -*- coding: utf-8 -*-
import os
import re
import datetime
import sys
import codecs
import logging
import fnmatch
import time
import argparse
from antlr4 import *
from antlr4.tree.Trees import Trees
from lxml import etree

from AknJudgementClass import AknJudgementXML
from AknLegalReferencesClass import AknLegalReferences
from functions import (
    validateXML,
    findDatesOfInterest,
    setupLogger,
    fixStringXML,
    CheckXMLvalidity,
)
from variables import *
from grammars.gen.SupremeCourtLexer import SupremeCourtLexer
from grammars.gen.SupremeCourtParser import SupremeCourtParser
from grammars.gen.SupremeCourtListener import SupremeCourtListener
from grammars.gen.Legal_refLexer import Legal_refLexer
from grammars.gen.Legal_refParser import Legal_refParser
from grammars.gen.Legal_refListener import Legal_refListener
from grammars.gen.Legal_refVisitor import Legal_refVisitor

program_description = (
    'A Command Line Interface to transform judgments '
    'published by the Supreme Civil and Criminal court '
    '(Areios Pagos) into XML using Akoma Ntoso prototype.'
)

parser = argparse.ArgumentParser(description=program_description)
parser.add_argument(
    '-year',
    help='choose a specific year for judgment(s) to be processed'
)
parser.add_argument(
    '-fn',
    metavar='FILENAME',
    help='choose a specific file to be transformed to Akoma Ntoso '
         '(if argument is present -year parameter must be declared)'
)
args = parser.parse_args()

if __name__ == '__main__':
    # compile regexes
    publicHearingDateObj = re.compile(publicHearingDatePattern)
    courtConferenceDateObj = re.compile(courtConferenceDatePattern)
    decisionPublicationDateObj = re.compile(decisionPublicationDatePattern)
    paragraphPatternObj = re.compile(paragraphPattern)

    # select file pattern
    if args.fn:
        if not args.year:
            parser.error('You must provide -year when using -fn')
        file_pattern = '*' + args.fn
    else:
        file_pattern = '*' + TXT_EXT

    # base folder
    source_path = os.path.join(os.getcwd(), LEGAL_TEXTS, AREIOS_PAGOS)
    if args.year:
        source_path = os.path.join(source_path, args.year)

    for root, dirs, files in os.walk(source_path):
        logs_path = root.replace(
            os.path.join(os.getcwd(), LEGAL_TEXTS),
            os.path.join(os.getcwd(), LOGS)
        )
        xml_path = root.replace(
            os.path.join(os.getcwd(), LEGAL_TEXTS),
            os.path.join(os.getcwd(), XML)
        )
        ner_path = root.replace(
            os.path.join(os.getcwd(), LEGAL_TEXTS),
            os.path.join(os.getcwd(), NER)
        )

        os.makedirs(logs_path, exist_ok=True)
        os.makedirs(xml_path, exist_ok=True)

        for name in files:
            if not fnmatch.fnmatch(name, file_pattern):
                continue

            print("judgment decision:", name)

            # prepare paths
            year_part = name.split('.')[0].split('_')[-1]
            log_file = os.path.join(logs_path, name)
            xml_file = os.path.join(xml_path, name.split('.')[0] + XML_EXT)
            text_file = os.path.join(xml_path, name.split('.')[0] + TXT_EXT)
            gate_xml_file = os.path.join(ner_path, name + XML_EXT)

            # set up logger immediately
            Akn_LOGGER = setupLogger('Akn_LOGGER', log_file)
            Akn_LOGGER.info('Starting conversion of %s', name)

            # start high‑res timer
            start_time = time.perf_counter()

            try:
                # ─── METADATA ──────────────────────────────────────────────
                meta = {
                    'textType': 'judgment',
                    'author': '#SCCC',
                    'foreas': 'SCCC',
                }
                # extract from filename
                m = re.search(r'Ar?\s+(?P<decisionNumber>\d+)[_](?P<issueYear>\d+)', name)
                if m:
                    meta['decisionNumber'] = m.group('decisionNumber')
                    meta['issueYear'] = m.group('issueYear')

                # build judgment object
                judgmentObj = AknJudgementXML(
                    textType=meta['textType'],
                    author=meta['author'],
                    foreas=meta['foreas'],
                    issueYear=meta['issueYear'],
                    decisionNumber=meta['decisionNumber']
                )
                metaElem = judgmentObj.createMeta()

                # inject NER if available
                if os.path.isfile(gate_xml_file):
                    refs = metaElem.find('references')
                    idx = list(metaElem).index(refs)
                    newRefs = judgmentObj.modifyReferencesFromGateXml(gate_xml_file, refs)
                    metaElem.remove(refs)
                    metaElem.insert(idx, newRefs)

                # ─── LEGAL REFERENCES ──────────────────────────────────────
                fin = FileStream(os.path.join(root, name), encoding='utf-8')
                lex = Legal_refLexer(fin)
                tok = CommonTokenStream(lex)
                pref = Legal_refParser(tok)
                tre = pref.legal_text()
                answer = AknLegalReferences().visit(tre)

                # ─── STRUCTURE ──────────────────────────────────────────────
                Akn_LOGGER.info('Building parse tree for %s', name)
                inp = InputStream(answer)
                lex2 = SupremeCourtLexer(inp)
                tok2 = CommonTokenStream(lex2)
                pars = SupremeCourtParser(tok2)
                tre2 = pars.judgment()
                walker = ParseTreeWalker()
                walker.walk(judgmentObj, tre2)

                # embed in‑text NER
                if os.path.isfile(gate_xml_file):
                    judgmentObj.text = judgmentObj.createNamedEntitiesInText(
                        gate_xml_file, judgmentObj.text
                    )

                # build AkomaNtoso root
                akomaNtosoElem = judgmentObj.createAkomaNtosoRoot()
                judgmentObj.text = fixStringXML(judgmentObj.text, paragraphPatternObj)
                jElem = judgmentObj.XML()
                akomaNtosoElem.insert(0, jElem)
                jNode = akomaNtosoElem.find('judgment')
                jNode.insert(0, metaElem)

                # ─── DATES OF INTEREST ─────────────────────────────────────
                Akn_LOGGER.info('Searching for dates of interest...')
                hdr   = akomaNtosoElem.xpath('/akomaNtoso/judgment/header')[0]
                concl = akomaNtosoElem.xpath('/akomaNtoso/judgment/conclusions')[0]
                wf    = akomaNtosoElem.xpath('/akomaNtoso/judgment/meta/workflow')[0]
                refs  = metaElem.xpath('/akomaNtoso/judgment/meta/references')[0]
                frbrW = akomaNtosoElem.xpath(
                    '/akomaNtoso/judgment/meta/identification/FRBRWork/FRBRdate'
                )[0]
                frbrE = akomaNtosoElem.xpath(
                    '/akomaNtoso/judgment/meta/identification/FRBRExpression/FRBRdate'
                )[0]

                def _add_date(node, regex, name):
                    res = findDatesOfInterest(node, regex, name, meta['author'])
                    if not res:
                        return
                    _, step, tlc = res
                    wf.insert(0, step)
                    refs.append(tlc)
                    frbrW.set('date', step.get('date'))
                    frbrW.set('name', name)
                    frbrE.set('date', step.get('date'))
                    frbrE.set('name', name)

                # public‐hearing
                _add_date(hdr, publicHearingDateObj, 'publicHearingDate')
                # court‐conference
                _add_date(concl, courtConferenceDateObj, 'courtConferenceDate')
                # decision‐publication: first try the whole <conclusions>
                _add_date(concl, decisionPublicationDateObj, 'decisionPublicationDate')

                # if no <date refersTo="decisionPublicationDate"> yet, try each <p> in conclusions
                if not concl.find('.//date[@refersTo="decisionPublicationDate"]'):
                    for p in concl.findall('p'):
                        _add_date(p, decisionPublicationDateObj, 'decisionPublicationDate')
                        if concl.find('.//date[@refersTo="decisionPublicationDate"]'):
                            break
                # …and finally the “ΔΗΜΟΣΙΕΥΘΗΚΕ”→next-<p> trick
                if not concl.find('.//date[@refersTo="decisionPublicationDate"]'):
                    ps = concl.findall('p')
                    for i, p in enumerate(ps[:-1]):
                        if 'ΔΗΜΟΣΙΕΥΘΗΚΕ' in ''.join(p.itertext()):
                            _add_date(ps[i+1], decisionPublicationDateObj, 'decisionPublicationDate')
                            break

                # ─── SERIALIZE & VALIDATE ───────────────────────────────────
                tree = etree.ElementTree(akomaNtosoElem)
                xml_bytes = (
                    etree.tostring(
                        tree,
                        pretty_print=True,
                        encoding='UTF-8',
                        xml_declaration=True
                    )
                    .replace(b'&gt;', b'>')
                )
                with open(xml_file, 'wb') as fout:
                    fout.write(xml_bytes)

                validateXML('akomantoso30.xsd', xml_file, log_file)

            except KeyboardInterrupt:
                raise

            except etree.XMLSyntaxError as e:
                Akn_LOGGER.error('XML syntax error in %s: %s', name, e)
                with open(text_file, 'w', encoding='utf-8') as fout:
                    fout.write(judgmentObj.text or '')

            except Exception as e:
                Akn_LOGGER.error('Unhandled error in %s: %s', name, e)
                with open(text_file, 'w', encoding='utf-8') as fout:
                    fout.write('')

            finally:
                # stop timer & log duration
                end_time = time.perf_counter()
                duration = round(end_time - start_time, 2)
                Akn_LOGGER.info('Finished %s in %ss', name, duration)
                logging.shutdown()