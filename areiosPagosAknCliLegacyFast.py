# akn_cli_legacy_fast.py
# -*- coding: utf-8 -*-
import os
import re
import fnmatch
import time
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed

from antlr4 import FileStream, InputStream, CommonTokenStream, ParseTreeWalker
from lxml import etree

# === Project imports (όπως στο αρχικό σου script) ============================
from AknJudgementClass import AknJudgementXML
from AknLegalReferencesClass import AknLegalReferences
from functions import (
    validateXML,
    findDatesOfInterest,
    setupLogger,
    fixStringXML,
)
from variables import *
from grammars.gen.SupremeCourtLexer import SupremeCourtLexer
from grammars.gen.SupremeCourtParser import SupremeCourtParser
from grammars.gen.SupremeCourtListener import SupremeCourtListener
from grammars.gen.Legal_refLexer import Legal_refLexer
from grammars.gen.Legal_refParser import Legal_refParser
from grammars.gen.Legal_refListener import Legal_refListener
from grammars.gen.Legal_refVisitor import Legal_refVisitor

# === Ζέσταμα ανά worker (μία φορά) ===========================================
REGEXES = {}
XPATHS = {}

def init_worker():
    """Precompile regex & XPath μία φορά ανά process για ταχύτητα."""
    global REGEXES, XPATHS
    REGEXES = {
        'public': re.compile(publicHearingDatePattern),
        'conf': re.compile(courtConferenceDatePattern),
        'pub': re.compile(decisionPublicationDatePattern),
        'para': re.compile(paragraphPattern),
    }
    XPATHS = {
        'hdr': etree.XPath('/akomaNtoso/judgment/header'),
        'concl': etree.XPath('/akomaNtoso/judgment/conclusions'),
        'wf': etree.XPath('/akomaNtoso/judgment/meta/workflow'),
        'refs': etree.XPath('/akomaNtoso/judgment/meta/references'),
        'frbrW': etree.XPath('/akomaNtoso/judgment/meta/identification/FRBRWork/FRBRdate'),
        'frbrE': etree.XPath('/akomaNtoso/judgment/meta/identification/FRBRExpression/FRBRdate'),
    }

# === Helper για dates (ίδια λογική) ==========================================
def _add_date(node, regex, name, wf, refs, frbrW, frbrE, author):
    res = findDatesOfInterest(node, regex, name, author)
    if not res:
        return
    _, step, tlc = res
    wf.insert(0, step)
    refs.append(tlc)
    frbrW.set('date', step.get('date')); frbrW.set('name', name)
    frbrE.set('date', step.get('date')); frbrE.set('name', name)

# === Επεξεργασία ενός αρχείου (πανομοιότυπο output) ==========================
def process_one(task):
    root, name = task
    t0 = time.perf_counter()

    base_texts_root = os.path.join(os.getcwd(), LEGAL_TEXTS)
    logs_path = root.replace(base_texts_root, os.path.join(os.getcwd(), LOGS))
    xml_path  = root.replace(base_texts_root, os.path.join(os.getcwd(), XML))
    ner_path  = root.replace(base_texts_root, os.path.join(os.getcwd(), NER))

    os.makedirs(logs_path, exist_ok=True)
    os.makedirs(xml_path,  exist_ok=True)

    log_file = os.path.join(logs_path, name)
    xml_file = os.path.join(xml_path, name.split('.')[0] + XML_EXT)
    text_file = os.path.join(xml_path, name.split('.')[0] + TXT_EXT)
    gate_xml_file = os.path.join(ner_path, name + XML_EXT)

    logger = setupLogger('Akn_LOGGER', log_file)
    logger.info('Starting conversion of %s', name)

    try:
        print("judgment decision:", name)

        # --- METADATA (ίδιο extraction) ---
        meta = {
            'textType': 'judgment',
            'author': '#SCCC',
            'foreas': 'SCCC',
            'issueYear': '',
            'decisionNumber': '',
        }
        m = re.search(r'Ar?\s+(?P<decisionNumber>\d+)[_](?P<issueYear>\d+)', name)
        if m:
            meta['decisionNumber'] = m.group('decisionNumber')
            meta['issueYear'] = m.group('issueYear')

        # --- LEGAL REFERENCES (1ο πέρασμα) ---
        fin = FileStream(os.path.join(root, name), encoding='utf-8')
        lex = Legal_refLexer(fin)
        tok = CommonTokenStream(lex)
        pref = Legal_refParser(tok)
        tre = pref.legal_text()
        answer = AknLegalReferences().visit(tre)

        # --- STRUCTURE (2ο πέρασμα) ---
        inp = InputStream(answer)
        lex2 = SupremeCourtLexer(inp)
        tok2 = CommonTokenStream(lex2)
        pars = SupremeCourtParser(tok2)
        tre2 = pars.judgment()
        walker = ParseTreeWalker()

        judgmentObj = AknJudgementXML(
            textType=meta['textType'],
            author=meta['author'],
            foreas=meta['foreas'],
            issueYear=meta['issueYear'],
            decisionNumber=meta['decisionNumber']
        )
        walker.walk(judgmentObj, tre2)

        metaElem = judgmentObj.createMeta()

        # --- Inject NER references (ίδιο) ---
        if os.path.isfile(gate_xml_file):
            refs_node = metaElem.find('references')
            if refs_node is not None:
                idx = list(metaElem).index(refs_node)
                newRefs = judgmentObj.modifyReferencesFromGateXml(gate_xml_file, refs_node)
                metaElem.remove(refs_node); metaElem.insert(idx, newRefs)

        # --- In-text NER (ίδιο) ---
        if os.path.isfile(gate_xml_file):
            judgmentObj.text = judgmentObj.createNamedEntitiesInText(
                gate_xml_file, judgmentObj.text
            )

        # --- Build AkomaNtoso (ίδιο) ---
        akn = judgmentObj.createAkomaNtosoRoot()
        judgmentObj.text = fixStringXML(judgmentObj.text, REGEXES['para'])
        jElem = judgmentObj.XML()
        akn.insert(0, jElem)
        jNode = akn.find('judgment')
        jNode.insert(0, metaElem)

        # --- Dates of interest (ίδιο αποτέλεσμα, πιο γρήγορα με precompiled XPath) ---
        hdr   = XPATHS['hdr'](akn)[0]
        concl = XPATHS['concl'](akn)[0]
        wf    = XPATHS['wf'](akn)[0]
        refs  = XPATHS['refs'](akn)[0]
        frbrW = XPATHS['frbrW'](akn)[0]
        frbrE = XPATHS['frbrE'](akn)[0]

        _add_date(hdr,   REGEXES['public'], 'publicHearingDate',     wf, refs, frbrW, frbrE, meta['author'])
        _add_date(concl, REGEXES['conf'],   'courtConferenceDate',   wf, refs, frbrW, frbrE, meta['author'])
        _add_date(concl, REGEXES['pub'],    'decisionPublicationDate', wf, refs, frbrW, frbrE, meta['author'])

        if not concl.find('.//date[@refersTo="decisionPublicationDate"]'):
            for p in concl.findall('p'):
                _add_date(p, REGEXES['pub'], 'decisionPublicationDate', wf, refs, frbrW, frbrE, meta['author'])
                if concl.find('.//date[@refersTo="decisionPublicationDate"]'):
                    break

        if not concl.find('.//date[@refersTo="decisionPublicationDate"]'):
            ps = concl.findall('p')
            for i, p in enumerate(ps[:-1]):
                if 'ΔΗΜΟΣΙΕΥΘΗΚΕ' in ''.join(p.itertext()):
                    _add_date(ps[i+1], REGEXES['pub'], 'decisionPublicationDate', wf, refs, frbrW, frbrE, meta['author'])
                    break

        # --- Serialize (απολύτως ίδιο με πριν) ---
        tree = etree.ElementTree(akn)
        xml_bytes = etree.tostring(
            tree,
            pretty_print=True,          # ίδιο με πριν
            encoding='UTF-8',
            xml_declaration=True
        )
        # Σκόπιμα κρατάμε το ΠΑΛΙΟ global replace για απόλυτη ταυτότητα bytes
        xml_bytes = xml_bytes.replace(b'&gt;', b'>')

        with open(xml_file, 'wb') as fout:
            fout.write(xml_bytes)

        # --- Validation (ίδια κλήση για να μη διαφέρει τίποτα) ---
        validateXML('akomantoso30.xsd', xml_file, log_file)

        status = 'ok'

    except etree.XMLSyntaxError as e:
        status = 'xml_syntax_error'
        logger.error('XML syntax error in %s: %s', name, e)
        try:
            with open(text_file, 'w', encoding='utf-8') as fout:
                fout.write(judgmentObj.text or '')
        except Exception:
            pass

    except KeyboardInterrupt:
        raise

    except Exception as e:
        status = 'unhandled_error'
        logger.error('Unhandled error in %s: %s', name, e)
        try:
            with open(text_file, 'w', encoding='utf-8') as fout:
                fout.write('')
        except Exception:
            pass

    finally:
        dur = round(time.perf_counter() - t0, 3)
        logger.info('Finished %s in %ss', name, dur)

    return (name, status, dur)

# === Συγκέντρωση εργασιών ====================================================
def enumerate_tasks(source_path: str, file_pattern: str):
    for root, _, files in os.walk(source_path):
        # Δημιούργησε τα output dirs ανά φάκελο (λιγότερο contention)
        base_texts_root = os.path.join(os.getcwd(), LEGAL_TEXTS)
        logs_path = root.replace(base_texts_root, os.path.join(os.getcwd(), LOGS))
        xml_path  = root.replace(base_texts_root, os.path.join(os.getcwd(), XML))
        os.makedirs(logs_path, exist_ok=True)
        os.makedirs(xml_path,  exist_ok=True)

        for name in files:
            if fnmatch.fnmatch(name, file_pattern):
                yield (root, name)

# === main ====================================================================
def main():
    program_description = (
        'Γρήγορο CLI που παράγει IDENTICAL (byte-for-byte) XML με το παλιό script, '
        'αλλά με σημαντικά καλύτερο throughput μέσω parallel επεξεργασίας.'
    )
    parser = argparse.ArgumentParser(description=program_description)
    parser.add_argument('-year', help='choose a specific year for judgment(s)')
    parser.add_argument('-fn', metavar='FILENAME',
                        help='choose a specific file (requires -year)')
    parser.add_argument('--workers', type=int, default=os.cpu_count(),
                        help='parallel processes (default: all CPUs)')
    args = parser.parse_args()

    if args.fn and not args.year:
        parser.error('You must provide -year when using -fn')

    # ίδιο selection logic
    file_pattern = '*' + (args.fn if args.fn else TXT_EXT)
    source_path = os.path.join(os.getcwd(), LEGAL_TEXTS, AREIOS_PAGOS)
    if args.year:
        source_path = os.path.join(source_path, args.year)

    tasks = list(enumerate_tasks(source_path, file_pattern))
    if not tasks:
        print('No files matched.')
        return

    t_start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=args.workers, initializer=init_worker) as ex:
        futs = [ex.submit(process_one, t) for t in tasks]
        done = 0
        ok = 0
        for fut in as_completed(futs):
            name, status, dur = fut.result()
            done += 1
            ok += 1 if status == 'ok' else 0
            # ελαφρύ progress
            if done % 50 == 0 or status != 'ok':
                print(f'[{done}/{len(futs)}] {name}: {status} ({dur}s)')

    total = round(time.perf_counter() - t_start, 2)
    print(f'All done in {total}s. Files: {len(tasks)}. Workers: {args.workers}. OK: {ok}.')

if __name__ == '__main__':
    main()
