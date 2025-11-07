# steAknCliLegacyFast.py
# -*- coding: utf-8 -*-
from __future__ import print_function
import os, re, datetime, fnmatch, time, argparse, traceback, logging, unicodedata

from concurrent.futures import ProcessPoolExecutor, as_completed
from antlr4 import FileStream, CommonTokenStream, InputStream, ParseTreeWalker
from antlr4.error.ErrorListener import ErrorListener
from lxml import etree

from AknJudgementClass import AknJudgementXML
from AknLegalReferencesClass import AknLegalReferences
from functions import validateXML, findDatesOfInterest, setupLogger, fixStringXML
from variables import (
    LEGAL_TEXTS, STE, LOGS, XML, NER, STE_METADATA,
    TXT_EXT, XML_EXT,
    publicHearingDatePattern, courtConferenceDatePattern,
    decisionPublicationDatePattern, paragraphPattern
)
from grammars.gen.CouncilOfStateLexer import CouncilOfStateLexer
from grammars.gen.CouncilOfStateParser import CouncilOfStateParser
from grammars.gen.Legal_refLexer import Legal_refLexer
from grammars.gen.Legal_refParser import Legal_refParser

# ───────── Silent ANTLR diagnostics ─────────
class SilentErrorListener(ErrorListener):
    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        pass

# ---------- Globals per worker (precompiled) ----------
REGEXES, XPATHS = {}, {}

def init_worker():
    global REGEXES, XPATHS
    REGEXES = {
        'public': re.compile(publicHearingDatePattern),
        'conf':   re.compile(courtConferenceDatePattern),
        'pub':    re.compile(decisionPublicationDatePattern),
        'para':   re.compile(paragraphPattern),
    }
    XPATHS = {
        'hdr':    etree.XPath("/akomaNtoso/judgment/header"),
        'intro':  etree.XPath("/akomaNtoso/judgment/judgmentBody/introduction"),
        'concl':  etree.XPath("/akomaNtoso/judgment/conclusions"),
        'wf':     etree.XPath("/akomaNtoso/judgment/meta/workflow"),
        'frbrW':  etree.XPath("/akomaNtoso/judgment/meta/identification/FRBRWork/FRBRdate"),
        'frbrE':  etree.XPath("/akomaNtoso/judgment/meta/identification/FRBRExpression/FRBRdate"),
        'refsInMeta': etree.XPath("/akomaNtoso/judgment/meta/references"),
    }

def safe_to_str(data):
    if isinstance(data, bytes): return data.decode('utf-8')
    if isinstance(data, str):   return data
    raise TypeError("Unexpected type: {}".format(type(data)))

def normalize_gr(s):
    s = s.strip().lower()
    s = unicodedata.normalize('NFD', s)
    return ''.join(ch for ch in s if unicodedata.category(ch) != 'Mn')

def find_index(lines, predicate):
    for i, ln in enumerate(lines):
        if predicate(ln): return i
    return -1

# ---------- Dates helper ----------
def add_date(node, regex, name, wf_node, refs_node, frbrW, frbrE, author):
    res = findDatesOfInterest(node, regex, name, author)
    if not res: return
    _, step, tlc = res
    wf_node.insert(0, step)
    if refs_node is not None: refs_node.append(tlc)
    frbrW.set('date', step.get('date')); frbrW.set('name', name)
    frbrE.set('date', step.get('date')); frbrE.set('name', name)

# ---------- Core processing ----------
def process_one(task):
    root, name = task
    start_time = time.perf_counter()

    base_texts_root = os.path.join(os.getcwd(), LEGAL_TEXTS)
    logs_path = root.replace(base_texts_root, os.path.join(os.getcwd(), LOGS))
    xml_path  = root.replace(base_texts_root, os.path.join(os.getcwd(), XML))
    ner_path  = root.replace(base_texts_root, os.path.join(os.getcwd(), NER))
    ste_meta_path = root.replace(base_texts_root, os.path.join(os.getcwd(), STE_METADATA))

    os.makedirs(logs_path, exist_ok=True)
    os.makedirs(xml_path,  exist_ok=True)

    log_file = os.path.join(logs_path, name)
    xml_file = os.path.join(xml_path, name.rsplit('.',1)[0] + XML_EXT)
    txt_file = os.path.join(root, name)
    gate_xml = os.path.join(ner_path, name + XML_EXT)

    logger = setupLogger('Akn_LOGGER', log_file)
    logger.info("Converting {}".format(name))

    try:
        print("▷ Processing {}".format(name))

        # ─── METADATA ─────────────────────────────────────────────────
        meta = {'textType': "judgment", 'author': "#COS", 'foreas': "COS"}
        year_part = name.rsplit('.',1)[0].split('_')[-1]
        meta_file = os.path.join(ste_meta_path, name.rsplit('.',1)[0] + '_meta' + TXT_EXT)

        if os.path.isfile(meta_file):
            lines = open(meta_file, encoding='utf-8').read().splitlines()
            num, yr = lines[0].split('/')
            meta.update({
                'decisionNumber': num + "/" + yr,
                'issueYear': yr,
                'ECLI': None if lines[7].strip() in ('','-') else lines[7].strip()
            })
            try:
                d = datetime.datetime.strptime(lines[3].strip(), '%d/%m/%Y').date()
            except ValueError:
                d = datetime.date(int(lines[3].strip()), 1, 1)
            meta['publicationDate'] = str(d)
        else:
            meta.update({
                'decisionNumber': name.split('_')[0],
                'issueYear': year_part,
                'ECLI': None,
                'publicationDate': None
            })

        # ─── READ RAW TEXT ────────────────────────────────────────────
        raw_lines = open(txt_file, encoding='utf-8').read().splitlines()
        raw_norm  = [normalize_gr(ln) for ln in raw_lines]
        raw_text  = "\n".join(raw_lines)

        idx_num = find_index(raw_norm, lambda ln: ln.startswith('αριθμος') or ln.startswith('αριθμ.'))
        intro_idx = find_index(raw_norm, lambda ln: ln.startswith('για να δικ'))

        can_override_header = (idx_num != -1)
        can_override_intro  = (intro_idx != -1)

        docNumber = docProponent = subDepartment = headerDetails = ''
        introduction_block = ''

        if can_override_header:
            orig_line = raw_lines[idx_num].strip()
            parts = orig_line.split(" ", 1)
            docNumber = parts[1].strip() if len(parts) > 1 else ''
            idx = idx_num + 1
            while idx < len(raw_lines) and not raw_lines[idx].strip(): idx += 1
            if idx < len(raw_lines): docProponent = raw_lines[idx].strip()
            idx += 1
            while idx < len(raw_lines) and not raw_lines[idx].strip(): idx += 1
            if idx < len(raw_lines): subDepartment = raw_lines[idx].strip()
            hd_start = idx + 1
            hdr_slice = raw_lines[hd_start:(intro_idx if can_override_intro else len(raw_lines))]
            if not can_override_intro:
                # μέχρι την πρώτη άδεια γραμμή
                j = hd_start
                while j < len(raw_lines) and raw_lines[j].strip(): j += 1
                hdr_slice = raw_lines[hd_start:j]
            headerDetails = " ".join(ln.strip() for ln in hdr_slice if ln.strip())

        if can_override_intro:
            introduction_block = "\n\n".join(raw_lines[intro_idx:]).strip()

        # ─── Build judgment object ─────────────────────────────────────
        judgmentObj = AknJudgementXML(
            textType=meta['textType'], author=meta['author'], foreas=meta['foreas'],
            issueYear=meta['issueYear'], decisionNumber=meta['decisionNumber'],
            ECLI=meta.get('ECLI'), publicationDate=meta.get('publicationDate')
        )
        metaElem = judgmentObj.createMeta()

        if os.path.isfile(gate_xml):
            refs = metaElem.find('references')
            if refs is not None:
                idx0 = list(metaElem).index(refs)
                newr = judgmentObj.modifyReferencesFromGateXml(gate_xml, refs)
                metaElem.remove(refs); metaElem.insert(idx0, newr)

        # ─── LEGAL REFERENCES (1ο πέρασμα) — with silent listeners & safe fallback ──
        answer = None
        try:
            l1 = Legal_refLexer(FileStream(txt_file, encoding='utf-8'))
            p1 = Legal_refParser(CommonTokenStream(l1))
            silent = SilentErrorListener()
            l1.removeErrorListeners(); p1.removeErrorListeners()
            l1.addErrorListener(silent); p1.addErrorListener(silent)
            tree1 = p1.legal_text()
            answer = AknLegalReferences().visit(tree1)
        except Exception:
            answer = None

        # Fallback: αν το visit δεν επέστρεψε string ή είναι κενό, δώσε στον 2ο parser το raw text
        if not isinstance(answer, str) or not answer.strip():
            answer = raw_text

        # ─── STRUCTURE PARSING (2ο πέρασμα) — silent listeners ─────────
        l2 = CouncilOfStateLexer(InputStream(answer))
        p2 = CouncilOfStateParser(CommonTokenStream(l2))
        silent2 = SilentErrorListener()
        l2.removeErrorListeners(); p2.removeErrorListeners()
        l2.addErrorListener(silent2); p2.addErrorListener(silent2)

        tree2 = p2.judgment()
        walker = ParseTreeWalker()
        walker.walk(judgmentObj, tree2)

        if os.path.isfile(gate_xml):
            judgmentObj.text = judgmentObj.createNamedEntitiesInText(gate_xml, judgmentObj.text)

        # ─── Build Akoma Ntoso root ────────────────────────────────────
        ak = judgmentObj.createAkomaNtosoRoot()
        judgmentObj.text = fixStringXML(judgmentObj.text, REGEXES['para'])
        judgmentElem = judgmentObj.XML()
        ak.insert(0, judgmentElem)
        jud_node = ak.find('judgment'); jud_node.insert(0, metaElem)

        # ─── OVERRIDE HEADER / INTRO (μόνο αν βρέθηκαν markers) ────────
        if can_override_header:
            hdr_nodes = XPATHS['hdr'](ak)
            if hdr_nodes:
                hdr_node = hdr_nodes[0]
                for c in list(hdr_node): hdr_node.remove(c)
                p1 = etree.SubElement(hdr_node, 'p'); etree.SubElement(p1, 'docNumber').text = docNumber
                p2 = etree.SubElement(hdr_node, 'p'); etree.SubElement(p2, 'docProponent').text = docProponent
                p3 = etree.SubElement(hdr_node, 'p'); p3.text = subDepartment
                if headerDetails: etree.SubElement(hdr_node, 'p').text = headerDetails
        else:
            logger.warning("Header override skipped (no 'Αριθμός' line) for {}".format(name))

        if can_override_intro:
            intro_nodes = XPATHS['intro'](ak)
            if intro_nodes:
                intro_node = intro_nodes[0]
                for c in list(intro_node): intro_node.remove(c)
                for para in [p for p in introduction_block.split('\n\n') if p]:
                    etree.SubElement(intro_node, 'p').text = para.strip()
        else:
            logger.warning("Introduction override skipped (no 'Για να δικάσει' line) for {}".format(name))

        # ─── DATES OF INTEREST ─────────────────────────────────────────
        wf_node   = XPATHS['wf'](ak)[0]
        refs_meta_nodes = XPATHS['refsInMeta'](ak)
        refs_node = refs_meta_nodes[0] if refs_meta_nodes else None
        frbrW     = XPATHS['frbrW'](ak)[0]
        frbrE     = XPATHS['frbrE'](ak)[0]

        hdr_nodes = XPATHS['hdr'](ak)
        if hdr_nodes:
            add_date(hdr_nodes[0], REGEXES['public'], 'publicHearingDate', wf_node, refs_node, frbrW, frbrE, meta['author'])

        concl_nodes = XPATHS['concl'](ak)
        if concl_nodes:
            concl_node = concl_nodes[0]
            add_date(concl_node, REGEXES['conf'], 'courtConferenceDate', wf_node, refs_node, frbrW, frbrE, meta['author'])
            add_date(concl_node, REGEXES['pub'],  'decisionPublicationDate', wf_node, refs_node, frbrW, frbrE, meta['author'])

        # ─── SERIALIZE & VALIDATE (ίδια bytes) ─────────────────────────
        tree = etree.ElementTree(ak)
        xml_bytes = etree.tostring(tree, pretty_print=True, encoding='UTF-8', xml_declaration=True)
        xml_str = safe_to_str(xml_bytes).replace('&gt;', '>')
        with open(xml_file, 'w', encoding='utf-8') as fout:
            fout.write(xml_str)

        validateXML('akomantoso30.xsd', xml_file, log_file)
        logger.info("Wrote XML → {}".format(xml_file))
        status = 'ok'

    except KeyboardInterrupt:
        raise
    except Exception:
        tb = traceback.format_exc()
        print("Error processing {}:\n{}".format(name, tb))
        logger.error("❌ Failed {}\n{}".format(name, tb))
        status = 'error'
    finally:
        elapsed = round(time.perf_counter() - start_time, 2)
        logger.info("Finished {} in {}s".format(name, elapsed))

    return (name, status, elapsed)

# ---------- Task enumeration ----------
def enumerate_tasks(source_base, file_pattern):
    for root, _, files in os.walk(source_base):
        base_texts_root = os.path.join(os.getcwd(), LEGAL_TEXTS)
        logs_path = root.replace(base_texts_root, os.path.join(os.getcwd(), LOGS))
        xml_path  = root.replace(base_texts_root, os.path.join(os.getcwd(), XML))
        os.makedirs(logs_path, exist_ok=True)
        os.makedirs(xml_path,  exist_ok=True)
        for name in files:
            if fnmatch.fnmatch(name, file_pattern):
                yield (root, name)

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(
        description="Transform Council of State texts into Akoma Ntoso XML (IDENTICAL output; robust fallbacks; faster)."
    )
    parser.add_argument('-year', help='Year to process (e.g. 2017)')
    parser.add_argument('-fn', metavar='FILENAME', help='Specific file to process (requires -year)')
    parser.add_argument('--workers', type=int, default=os.cpu_count(), help='Parallel processes (default: all CPUs)')
    args = parser.parse_args()

    if args.fn and not args.year:
        parser.error("When using -fn, you must also specify -year")

    file_pattern = '*' + (args.fn if args.fn else TXT_EXT)
    source_base  = os.path.join(os.getcwd(), LEGAL_TEXTS, STE)
    if args.year:
        source_base = os.path.join(source_base, args.year)

    tasks = list(enumerate_tasks(source_base, file_pattern))
    if not tasks:
        print("No files matched.")
        return

    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=args.workers, initializer=init_worker) as ex:
        futs = [ex.submit(process_one, t) for t in tasks]
        done = ok = 0
        for fut in as_completed(futs):
            name, status, dur = fut.result()
            done += 1; ok += 1 if status == 'ok' else 0
            if (done % 50 == 0) or (status != 'ok'):
                print("[{}/{}] {}: {} ({}s)".format(done, len(futs), name, status, dur))

    total = round(time.perf_counter() - t0, 2)
    print("All done in {}s. Files: {}. Workers: {}. OK: {}."
          .format(total, len(tasks), args.workers, ok))

if __name__ == '__main__':
    main()
