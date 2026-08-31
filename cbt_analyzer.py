#!/usr/bin/env python

from ftplib import FTP
import parse
import re
import pandas as pd
import xlsxwriter
import os
import zipfile, io 
import xmi, json
import time
import datetime
import tempfile
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
import subprocess
import glob
import math
import yaml
import shlex
import argparse 
import shutil

# Errors found .... or not yet correctly parsing?
# FILE003	NO XMI??	ERROR	ERROR	ERROR
# FILE001	NO XMI??	ERROR	ERROR	ERROR
# FILE476	PROGLIB.ZILCBT.LISP	ERROR	.x-lisp	text/x-lisp
# FILE476	PROGLIB.ZILCBT.OBJ	ERROR	.bin	application/octet-stream
# FILE476	PROGLIB.ZILCBT.TEXT	ERROR	.txt	text/plain
# FILE157	NO XMI??	ERROR	ERROR	ERROR
# FILE062	ERROR	ERROR	ERROR	ERROR
# FILE932	NO XMI??	ERROR	ERROR	ERROR
# FILE973	SBGOLOB.WATFIV.OBJLIB	ERROR	.bin	application/octet-stream
# FILE933	NO XMI??	ERROR	ERROR	ERROR
# FILE004	NO XMI??	ERROR	ERROR	ERROR
# FILE890	SYS2.SIMULA.LINKLIB	ERROR	.bin	application/octet-stream
# FILE064	ERROR	ERROR	ERROR	ERROR
# FILE099	NO XMI??	ERROR	ERROR	ERROR
# FILE808	NO XMI??	ERROR	ERROR	ERROR
# FILE005	NO XMI??	ERROR	ERROR	ERROR
# FILE018	NO XMI??	ERROR	ERROR	ERROR
# FILE007	NO XMI??	ERROR	ERROR	ERROR
# FILE002	NO XMI??	ERROR	ERROR	ERROR
# FILE230	NO XMI??	ERROR	ERROR	ERROR
# FILE110	NO XMI??	ERROR	ERROR	ERROR
# FILE063	ERROR	ERROR	ERROR	ERROR


def getxmidata(xmifile):
    xmijson = json.loads(xmi.open_file(xmifile,quiet=True).get_json())
    try:
        dsnam = xmijson['INMR02']['1']['INMDSNAM']
    except:
        try:
            dsnam = xmijson['INMR02']['2']['INMDSNAM']
        except:
            return False, False, False, False, False
    dsorg = xmijson['INMR02']['1']['INMDSORG']
    lrecl = xmijson['INMR02']['1']['INMLRECL']
    if dsorg != "PS":
        mbrs = [x for x in xmijson['file'][dsnam]['members']]
    else:
        mbrs = []
    try:
        recfm  = xmijson['file'][dsnam]['COPYR1']['DS1RECFM']
    except:
        recfm = 'n.a.'
    members = {}
    for m in mbrs:
        if 'mimetype' in xmijson['file'][dsnam]['members'][m]:
            mt = xmijson['file'][dsnam]['members'][m]['mimetype']
        else:
            mt = 'application/octet-stream' # force it :)
        if 'datatype' in xmijson['file'][dsnam]['members'][m]:
            dt = xmijson['file'][dsnam]['members'][m]['datatype']
        else:
            dt = 'binary'
        if 'extension' in xmijson['file'][dsnam]['members'][m]:
            ext = xmijson['file'][dsnam]['members'][m]['extension']
        else:
            ext = '.bin'
        members[m] = {'mimetype': mt, 'datatype':dt,'ext':ext}
    return dsnam, dsorg, lrecl, recfm, members


def dexmi(xmifile, into=None):
    if into is None:
        into = tempfile.gettempdir()
    dsnam, _, _, _, _ = getxmidata(xmifile)
    xmi_obj = xmi.open_file(xmifile, quiet=True)
    xmi_obj.set_output_folder(into)
    xmi_obj.set_quiet(True)
    xmi_obj.extract_all()
    return os.path.join(into, dsnam)


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, description="""Create some analytics on CBT tape files. Very much needed to get the code working for all types.
    Very much wanted for insights and nice xlsx-es. Can read the pickle file from get-cbttapes-locally.py so don't need to parse site data again. Also updataes the pickle file :)""")

    parser.add_argument("--stage", type=str,
                        default=str(Path.cwd() / "stage"),
                        help=f"""Full path to stage-foler. 
    This is where all zip files from cbttape.org were downloaded to.
    Defaults to {Path.cwd() / 'stage'}""")

    parser.add_argument("--only", type=int,
                        default=0,
                        help="""Only process this CBT Tape""")

    parser.add_argument("--pickle", type=str,
                        default='.cbtscan.pkl',
                        help="""Panda pickle file with parsed UPDATESTOC.txt information. Will be updated during this run. Defaults to .cbtscan.pkl""")

    parser.add_argument("--noparse",
                        action="store_true",
                        help="Use existing pickle from earlier run. Don't parse stage again")

    args = parser.parse_args()

    stage    = Path(args.stage)
    only     = args.only
    pickle_path = Path(args.pickle)

    toprocess= []

    if not args.noparse:
        flist = os.listdir(stage) if stage.exists() else []

        for i,filename in enumerate(flist):
            # If we selected a CBT, only do that one
            if only > 0:
                cbtn = f"CBT{only:003d}.zip" 
                if filename != cbtn: 
                    continue
            # otherwise process this src CBT file..
            src = stage / filename
            # checking if it is a file
            if src.is_file():
                toprocess.append(str(src))
            else:
                print(f"Sorry, {src} not found. This really shouldn't happen.")

        print(f"Need to process {len(toprocess)} CBT zips")

        # what do we want to know about CBT-files?
        cbtinfo = {}
        cbtinfo['cbt']        = []         # CBTnnnnn
        cbtinfo['contains']   = []         # What's in the .xmi --> SOME.DATASET.PS.OR.PO
        cbtinfo['member']     = []         # If content is PO, line per member, if content is PS -> empty 
        cbtinfo['extension']  = []         # extenstion as detected by cpython (from xmilib)
        cbtinfo['mimetype']   = []
        cbtinfo['subcontent'] = []         # If this is another XMI.. this field is anohter 'contains' in the same cbt... (still makes sense?)

        for i,z in enumerate(toprocess):
            try:
                base_name = Path(z).name
                cbtnum = base_name.replace("CBT", "").replace(".zip", "")
            except Exception:
                cbtnum = "0"
                print(f"eeek: {z}")
            pct = math.floor((i/len(toprocess))*100) if toprocess else 100
            done = math.floor((pct/100)*40)
            todo = 40 - done
            done_bar = "✅" * done
            todo_bar = "🟩" * todo
            print(f'{done_bar}{todo_bar} {z} ({pct}%)', end='\r', flush=True)      
            with zipfile.ZipFile(z, 'r') as zip_ref:
                info =  zip_ref.infolist()
                if len(info) > 1:
                    print(f"More than one file in zip??? {z} => {info}, passing")
                    continue
                else:
                    temp_dir = tempfile.gettempdir()
                    xmifile = os.path.join(temp_dir, info[0].filename)
                    zip_ref.extractall(temp_dir)
                    try:
                        contents = xmi.list_all(xmifile)
                    except:
                        cbtinfo['cbt'].append(f"FILE{int(cbtnum):003d}")
                        cbtinfo['contains'].append('NO XMI??')
                        cbtinfo['member'].append('ERROR')
                        cbtinfo['extension'].append('ERROR')
                        cbtinfo['mimetype'].append('ERROR')
                        cbtinfo['subcontent'].append('ERROR')
                        continue
                    dsnam, dsorg, lrecl, recfm, members = getxmidata(xmifile)
                    if not dsnam:
                        cbtinfo['cbt'].append(f"FILE{int(cbtnum):003d}")
                        cbtinfo['contains'].append('ERROR')
                        cbtinfo['member'].append('ERROR')
                        cbtinfo['extension'].append('ERROR')
                        cbtinfo['mimetype'].append('ERROR')
                        cbtinfo['subcontent'].append('ERROR')
                        continue
                    if dsorg != 'PS':
                        for m in members:
                            cbtinfo['cbt'].append(f"FILE{int(cbtnum):003d}")
                            cbtinfo['contains'].append(dsnam)
                            cbtinfo['member'].append(m)
                            cbtinfo['extension'].append(members[m]['ext'])
                            cbtinfo['mimetype'].append(members[m]['mimetype'])
                            cbtinfo['subcontent'].append('XMIT' if members[m]['mimetype'] == 'application/xmit' else 'noXMIT')
                            if members[m]['mimetype'] == 'application/xmit':
                                xtract = dexmi(xmifile, into=temp_dir)
                                xtract_member = os.path.join(xtract, f"{m}{members[m]['ext']}")
                                n_dsnam, n_dsorg, n_lrecl, m_recfm, n_members = getxmidata(xtract_member)
                                if n_dsorg != 'PS':
                                    for n_m in n_members:
                                        cbtinfo['cbt'].append(f"FILE{int(cbtnum):003d}")
                                        cbtinfo['contains'].append(n_dsnam)
                                        cbtinfo['member'].append(n_m)
                                        cbtinfo['extension'].append(n_members[n_m]['ext'])
                                        cbtinfo['mimetype'].append(n_members[n_m]['mimetype'])
                                        cbtinfo['subcontent'].append('n.a.')
                                else:
                                    cbtinfo['cbt'].append(f"FILE{int(cbtnum):003d}")
                                    cbtinfo['contains'].append(n_dsnam)
                                    cbtinfo['member'].append('n.a.')
                                    cbtinfo['extension'].append('n.a')
                                    cbtinfo['mimetype'].append('n.a.')
                                    cbtinfo['subcontent'].append('n.a')
                                # cleanup temp again
                                if os.path.exists(xtract):
                                    shutil.rmtree(xtract, ignore_errors=True)
                    else:
                        cbtinfo['cbt'].append(f"FILE{int(cbtnum):003d}")
                        cbtinfo['contains'].append(dsnam)
                        cbtinfo['member'].append('n.a.')
                        cbtinfo['extension'].append('n.a')
                        cbtinfo['mimetype'].append('n.a.')
                        cbtinfo['subcontent'].append('n.a')
                # Cleanup temp file
                if os.path.exists(xmifile):
                    try:
                        os.remove(xmifile)
                    except OSError:
                        pass
        cbt = pd.DataFrame.from_dict(cbtinfo)
        cbt.to_pickle(str(pickle_path))
    else:
        cbt = pd.read_pickle(str(pickle_path))

    xlsx_path = Path("cbt.xlsx")
    writer = pd.ExcelWriter(str(xlsx_path), engine='xlsxwriter')
    cbt.to_excel(writer, sheet_name='CBTTAPES', index=False)
    writer.close()


if __name__ == "__main__":
    main()
