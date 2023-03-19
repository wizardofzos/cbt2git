#!/bin/env python

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

import subprocess
import glob

import math
import yaml
import shlex

import argparse 
import subprocess 

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


parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, description="""Create, or update a GitHub profile with data from CBTTape.org.

    1. Step One: Extract all CBTTapes and create GitHub Repo if needed.
       Process all files that were downloaded from cbttape.org. All the zipfiles stored in the path you specify with --stage. are processed. They are copied to the --cbtrepos folder if
they're new. If they're the same as  what's already present there, nothing happens :) Otherwise, they're new a repote repo
at GITHUB_NAME_OR_ORG from config.yaml will be created.
Zip files are expaned to their containing PDS/SEQ. This PDS will be scanned, nested XMIT files will be expanded into the repo too.
Binary files that do not resolve to a (paritioned) sequential file will be stored as a new xmit in the /xmits folder.""")

parser.add_argument("--stage", type=str,
                    default=f'{os.getcwd()}/stage',
                    help=f"""Full path to stage-foler. 
This is where all zip files from cbttape.org were downloaded to.
Defaults to {os.getcwd()}/stage""")


parser.add_argument("--only", type=int,
                    default=0,
                    help=f"""Only process this CBT Tape""")

parser.add_argument("--pickle", type=str,
                    default=f'.cbtscan.pkl',
                    help=f"""Panda pickle file with parsed UPDATESTOC.txt information. Will be updated during this run. Defaults to .cbtscan.pkl""")

parser.add_argument("--noparse",
                    action="store_true",
                    help=f"Use existing pickle from earlier run. Don't parse stage again")



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


def dexmi(xmifile, into="/tmp"):
    dsnam, _, _, _, _ = getxmidata(xmifile)
    xmi_obj = xmi.open_file(xmifile,quiet=True)
    xmi_obj.set_output_folder('/tmp')
    xmi_obj.set_quiet(True)
    xmi_obj.extract_all()
    return f"{into}/{dsnam}"
    

args = parser.parse_args()

stage    = args.stage
only     = args.only
pickle   = args.pickle

import filecmp
import shutil

toprocess= []

if not args.noparse:
    flist = os.listdir(stage)

    for i,filename in enumerate(flist):
        # I've we selected a CBT, oly do that one
        if only > 0:
            cbtn = f"CBT{only:003d}.zip" 
            if filename != cbtn: 
                continue
        # otherwise process this src CBT file..
        src = os.path.join(stage, filename)
        # checking if it is a file
        if os.path.isfile(src):
            toprocess.append(src)
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
            cbtnum = z.split('/CBT')[1].split('.')[0]
        except:
            print(f"eeek: {z}")
        pct = math.floor((i/len(toprocess))*100) 
        done = math.floor((pct/100)*40)
        todo = 40 - done
        done = done * "✅" 
        todo = todo * "🟩"
        print(f'{done}{todo} {z} ({pct}%)', end='\r', flush=True)      
        with zipfile.ZipFile(z, 'r') as zip_ref:
            info =  zip_ref.infolist()
            if len(info) > 1:
                print(F"More than onze file in zip??? {z} => {info}, passing")
                continue
            else:
                xmifile = f"/tmp/{info[0].filename}"
                zip_ref.extractall('/tmp')
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
                            xtract = dexmi(xmifile, into='/tmp')
                            n_dsnam, n_dsorg, n_lrecl, m_recfm, n_members = getxmidata(xtract+"/"+m+members[m]['ext'])
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
                            # cleanup /tmp again :)
                            os.system(f"rm -rf {xtract}")
                else:
                    cbtinfo['cbt'].append(f"FILE{int(cbtnum):003d}")
                    cbtinfo['contains'].append(dsnam)
                    cbtinfo['member'].append('n.a.')
                    cbtinfo['extension'].append('n.a')
                    cbtinfo['mimetype'].append('n.a.')
                    cbtinfo['subcontent'].append('n.a')
            # Cleanup tmp plox
            os.system(f"rm -rf {xmifile}")
    cbt = pd.DataFrame.from_dict(cbtinfo)
    cbt.to_pickle(f'{args.pickle}')
else:
    cbt = pd.read_pickle(args.pickle)


xlsx = 'cbt.xlsx'
writer = pd.ExcelWriter(f'{xlsx}', engine='xlsxwriter')
cbt.to_excel(writer, sheet_name='CBTTAPES', index=False)
writer.save()
