#!/bin/env python
"""Downloads (and refreshes) all CBT-files from cbttape.org to your local machine.
Initial download of all takes about 3.5 minutes.
Refresh without any new or updated files takes abount 90 seconds.
Don't go beyond 15 simultaneous threads when downloading as ftp.cbttape.org doesn't really like that.

Usage:
    ./usage: get-cbtzips-locally.py [-h] [--stage STAGE] [--threads THREADS] [--pickle PICKLE] [--force] [--updates]
    options:
    -h, --help         show this help message and exit
    --stage STAGE      Full path to stage-foler. 
                        This is where all files from cbttape.org are staged.
                        Defaults to {cwd}/stage
    --threads THREADS  Simultaneous FTP threads for downloads. Defaults to 15
    --pickle PICKLE    Panda pickle file to save CBT's UPDATESTOC.txt information to. Defaults to {cwd}/cbt.pkl
    --force            Ingore filesizes, always download everything.
    --updates          Only check and download the updates from cbttape.org.    

Data from UPDATESTOC.txt is parsed and stored in a Pandas DataFrame

     cbtnum                     path                                            comment  updated      info
0       001   pub/updates/CBT001.zip           CBT DOC - Final File 001 for Version 504     True  DOC FILE
1       002       pub/cbt/CBT002.zip  CBT973 Compression-Decompression Program for F...    False  DOC FILE
2       003       pub/cbt/CBT003.zip          JCL member to load each tape file to disk    False  DOC FILE
3       004       pub/cbt/CBT004.zip      Put ./ ADD cards into this file to make a PDS    False  DOC FILE
4       005       pub/cbt/CBT005.zip          VMREXX exec to load CBT tape to VM - V2.0    False  DOC FILE
...     ...                      ...                                                ...      ...       ...
1035   1036  pub/updates/CBT1036.zip     OS/360 Sort/Merge fixed by Tom Armstrong w/doc     True  DOC FILE
1036   1037  pub/updates/CBT1037.zip  Disassembler from Gerhard Postpischil and T.Ar...     True  DOC FILE
1037   1038  pub/updates/CBT1038.zip     System Rexx to Modify the Linklist dynamically     True  DOC FILE
1038   1039  pub/updates/CBT1039.zip    Version of the LOOK storage browser for MVS 3.8     True  DOC FILE
1039   1040  pub/updates/CBT1040.zip  Frank Clarke execs-enhance PL/I listings and s...     True  DOC FILE


Author:
    Wizard of z/OS 

Version:
    1.0 : Inital Version
    1.1 : Added some argparsing
    1.2 : More docs (I'll thank myself later)

Todo:
    - Use MLSD to parse full list and get remote dates. Stick in pandas dataframe as column
"""
from ftplib import FTP
import parse
import re
import pandas as pd
import os
import zipfile, io 
import xmi
import time
import datetime

import subprocess
import glob

import math
import threading 

import argparse 


parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, description="""Collect and keep a local copy of all the files from cbttape.org.
All the zipfiles are stored in the path you specify with --stage. 
Files are downloaded to the stage folder, if the remote (ftp.cbttape.org) file size differs to what's in the stage folder.""")
parser.add_argument("--stage", type=str,
                    default=f'{os.getcwd()}/stage',
                    help=f"""Full path to stage-foler. 
This is where all files from cbttape.org are staged.
Defaults to {os.getcwd()}/stage""")

parser.add_argument("--threads", type=int,
                    default=15,
                    help=f"""Simultaneous FTP threads for downloads. Defaults to 15""")

parser.add_argument("--pickle", type=str,
                    default=f'.cbt.pkl',
                    help=f"""Panda pickle file to save CBT's UPDATESTOC.txt information to. Defaults to ./cbt.pkl""")



parser.add_argument("--force",
                    action="store_true",
                    help=f"Ingore filesizes, always download everything.")

parser.add_argument("--updates",
                    action="store_true",
                    help=f"Only check and download the updates from cbttape.org.")


args = parser.parse_args()

MAX_THREAD_DOWNLOADS = args.threads
stage = args.stage

def threaded_download(remotefile,storeat,ftpserver):
    """Do a threaded download.

    Args:
        remotefile (string): remote filename
        storeat (string): local filename
        ftpserver (string): ftp server to download from
    """
    ftpt = FTP(ftpserver)
    ftpt.login()
    with open(storeat, 'wb+') as fp:
        ftpt.retrbinary(f"RETR {remotefile}", fp.write)

def processthem(df, force=False):
    """Process all files from dataframe. Check if filesizes differ
    and fire a threaded download if they do. Overruled by force

    Args:
        df (DataFrame): CBTTAPE DataFrame
        force (bool, optional): Always download. Defaults to False.

    Returns:
        array: Any encountered errors
    """
    threads = []
    errors = []
    i = 0
    for index,data in df.iterrows():
        fname = data['path']
        i += 1
        pct = math.floor((i/len(df))*100) 
        done = math.floor((pct/100)*40)
        todo = 40 - done
        done = done * "✅" 
        todo = todo * "🟩"
        try:
            # looks weird to check size even if we can force, but this way we no need error handling in thread :)
            filesize_remote = ftp.size(data['path'])
        except:
            errors.append(f"{data['path']} not present on server (but is in TOC!), skipping..")
            continue
        if force:
            filesize_remote = -10
        stagefile = f"{stage}/{data['path'].split('/')[-1]}"
        try:
            filesize_local  = os.stat(stagefile).st_size
        except:
            filesize_local = 0

        if filesize_local != filesize_remote:
                print(f'{done}{todo} {fname} ({pct}%) [downloading, active threads={threading.active_count()}]', end='\r', flush=True)
                while threading.active_count() >= MAX_THREAD_DOWNLOADS:
                    time.sleep(0.5) # give it some rest :) 
                t = threading.Thread(target=threaded_download,args=(data['path'],stagefile,ftpserver))
                threads.append(t)
                t.start()

        else:
            print(f'{done}{todo} {fname} ({pct}%) [up-to-date , active threads={threading.active_count()}]', end='\r', flush=True)

    while threading.active_count() > 1:
        print(f'{done}{todo} {fname} ({pct}%) [waiting to finish {threading.active_count()} active threads]  ', end='\r', flush=True)
        time.sleep(0.5)
    return errors
        


start = time.time()

ftpserver = 'ftp.cbttape.org'
print(f'Connecting to FTP server: {ftpserver}')
ftp = FTP(ftpserver)
ftp.login() # per default anonymous :)
print(f'Anonymous login to {ftpserver} succeeded, retreiving UPDATESTOC.txt')
with open('updates', 'wb') as fp:
    ftp.retrbinary('RETR pub/updates/UPDATESTOC.txt', fp.write)

# we now have the updates file

cbtinfo = {}
for k in ['cbtnum', 'path', 'comment','updated','info']:
    cbtinfo[k] = []


with open('updates') as updt:
    updates = updt.readlines()

print(f'Reading and parsing downloaded UPDATESTOC.txt')
for update in updates:
    # check for weird last line...
    if len(update) < 5:
        continue
    # where's parse var when u need it :)
    file, comment, updated, info = parse.parse('//*+{}:  {}*{}  {}\n',update)
    try:
        cbtnum = file.split('FILE')[1].strip()
    except:
        cbtnum = file.split('File')[1].strip()
    if updated =='#':
        updated = True
        dlpath = f'pub/updates/CBT{cbtnum}.zip'
    else:
        updated = False
        dlpath = f'pub/cbt/CBT{cbtnum}.zip'
    cbtinfo['cbtnum'].append(cbtnum)
    cbtinfo['path'].append(dlpath)
    cbtinfo['comment'].append(comment.strip())
    cbtinfo['updated'].append(updated)
    cbtinfo['info'].append(info)
    

cbt = pd.DataFrame.from_dict(cbtinfo)


cbt.to_pickle(f'{args.pickle}')
print(f'Dataframe saved as {args.pickle}, {len(cbt)} CBT-files indexed')

if args.force:
    extra = "(forcing download, not comparing remote/local filesizes)"
else:
    extra = ''

updates = cbt.loc[cbt.updated==True]
print(f'Processing {len(updates)} files with the update flag {extra}')
errors = processthem(updates, force=args.force)
print('')
if len(errors) > 0:
    print("Some errors occured.")
    print('\n'.join(errors))

if not args.updates:
    rest   = cbt.loc[cbt.updated==False]
    print(f'Processing {len(rest)} files without the update flag {extra}')
    processthem(rest,force=args.force)
    print('')


stop = time.time()

print(f'All requested CBT files updated from cbttape.org into {args.stage}')
print(f'This operation took {datetime.timedelta(seconds=stop-start)}')
