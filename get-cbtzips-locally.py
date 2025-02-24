import argparse
import os
import time
from ftplib import FTP, error_perm
import parse
import pandas as pd
import math
import logging
import threading 
import time
import datetime

# Global variables
PROGRESS_LENGTH = 40
FTP_SERVER = 'ftp.cbttape.org'
MAX_THREAD_DOWNLOADS = 15

# Setup logging
logging.basicConfig(level=logging.INFO)

def parse_arguments():
    """Parse all arguments."""
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description = "collect and keep a local copy of all the files from cbttape.org.")
    parser.add_argument("--stage",
                        type = str,
                        default = f'{os.getcwd()}/stage',
                        help = f"Full path to stage folder where files from cbttape.org are staged. Defaults to {os.getcwd()}/stage.")
    parser.add_argument("--threads",
                        type = int,
                        default = 15,
                        help = "Simultaneous FTP threads for downloads. Defaults to 15.")
    parser.add_argument("--pickle",
                        default=f'.cbt.pkl',
                        help = "Panda pickle file to save CBT's UPDATESTOC.txt information to. Defaults to ./cbt.pkl.")
    parser.add_argument("--force",
                        action = "store_true",
                        help = "Ingore filesizes, always download everything.")
    parser.add_argument("--updates",
                        action="store_true",
                        help="Only check and download the updates from cbttape.org.")
    
    return parser.parse_args()

def threaded_download(remotefile, localfile):
    """Do a threaded download.
    
    Keyword arguments:
    remotefile -- remote filename
    localfile -- local filename
    """
    try:
        ftp = FTP(FTP_SERVER)
        ftp.login()
        print(f'Anonymous login succeeded, retrieving {remotefile}')
        with open(localfile, 'wb') as fp:
            ftp.retrbinary(f'RETR {remotefile}', fp.write)
        return ftp
    except Exception as e:
        logging.error(f"Failed to retrieve {remotefile} from {FTP_SERVER}: {e}")
        return

def parse_updates():
    """Parse the UPDATESTOC.txt file for new updates"""
    cbtinfo = {'cbtnum': [], 'path': [], 'comment': [], 'updated': [], 'info': []}

    with open('updates') as updt:
        updates = updt.readlines()
    
    for update in updates[:-1]:  # Skip the last line
        file, comment, updated, info = parse.parse('//*+{}:  {}*{}  {}\n', update)
        cbtnum = file.split('FILE')[1].strip() if 'FILE' in file else file.split('File')[1].strip()
        updated = updated == '#'
        dlpath = f'pub/updates/CBT{cbtnum}.zip' if updated else f'pub/cbt/CBT{cbtnum}.zip'
        
        cbtinfo['cbtnum'].append(cbtnum)
        cbtinfo['path'].append(dlpath)
        cbtinfo['comment'].append(comment.strip())
        cbtinfo['updated'].append(updated)
        cbtinfo['info'].append(info)
    
    return cbtinfo

def process_files(df, ftp, stage, force=False):
    """Process all files from dataframe.

    Keyword arguments: 
    df -- CBTTAPE DataFrame
    ftp -- ftp server to download from
    stage -- Path to stage folder
    force -- Always download (default False)
    """
    threads = []
    i = 0
    
    for index, data in df.iterrows():
        i += 1
        pct = math.floor((i/len(df))*100)
        done = math.floor((pct/100)*40)
        todo = 40 - done
        done = done * "✅"
        todo = todo * "🟩"
        
        fname = data['path']
        stagefile = f"{stage}/{fname.split('/')[-1]}"

        # Attempt to retrieve remote file size
        try:
            filesize_remote = ftp.size(fname)
            if force:
                filesize_remote = -10
        except error_perm:
            logging.warning(f"{fname} in TOC not present on server, skipping..")
        except Exception as e:
            logging.error(f"Error retrieving size for {fname}: {e}")
            continue

        # Attempt to retrieve local file size
        try:
            filesize_local = os.stat(stagefile).st_size
        except FileNotFoundError: # local file is missing
            filesize_local = 0
        except Exception as e:
            logging.error(f"Error retrieving size for local file {stagefile}: {e}")
            continue

        # Download remote file
        if filesize_local != filesize_remote:
            print(f'{done}{todo} {fname} ({pct}%) [downloading, active threads={threading.active_count()}]', end='\r', flush=True)
            while threading.active_count() >= MAX_THREAD_DOWNLOADS:
                time.sleep(0.5)
            t = threading.Thread(target=threaded_download, args=(fname, stagefile))
            threads.append(t)
            t.start()
        else:
            print(f'{done}{todo} {fname} ({pct}%) [up-to-date , active threads={threading.active_count()}]', end='\r', flush=True)

    while threading.active_count() > 1:
        print(f'{done}{todo} {fname} ({pct}%) [waiting to finish {threading.active_count()} active threads]  ', end='\r', flush=True)
        time.sleep(0.5)

def main():
    args = parse_arguments()

    MAX_THREAD_DOWNLOADS = args.threads
    stage = args.stage

    # Create stage directory if it doesn't exist
    os.makedirs(stage, exist_ok=True)

    start = time.time()

    # Retrieve the UPDATESTOC.txt file from cbttage.org
    print(f'Connecting to FTP server: {FTP_SERVER}')
    ftp = threaded_download('pub/updates/UPDATESTOC.txt', 'updates')

    # Parse the UPDATESTOC.txt file for new updates
    cbtinfo = parse_updates()

    # Save as pickle
    cbt = pd.DataFrame.from_dict(cbtinfo)
    cbt.to_pickle(f'{args.pickle}')
    print(f'Dataframe saved as {args.pickle}, {len(cbt)} CBT-files indexed')

    # Process updated files
    extra = "(forcing download, not comparing remote/local filesizes)" if args.force else ""
    updates = cbt.query('updated == True')

    print(f'\nProcessing {len(updates)} files with the update flag {extra}\n')
    try:
        process_files(updates, ftp, stage, force=args.force)
    except Exception as e:
        logging.error(f"An error occurred while processing the updates: {e}")

    if not args.updates:
        rest = cbt.loc[cbt.updated==False]
        print(f'\nProcessing {len(rest)} files without the update flag {extra}\n')
        process_files(rest, ftp, stage, force=args.force)

    stop = time.time()
    print(f'All requested CBT files updated from cbttape.org into {args.stage}')
    print(f'This operation took {datetime.timedelta(seconds=stop-start)}')

if __name__ == '__main__':
    main()
