import argparse
import os
import shutil
import time
from ftplib import FTP, error_perm
from pathlib import Path
import parse
import pandas as pd
import math
import threading 
import datetime
import zipfile
import sys

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

# Global variables
FTP_SERVER = 'ftp.cbttape.org'

def parse_arguments():
    """Parse all arguments."""
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description = "Collect and keep a local copy of all the files from cbttape.org.")
    parser.add_argument("--stage",
                        type = str,
                        default = str(Path.cwd() / "stage"),
                        help = f"Full path to stage folder where files from cbttape.org are staged. Defaults to {Path.cwd() / 'stage'}.")
    parser.add_argument("--threads",
                        type = int,
                        default = 15,
                        help = "Simultaneous FTP threads for downloads. Defaults to 15.")
    parser.add_argument("--pickle",
                        default='.cbt.pkl',
                        help = "Panda pickle file to save CBT's UPDATESTOC.txt information to. Defaults to ./cbt.pkl.")
    parser.add_argument("--force",
                        action = "store_true",
                        help = "Ignore filesizes, always download everything.")
    parser.add_argument("--updates",
                        action="store_true",
                        help="Only check and download the updates from cbttape.org.")
    parser.add_argument("--clean",
                    action = "store_true",
                    help = "Cleans stage folder.")
    return parser.parse_args()

def download_file(remotefile, localfile):
    """Download remote file to local file.
    
    Keyword arguments:
    remotefile -- remote filename
    localfile -- local filename
    """
    try:
        ftp = FTP(FTP_SERVER)
        ftp.login()
        with open(localfile, 'wb') as fp:
            ftp.retrbinary(f'RETR {remotefile}', fp.write)
        return ftp
    except Exception as e:
        print(f"Failed to retrieve {remotefile} from {FTP_SERVER}: {e}")
        return


def main():
    args = parse_arguments()
    stage = args.stage
    stage_path = Path(stage)

    if args.clean and stage_path.exists():
        for item in stage_path.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        print("Removed stage directory contents.")

    # Create stage directory if it doesn't exist
    os.makedirs(stage, exist_ok=True)

    start = time.time()

    # Retrieve the UPDATESTOC.txt file from cbttage.org
    print(f'Connecting to FTP server: {FTP_SERVER}')
    ftp = download_file('pub/updates/UPDATESTOC.txt', 'updates')
    print(f'Anonymous login succeeded, retrieved UPDATESTOC.txt')

    # Retrieve CBTF1  
    download_file('pub/cbt/CBTF1.zip', 'CBTF1.zip')
    print(f'Anonymous login succeeded, retrieved CBTF1.zip')
    # Unzip  
    try:
        print("Unzipping CBTF1.zip to CBTF1.txt")
        zip_ref = zipfile.ZipFile("CBTF1.zip", 'r')
        os.remove("CBTF1.zip")
    except Exception as e: 
        print(f"ZIP CBTF1.zip is not a zip file: {e}")
    try: 
        # Extract zip to current directory
        zip_ref.extractall()
    except Exception as e:
        print(f"Unable to extract CBTF1: {e}")

    # Parse the UPDATESTOC.txt file for updates 
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

    # Save as pickle
    cbt = pd.DataFrame.from_dict(cbtinfo)
    cbt.to_pickle(f'{args.pickle}')
    print(f'Dataframe saved as {args.pickle}, {len(cbt)} CBT-files indexed')

    to_process = cbt.query(f'updated == True') if args.updates else cbt # Only process updated files if specified 
    extra = "Only processing files with the update flag." if args.updates else "" 
    extra2 = "(Forcing download, not comparing remote/local filesizes)." if args.force else ""

    print(f'\nProcessing {len(to_process)} files. {extra} {extra2}\n')

    threads = []
    i = 0
    for index, data in to_process.iterrows():
        i += 1
        pct = math.floor((i/len(to_process))*100)
        done = math.floor((pct/100)*40)
        todo = 40 - done
        done = done * "✅"
        todo = todo * "🟩"
        
        fname = data['path']
        stagefile = os.path.join(stage, os.path.basename(fname))

        # Attempt to retrieve remote file size
        try:
            filesize_remote = ftp.size(fname)
            if args.force:
                filesize_remote = -10
        except error_perm:
            print(f"{fname} in TOC not present on server, skipping...")
            continue
        except Exception as e:
            print(f"Error retrieving size for {fname}: {e}")
            continue

        # Attempt to retrieve local file size
        try:
            filesize_local = os.stat(stagefile).st_size
        except FileNotFoundError: # local file is missing
            filesize_local = 0
        except Exception as e:
            print(f"Error retrieving size for local file {stagefile}: {e}")
            continue

        # Download remote file
        if filesize_local != filesize_remote:
            print(f'{done}{todo} {fname} ({pct}%) [downloading, active threads={threading.active_count()}]', end='\r', flush=True)
            while threading.active_count() >= args.threads:
                time.sleep(0.5)
            t = threading.Thread(target=download_file, args=(fname, stagefile))
            threads.append(t)
            t.start()
        else:
            print(f'{done}{todo} {fname} ({pct}%) [up-to-date , active threads={threading.active_count()}]', end='\r', flush=True)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    stop = time.time()
    print(f'All requested CBT files updated from cbttape.org into {args.stage}')
    print(f'This operation took {datetime.timedelta(seconds=stop-start)}')


if __name__ == "__main__":
    main()
