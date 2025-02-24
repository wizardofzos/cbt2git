from ftplib import FTP
import parse
import re
import pandas as pd
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
import pprint
from github import Github
import logging
import filecmp
import shutil

# Setup logging
logfile = f'cbt2git-log-{datetime.datetime.now().strftime("%Y-%j-%H-%M-%S")}'
logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)

class XMIObject:
    def __init__(self, xmi_json):
        self.data = xmi_json
        self.pdsfile = list(xmi_json['file'].keys())[0]
        self.type = xmi_json['file'][self.pdsfile]['COPYR1']['type']
        self.members = xmi_json['file'][self.pdsfile]['members']

        #for key, value in self.data.items():
        #    setattr(self, key, value)

def parse_arguments():
    """Parse all arguments."""
    parser = argparse.ArgumentParser(formatter_class = argparse.RawTextHelpFormatter, 
                                    description= "Create, or update a GitHub profile with data from CBTTape.org.")
    parser.add_argument("--stage", 
                        type = str,
                        default = f'{os.getcwd()}/stage',
                        help=f"Full path to stage-folder where zipe files from cbttape.org were downloaded to. Defaults to {os.getcwd()}/stage.")
    parser.add_argument("--cbtfiles", 
                        type = str,
                        default = f'.cbtfiles',
                        help=f"Full path to the cbtfiles. This is all up-to-date zip files (if you ran --update). Defaults to {os.getcwd()}/.cbtfiles.")
    parser.add_argument("--repos", 
                        type = str,
                        default = f'.cbtrepos',
                        help = f"Full path to local repos folder. Defaults to {os.getcwd()}/.cbtrepos.")
    parser.add_argument("--only", 
                        type = str,
                        help = "Only process this CBT Tape.")
    parser.add_argument("--pickle", 
                        type = str,
                        default = f'.cbt.pkl',
                        help = "Panda pickle file with parsed UPDATESTOC.txt information. Will be updated during this run. Defaults to .cbt.pkl")
    parser.add_argument("--clean",
                        action = "store_true",
                        help = "Cleans everything except stage folder. Does not take --only into account.")
    parser.add_argument("--force",
                        action = "store_true",
                        help = "Ingore filesizes, always download everything.")
    parser.add_argument("--noremote",
                        action = "store_true",
                        help = f"Do everything, except remote GitHub actions (don't create or updates repos).")

    args = parser.parse_args()
    return args

def github_login(github):
    with open('config.yml', 'r') as f:
        config = yaml.safe_load(f)
    me = github.get_user()

    try:
        GITHUB_USER = me.login
        print(f"Token has logged onto {me.name} acting in github user github.com/{GITHUB_USER}")
    except Exception as e:
        print(f"Error logging into {me}: {e}")
        exit(4)

def clean_remote_repos():
    """Removes remote repositories."""
    repos = list(github.get_user().get_repos())
    total_repos = len(repos)

    # Check if there are any repos to process
    if total_repos == 0:
        print("No repositories to delete.")
        return
    
    print(f"Total Repos: {total_repos}")

    while total_repos > 0:
        for repo in repos:
            if repo.name[:3] == "CBT":
                rate_used, rate_init = github.rate_limiting
                gracetime = (github.rate_limiting_resettime-math.floor(time.time())) / 1000
                print(f"Deleting {repo.name:8} (gracetime = {gracetime}, ratelimits = {rate_used}/{rate_init})", end=' ', flush=True)
                
                try:
                    repo.delete()
                    # Update the repo list/total count after deletion
                    repos.remove(repo)
                    total_repos -= 1
                except Exception as e:
                    print(f"Error deleting {repo.name}: {e}")
                
                # Check if close to hitting the rate limit 
                if rate_used >= rate_init * 0.9:  
                    time_to_wait = gracetime
                    print(f"\nRate limit reached, waiting for {time_to_wait:.2f}s...")
                    time.sleep(time_to_wait + 1)  
                
                print(f"Deleted {repo.name}       ", end='\r', flush=True)

            # Ignore non-CBT repos
            else:
                repos.remove(repo)
                total_repos -= 1
    
    print("\nFinished deleting CBT repositories.")

def copy_CBT_file(filename):
    """ Copies CBT file to cbtfiles repo if new or different.""" 
    src = os.path.join(stage, filename)
    # checking if it is a file
    if os.path.isfile(src):
        dst = os.path.join(cbtfiles, filename)
        # copy to destintation if new or different
        if not os.path.exists(dst) or not filecmp.cmp(src, dst):
            shutil.copyfile(src, dst)
        return dst
    else:
        logging.exception(f"{src} not found.")
        if only:
            exit(4)
    
def extract_xmi_json(zip):
    zip_ref = zipfile.ZipFile(zip, 'r')

    info = zip_ref.infolist()

    if len(info) == 0:
        logging.exception(f"No files found in ZIP {zip}")
        return

    if len(info) > 1:
        logging.exception(f"More than one file in zip {zip} => {', '.join([file.filename for file in info])}")
        return

    zip_ref.extractall('/tmp')
    # Return xmi filename

    xmi_file = f"/tmp/{info[0].filename}"
    try:
        contents = xmi.list_all(xmi_file)

        if not contents:
            # Empty or invalid XMI
            logging.exception(f"XMI file {xmi_file} is empty or invalid.")
            return
    except Exception as e:
        # Log an error if the file is not a valid XMI file
        logging.exception(f"Error processing {xmi_file} from ZIP file {zip}: {str(e)}")
        return
    
    try:
        xmi_obj = xmi.open_file(xmi_file, quiet=True)
    except Exception as e:
        logging.exception(f"Error opening {xmi_file}: {str(e)}")
        return

    xmi_obj.set_output_folder('/tmp')
    xmi_obj.set_quiet(True)

    try:
        xmi_obj.extract_all()
    except Exception as e:
        logging.exception(f"Error extracting {xmi_file}: {str(e)}")
        return
    
    # Return xmi json
    return json.loads(xmi_obj.get_json())

def remove_extensions(repopath):
    for f in glob.glob(f'{repopath}/*'):
        path, file = os.path.split(f)
        
        # Split the filename by the last dot to handle extensions properly
        newfile = file.rsplit('.', 1)[0]  # Keeps everything before the last dot

        # Construct the new file path
        noext = os.path.join(path, newfile)

        # Escape dollar signs
        f = f.replace('$', r'\$')
        noext = noext.replace('$', r'\$')

        # Move the file using shutil
        try:
            shutil.move(f, noext)
        except Exception as e:
            logging.exception(f"Error moving file {f}: {e}")


def main():
    # Parse arguments
    args = parse_arguments()

    global repos, stage, only, cbtfiles, noremote

    repos    = args.repos 
    stage    = args.stage
    only     = args.only
    cbtfiles = args.cbtfiles
    noremote = args.noremote

    # Log into GitHub using token in config.yml
    global github
    if not noremote:
        with open('config.yml', 'r') as f:
            config = yaml.safe_load(f)

        github = Github(config['token'])
        github_login(github)
    else:
        logging.info("Running locally only, no updates to GitHub.")
    
    # Clean local/remote repositories 
    if args.clean:
        os.system(f'rm -rf {cbtfiles}/*')
        os.system(f'rm -rf {repos}/*')
        if not noremote:
            clean_remote_repos()
    
    # Create repo/cbtfile directory if they don't exist
    os.makedirs(repos, exist_ok = True)
    os.makedirs(cbtfiles, exist_ok = True)

    # Read pickle
    cbt = pd.read_pickle(args.pickle)
    print(f"Loaded our dataframe, {len(cbt)} CBT-files ready to be processed.")

    # Copy CBT file to new destination if new/different
    toprocess = []
    flist = os.listdir(stage)

    for index, filename in enumerate(flist):
        if only:
            cbtn = f"{only}.zip" 
            if filename != cbtn: 
                continue

        # Add path to copied file to list of CBT zips to process
        dst = copy_CBT_file(filename)
        if dst:
            toprocess.append(dst)
    
    print(f"Need to process {len(toprocess)} CBT zips.")

    toprocess = sorted(toprocess)
    for index, zip in enumerate(toprocess):
        pct = math.floor((index/len(toprocess))*100) 
        done = math.floor((pct/100)*40)
        todo = 40 - done
        done = done * "✅" 
        todo = todo * "🟩"
        print(f'{done}{todo} {zip} ({pct}%)', end='\r', flush=True)

        cbtnum = zip.split('/CBT')[1].split('.')[0]
        logging.info(f'{datetime.datetime.now()} - Initialized conversion of CBT{cbtnum}.')

        xmi_json = extract_xmi_json(zip)
        # Skip file if unable to extract xmi json 
        if not xmi_json:
            continue

        xmi_object = XMIObject(xmi_json)

        # Skip if XMI is not a PDS
        if xmi_object.type != 'PDS':
            logging.info(f"{datetime.datetime.now()} - No PDS in CBTNUM{cbtnum}.")
            continue

        pdsfile = xmi_object.pdsfile
        logging.info(f'{datetime.datetime.now()} - Received {pdsfile} from CBT{cbtnum}.XMI')

        # Create new repo to do... something 
        repopath = repos + "/CBT" + cbtnum
        os.makedirs(repopath, exist_ok = True)
        # Create target PDS folder
        pdsfolder = repopath + "/pdsfile"
        os.makedirs(pdsfolder, exist_ok = True)

        members = xmi_object.members
        # Do something with the PDS file here... not really sure what's going on
        for member in members:
            member_info = members[member]

            # Detect mimetype 
            if 'mimetype' in member_info:
                mimetype = member_info['mimetype']
            else:
                mimetype = 'application/octet-stream' # PDS? 
            
            # Detect file type? 
            if 'extension' in member_info:
                ext = member_info['extension']
            else:
                ext = '.bin'
            
            src_file = f"/tmp/{pdsfile}/{member}{ext}"

            # text file 
            if mimetype.split('/')[0] == 'text':
                dst_file = f"{pdsfolder}/{member}"

                # Copy text files into repopath
                try:
                    shutil.copy2(src_file, dst_file)
                except Exception as e:
                    logging.exception(f"Error copying file {member}{ext}: {e}")
                
                member_info['ispf'] = member_info.get('ispf', 
                                                      {'version': '01.00',
                                                       'flags': 0,
                                                       'createdate': '1976-06-12T00:00:00.000000',
                                                       'modifydate': '1976-06-12T22:18:12.000000',
                                                       'lines': 0,
                                                       'newlines': 0,
                                                       'modlines': 0,
                                                       'user': 'CBT2GIT'})
                
                logging.info(f'{datetime.datetime.now()} - Found {member}{ext} in {pdsfile}, moved to {dst_file}')

            # nexted xmi? 
            elif mimetype == 'application/xmit':
                # I can prb add this part to a separate function... 
                nested_content = xmi.list_all(src_file)
                try:
                    nested_xmi_obj = xmi.open_file(src_file, quiet=True)
                except Exception as e:
                    logging.exception(f"Error opening {src_file}: {str(e)}")
                    return

                nested_xmi_obj.set_output_folder(f'{repopath}')
                nested_xmi_obj.set_quiet(True)

                try:
                    nested_xmi_obj.extract_all()
                except Exception as e:
                    logging.exception(f"Error extracting {src_file}: {str(e)}")
                    return
                
                # Not really sure why it's called this... 
                src_path  = f"{repopath}/{nested_content[0].split('(')[0]}"
                dst_path = f"{repopath}/{'.'.join(src_path.split('.')[-2:])}"

                # Append .txt to end if not a PDS (honestly there's prb a better way to do this)
                if not os.path.exists(src_path):
                    src_path += '.txt'
                
                # Move to correct place
                try:
                    shutil.move(src_path, dst_path)
                except Exception as e:
                    logging.exception(f"Error moving file {src_path}: {e}")
                
                # Add nested XMI to root of repo
                dst_file = repopath + "/" + member + ext
                try:
                    shutil.copy2(src_file, dst_file)  # Copy the file
                except Exception as e:
                    logging.exception(f"Error copying file {member}{ext}: {e}")
                
                remove_extensions(dst_path)

if __name__ == '__main__':
    main()

