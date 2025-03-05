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
import threading

MAX_THREAD_DOWNLOADS = 15
DIR = '/Users/alisonzhang/Desktop/cs4442/tmp/'
docmimetypes = ['application/msword', 'application/epub+zip', 'application/pdf', 
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                'application/vnd.oasis.opendocument.text','application/vnd.oasis.opendocument.text',
                'application/vnd.ms-powerpoint','application/vnd.ms-excel',
                'application/vnd.openxmlformats-officedocument.presentationml.presentation']

# Configure logging to write to a file
logging.basicConfig(
    filename='file_check.log',  
    level=logging.INFO       
)

class XMIObject:
    def __init__(self, xmi_file, repopath):
        self.xmi_file = xmi_file
        self.repopath = repopath
        os.makedirs(repopath , exist_ok = True) # Create target directory
        self.xmi_json = self.extract_xmi() # Get json for XMI file 

        if self.xmi_json:
            # Set attributes from json file if it exists
            self._set_attributes()   
    
    def extract_xmi(self):
        """Opens the XMI file and extracts its contents as JSON."""
        try:
            xmi_obj = xmi.open_file(self.xmi_file, quiet = True)
        except Exception as e:
            logging.error(f"Error opening {self.xmi_file}: {str(e)}")
            return

        xmi_obj.set_output_folder(DIR)
        xmi_obj.set_quiet(True)
        try:
            xmi_obj.extract_all()
        except Exception as e:
            logging.error(f"Error extracting {self.xmi_file}: {str(e)}")
            return
        
        # Check if contents are empty 
        try:
            if not xmi.list_all(self.xmi_file):
                # Empty or invalid XMI
                print(f"XMI file {self.xmi_file} is empty or invalid.")
                return

        except Exception as e:
            # Log an error if the file is not a valid XMI file
            logging.error(f"Error processing {self.xmi_file} from ZIP file {zip}: {str(e)}")
            return
        
        return json.loads(xmi_obj.get_json()) # Return the XMI json file 

    def _set_attributes(self):
        """Set additional attributes based on contents of the XMI JSON file."""
        for key, value in self.xmi_json.items():
            # Iterate through each item in json file
            if (key != 'file'): 
                # Set attribute if not at file information
                setattr(self, key, value)
            else:
                # Get mainframe file information
                content = list(value.keys())[0] # mainframe file name  
                content_info = value[content] # mainframe file info

                if not content_info.get('COPYR1'):
                    # For non-PDS XMI's
                    self.content = ''
                    self.member = XMIMember(content, content_info, self)

                else:
                    # For XMI's with a PDS
                    self.PDS = True
                    self.members = {} # Create dict to store PDS members
                    os.makedirs(f'{self.repopath}/PDS' , exist_ok = True) # Create repo for PDS members
                    self.content = content # Set mainframe file name as attribute

                    for k, v in content_info.items():
                        # Iterate through each item in content
                        if (k != 'members'):
                            # Set attribute if not at content members
                            setattr(self, k, v) 

                        else:
                            # Add each member as a child XMIObject
                             for name, data in v.items():
                                # Get each member
                                if (data.get('mimetype') == 'application/xmit'):
                                    # Create new XMIObject if member is an xmi file 
                                    self.members[name] = XMIObject(f'{DIR}{content}/{name}.xmi', f'{self.repopath}/{name}')
                                # For all members, create a new XMIMember 
                                self.members[name] = XMIMember(name, data, self)

class XMIMember:
    def __init__(self, name, data, parent):
        self.name = name
        self.parent = parent
        self.data = data

        for key, value in data.items():
            # Set attributes based on metadat 
            setattr(self, key, value)

        # Set defaults if missing
        setattr(self, 'mimetype', getattr(self, 'mimetype', 'application/octet-stream'))
        setattr(self, 'extension', getattr(self, 'extension', '.bin'))
        # This doesn't work for 433/874/942/967... not sure what to do about that 

        self.move_file(hasattr(parent, 'PDS'))

    def move_file(self, PDS):
        """Move member file to destination directory based on mimetype"""
        src_dir = f'{DIR}{self.parent.content}'
        dst_dir = self.parent.repopath

        src = f'{src_dir}/{self.name}{self.extension}' # Set file source 
        dst = f'{dst_dir}/{self.name}' # Set file destination
            
        if self.mimetype in docmimetypes:
            # For document types
            doc_dir = f'{dst_dir}/docs'
            os.makedirs(doc_dir , exist_ok = True) # Create directory for docs 
            copy_file(src, f'{doc_dir}/{self.name}{self.extension}')
        
        elif self.mimetype in ['application/zip', 'application/java-archive']:
            dst = f'{dst_dir}/{self.name}' # Set file destination

            # Zip files
            try:
                with zipfile.ZipFile(src, 'r') as zip:
                    try:
                        zip.extractall(dst)
                    except Exception as e:
                        print(f"ZIP {zip} is not a zip file: {e}")
                        return
                    
            except Exception as e:
                # Just copy file if unable to unzip 
                copy_file(src, dst)
        
        elif self.mimetype == 'application/octet-stream':
            # Copy with extension still present
            if PDS:
                copy_file(src, f'{dst_dir}/PDS/{self.name}{self.extension}')
            else:
                copy_file(src, f'{dst}{self.extension}')
        
        else:
            # Copy without extension still present
            if PDS:
                copy_file(src, f'{dst_dir}/PDS/{self.name}')
            else:
                copy_file(src, dst)

            if self.mimetype.split('/')[0] == 'text':
                # Set ISPFSTATS for text types
                setattr(self, 'ispf', getattr(self, 'ispf', 
                                            {'version': '01.00', 'flags': 0, 'createdate': '1976-06-12T00:00:00.000000', 
                                            'modifydate': '1976-06-12T22:18:12.000000', 'lines': 0, 'newlines': 0, 
                                            'modlines': 0, 'user': 'CBT2GIT'}))


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
                        default = False,
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

def clean_repos():
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
                    logging.error(f"Error deleting {repo.name}: {e}")
                
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

def copy_file(src, dst):
    """ Copies src to dst if new or different.""" 
    if os.path.isfile(src):
        if not os.path.exists(dst) or not filecmp.cmp(src, dst):
            shutil.copyfile(src, dst)
        return dst
    else:
        print(f"{src} not found.")
        if only: 
            # Stop if only processing one file 
            exit(4)
        return

def files_to_process(flist):
    to_process = []

    for index, filename in enumerate(flist):
        if only:
            cbtn = f"{only}.zip" 
            if filename != cbtn: 
                continue

        # Add path to copied file to list of CBT zips to process
        src = os.path.join(stage, filename)
        dst = os.path.join(cbtfiles, filename)
        if copy_file(src, dst):
            to_process.append(dst)
    
    print(f"Need to process {len(to_process)} CBT zips.")
    return sorted(to_process)

def extract_xmi(zip):
    try:
        zip_ref = zipfile.ZipFile(zip, 'r')
    except Exception as e: 
        print(f"ZIP {zip} is not a zip file: {e}")
        return

    # Check that there is was one file unzipped 
    info = zip_ref.infolist()
    if len(info) == 0:
        print(f"No files found in ZIP {zip}")
        return
    if len(info) > 1:
        print(f"More than one file in ZIP {zip} => {', '.join([file.filename for file in info])}")
        return
    
    try: 
        # Extract zip to source directory
        zip_ref.extractall(DIR)
    except Exception as e:
        print(f"Unable to extract ZIP {zip}: {e}")
        return
    
    xmi_file = f'{DIR}{info[0].filename}' # Get extracted XMI

    # Set reponame for destination directory for each file
    reponame = zip.split('/')[1].split('.')[0]
    repopath = f'{repos}/{reponame}'

    myXMI = XMIObject(xmi_file, repopath)


def main():
    global repos, stage, only, cbtfiles, noremote
    # Parse arguments
    args = parse_arguments()

    repos    = args.repos 
    stage    = args.stage
    only     = args.only
    cbtfiles = args.cbtfiles
    noremote = args.noremote

    if not noremote:
        global github
        with open('config.yml', 'r') as f:
            config = yaml.safe_load(f)

        github = Github(config['token'])
        github_login(github)
    else:
        print("Running locally only, no updates to GitHub.")
    
    # Clean local/remote repositories 
    if args.clean:
        os.system(f'rm -rf {cbtfiles}/*')
        os.system(f'rm -rf {repos}/*')
        if not noremote:
            clean_repos()
    
    # Create repo/cbtfile directory if they don't exist
    os.makedirs(repos, exist_ok = True)
    os.makedirs(cbtfiles, exist_ok = True)

    # Read pickle
    cbt = pd.read_pickle(args.pickle)
    print(f"Loaded our dataframe, {len(cbt)} CBT-files ready to be processed.")

    # Get list of files to process
    to_process = files_to_process(os.listdir(stage))

    threads = []
    for index, zip in enumerate(to_process):
        pct = math.floor((index / len(to_process)) * 100)
        done = math.floor((pct / 100) * 40)
        todo = 40 - done
        done_bar = "✅" * done
        todo_bar = "🟩" * todo

        cbtnum = zip.split('/CBT')[1].split('.')[0]
        print(f'{done_bar}{todo_bar} {zip} ({pct}%) [converting, active threads={threading.active_count()}]', end='\r', flush=True)
        while threading.active_count() >= MAX_THREAD_DOWNLOADS + 1:  # +1 for the main thread
            time.sleep(0.5)
        
        # Extract XMI and copy contents to target directories
        thread = threading.Thread(target=extract_xmi, args=(zip, ))
        thread.start()
        threads.append(thread)

if __name__ == '__main__':
    main()
