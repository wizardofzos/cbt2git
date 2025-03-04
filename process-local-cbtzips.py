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

# for testing purposes lol
DIR = '/Users/alisonzhang/Desktop/cs4442/tmp/'
docmimetypes = ['application/msword', 'application/epub+zip', 'application/pdf', 
                            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                            'application/vnd.oasis.opendocument.text','application/vnd.oasis.opendocument.text',
                            'application/vnd.ms-powerpoint','application/vnd.ms-excel',
                            'application/vnd.openxmlformats-officedocument.presentationml.presentation']

class XMIObject:
    def __init__(self, xmi_file, repopath):
        os.makedirs(repopath , exist_ok = True) # Create target directory
        self.xmi_file = xmi_file
        self.repopath = repopath
        self.xmi_json = self.extract_xmi()

        if self.xmi_json:
            # Set attributes from json file
            self._set_attributes()        

    def extract_xmi(self):
        """Opens the XMI file and extracts its contents as JSON."""
        try:
            xmi_obj = xmi.open_file(self.xmi_file, quiet=True)
        except Exception as e:
            print(f"Error opening {self.xmi_file}: {str(e)}")
            return
        
        xmi_obj = xmi.open_file(self.xmi_file, quiet = True)
        xmi_obj.set_output_folder(DIR)
        xmi_obj.set_quiet(True)
        try:
            xmi_obj.extract_all()
        except Exception as e:
            print(f"Error extracting {self.xmi_file}: {str(e)}")
            return
        
        # Check if contents are empty 
        try:
            if not xmi.list_all(self.xmi_file):
                # Empty or invalid XMI
                print(f"XMI file {self.xmi_file} is empty or invalid.")
                return

        except Exception as e:
            # Log an error if the file is not a valid XMI file
            print(f"Error processing {self.xmi_file} from ZIP file {zip}: {str(e)}")
            return
        
        return json.loads(xmi_obj.get_json())
    
    def _set_attributes(self):
        for key, value in self.xmi_json.items():
            if (key == 'file'): 
                file = list(value.keys())[0]
                file_info = value[file]
                
                if file_info.get('COPYR1'):
                    # Check if XMI is a PDS
                    self.members = {} # Create dict to store PDS members
                    os.makedirs(f'{self.repopath}/PDS' , exist_ok = True)
                    self.file = f'{DIR}{file}'

                    for k, v in file_info.items():
                        if (k == 'members'):
                            # Add each member as a child XMIObject
                            for name, data in v.items():
                                if (data.get('mimetype') == 'application/xmit'):
                                    # Create new XMIObject if member is an xmi file 
                                    self.members[name] = XMIObject(f'{DIR}{file}/{name}.xmi', f'{self.repopath}/{name}')
                                # Non-XMI members 
                                self.members[name] = XMIMember(data, name, self, True)

                        else:
                            setattr(self, k, v) 
                else:
                    # For non-PDS XMI's
                    self.file = f'{DIR}'
                    self.member = XMIMember(file_info, self, False)

            else:
                setattr(self, key, value)

class XMIMember:
    def __init__(self, member_info, name, parent, in_pds):
        self.name = name
        self.parent = parent
        for key, value in member_info.items():
            setattr(self, key, value)
        
        # Set defaults if missing
        setattr(self, 'mimetype', getattr(self, 'mimetype', 'application/octet-stream'))
        setattr(self, 'extension', getattr(self, 'extension', '.bin'))

        self.copy_file(in_pds)

    def copy_file(self, in_pds):
        src_dir = self.parent.file
        dst_dir = self.parent.repopath

        if self.mimetype.split('/')[0] == 'text' or self.mimetype == 'application/xmit':
            # Copy to new location if text file
            # Set source/destination of file for copying 
            src = f'{src_dir}/{self.name}{self.extension}'
            if in_pds:
                # Check if in PDS subdirectory 
                dst = f'{dst_dir}/PDS/{self.name}'
            else:
                dst = f'{dst_dir}/{self.name}'

            if not os.path.exists(dst) or not filecmp.cmp(src, dst):
                shutil.copyfile(src, dst)

            # Set ISPFSTATS
            setattr(self, 'ispf', getattr(self, 'ispf', 
                                          {'version': '01.00', 'flags': 0, 'createdate': '1976-06-12T00:00:00.000000', 
                                           'modifydate': '1976-06-12T22:18:12.000000', 'lines': 0, 'newlines': 0, 
                                           'modlines': 0, 'user': 'CBT2GIT'}))
            
            #print(f'{datetime.datetime.now()} - Found {src}, moved to {dst}' + '\n')

        elif self.mimetype in docmimetypes:
            # Doc types
            os.makedirs(f'{dst_dir}/docs' , exist_ok = True) # Create directory for docs 
            src = f'{src_dir}/{self.name}{self.extension}'
            dst = f'{dst_dir}/docs/{self.name}{self.extension}'

            if not os.path.exists(dst) or not filecmp.cmp(src, dst):
                shutil.copyfile(src, dst)
        
        elif self.mimetype == 'application/zip':
            print(self.name)

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

def unzip_xmi(zip):
    """Unzips zip file containing xmi"""
    try:
        zip_ref = zipfile.ZipFile(zip, 'r')
    except:
        print(f"ZIP {zip} is not a zip file.")
        return
    
    info = zip_ref.infolist()

    if len(info) == 0:
        print(f"No files found in ZIP {zip}")
        return

    if len(info) > 1:
        print(f"More than one file in zip {zip} => {', '.join([file.filename for file in info])}")
        return

    zip_ref.extractall(DIR)
    xmi_file = f'{DIR}{info[0].filename}'
    
    return xmi_file # return unzipped xmi filename

def main():
    # Parse arguments
    args = parse_arguments()

    global repos, stage, only, cbtfiles, noremote

    repos    = args.repos 
    stage    = args.stage
    only     = args.only
    cbtfiles = args.cbtfiles
    noremote = args.noremote

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
        print(f'{datetime.datetime.now()} - Initialized conversion of CBT{cbtnum}.')
        xmi_file = f'{DIR}FILE{cbtnum}.XMI'
        xmi_file = unzip_xmi(zip)
        # Skip file if unable to unzip 
        if not xmi_file:
            continue

        if os.path.exists(xmi_file):
            # Set reponame for each file 
            reponame = zip.split('/')[1].split('.')[0]
            repopath = f'{repos}/{reponame}'
            # Exract XMI to new repo 
            myXMI = XMIObject(xmi_file, repopath)
    
        else:
            continue

if __name__ == '__main__':
    main()
