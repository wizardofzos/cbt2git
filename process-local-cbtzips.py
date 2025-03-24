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
from github import Github, GithubException
import filecmp
import shutil
import threading
import random
import requests
import logging


MAX_THREAD_DOWNLOADS = 15

# Logging info 
log_buffer = io.StringIO()
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler(log_buffer)]
)

class XMIObject:
    def __init__(self, xmi_file, repopath, parent = None):
        self.xmi_file = xmi_file 
        name = xmi_file.split('/')[-1]
        self.repopath = repopath
        os.makedirs(repopath , exist_ok = True) # Create target directory

        self.xmi_obj = self.open_xmi()
        if not self.xmi_obj:
            return
        self.pds = self.xmi_obj.get_file()

        self.loglines = []
        self.loglines.append(f'{datetime.datetime.now()} - Received {self.pds} from {name} ' + '\n')

        if (self.xmi_obj.is_pds(self.pds)):
            # if the file is a PDS, set members as new XMIMembers
            self.has_pds = True
            self.set_members() 

            if not parent:
                with open(f"{self.repopath}/cbt2git.log", "w") as file:
                    file.writelines(self.loglines)
            else:
                parent.loglines.append(str(self.loglines))

        else:
            # add the file as the only member
            filename = self.pds
            info = self.xmi_obj.get_file_info_simple(filename)
            self.pds = ''
            XMIMember(filename, info, self)

    def open_xmi(self):
        """Opens the XMI file and extracts its contents."""
        try:
            xmi_obj = xmi.open_file(self.xmi_file, quiet = True)
        except Exception as e:
            logging.error(f"{datetime.datetime.now()} - Error opening {self.xmi_file}: {str(e)}")
            return None

        xmi_obj.set_output_folder('/tmp')
        xmi_obj.set_quiet(True)

        try:
            xmi_obj.extract_all()
        except Exception as e:
            logging.error(f"{datetime.datetime.now()} - Error extracting {self.xmi_file}: {str(e)}")
            return

        # Check if contents are empty 
        try:
            if not xmi.list_all(self.xmi_file):
                # Empty or invalid XMI
                logging.error(f"{datetime.datetime.now()} - XMI file {self.xmi_file} is empty or invalid.")
                return

        except Exception as e:
            # Log an error if the file is not a valid XMI file
            logging.error(f"{datetime.datetime.now()} - Error processing {self.xmi_file} from ZIP file {zip}: {str(e)}")
            return
        
        return xmi_obj # Return the XMI object 

    def set_members(self):
        """Creates a child XMIObject for each member """
        xmi_members = self.xmi_obj.get_members(self.pds)
        pds_folder = f'{self.repopath}/PDS'
        os.makedirs(pds_folder, exist_ok = True) # Create repo for PDS members
        for m in xmi_members:
            info = self.xmi_obj.get_member_info(self.pds, m)
            if info.get('alias'):
                # If the member is an alias
                alias = info.get('alias')
                try:
                    os.symlink(alias, f'{pds_folder}/{m}')
                except FileExistsError:
                    logging.error(f"{datetime.datetime.now()} - Symlink {m} to {alias} already exists")
                except Exception as e:
                    logging.error(f"{datetime.datetime.now()} - Error creating alias {m} to {alias} in {self.pds}")  
                    continue

                self.loglines.append(f'{datetime.datetime.now()} - Found alias {m} to {alias} in {self.pds}, moved to PDS/{m}' + '\n')

            else:
                # Otherwise create a new member
                XMIMember(m, info, self)

            if info.get('mimetype') == 'application/xmit':
                # Create new XMIObject if member is an xmi file 
                XMIObject(f'/tmp/{self.pds}/{m}.xmi', f'{self.repopath}/{m}', self)
        
class XMIMember:
    def __init__(self, name, info, parent):
        self.name = name
        self.pds = getattr(parent, 'pds', '')
        self.parent = parent
        
        for key, value in info.items():
            # Set attributes based on info dict  
            setattr(self, key, value)

        # Set defaults if missing
        setattr(self, 'mimetype', getattr(self, 'mimetype', 'application/octet-stream'))
        setattr(self, 'extension', getattr(self, 'extension', '.bin'))

        self.move_member()

    def move_member(self):
        """Move member file to destination directory based on mimetype"""
        src_dir = f'/tmp/{self.pds}'
        dst_dir = self.parent.repopath
        src = f'{src_dir}/{self.name}{self.extension}'
        docmimetypes = ['application/msword', 'application/epub+zip', 'application/pdf', 
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'application/vnd.oasis.opendocument.text','application/vnd.oasis.opendocument.text',
            'application/vnd.ms-powerpoint','application/vnd.ms-excel',
            'application/vnd.openxmlformats-officedocument.presentationml.presentation']
    
        if self.mimetype in docmimetypes:
            # For document types
            doc_dir = f'{dst_dir}/docs'
            dst = f'{doc_dir}/{self.name}{self.extension}'

            os.makedirs(doc_dir , exist_ok = True) # Create directory for docs 
            copy_file(src, dst)

        elif self.mimetype in ['application/zip', 'application/java-archive']:
            # Zip files
            dst = f'{dst_dir}/{self.name}' # Set file destination

            try:
                # Try to unzip 
                with zipfile.ZipFile(src, 'r') as zip:
                    try:
                        zip.extractall(dst)
                    except Exception as e:
                        logging.error(f"{datetime.datetime.now()} - ZIP {zip} in {self.pds} is not a zip file: {e}")                    
            except Exception as e:
                # Just copy file if unable to unzip 
                copy_file(src, dst)
        
        else:
            # Copy without extension still present
            if hasattr(self.parent, 'has_pds'):
                # If the member is in a PDS
                dst = f'{dst_dir}/PDS/{self.name}' 
            else:
                dst = f'{dst_dir}/{self.name}' 

            copy_file(src, dst)
        
        logline = f'{datetime.datetime.now()} - Found {self.name}{self.extension} ({self.mimetype}) in {self.pds}, moved to PDS/{self.name}' + '\n'
        self.parent.loglines.append(logline)
    
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

def clean_repos(github):
    """Removes remote repositories."""
    git_repos = list(github.get_user().get_repos())
    total_repos = len(git_repos)
    # Check if there are any repos to process
    if total_repos == 0:
        print("No repositories to delete.")
        return
    
    if only:
        try:
            repo = github.get_user().get_repo(only) 
            repo.delete()
            print(f"Successfully deleted repository: {only}")
        except GithubException as e:
            if e.status == 404:
                print(f"Repository '{only}' does not exist.")
            else:
                logging.error(f"{datetime.datetime.now()} - Error deleting '{only}': {e}")
        except Exception as e:
            logging.error(f"{datetime.datetime.now()} - Unexpected error deleting '{only}': {e}")
        return

    print(f"Total Repos: {total_repos}")
    while total_repos > 0:
        for repo in git_repos:
            if repo.name[:3] == "CBT":
                rate_used, rate_init = github.rate_limiting
                gracetime = (github.rate_limiting_resettime-math.floor(time.time())) / 1000
                print(f"Deleting {repo.name:8} (gracetime = {gracetime}, ratelimits = {rate_used}/{rate_init})", end=' ', flush=True)
                
                try:
                    repo.delete()
                    # Update the repo list/total count after deletion
                    git_repos.remove(repo)
                    total_repos -= 1
                except Exception as e:
                    logging.error(f"{datetime.datetime.now()} - Error deleting {repo.name}: {e}")
                
                if rate_used >= rate_init * 0.9:  
                    # Check if close to hitting the rate limit 
                    time_to_wait = gracetime
                    print(f"\nRate limit reached, waiting for {time_to_wait:.2f}s...")
                    time.sleep(time_to_wait + 1)  
                
                print(f"Deleted {repo.name}       ", end='\r', flush=True)

            # Ignore non-CBT repos
            else:
                git_repos.remove(repo)
                total_repos -= 1
    
    print("\nFinished deleting CBT repositories.")

def copy_file(src, dst): 
    """ Copies src to dst if new or different.""" 
    if os.path.isfile(src):
        if not os.path.exists(dst) or not filecmp.cmp(src, dst):
            shutil.copyfile(src, dst)
        return dst
    else:
        logging.error(f"{datetime.datetime.now()} - {src} not found.")
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
        logging.error(f"{datetime.datetime.now()} - No files found in ZIP {zip}")
        return
    if len(info) > 1:
        logging.error(f"{datetime.datetime.now()} - More than one file in ZIP {zip} => {', '.join([file.filename for file in info])}")
        return
    
    try: 
        # Extract zip to source directory
        zip_ref.extractall('/tmp')
    except Exception as e:
        logging.error(f"{datetime.datetime.now()} - Unable to extract ZIP {zip}: {e}")
        return
    
    xmi_file = f'/tmp/{info[0].filename}' # Get extracted XMI

    # Set reponame for destination directory for each file
    reponame = zip.split('/')[1].split('.')[0]
    repopath = f'{repos}/{reponame}'

    XMIObject(xmi_file, repopath)

def get_github_token():
    try:
        with open('config.yml', 'r') as f:
            config = yaml.safe_load(f)
        
        token = config.get('token')
        if not token:
            print("Error: GitHub token not found in config.yml")
            exit(4)

        github = Github(token)
        return github
            
    except Exception as e:
        print(f"Unexpected error getting GithubToken: {e}")
        exit(4)

def create_git_repo(github, reponame):
    me = github.get_user()
    user = me.login

    try:
        # Try to get repourl and return if it exists
        repourl = me.get_repo(reponame).ssh_url
        return repourl
    
    except GithubException as e:
        # If repository does not exist, create a new one
        if e.status != 404:
            logging.error(f"{datetime.datetime.now()} - Error getting GitHub repo {reponame}: {e}")

    retry_count = 0
    max_retries = 5
    while retry_count < max_retries:
        # Retry if rate limits hit
        try:
            rate_used, rate_init = github.rate_limiting
            gracetime = (github.rate_limiting_resettime-math.floor(time.time())) / 1000
            if rate_used >= rate_init * 0.9:  # If close to limit, wait
                print(f"Approaching rate limit. Waiting {gracetime}s...")
                time.sleep(gracetime + 1)

            # Create new repo 
            new_repo = me.create_repo(reponame)
            print(f"Repository {reponame} created. SSH URL: {new_repo.ssh_url}")
            return new_repo.ssh_url   
        
        except requests.exceptions.ConnectionError as e:
            # Retry if connection fails 
            wait_time = min(30 * (2 ** retry_count) + random.uniform(0, 5), 300)  
            print(f"Connection error: {e}. Retrying in {wait_time:.2f}s...")
            time.sleep(wait_time)
            retry_count += 1

        except GithubException as e:
            if e.status == 403 and "secondary rate limit" in str(e):
                # Retry if secondary rate limit hit
                wait_time = min(60 * (2 ** retry_count) + random.uniform(0, 5), 600)
                print(f"Secondary rate limit hit. Waiting {wait_time:.2f}s before retrying...")
                time.sleep(wait_time)
                retry_count += 1

            else:
                # Other errors 
                logging.error(f"{datetime.datetime.now()} - Error creating github repo {reponame}: {e}")
                time.sleep(600)
                return None
        
    # Failed after max retries 
    logging.error(f"{datetime.datetime.now()} - Unable to create github repo {reponame} due to rate limits.")
    return None  

def update_git_repo(github, reponame, remote_name="origin", branch="main"):
    repourl = create_git_repo(github, reponame)
    if not repourl:
        return

    repopath = f'{repos}/{reponame}'

    # Create README file 
    files  = glob.glob(f'{repopath}/PDS/@FIL*')
    if len(files) != 1:
        # I guess I'd have to look in CBT001 here 
        readme_content = 'echo "No @FILE in PDS"'
        print(f"No @FIL(E) detected for {reponame}, creating a README.md without extra info.")
    else:
        with open(files[0], 'r') as f:
            readme_content = f.read()
        readme_content = f"```\n{readme_content}\n```" # Wrap in code block to avoid problems with slashes... 

    readme = f"""# {reponame}
Converted to GitHub via [cbt2git](https://github.com/wizardofzos/cbt2git)

This is still a work in progress. GitHub repos will be deleted and created during this period...

{readme_content}
"""

    # Write to README.md
    with open(f"{repopath}/README.md", "w") as file:
        file.write(readme)

    # Create .gitattributes file 
        attributes=f"""*                git-encoding=iso8859-1 zos-working-tree-encoding=ibm-1047 
.gitattributes    git-encoding=iso8859-1 zos-working-tree-encoding=iso8859-1
.gitignore        git-encoding=iso8859-1 zos-working-tree-encoding=iso8859-1
*.docm binary
*.docx binary
*.doc  binary
*.pdf  binary
*.epub binary
*.mobi binary
*.azw3 binary
*.pdf binary"""

    # Write to README.md
    with open(f"{repopath}/.gitattributes", "w") as file:
        file.write(attributes)

    try:
        # Ensure repository is initialized
        if not os.path.isdir(os.path.join(repopath, ".git")):
            print("Initializing new Git repository...")
            subprocess.run(["git", "init", "--initial-branch=main"], cwd=repopath, check=True, stdout=subprocess.DEVNULL)

        # Check if remote already exists
        remotes = subprocess.run(["git", "remote"], cwd=repopath, capture_output=True, text=True)
        if remote_name not in remotes.stdout.split():
            subprocess.run(["git", "remote", "add", remote_name, repourl], cwd=repopath, check=True, stdout=subprocess.DEVNULL)
            print(f"Remote '{remote_name}' added: {repourl}")

        # Check if there are changes to commit
        git_status = subprocess.run(["git", "status", "--porcelain"], cwd=repopath, capture_output=True, text=True)
        if not git_status.stdout.strip():
            print("No changes to commit.")
            return

        # Stage all changes
        subprocess.run(["git", "add", "."], cwd=repopath, check=True, stdout=subprocess.DEVNULL)

        # Commit changes
        commit_message = f'Updates from cbttape.org ({datetime.datetime.now().strftime("%Y-%m-%d")})'
        subprocess.run(["git", "commit", "-m", commit_message, "--quiet"], cwd=repopath, check=True, stdout=subprocess.DEVNULL)
        print("Changes committed successfully.")

        # Push changes
        subprocess.run(["git", "push", remote_name, branch], cwd=repopath, check=True, stdout=subprocess.DEVNULL)
        print(f"Changes pushed to {remote_name}/{branch} successfully.")

    except subprocess.CalledProcessError as e:
        logging.error(f"{datetime.datetime.now()} - Error running Git command for repo {reponame}: {e}")

    return

def main():
    global repos, stage, only, cbtfiles, noremote

    # Parse arguments
    args = parse_arguments()

    repos    = args.repos 
    stage    = args.stage
    only     = args.only
    cbtfiles = args.cbtfiles
    noremote = args.noremote
    no_unzip = False

    if not noremote:    
        github = get_github_token()
        try:
            me = github.get_user()
            user = me.login
            print(f"Token has logged onto {me.name} acting in GitHub user github.com/{user}")
        except Exception as e:
            print(f"Unable to log into GitHub: {e}")
            exit(4)

    else:
        print("Running locally only, no updates to GitHub.")
    
    # Clean local/remote repositories 
    if args.clean:
        if only:
            os.system(f'rm -rf {cbtfiles}/{only}')
            os.system(f'rm -rf {repos}/{only}')
        else:
            os.system(f'rm -rf {cbtfiles}/*')
            os.system(f'rm -rf {repos}/*')
        if not noremote:
            clean_repos(github)
    
    if not no_unzip:
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

            print(f'{done_bar}{todo_bar} {zip} ({pct}%) [converting, active threads={threading.active_count()}]', end='\r', flush=True)
            while threading.active_count() >= MAX_THREAD_DOWNLOADS + 1:  # +1 for the main thread
                time.sleep(0.5)
            
            # Extract XMI and copy contents to target directories
            thread = threading.Thread(target=extract_xmi, args=(zip, ))
            thread.start()
            threads.append(thread)

        done = 40 * "✅" 
        pct = 100
        z=''
        print(f'{done} {z} ({pct}%)', flush=True)
        time.sleep(20) # Wait for threads to finish

    if not noremote:
        my_repos = sorted(os.listdir(repos)) # list of CBT repos
        threads = []

        for index, reponame in enumerate(my_repos):
            if only:
                if reponame != only:
                    continue 
            
            if reponame[:3] != "CBT":
                continue

            pct = math.floor((index / len(my_repos)) * 100)
            done = math.floor((pct / 100) * 40)
            todo = 40 - done
            done_bar = "✅" * done
            todo_bar = "🟩" * todo

            print(f'{done_bar}{todo_bar} {reponame} ({pct}%) [updating, active threads={threading.active_count()}]', end='\r', flush=True)
            
            while threading.active_count() >= MAX_THREAD_DOWNLOADS + 1:  # +1 for the main thread
                time.sleep(0.5)

            # Create new repo if missing, and commit any updates
            thread = threading.Thread(target=update_git_repo, args=(github, reponame))
            thread.start()
            threads.append(thread)

        done = 40 * "✅" 
        pct = 100
        z=''
        print(f'{done} {z} ({pct}%)', flush=True)

    # Write buffered logs to file 
    logfile = f'cbt2git-log-{datetime.datetime.now().strftime("%Y-%j-%H-%M-%S")}'
    with open(logfile, "w") as f:
        f.write(log_buffer.getvalue())
    log_buffer.close()
    
if __name__ == '__main__':
    main()
