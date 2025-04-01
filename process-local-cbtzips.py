import argparse
import os
import math
import shutil
import logging
import filecmp
import math
import time
import datetime
import xmi
import pandas as pd
import zipfile
import yaml
from github import Github, GithubException
import random
import requests
import glob
import re
import subprocess

# Global variables
CONFIG_FILE = "config.yml"
SKIP = ["CBT001", "CBT002", "CBT003", "CBT004", "CBT005", "CBT007", "CBT018"] # Don't contain XMI files 

# Configure logfile info
logfile = f'cbt2git-log-{datetime.datetime.now().strftime("%Y-%j-%H-%M-%S")}'
logging.basicConfig(
    filename=logfile,  
    filemode="w", 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# Suppress xmi logging info
logging.getLogger("xmi").setLevel(logging.WARNING)  # Prevent DEBUG logs
logging.getLogger("xmi").propagate = False  # Ensure xmi logs don’t pass to root
# Create main logger
logger = logging.getLogger(__name__)

class XMIObject:
    def __init__(self, name, xmi_file, parent = None):
        self.name = name
        self.xmi_file = xmi_file            

        self.xmi_obj = self.extract_xmi()
        if not self.xmi_obj:
            # Return if there was an error extracting the XMI 
            return None

        self.repopath = f'{repos}/{name}'
        os.makedirs(self.repopath , exist_ok = True) # Create target directory
        
        filename  = self.xmi_obj.get_file() # Get the pds file 
        self.loglines = [] # For cbt2git log 
        xmi_filename = xmi_file.split('/')[-1]
        self.loglines.append(f'{datetime.datetime.now()} - Received {filename} from {xmi_filename} ' + '\n')

        if (self.xmi_obj.is_pds(filename)):
            # if the file is a PDS, set members as new XMIMembers
            self.pds = filename
            self.create_members() 

            if not parent:
                # If not a nested XMI, create cbt2git log file 
                with open(f"{self.repopath}/cbt2git.log", "w") as file:
                    file.writelines(self.loglines)
                self.success = True
            
            else:
                # If nested, add loglines to parent loglines 
                parent.loglines.append(str(self.loglines))
            
            shutil.rmtree(f"/tmp/{self.pds}") # Remove PDS directory in /tmp

        
        else:
            # If not a PDS, add the file as the only member
            logger.info(f"Adding {filename} as the only member of {self.name}")
            info = self.xmi_obj.get_file_info_simple(filename) # Get file info 
            xmi_member = XMIMember(filename, info, self) # Create new member 
            os.remove(f"/tmp/{filename}{xmi_member.extension}") # Remove file from previous location

        # Remove xmi_file from /tmp folder
        os.remove(self.xmi_file)      

    def extract_xmi(self):
        """Opens the XMI file and extracts its contents."""
        try:
            # Open XMI file 
            xmi_obj = xmi.open_file(self.xmi_file, quiet = True)
        except Exception as e:
            logger.error(f"Error opening {self.xmi_file}: {str(e)}")
            return None

        try:
            # Extract XMI file contents
            xmi_obj.set_output_folder('/tmp')
            xmi_obj.set_quiet(True)
            xmi_obj.extract_all()
        except Exception as e:
            logger.error(f"Error extracting {self.xmi_file}: {str(e)}")
            return None

        try:
            # Check if XMI file is empty
            if not xmi.list_all(self.xmi_file):
                # Empty or invalid XMI
                logger.error(f"XMI file {self.xmi_file} is empty or invalid.")
                return None
        except Exception as e:
            # Log an error if the file is not a valid XMI file
            logger.error(f"Error processing {self.xmi_file} from {self.name}: {str(e)}")
            return None
        
        logger.info(f"Received {self.xmi_file} from {self.name}.")
        return xmi_obj # Return the XMI object 

    def create_members(self):
        """Creates a child XMIObject for each member """
        xmi_members = self.xmi_obj.get_members(self.pds)
        pds_folder = f'{self.repopath}/PDS'
        os.makedirs(pds_folder, exist_ok = True) # Create repo for PDS members

        for m in xmi_members:
            # Iterate through each member in the PDS
            info = self.xmi_obj.get_member_info(self.pds, m) # Get member info

            if info.get('alias'):
                # If the member is an alias
                alias = info.get('alias')
                try:
                    # Create a symlink 
                    os.symlink(alias, f'{pds_folder}/{m}')
                except FileExistsError:
                    logger.warning(f"Symlink {m} to {alias} already exists for CBT{self.name}.")
                except Exception as e:
                    logger.error(f"Error creating alias {m} to {alias} in {self.pds} for CBT{self.name}.")  
                    continue

                self.loglines.append(f'{datetime.datetime.now()} - Found alias {m} to {alias} in {self.pds}, moved to PDS/{m}' + '\n')

            else:
                # Otherwise create a new member
                XMIMember(m, info, self)

                if info.get('mimetype') == 'application/xmit':
                    # Create new XMIObject if member is an xmi file 
                    XMIObject(f'{self.name}/{m}', f'/tmp/{self.pds}/{m}.xmi', self)

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
        if hasattr(self.parent, 'pds'):
            src_dir = f'/tmp/{self.pds}'
        else:
            src_dir = f'/tmp'
        src = f'{src_dir}/{self.name}{self.extension}' # source filepath 
        dst_dir = self.parent.repopath 

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
                        logger.error(f"ZIP {zip} in {self.pds} is not a zip file: {e}")                    
            except Exception as e:
                # Just copy file if unable to unzip 
                copy_file(src, dst)
        
        else:
            # Copy without extension still present
            if hasattr(self.parent, 'pds'):
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
                                    description= "Extract CBTTapes and update a GitHub profile with data from CBTTape.org if needed.")
    parser.add_argument("--stage", 
                        type = str,
                        default = f'{os.getcwd()}/stage',
                        help=f"Full path to stage-folder where zip files from cbttape.org were downloaded to. Defaults to {os.getcwd()}/stage.")
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
                        help = "Only process this CBT Tape (e.g. CBT010).")
    parser.add_argument("--pickle", 
                        type = str,
                        default = f'.cbt.pkl',
                        help = "Panda pickle file with parsed UPDATESTOC.txt information. Will be updated during this run. Defaults to .cbt.pkl")
    parser.add_argument("--clean",
                        action = "store_true",
                        help = "Cleans everything except stage folder.")
    parser.add_argument("--noremote",
                        action="store_true",
                        help=f"Do everything, except remote GitHub actions. (doen't create or updates repos")
    args = parser.parse_args()
    return args

def clean_repos():
    """Cleans all local (and remote) repos except stage folder if --clean is specified."""
    # Clean local repositories
    if only:
        # Only delete the CBT repo indicated by only
        os.system(f'rm -rf {cbtfiles}/{only}')
        os.system(f'rm -rf {repos}/{only}')
        print(f"Removed local repository: {only}")
    else:
        os.system(f'rm -rf {cbtfiles}/*')
        os.system(f'rm -rf {repos}/*')
        print("Removed all local repositories.")
    if not noremote:
        # Remove remote repositories
        GIT_REPOS = list(GITHUB_USER.get_repos())
        total_repos = len(GIT_REPOS)
        if total_repos == 0:
            print("No remote repositories to delete.")
        else:
            if only:
                # Only delete the CBT repo indicated by only
                try:
                    repo = GITHUB_USER.get_repo(only) 
                    repo.delete()
                    print(f"Removed remote repository: {only}")
                except GithubException as e:
                    if e.status == 404:
                        print(f"Repository '{only}' does not exist.")
                    else:
                        logger.error(f"Error deleting {only}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error deleting {only}: {e}")
            else:
                print(f"Total Repos: {total_repos}")
                while total_repos > 0:
                    for repo in GIT_REPOS:
                        if repo.name[:3] == "CBT":
                            # Check if the repo is a CBT repo before deleting
                            rate_used, rate_init = GITHUB_CLIENT.rate_limiting
                            gracetime = (GITHUB_CLIENT.rate_limiting_resettime-math.floor(time.time())) / 1000
                            print(f"Deleting {repo.name:8} (gracetime = {gracetime}, ratelimits = {rate_used}/{rate_init})", end=' ', flush=True)
                            
                            try:
                                repo.delete()
                                # Update the repo list/total count after deletion
                                GIT_REPOS.remove(repo)
                                total_repos -= 1
                            except Exception as e:
                                logger.error(f"Error deleting {repo.name}: {e}")
                            
                            if rate_used >= rate_init * 0.9:  
                                # Check if close to hitting the rate limit 
                                time_to_wait = gracetime
                                # print(f"\nRate limit reached, waiting for {time_to_wait:.2f}s...")
                                time.sleep(time_to_wait + 1)  
                            
                            print(f"Deleted {repo.name}       ", end='\r', flush=True)

                        # Ignore non-CBT repos
                        else:
                            GIT_REPOS.remove(repo)
                            total_repos -= 1

def copy_file(src, dst): 
    """Copies src to dst if new or different.""" 
    if os.path.isfile(src):
        if not os.path.exists(dst) or not filecmp.cmp(src, dst):
            shutil.copyfile(src, dst)
        return dst
    else:
        logger.error(f"{src} not found.")
        if only: 
            # Stop if only processing one file 
            exit(4)
        return None

def unzip_xmi(cbt_zip):
    """Unzip CBT zipfile and extract contents to /tmp. Returns extracted XMI file."""
    try:
        zip_ref = zipfile.ZipFile(cbt_zip, 'r')
    except Exception as e: 
        logger.error(f"ZIP {cbt_zip} is not a zip file: {e}")
        return None
    
    # Check that there is was only one file unzipped 
    info = zip_ref.infolist()
    if len(info) == 0:
        logger.error(f"No files found in ZIP {zip}")
        return None
    if len(info) > 1:
        logger.error(f"More than one file in ZIP {zip} => {', '.join([file.filename for file in info])}")
        return None
    
    try: 
        # Extract zip to /tmp directory
        zip_ref.extractall('/tmp')
    except Exception as e:
        logger.error(f"Unable to extract ZIP {zip}: {e}")
        return None
    
    xmi_file = f'/tmp/{info[0].filename}' # Get extracted XMI
    return xmi_file

def readme_file(reponame):
    """Create README file for the repository."""
    repopath = f'{repos}/{reponame}'
    # Create README file 
    files  = glob.glob(f'{repopath}/PDS/@FIL*')
    if len(files) != 1:
        logger.info(f"Creating readme file for {reponame} using info from CBTF1.txt.")
        with open("CBTF1.txt", "r", encoding="Windows-1252", errors="replace") as f1:
            lines = f1.readlines()
        
        cbtnum = reponame.split("CBT")[1]
        if (len(cbtnum) == 3):
            pattern = re.compile(fr"\*+   FILE {re.escape(cbtnum)}\b") # Regular expression to match "*   FILE {cbtnum}"
        else:
            pattern = re.compile(fr"\*+   FILE{re.escape(cbtnum)}\b")  # Regular expression to match "*   FILE{cbtnum}"

        readme_lines= [line.strip() for line in lines if pattern.search(line)] # Filter lines matching the pattern
        readme_content = '\n'.join(readme_lines)
        readme_content = f"```\n{readme_content}\n```"

        if not readme_content: 
            # If no lines are found in CBT001 for the CBTTape
            readme_content = 'echo "No @FILE in PDS"'
            logger.info(f"No @FIL(E) detected for {reponame}, creating a README.md without extra info.")
    else:
        with open(files[0], 'r') as f:
            readme_content = f.read()
    
        readme_content = f"```\n{readme_content}```" # Wrap in code block to avoid problems with slashes... 

    readme = f"""# {reponame}
Converted to GitHub via [cbt2git](https://github.com/wizardofzos/cbt2git)

This is still a work in progress. GitHub repos will be deleted and created during this period...

{readme_content}
"""
    # Write to README.md
    with open(f"{repopath}/README.md", "w") as file:
        file.write(readme)

def git_attributes(reponame):
    """Create .gitattributes file for the repository."""
    repopath = f'{repos}/{reponame}'
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

    # Write to .gitattributes file
    with open(f"{repopath}/.gitattributes", "w") as file:
        file.write(attributes)

def create_git_repo(reponame):
    """Creates new GitHub repo."""
    retry_count = 0
    max_retries = 10
    while retry_count < max_retries:
        # Retry if rate limits hit
        rate_used, rate_init = GITHUB_CLIENT.rate_limiting
        gracetime = (GITHUB_CLIENT.rate_limiting_resettime-math.floor(time.time())) / 1000
        if rate_used >= rate_init * 0.9:  
            # Check if close to hitting the rate limit 
            time_to_wait = gracetime
            # print(f"Rate limit reached, waiting for {time_to_wait:.2f}s...")
            time.sleep(time_to_wait + 1)  
        try:
            # Create new repo 
            new_repo = GITHUB_USER.create_repo(reponame)
            repourl = new_repo.ssh_url   
            logger.info(f"Created GitHub repo {repourl}.")
            time.sleep(10) # Sleep after repo creation
            return repourl
        
        except requests.exceptions.ConnectionError as e:
            # Retry if connection fails 
            wait_time = min(30 * (2 ** retry_count) + random.uniform(0, 5), 300)  
            # print(f"Connection error: {e}. Retrying in {wait_time:.2f}s...")
            time.sleep(wait_time)
            retry_count += 1

        except GithubException as e:
            if e.status == 403 and "secondary rate limit" in str(e):
                # Retry if secondary rate limit hit
                wait_time = min(60 * (2 ** retry_count) + random.uniform(0, 5), 600)
                # print(f"Secondary rate limit hit. Waiting {wait_time:.2f}s before retrying...")
                time.sleep(wait_time)
                retry_count += 1
            else:
                # Other errors 
                logger.error(f"Error creating github repo {reponame}: {e}")
                time.sleep(600)
                return None
        
    # Failed after max retries 
    logger.error(f"Unable to create github repo {reponame} due to rate limits.")
    time.sleep(600)
    return None  

def commit_git_repo(reponame, repourl, remote_name="origin", branch="main"):
    """Commits any updates from reponame to repourl."""
    repopath = f'{repos}/{reponame}'

    try:
        # Ensure repository is initialized
        if not os.path.isdir(os.path.join(repopath, ".git")):
            subprocess.run(["git", "init", "--initial-branch=main", "--quiet"], cwd=repopath, check=True)

        # Check if remote already exists
        remote_url = subprocess.run(["git", "remote", "get-url", remote_name], cwd=repopath, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        current_url = remote_url.stdout.strip()
        if current_url: 
            if current_url != repourl:  
                # Remove remote if it doesn't match the repourl
                subprocess.run(["git", "remote", "remove", remote_name], cwd=repopath, check=True)
                subprocess.run(["git", "remote", "add", remote_name, repourl], cwd=repopath, check=True)
        else:
            # Add remote if none exists
            subprocess.run(["git", "remote", "add", remote_name, repourl], cwd=repopath, check=True)
        
        # Check if there are changes to commit
        git_status = subprocess.run(["git", "status", "--porcelain"], cwd=repopath, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if not git_status.stdout.strip():
            logger.info(f"No updates found for {repourl}.")
            return

        # Stage all changes
        subprocess.run(["git", "add", "."], cwd=repopath, check=True)

        # Commit changes
        commit_message = f'Updates from cbttape.org ({datetime.datetime.now().strftime("%Y-%m-%d")})'
        subprocess.run(["git", "commit", "-m", commit_message, "--quiet"], cwd=repopath, check=True)

        # Push changes
        subprocess.run(["git", "push", remote_name, branch, "--quiet", "--force"], cwd=repopath, check=True)
        logger.info(f"Committed updates to GitHub repo {repourl}.")

    except subprocess.CalledProcessError as e:
        logger.error(f"Error running Git command for repo {repourl}: {e}")
    return

# Parse all arguments 
args      = parse_arguments()
repos     = args.repos 
stage     = args.stage
only      = args.only
cbtfiles  = args.cbtfiles
noremote  = args.noremote

if f"{only}" in SKIP:
    print(f"{only} does not contain an XMI file.")
    exit(4)

if not noremote:
    # Retrieve GitHub token 
    with open(CONFIG_FILE, "r") as file:
        config = yaml.safe_load(file)
    GITHUB_TOKEN = config.get('token')
    if not GITHUB_TOKEN:
        print("Error: GitHub token not found in config.yml")
        exit(4)

    # Try logging in with given token 
    try:
        GITHUB_CLIENT = Github(GITHUB_TOKEN)
        GITHUB_USER = GITHUB_CLIENT.get_user()
        username = GITHUB_USER.login
        print(f"Token has logged onto {GITHUB_USER} acting as GitHub user github.com/{username}")
    except Exception as e:
        print(f"Unable to log into GitHub: {e}")
        exit(4)

if args.clean:
    clean_repos()

# Create repo/cbtfile directory if they don't exist
os.makedirs(repos, exist_ok = True)
os.makedirs(cbtfiles, exist_ok = True)

# Read pickle
cbt = pd.read_pickle(args.pickle)
print(f"Loaded our dataframe, {len(cbt)} CBT-files ready to be processed.")

# Get list of files to process
to_process = []
for index, filename in enumerate(os.listdir(stage)):
    if only:
        cbtn = f"{only}.zip" 
        if filename != cbtn: 
            continue

    # Add path to copied file to list of CBT zips to process
    src = os.path.join(stage, filename)
    dst = os.path.join(cbtfiles, filename)
    if copy_file(src, dst):
        to_process.append(dst)

if only and len(to_process) == 0:
    print(f"CBT-file {only} not found.")
    exit(4)

print(f"Need to process {len(to_process)} CBT zips.")

start = time.time()

for index, zip in enumerate(sorted(to_process)):
    # Unzip CBT zips to extract XMI files
    pct = math.floor((index / len(to_process)) * 100)
    done = math.floor((pct / 100) * 40)
    todo = 40 - done
    done_bar = "✅" * done
    todo_bar = "🟩" * todo

    name = zip.split('/')[1].split('.')[0]
    if name in SKIP:
        # Skip for CBT tapes that do not contain XMI files 
        continue
    
    print(f'{done_bar}{todo_bar} Converting {zip} ({pct}%)', end='\r', flush=True)
    logger.info(f"Initialized conversion of {name}.")
    xmi_file = unzip_xmi(zip) # Extract XMI from zip 

    if xmi_file:
        # Create an XMIObject to create local repo if xmi was extracted from zip
        xmi_obj = XMIObject(name, xmi_file)

    if hasattr(xmi_obj, 'success'):
        # If creating local repo was successful
        repopath = xmi_obj.repopath
        logger.info(f"Added all members of {name} to {repopath}.")
    else:
        logger.error(f"Error moving members of {name}.")
        continue

    if not noremote:
        # Skip the GitHub steps if noremote specified
        print(f'{done_bar}{todo_bar} Updating GitHub repo {name} ({pct}%)     ', end='\r', flush=True)
        readme_file(name) # Create README file
        git_attributes(name) # Create .gitattributes file

        rate_used, rate_init = GITHUB_CLIENT.rate_limiting
        gracetime = (GITHUB_CLIENT.rate_limiting_resettime-math.floor(time.time())) / 1000
        if rate_used >= rate_init * 0.9:  
            # Check if close to hitting the rate limit 
            time_to_wait = gracetime
            # print(f"Rate limit reached, waiting for {time_to_wait:.2f}s...")
            time.sleep(time_to_wait + 1)  

        try:
            # Try to get repourl if it exists
            repourl = GITHUB_USER.get_repo(name).ssh_url
            logger.info(f"Repo {repourl} already exists.")
        except GithubException as e:
            # If repository does not exist, create a new one
            if e.status == 404:
                repourl = create_git_repo(name)
            else:
                logger.error(f"Error getting GitHub repo {name}: {e}")
                continue
        
        if not repourl:
            # Continue if error creating GitHub repo
            continue

        rate_used, rate_init = GITHUB_CLIENT.rate_limiting
        gracetime = (GITHUB_CLIENT.rate_limiting_resettime-math.floor(time.time())) / 1000
        if rate_used >= rate_init * 0.9:  
            # Check if close to hitting the rate limit 
            time_to_wait = gracetime
            # print(f"Rate limit reached, waiting for {time_to_wait:.2f}s...")
            time.sleep(time_to_wait + 1)  
        
        commit_git_repo(name, repourl) # Commit updates 

        rate_used, rate_init = GITHUB_CLIENT.rate_limiting
        gracetime = (GITHUB_CLIENT.rate_limiting_resettime-math.floor(time.time())) / 1000
        # print(f"Rate critical ({rate_used}/{rate_init}), gracetime={gracetime}")
        if gracetime > 0:
            # print(f'Sleep for twice the grace time...{gracetime*2:.2f}s')
            time.sleep(gracetime*2)

        if index % 10 == 0 and only == 0:
            # print("Sleep for 30s...")
            time.sleep(30)
    
    logger.info(f"Completed conversion of {name}.")

done = 40 * "✅" 
pct = 100
z=''
print(f'{done} {z} ({pct}%)', flush=True)

stop = time.time()
if not noremote:
    print(f'All requested CBT files converted to Github repos in github.com/{username}.')
else:
    print(f'All requested CBT files converted and moved to {repos}')
print(f'This operation took {datetime.time(minute=stop-start)}')

        

