import argparse
import os
import math
import shutil
import logging
import filecmp
import math
import time
import datetime
import xmi, json
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
# Don't contain XMI files 
SKIP = ["CBT001", "CBT002", "CBT003", "CBT004", "CBT005", "CBT007", 
        "CBT018", "CBT063", "CBT064", "CBT110", "CBT157", "CBT230"] 

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
            self.zigidsn = (f'{filename} PO FB 80 32720' + "\n")
            self.zigispf = {}
            self.zigispf[self.name] = []

            self.create_members() # get all members of the PDS

            if not parent:
                # create cbt2git log file if not nested
                with open(f"{self.repopath}/cbt2git.log", "w") as file:
                    file.writelines(self.loglines)
                
                # Create folder for .zigi files 
                zigi_dir = f"{self.repopath}/.zigi"
                os.makedirs(zigi_dir, exist_ok = True) 
                # Create .zigi/dsn file 
                with open(f"{zigi_dir}/dsn", "w") as file:
                    file.write(self.zigidsn)

                for f in self.zigispf:
                    # Create .zigi/name file for parent/any nested XMI's
                    with open(f"{zigi_dir}/{f}", "w") as file:
                        file.writelines(self.zigispf[f])
                
                self.success = True
            
            else:
                # If nested, add loglines to parent loglines 
                parent.loglines.append(str(self.loglines))
                # Add zigi info 
                parent.zigidsn += self.zigidsn
                parent.zigispf[self.name.split('/')[1]] = self.zigispf[self.name]
            
            shutil.rmtree(f"/tmp/{self.pds}") # Remove PDS directory in /tmp

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
        # xmi_members = self.xmi_obj.get_members(self.pds)
        xmi_members = json.loads(self.xmi_obj.get_json()).get('file').get(self.pds).get('members')
        pds_folder = f'{self.repopath}/PDS'
        os.makedirs(pds_folder, exist_ok = True) # Create repo for PDS members

        for m, info in xmi_members.items():
            # Iterate through each member in the PDS
            if info.get('alias'):
                # If the member is an alias
                alias = self.xmi_obj.get_alias(self.pds, m)
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
                xmi_member = XMIMember(m, info, self)
                if hasattr(xmi_member, 'ispfline'):
                    # Add ispf stats if available
                    self.zigispf[self.name].append(xmi_member.ispfline)

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

        # Add ispf stats (not sure if I should do this for all members or not)
        if not hasattr(self, 'ispf'):
            self.ispf = {'version': '01.00', 'flags': 0, 'createdate': '1976-06-12T00:00:00.000000', 'modifydate': '1976-06-12T22:18:12.000000', 
                         'lines': 0, 'newlines': 0, 'modlines': 0, 'user': 'CBT2GIT'}
        self.ispfstats()
    
    def ispfstats(self):
        # Set creation date 
        crdat = self.ispf.get('createdate').split('T')[0][2:].replace('-','/')

        # Set modification date
        if self.ispf.get('modifydate'):
            mddat = self.ispf.get('modifydate').split('T')[0][2:].replace('-','/')
            mdtime = self.ispf.get('modifydate').split('T')[1].split('.')[0]
        else:
            # If missing 
            mddat  = '        '
            mdtime = '        '
        
        # Set version
        v = int(self.ispf.get('version').split('.')[0])
        m = int(self.ispf.get('version').split('.')[1])

        # Set number of lines 
        olines = int(self.ispf.get('lines'))
        nlines = int(self.ispf.get('newlines'))

        self.ispfline =  f"{self.name:<8} {crdat} {mddat} {v:>2} {m:>2} {mdtime} {olines:>5} {nlines:>5} {0:>5} {self.ispf.get('user')}\n"
        
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
            time.sleep(time_to_wait + 1)  
        try:
            # Create new repo 
            new_repo = GITHUB_USER.create_repo(reponame)
            repourl = new_repo.ssh_url   
            logger.info(f"Created GitHub repo {repourl}.")
            time.sleep(10) # Sleep after repo creation
            return repourl
        
        except requests.exceptions.ConnectionError as e:
            wait_time = min(30 * (2 ** retry_count) + random.uniform(0, 5), 300)  
            time.sleep(wait_time)
            retry_count += 1

        except GithubException as e:
            if e.status == 403 and "secondary rate limit" in str(e):
                # Retry if secondary rate limit hit
                wait_time = min(60 * (2 ** retry_count) + random.uniform(0, 5), 600)
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
        changed_files = [line.split()[-1] for line in git_status.stdout.strip().split("\n") if line]
        # Filter out cbt2git.log
        non_log_changes = [f for f in changed_files if os.path.basename(f) != "cbt2git.log"]
        if not non_log_changes:
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
    # Remove local repositories
    if only:
        # Only delete the CBT repo indicated by only
        os.system(f'rm -rf {cbtfiles}/{only}')
        os.system(f'rm -rf {repos}/{only}')
        print(f"Removed local repository: {only}")
    else:
        os.system(f'rm -rf {cbtfiles}/*')
        os.system(f'rm -rf {repos}/*')
        print("Removed all local repositories.")

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
    
    print(f'{done_bar}{todo_bar} Converting {zip} ({pct}%)           ', end='\r', flush=True)
    logger.info(f"Initialized conversion of {name}.")
    xmi_file = unzip_xmi(zip) # Extract XMI from zip 

    if xmi_file:
        # Create an XMIObject to create local repo if xmi was extracted from zip
        xmi_obj = XMIObject(name, xmi_file)
    else:
        # Skip if unable to unzip
        continue

    if hasattr(xmi_obj, 'success'):
        # If creating local repo was successful
        repopath = xmi_obj.repopath
        logger.info(f"Added all members of {name} to {repopath}.")
    else:
        logger.error(f"Error moving members of {name}.")
        continue

    if not noremote:
        # Skip the GitHub steps if noremote specified
        print(f'{done_bar}{todo_bar} Updating GitHub repo {name} ({pct}%)             ', end='\r', flush=True)
        readme_file(name) # Create README file
        git_attributes(name) # Create .gitattributes file

        new_repo = False
        try:
            repo = GITHUB_USER.get_repo(name)
            repourl = repo.ssh_url
        except GithubException as e:
            if e.status == 404:
                logger.info(f"Repo {repourl} does not exist.")
                new_repo = True
            else:
                logger.error(f"Error getting GitHub repo {name}: {e}")
                continue

        if not new_repo and args.clean:
            # Remove repo if clean specified
            rate_used, rate_init = GITHUB_CLIENT.rate_limiting
            gracetime = (GITHUB_CLIENT.rate_limiting_resettime-math.floor(time.time())) / 1000
            if rate_used >= rate_init * 0.9:  
                # Check if close to hitting the rate limit 
                time_to_wait = gracetime
                print(f'{done_bar}{todo_bar} Rate limit reached, waiting for {time_to_wait:.2f}s ({pct}%) ', end='\r', flush=True)
                time.sleep(time_to_wait + 1)  

            print(f'{done_bar}{todo_bar} Deleting GitHub repo {name} ({pct}%)            ', end='\r', flush=True)
            try: 
                repo.delete()
                new_repo = True
                time.sleep(10)
                logger.info(f"Removed Github repo {repourl}.")
            except Exception as e:
                logger.error(f"Error deleting Github repo {repourl}: {e}")

        if new_repo:
            # If repository does not exist, create a new one
            print(f'{done_bar}{todo_bar} Creating GitHub repo {name} ({pct}%)            ', end='\r', flush=True)
            repourl = create_git_repo(name)
            time.sleep(10)
            if not repourl:
                # Continue if problems creating git repo
                continue
        
        rate_used, rate_init = GITHUB_CLIENT.rate_limiting
        gracetime = (GITHUB_CLIENT.rate_limiting_resettime-math.floor(time.time())) / 1000
        if rate_used >= rate_init * 0.9:  
            # Check if close to hitting the rate limit 
            time_to_wait = gracetime
            print(f'{done_bar}{todo_bar} Rate limit reached, waiting for {time_to_wait:.2f}s ({pct}%) ', end='\r', flush=True)
            time.sleep(time_to_wait + 1)  

        print(f'{done_bar}{todo_bar} Updating GitHub repo {name} ({pct}%)             ', end='\r', flush=True) 
        commit_git_repo(name, repourl) # Commit updates 

        rate_used, rate_init = GITHUB_CLIENT.rate_limiting
        gracetime = (GITHUB_CLIENT.rate_limiting_resettime-math.floor(time.time())) / 1000
        if gracetime > 0:
            print(f'{done_bar}{todo_bar} Rate critical ({rate_used}/{rate_init}), gracetime={gracetime} ({pct}%) '   , end='\r', flush=True)
            time.sleep(gracetime*3)

        if index != 0 and index % 10 == 0:
            print(f'{done_bar}{todo_bar} Sleep for 60s ({pct}%)                             ', end='\r', flush=True)
            time.sleep(60)
    
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
