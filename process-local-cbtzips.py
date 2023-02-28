#!/bin/env python

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

parser.add_argument("--cbtfiles", type=str,
                    default=f'.cbtfiles',
                    help=f"""Full path to the cbtfiles. 
This is all up-to-date zip files (if you ran --update).
Defaults to '~/.cbtfiles""")

parser.add_argument("--repos", type=str,
                    default=f'.cbtrepos',
                    help=f"""Full path to local repos folder. 
Defaults to ~/stage""")

parser.add_argument("--only", type=int,
                    default=0,
                    help=f"""Only process this CBT Tape""")

parser.add_argument("--pickle", type=str,
                    default=f'.cbt.pkl',
                    help=f"""Panda pickle file with parsed UPDATESTOC.txt information. Will be updated during this run. Defaults to .cbt.pkl""")

parser.add_argument("--clean",
                    action="store_true",
                    help=f"Cleans everything except stage folder.")

parser.add_argument("--force",
                    action="store_true",
                    help=f"Ingore filesizes, always download everything.")




args = parser.parse_args()


repos    = args.repos 
stage    = args.stage
only     = args.only
cbtfiles = args.cbtfiles
pickle   = args.pickle

    




# No store user/pass in scripts lol
# Get token via https://github.com/settings/tokens/
# testing with GITHUB_TOKEN="6"
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

GITHUB_TOKEN = config['github']['token']
GITHUB_USER_OR_ORG = config['github']['name']



from github import Github
github = Github(GITHUB_TOKEN)
me = github.get_organization(GITHUB_USER_OR_ORG)

try:
    GITHUB_USER = me.login
except:
    print("Bad token... bad")
    exit(4)

print(f"Token has logged on {github.get_user(GITHUB_USER).name} acting in github user/org github.com/{GITHUB_USER}")






if args.clean:
    """Removes all local and remote repositories. 
    Basically a full hard reset
    """
    os.system(f'rm -rf {cbtfiles}/*')
    os.system(f'rm -rf {repos}/*')
    for repo in me.get_repos():
        if repo.name[:3] == "CBT":
            print(f"Deleting {repo.name}", end=' ', flush=True)
            r = me.get_repo(repo.name)
            r.delete()
            print("🗑️", flush=True)

if not os.path.isdir(repos):
    os.mkdir(repos)

if not os.path.isdir(cbtfiles):
    os.mkdir(cbtfiles)





cbt = pd.read_pickle(pickle)
print(f'Loaded our dataframe, {len(cbt)} CBT-files ready to be processed')

def ispfstatsfromxmi(mbr, xmijson):
    crdat = xmijson['createdate'].split('T')[0][2:].replace('-','/')
    mddat = xmijson['modifydate'].split('T')[0][2:].replace('-','/')
    mdtime = xmijson['modifydate'].split('T')[1].split('.')[0]
    v     = int(xmijson['version'].split('.')[0])
    m     = int(xmijson['version'].split('.')[1])
    olines = int(xmijson['lines'])
    nlines = int(xmijson['newlines'])
    ispfline =  f"{mbr:<8} {crdat} {mddat} {v:>2} {m:>2} {mdtime} {olines:>5} {nlines:>5} {0:>5} {xmijson['user']}"
    return ispfline 

def attribfile(path):
    lines=f"""*                git-encoding=iso8859-1 zos-working-tree-encoding=ibm-1047 
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
    with open(f'{path}/.gitattributes', 'w') as a:
        a.writelines(lines)








import filecmp
import shutil

toprocess= []


flist = os.listdir(stage)

for i,filename in enumerate(flist):
    # I've we selected a CBT, oly do that one
    if only > 0:
        cbtn = f"CBT{only:002d}.zip" 
        if filename != cbtn: 
            continue
    # otherwise process this src CBT file..
    src = os.path.join(stage, filename)
    # checking if it is a file
    if os.path.isfile(src):
        dst = os.path.join(cbtfiles, filename)
        # copy to destintation if new or different
        if not os.path.exists(dst) or not filecmp.cmp(src, dst):
            shutil.copyfile(src, dst)
            # and add to our list of things to do :)
            toprocess.append(dst)
    else:
        print(f"Sorry, {src} not found. This really shouldn't happen.")

print(f"Need to process {len(toprocess)} CBT zips")



for i,z in enumerate(toprocess):
    cbtnum = z.split('/CBT')[1].split('.')[0]
    pct = math.floor((i/len(toprocess))*100) 
    done = math.floor((pct/100)*40)
    todo = 40 - done
    done = done * "✅" 
    todo = todo * "🟩"
    print(f'{done}{todo} {z} ({pct}%)', end='\r', flush=True)
    dotzigispf = {} # list of lines per PDS in the repo for .zigi/{PDS} files (ISPFSTATS)
    dotzigidsn = [] # list of lines for .zigi/dsn file
    loglines = []
    loglines.append(f'{datetime.datetime.now()} - Initialized conversion of CBT{cbtnum}' + '\n')
    with zipfile.ZipFile(z, 'r') as zip_ref:
        info =  zip_ref.infolist()
        if len(info) > 1:
            print(F"More than onze file in zip??? {z} => {info}")
        else:
            xmifile = f"/tmp/{info[0].filename}"
            zip_ref.extractall('/tmp')
            try:
                contents = xmi.list_all(xmifile)
            except:
                print(f"** ALERT ** {z} has no XMI??")
                continue
            pdsfile  = contents[0].split('(')[0]
            
            try:
                
                xmi_obj = xmi.open_file(xmifile,quiet=True)
            except:
                print(f"De-XMI error for {z}")
                continue
            xmijson = xmi_obj.get_json()
            new_repo = False
            repopath = repos + "/" + z.split('/')[1].split('.')[0]
            reponame = z.split('/')[1].split('.')[0]
            if not os.path.isdir(repopath):
                os.mkdir(repopath)
                new_repo = True
            xmi_obj.set_output_folder('/tmp')
            xmi_obj.set_quiet(True)
            try:
                xmi_obj.extract_all()
            except:
                print(f"De-XMI error for {z}")
                continue
            # We have the dexmied file in /tmp/something lets see what's there
            xmijson = json.loads(xmi_obj.get_json())
            xmi_file = list(xmijson['file'].keys())[0]
            xmi_has  = xmijson['file'][xmi_file]['COPYR1']['type']
            if xmi_has != "PDS":
                print(f"** ALERT? ** No PDS in {xmifile}")
                continue
            loglines.append(f'{datetime.datetime.now()} - Received {pdsfile} from {info[0].filename} ' + '\n')
            # Create our target PDS-folder in repopath
            mainpds = xmi_file.split('.')[-1]
            pdsfolder = repopath + "/" + mainpds # last qualifier should do
            os.mkdir(pdsfolder)
            # add line to the .zigi/dsn file
            dotzigidsn.append(f'{mainpds} PO FB 80 32720' + "\n")
            # create placeholder for ISPFSTATS
            dotzigispf[mainpds] = []
            for member in xmijson['file'][xmi_file]['members']:
                member_info = xmijson['file'][xmi_file]['members'][member]
                mimetype = member_info['mimetype']
                ext = member_info['extension']
                newmember = member.split('.')[0]                
                if mimetype == 'text/plain':
                    # we just copy this over inside our repopath
                    # usssafe anyone :)
                    os.system(f"cp '/tmp/{pdsfile}/{member}{ext}' '{pdsfolder}/{newmember}'")
                    # add the ISPFSTATS 
                    if not member_info['ispf']:
                        member_info['ispf'] = {'version': '01.00', 'flags': 0, 'createdate': '1976-06-12T00:00:00.000000', 'modifydate': '1976-06-12T22:18:12.000000', 'lines': 0, 'newlines': 0, 'modlines': 0, 'user': 'CBT2GIT'}
                    dotzigispf[mainpds].append(ispfstatsfromxmi(member, member_info['ispf'])+"\n")
                    loglines.append(f'{datetime.datetime.now()} - Found {member}{member_info["extension"]} ({member_info["mimetype"]}) in {pdsfile} moved to {mainpds}/{member}' + '\n')
                elif mimetype == 'application/xmit':
                    # we should assume this has no more nested xmi's and de-xmit it outside of the pds as a new pds
                    # dexmi, move
                    nested_content = xmi.list_all(f'/tmp/{pdsfile}/{member}{ext}')
                    nested_pdsfile  = nested_content[0].split('(')[0] # last two qualifier should do all... 
                    nested_xmi_obj = xmi.open_file(f'/tmp/{pdsfile}/{member}{ext}',quiet=True)
                    nested_xmi_obj.set_output_folder(repopath)
                    nested_xmi_obj.extract_all()
                    # skip all but last 2 qualifiers
                    print('',nested_pdsfile)
                    newnestpds = '.'.join(nested_pdsfile.split('.')[-2:])
                    print('',newnestpds)
                    loglines.append(f'{datetime.datetime.now()} - Found {member}{member_info["extension"]} ({member_info["mimetype"]}) in {pdsfile}'+ '\n')
                    # move to correct spot
                    print('',f'break here>?   {repopath}/{nested_pdsfile}/ +newnestpds = {newnestpds}')
                    os.system(f'mv {repopath}/{nested_pdsfile} {repopath}/{newnestpds}')
                    newxmi = repopath + "/" + member + ext
                    os.system(f'cp /tmp/{pdsfile}/{member}{ext} {newxmi}')
                    # add ispfstats
                    dotzigispf[newnestpds] = []
                    nested_xmijson = json.loads(nested_xmi_obj.get_json())
                    nested_xmi_file = list(nested_xmijson['file'].keys())[0]
                    print('',f'break here>?   {repopath}/{nested_pdsfile}/ +newnestpds = {newnestpds}')
                    if 'COPYR1' in nested_xmijson:
                        # FOR A PDS...
                        for m in nested_xmijson['file'][nested_xmi_file]['members']:
                            dotzigispf[newnestpds].append(ispfstatsfromxmi(m, nested_xmijson['file'][nested_xmi_file]['members'][m]['ispf'])+"\n")
                            loglines.append(f'{datetime.datetime.now()} - Found {m}{nested_xmijson["file"][nested_xmi_file]["members"][m]["extension"]} ({nested_xmijson["file"][nested_xmi_file]["members"][m]["mimetype"]}) in {member}{member_info["extension"]}, moved to {newnestpds}/{m}' + '\n')
                        with open(f"{pdsfolder}/{newmember}",'w') as xmipds:
                            xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                            xmipds.write(f"# |{'CBT2GIT DETECTED THIS WAS AN XMI FILE'.center(55)}" +  "|\n")
                            xmipds.write(f"# |{'AND HAS DE-XMIED IT TO'.center(55)}" + "|\n")
                            newloc = reponame + "/" + newnestpds
                            xmipds.write(f"# |{newloc.center(55)}" + "|\n") 
                            xmipds.write(f"# |{'THE ORIGINAL XMI HAS MOVED TO'.center(55)}" + "|\n")
                            xmipds.write(f"# |{(reponame + '/' + member + ext).center(55)}" + "|\n") 
                            xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                        # add this member to the .zigi/<PDS> ispfstats, but change them...
                        dd = datetime.datetime.now().strftime("%y/%m/%d")
                        mm = datetime.datetime.now().strftime('%H:%M:%S')
                        newispf = f"{member:<8} {dd} {dd} {1:>2} {0:>2} {mm} {7:>5} {7:>5} {0:>5} CBT2GIT"
                        dotzigispf[mainpds].append(newispf + "\n")
                        # add line to .zigi/dsn
                        dotzigidsn.append(f'{newnestpds} PO FB 80 32720' + "\n")
                    else:
                        # Figure out .zigi/dsn from xmi ifo?
                        lrecl = nested_xmijson['INMR02']['1']['INMLRECL']
                        dsorg = nested_xmijson['INMR02']['1']['INMDSORG']
                        recfm = nested_xmijson['INMR02']['1']['INMRECFM']
                        blksz = nested_xmijson['INMR02']['1']['INMBLKSZ']
                        if recfm != "U":
                            ok = f'{repopath}/{newnestpds}'
                            loglines.append(f'{datetime.datetime.now()} - Extracted to {ok}' + '\n')
                            with open(f"{pdsfolder}/{newmember}",'w') as xmipds:
                                xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                                xmipds.write(f"# |{'CBT2GIT DETECTED THIS WAS AN XMI FILE'.center(55)}" +  "|\n")
                                xmipds.write(f"# |{'AND HAS RECEIVED IT TO'.center(55)}" + "|\n")
                                newloc = reponame + "/" + newnestpds
                                xmipds.write(f"# |{newloc.center(55)}" + "|\n") 
                                xmipds.write(f"# |{'THE ORIGINAL XMI HAS MOVED TO'.center(55)}" + "|\n")
                                xmipds.write(f"# |{(reponame + '/' + member + ext).center(55)}" + "|\n") 
                                xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                            dd = datetime.datetime.now().strftime("%y/%m/%d")
                            mm = datetime.datetime.now().strftime('%H:%M:%S')
                            newispf = f"{member:<8} {dd} {dd} {1:>2} {0:>2} {mm} {7:>5} {7:>5} {0:>5} CBT2GIT"
                            dotzigispf[mainpds].append(newispf + "\n")
                            dotzigidsn.append(f'{ok.split("/")[-1]} {dsorg} {recfm} {lrecl} {blksz}' + "\n")
                        else:
                            # RECFM = U.... hmmm
                            os.system(f"rm -rf {repopath}/{newnestpds}*")  # dunno why I can't find where I copy it in the beginning, but it has to go
                            os.system(f"cp '/tmp/{pdsfile}/{member}{ext}' '{repopath}/{member}{member_info['extension']}'") # as replaced with the XMI file
                            del dotzigispf[newnestpds] # no ispf stats, as it's  not a PDS :)
                            os.system(f"rm {repopath}/.zigi/{newnestpds}") # get rid of earlier generated ispfstats too
                            with open(f"{pdsfolder}/{newmember}",'w') as xmipds:
                                xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                                xmipds.write(f"# |{'CBT2GIT DETECTED THIS WAS AN XMI FILE'.center(55)}" +  "|\n")
                                xmipds.write(f"# |{'IT CONTAINED RECFM=U DATA'.center(55)}" + "|\n")
                                newloc = reponame + "/" + newnestpds
                                xmipds.write(f"# |{'---'.center(55)}" + "|\n") 
                                xmipds.write(f"# |{'THE ORIGINAL XMI HAS MOVED TO'.center(55)}" + "|\n")
                                xmipds.write(f"# |{(reponame + '/' + member + ext).center(55)}" + "|\n") 
                                xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                            loglines.append(f'{datetime.datetime.now()} - De-XMI showed RECFM=U, stored XMIT file as {member}{ext}' + '\n')
                            dd = datetime.datetime.now().strftime("%y/%m/%d")
                            mm = datetime.datetime.now().strftime('%H:%M:%S')
                            newispf = f"{member:<8} {dd} {dd} {1:>2} {0:>2} {mm} {7:>5} {7:>5} {0:>5} CBT2GIT"
                            dotzigispf[mainpds].append(newispf + "\n")


                    


                else:
                    # most likely a binary, move outside of pds in repo as bin file with extension as provided from xmilib
                    loglines.append(f'{datetime.datetime.now()} - Skipped de-xmi-ing {member} as it contained {mimetype}'+ '\n')
                    os.system(f"cp '/tmp/{pdsfile}/{member}{ext}' '{repopath}/{member}.xmi'") # as replaced with the XMI file
                    with open(f"{pdsfolder}/{newmember}",'w') as xmipds:
                                xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                                xmipds.write(f"# |{'CBT2GIT DETECTED THIS WAS AN XMI CONTAINING'.center(55)}" +  "|\n")
                                xmipds.write(f"# |{'AN UNSUPPORTED MIME-TYPE'.center(55)}" + "|\n")
                                xmipds.write(f"# |{mimetype.center(55)}" + "|\n") 
                                xmipds.write(f"# |{'XMI data STORED AS'.center(55)}" + "|\n")
                                place = f'{reponame}/{member}.xmi'
                                xmipds.write(f"# |{place.center(55)}" + "|\n") 
                                xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                    # add gitattribs...

            # Also get the @FILEnnn into README
            files  = glob.glob(repopath + f"/*{mainpds}/@FIL*")
            # sanity check. There should be only one match
            if len(files) != 1:
                print('Something wrong. More or less readme files than expected?. List follows')
                print(files)
                cccc = 'echo "NO @FILE in PDS???"'
            else:
                cccc = f'cat {files[0]}'

            # Now this is ugly as MD... so do some magix with it
            nl = "\n"
            os.system(f'echo "~~~~~~~~~~~~~~~~{nl}" > {repopath}/README.md')
            os.system(f'{cccc} >> {repopath}/README.md')
            os.system(f'echo "~~~~~~~~~~~~~~~~{nl}" >> {repopath}/README.md')

            # all parsed, write zigi files to repo
            os.system(f'mkdir {repopath}/.zigi')
            with open(f'{repopath}/.zigi/dsn', 'w') as dsnfile:
                dsnfile.writelines(dotzigidsn)
            for p in dotzigispf:
                with open(f'{repopath}/.zigi/{p}', 'w') as ispffile:
                    ispffile.writelines(dotzigispf[p])

            with open(f'{repopath}/cbt2git.log', 'w') as dalog:
                dalog.writelines(loglines)
            
            if new_repo:
                # do_inital_add_commit_if_first :)
                # cbt data via cbt.loc[cbt.cbtnum==cbtnum]['comment'].values[0]
                os.system(f'cd {repopath} && git init')
                os.system(f'cd {repopath} && git branch -M main')
                attribfile(repopath)
                msg = f"{z.split('/')[1].split('.')[0]} : Initial commit"
                os.system(f'cd {repopath} && git add . ')
                os.system(f'cd {repopath} && git commit -m "{msg}"')
            else:
                # we're newer, so update the things..
                os.system(f'cd {repopath} && git add .')
                os.system(f'cd {repopath} && git commit -m "Updates from cbttape.org"')
            
            create_repo = False

            try:
                repo = github.get_repo(f'{GITHUB_USER}/{reponame}')
            except:
                create_repo = True
            
            print(f"Check for {reponame} /  ==> have to create: {create_repo}")
            if create_repo:
                # create the repo at the github site
                me.create_repo(
                    reponame,
                    private=True,
                    description=cbt.loc[cbt.cbtnum==cbtnum]['comment'].values[0]
                )
                # sleep a bit....... github seems to lag on creation?
                
                time.sleep(6) # just to be sure :)
                repourl = f'git@github.com:cbttape/{reponame}.git' 
                repourl = me.get_repo(reponame).ssh_url
                print(f'Created {reponame} at {repourl} adding origin')
                os.system(f'cd {repopath} && git remote add origin {repourl}')
                print(f'cd {repopath} && git push -v -u origin main')
                os.system(f'cd {repopath} && git push -u origin main')
            
            # repo was there, we can justpush our updates
            os.system(f'cd {repopath} && git push origin main')
            rate_used, rate_init = github.rate_limiting
            print(f"Rate critical? ({rate_used}/{rate_init})")
            gracetime = (github.rate_limiting_resettime-math.floor(time.time())) / 1000
            if gracetime > 0:
                print(f'Sleep for grace time...{gracetime} secs')
                time.sleep(gracetime)
            while rate_used < 500:
                # Just to be on the safe side (https://docs.github.com/en/rest/overview/resources-in-the-rest-api#secondary-rate-limits)
                # we might wanna add some more sleeps....
                print(f"Sleep 60 to keep the rate-limit-things happy")
                time.sleep(60)

