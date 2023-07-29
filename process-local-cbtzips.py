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

import pprint


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
Defaults to {os.getcwd()}/.cbtfiles""")

parser.add_argument("--repos", type=str,
                    default=f'.cbtrepos',
                    help=f"""Full path to local repos folder. 
Defaults to {os.getcwd()}/.cbtrepos""")

parser.add_argument("--only", type=int,
                    default=0,
                    help=f"""Only process this CBT Tape""")

parser.add_argument("--pickle", type=str,
                    default=f'.cbt.pkl',
                    help=f"""Panda pickle file with parsed UPDATESTOC.txt information. Will be updated during this run. Defaults to .cbt.pkl""")

parser.add_argument("--clean",
                    action="store_true",
                    help=f"Cleans everything except stage folder. Does not take --only into account...")

parser.add_argument("--force",
                    action="store_true",
                    help=f"Ingore filesizes, always download everything.")

parser.add_argument("--noremote",
                    action="store_true",
                    help=f"Do everything, except remote GitHub actions. (doen't create or updates repos")




args = parser.parse_args()


repos    = args.repos 
stage    = args.stage
only     = args.only
cbtfiles = args.cbtfiles
pickle   = args.pickle
noremote = args.noremote

docmimetypes = ['application/msword', 'application/epub+zip', 'application/pdf', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document','application/vnd.oasis.opendocument.text','application/vnd.oasis.opendocument.text','application/vnd.ms-powerpoint','application/vnd.ms-excel','application/vnd.openxmlformats-officedocument.presentationml.presentation']

fulllog = []




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


if not noremote:
    try:
        GITHUB_USER = me.login
    except:
        print("Bad token... bad")
        exit(4)

    print(f"Token has logged on {github.get_user(GITHUB_USER).name} acting in github user/org github.com/{GITHUB_USER}")
else:
    print("Running locally only, no updates to GitHub")






if args.clean:
    """Removes all local and remote repositories. 
    Basically a full hard reset
    """
    os.system(f'rm -rf {cbtfiles}/*')
    os.system(f'rm -rf {repos}/*')
    if not noremote:
        for repo in me.get_repos():
            if repo.name[:3] == "CBT":
                rate_used, rate_init = github.rate_limiting
                gracetime = (github.rate_limiting_resettime-math.floor(time.time())) / 1000
                print(f"Deleting {repo.name:8} (gracetime={gracetime}, ratelimits={rate_used}/{rate_init})", end=' ', flush=True)
                r = me.get_repo(repo.name)
                time.sleep(gracetime/6) 
                r.delete()
                print(f" ", end='          \r', flush=True)

print('')


if not os.path.isdir(repos):
    os.mkdir(repos)

if not os.path.isdir(cbtfiles):
    os.mkdir(cbtfiles)





cbt = pd.read_pickle(pickle)
print(f'Loaded our dataframe, {len(cbt)} CBT-files ready to be processed')

def ispfstatsfromxmi(mbr, xmijson):
    if xmijson:
        crdat = xmijson['createdate'].split('T')[0][2:].replace('-','/')
        if xmijson['modifydate'] != '':
            mddat = xmijson['modifydate'].split('T')[0][2:].replace('-','/')
            mdtime = xmijson['modifydate'].split('T')[1].split('.')[0]
        else:
            # some pds-es have empty modifydate, we leave blank in ISPF stats 
            mddat  = '        '
            mdtime = '        '
        v     = int(xmijson['version'].split('.')[0])
        m     = int(xmijson['version'].split('.')[1])
        olines = int(xmijson['lines'])
        nlines = int(xmijson['newlines'])
        user   = xmijson['user']
    else:
        # weird that we need this? Probably calling ispfstatsfromxmi too eagerly?
        fulllog.append(f"{datetime.datetime.now()} - ** ALERT CBT{cbtnum} ** {mbr} no ispfstats from xmi?" + "\n")
        return f"{mbr:<8}"
    ispfline =  f"{mbr:<8} {crdat} {mddat} {v:>2} {m:>2} {mdtime} {olines:>5} {nlines:>5} {0:>5} {user}"
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

def getxmidata(xmifile):
    xmijson = json.loads(xmi.open_file(xmifile,quiet=True).get_json())
    try:
        dsnam = xmijson['INMR02']['1']['INMDSNAM']
    except:
        try:
            dsnam = xmijson['INMR02']['2']['INMDSNAM']
        except:
            return False, False, False, False, False
    dsorg = xmijson['INMR02']['1']['INMDSORG']
    lrecl = xmijson['INMR02']['1']['INMLRECL']
    if dsorg != "PS":
        mbrs = [x for x in xmijson['file'][dsnam]['members']]
    else:
        mbrs = []
    try:
        recfm  = xmijson['file'][dsnam]['COPYR1']['DS1RECFM']
    except:
        recfm = 'n.a.'
    members = {}
    for m in mbrs:
        if 'mimetype' in xmijson['file'][dsnam]['members'][m]:
            mt = xmijson['file'][dsnam]['members'][m]['mimetype']
        else:
            mt = 'application/octet-stream' # force it :)
        if 'datatype' in xmijson['file'][dsnam]['members'][m]:
            dt = xmijson['file'][dsnam]['members'][m]['datatype']
        else:
            dt = 'binary'
        if 'extension' in xmijson['file'][dsnam]['members'][m]:
            ext = xmijson['file'][dsnam]['members'][m]['extension']
        else:
            ext = '.bin'
        members[m] = {'mimetype': mt, 'datatype':dt,'ext':ext}
    return dsnam, dsorg, lrecl, recfm, members






import filecmp
import shutil

toprocess= []


flist = os.listdir(stage)

for i,filename in enumerate(flist):
    # I've we selected a CBT, oly do that one
    if only > 0:
        cbtn = f"CBT{only:003d}.zip" 
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
# Let's sort them :)
toprocess = sorted(toprocess)


def replace_pds(reponame, pdsfolder, member, ext, newmember, newnestpds):
    with open(f"{pdsfolder}/{newmember}",'w') as xmipds:
        xmipds.write(f"# +-------------------------------------------------------+" + "\n")
        xmipds.write(f"# |{'CBT2GIT DETECTED THIS WAS AN XMI FILE'.center(55)}" +  "|\n")
        xmipds.write(f"# |{'AND HAS DE-XMIED IT TO'.center(55)}" + "|\n")
        newloc = reponame + "/" + newnestpds
        xmipds.write(f"# |{newloc.center(55)}" + "|\n") 
        xmipds.write(f"# |{'THE ORIGINAL XMI HAS MOVED TO'.center(55)}" + "|\n")
        xmipds.write(f"# |{(reponame + '/' + member + ext).center(55)}" + "|\n") 
        xmipds.write(f"# +-------------------------------------------------------+" + "\n")

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
                fulllog.append(f"{datetime.datetime.now()} - ** ALERT CBT{cbtnum} **  {z} is zipped version of {xmifile} but that's no XMI??" + "\n")
                continue

            if contents == []:
                # FILE062 has this too ...
                fulllog.append(f"{datetime.datetime.now()} - ** ALERT CBT{cbtnum} ** {z} unzipped to {xmifile} but that's not an XMI??" + "\n")
                continue

            pdsfile  = contents[0].split('(')[0]

            try:
                xmi_obj = xmi.open_file(xmifile,quiet=True)
            except:
                fulllog.append(f"{datetime.datetime.now()} - ** ALERT CBT{cbtnum} ** De-XMI error for {z}" + "\n")
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
                fulllog.append(f"{datetime.datetime.now()} - ** ALERT CBT{cbtnum} ** De-XMI error for {z}" + "\n")
                continue
            # We have the dexmied file in /tmp/something lets see what's there
            xmijson = json.loads(xmi_obj.get_json())
            xmi_file = list(xmijson['file'].keys())[0]
            xmi_has  = xmijson['file'][xmi_file]['COPYR1']['type']
            if xmi_has != "PDS":
                fulllog.append(f"{datetime.datetime.now()} - ** ALERT CBT{cbtnum} ** No PDS in {xmifile}" + "\n")
                continue
            loglines.append(f'{datetime.datetime.now()} - Received {pdsfile} from {info[0].filename} ' + '\n')
            # Create our target PDS-folder in repopath
            mainpds = xmi_file.split('.')[-1]
            pdsfolder = repopath + "/" + mainpds # last qualifier should do
            os.system(f"mkdir -p {pdsfolder}")
            # add line to the .zigi/dsn file
            dotzigidsn.append(f'{mainpds} PO FB 80 32720' + "\n")
            # create placeholder for ISPFSTATS
            dotzigispf[mainpds] = []
            for member in xmijson['file'][xmi_file]['members']:
                # Loop all members in the received PDS from 'main' XMI
                member_info = xmijson['file'][xmi_file]['members'][member]
                if 'mimetype' in member_info:
                    mimetype = member_info['mimetype']
                else:
                    mimetype = 'application/octet-stream'
                if 'extension' in member_info:
                    ext = member_info['extension']
                else:
                    ext = '.bin'
                newmember = member.split('.')[0]                
                if mimetype.split('/')[0] == 'text':
                    # we just copy this over inside our repopath
                    # usssafe vai the single quotes :)
                    os.system(f"cp '/tmp/{pdsfile}/{member}{ext}' '{pdsfolder}/{newmember}' > /dev/null 2>&1")
                    # add the ISPFSTATS 
                    if not member_info['ispf']:
                        member_info['ispf'] = {'version': '01.00', 'flags': 0, 'createdate': '1976-06-12T00:00:00.000000', 'modifydate': '1976-06-12T22:18:12.000000', 'lines': 0, 'newlines': 0, 'modlines': 0, 'user': 'CBT2GIT'}
                    dotzigispf[mainpds].append(ispfstatsfromxmi(member, member_info['ispf'])+"\n")
                    loglines.append(f'{datetime.datetime.now()} - Found {member}{member_info["extension"]} ({member_info["mimetype"]}) in {pdsfile}, moved to {mainpds}/{member}' + '\n')
                elif mimetype == 'application/xmit':
                    # we should assume this has no more nested xmi's and de-xmit it outside of the pds as a new pds
                    # dexmi, move
                    nested_content = xmi.list_all(f'/tmp/{pdsfile}/{member}{ext}')
                    nested_pdsfile  = nested_content[0].split('(')[0] # last two qualifier should do all... 
                    nested_xmi_obj = xmi.open_file(f'/tmp/{pdsfile}/{member}{ext}',quiet=True)
                    nested_xmi_obj.set_output_folder(repopath)
                    try:
                        nested_xmi_obj.extract_all()
                    except:
                         # for 982, the XMI is 'broken' ?
                        fulllog.append(f"{datetime.datetime.now()} - ** ALERT CBT{cbtnum} ** {pdsfile}/{member}{ext} no ispf data when de-xmi-ing" + "\n")
                        continue
                    # skip all but last 2 qualifiers
                    newnestpds = '.'.join(nested_pdsfile.split('.')[-2:])
                    loglines.append(f'{datetime.datetime.now()} - Found {member}{member_info["extension"]} ({member_info["mimetype"]}) in {pdsfile}'+ '\n')
                    # move to correct spot
                    res = os.system(f'mv {repopath}/{nested_pdsfile} {repopath}/{newnestpds} > /dev/null 2>&1')
                    if res != 0:
                        # when PS not PDS, there's a .txt so we just redo?
                        os.system(f'mv {repopath}/{nested_pdsfile}.txt {repopath}/{newnestpds} > /dev/null 2>&1')

                    newxmi = repopath + "/" + member + ext
                    # add nested XMI to root of repo
                    os.system(f'cp /tmp/{pdsfile}/{member}{ext} {newxmi} > /dev/null 2>&1')
                    # chop off all dem extensions :)
                    for f in glob.glob(f'{repopath}/{newnestpds}/*'):
                        path, file = os.path.split(f)
                        newfile = file.split('.')[0]  # breaks sortof if dots in membername..but that's impossible anyway :)
                        noext = path + '/' + newfile
                        # Deal with the dollars :)
                        f = f.replace('$','\$')
                        noext = noext.replace('$','\$')
                        os.system(f'mv {f} {noext} > /dev/null 2>&1')

                    # add ispfstats
                    dotzigispf[newnestpds] = []
                    nested_xmijson = json.loads(nested_xmi_obj.get_json())
                    nested_xmi_file = list(nested_xmijson['file'].keys())[0]
                    if not 'members' in nested_xmijson['file'][nested_xmi_file]:
                        # TODO Phil: If no members, make the json have empty list for it?
                        nested_xmijson['file'][nested_xmi_file]['members'] = []
                    for nested_member in nested_xmijson['file'][nested_xmi_file]['members']:  
                        nested_member_info = nested_xmijson['file'][nested_xmi_file]['members'][nested_member]
                        dotzigispf[newnestpds].append(ispfstatsfromxmi(nested_member, nested_member_info['ispf'])+"\n")
                        
                    if 'COPYR1' in nested_xmijson:
                        # FOR A PDS...
                        print("NESTED PDS IN XMI XMI???? NEVER HAPPENS...")
                        1/0
                        for m in nested_xmijson['file'][nested_xmi_file]['members']:
                            print(f"Calling ditzigispf for {m} {nested_xmijson['file'][nested_xmi_file]['members'][m]['ispf']}")
                            dotzigispf[newnestpds].append(ispfstatsfromxmi(m, nested_xmijson['file'][nested_xmi_file]['members'][m]['ispf'])+"\n")
                            loglines.append(f'{datetime.datetime.now()} - Found {m}{nested_xmijson["file"][nested_xmi_file]["members"][m]["extension"]} ({nested_xmijson["file"][nested_xmi_file]["members"][m]["mimetype"]}) in {member}{member_info["extension"]}, moved to {newnestpds}/{m}' + '\n')
                        replace_pds(reponame, pdsfolder, member, ext, newmember, newnestpds)
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
                            loglines.append(f'{datetime.datetime.now()}   - Received to {reponame}/{newnestpds}' + '\n')
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
                            os.system(f"rm -rf {repopath}/{newnestpds}* > /dev/null 2>&1")  # dunno why I can't find where I copy it in the beginning, but it has to go
                            os.system(f"cp '/tmp/{pdsfile}/{member}{ext}' '{repopath}/{member}{member_info['extension']}' > /dev/null 2>&1") # as replaced with the XMI file
                            del dotzigispf[newnestpds] # no ispf stats, as it's  not a PDS :)
                            os.system(f"rm {repopath}/.zigi/{newnestpds} > /dev/null 2>&1") # get rid of earlier generated ispfstats too
                            with open(f"{pdsfolder}/{newmember}",'w') as xmipds:
                                xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                                xmipds.write(f"# |{'CBT2GIT DETECTED THIS WAS AN XMI FILE'.center(55)}" +  "|\n")
                                xmipds.write(f"# |{'IT CONTAINED RECFM=U DATA'.center(55)}" + "|\n")
                                newloc = reponame + "/" + newnestpds
                                xmipds.write(f"# |{'---'.center(55)}" + "|\n") 
                                xmipds.write(f"# |{'THE ORIGINAL XMI HAS MOVED TO'.center(55)}" + "|\n")
                                xmipds.write(f"# |{(reponame + '/' + member + ext).center(55)}" + "|\n") 
                                xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                            loglines.append(f'{datetime.datetime.now()}   - Received RECFM=U data, stored XMIT file as {reponame}/{member}{ext}' + '\n')
                            dd = datetime.datetime.now().strftime("%y/%m/%d")
                            mm = datetime.datetime.now().strftime('%H:%M:%S')
                            newispf = f"{member:<8} {dd} {dd} {1:>2} {0:>2} {mm} {7:>5} {7:>5} {0:>5} CBT2GIT"
                            dotzigispf[mainpds].append(newispf + "\n")


                    


                elif mimetype in docmimetypes:
                    # create the docs folder and move there
                    target = f'{repopath}/docs'
                    os.system(f'mkdir -p {target}')
                    # extract xmi to target
                    os.system(f"cp '/tmp/{pdsfile}/{member}{ext}' '{target}/{member}{ext}' > /dev/null 2>&1")
                    loglines.append(f'{datetime.datetime.now()} - De-xmi-ed {member} to {reponame}/docs/{member}{ext}'+ '\n')
                    with open(f"{pdsfolder}/{newmember}",'w') as xmipds:
                                xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                                xmipds.write(f"# |{'CBT2GIT DETECTED THIS WAS AN XMI CONTAINING'.center(55)}" +  "|\n")
                                xmipds.write(f"# |{'AN DOCUMENT MIME-TYPE'.center(55)}" + "|\n")
                                xmipds.write(f"# |{mimetype.center(55)}" + "|\n") 
                                xmipds.write(f"# |{'De-XMI-ed data STORED AS'.center(55)}" + "|\n")
                                place = f'{reponame}/docs/{member}{ext}'
                                xmipds.write(f"# |{place.center(55)}" + "|\n") 
                                xmipds.write(f"# +-------------------------------------------------------+" + "\n")

                elif mimetype == 'application/zip':
                    target = f'{repopath}/{member}'
                    canunzip = True
                    loglines.append(f'{datetime.datetime.now()} - Found {member}{ext} ({mimetype}), trying to unzip'+ '\n')
                    try:
                        with zipfile.ZipFile(f'/tmp/{pdsfile}/{member}{ext}', 'r') as inner_zip:
                            loglines.append(f'{datetime.datetime.now()} - Found {member}{ext} ({mimetype}), extracting to {reponame}/{member}'+ '\n')
                        
                            try:
                                inner_zip.extractall(target)
                            except Exception as e:
                                # This happens in CBT432 : zipfile.BadZipFile: Bad CRC-32 for file 'VB40016.DLL'
                                # This happens in BBT990 : zipfile.BadZipFile: File is not a zip file (for DEVTIPS@.zip)
                                loglines.append(f'{datetime.datetime.now()}   - {e}'+ '\n')
                    except Exception as e:
                        loglines.append(f'{datetime.datetime.now()}   - {e}, kept as member'+ '\n')
                        canunzip = False
                 
                    if canunzip:
                        with open(f"{pdsfolder}/{newmember}",'w') as xmipds:
                            xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                            xmipds.write(f"# |{'CBT2GIT DETECTED THIS WAS AN XMI CONTAINING'.center(55)}" +  "|\n")
                            xmipds.write(f"# |{'AN DOCUMENT MIME-TYPE'.center(55)}" + "|\n")
                            xmipds.write(f"# |{mimetype.center(55)}" + "|\n") 
                            xmipds.write(f"# |{'RECEIVED AND UNZIPPED TO'.center(55)}" + "|\n")
                            place = f'{reponame}/{member}'
                            xmipds.write(f"# |{place.center(55)}" + "|\n") 
                            xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                    else:
                        # weird stuff (990) just keep da member
                        os.system(f"cp '/tmp/{pdsfile}/{member}{ext}' '{pdsfolder}/{newmember}' > /dev/null 2>&1")

                elif mimetype in ['application/java-archive', 'message/rfc822']:
                    target = f'{repopath}/{member}'
                    os.system(f'mkdir -p {target}')
                    os.system(f"cp '/tmp/{pdsfile}/{member}{ext}' '{target}/{member}{ext}' > /dev/null 2>&1")
                    loglines.append(f'{datetime.datetime.now()} - Found {member}{ext} ({mimetype}), moved to {reponame}/{member}{ext}'+ '\n')
                    with open(f"{pdsfolder}/{newmember}",'w') as xmipds:
                        xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                        xmipds.write(f"# |{'CBT2GIT DETECTED THIS WAS AN XMI CONTAINING'.center(55)}" +  "|\n")
                        xmipds.write(f"# |{'AN DOCUMENT MIME-TYPE'.center(55)}" + "|\n")
                        xmipds.write(f"# |{mimetype.center(55)}" + "|\n") 
                        xmipds.write(f"# |{'RECEIVED MOVED TO'.center(55)}" + "|\n")
                        place = f'{reponame}/{member}{ext}'
                        xmipds.write(f"# |{place.center(55)}" + "|\n") 
                        xmipds.write(f"# +-------------------------------------------------------+" + "\n")

                else:
                    loglines.append(f'{datetime.datetime.now()} - Found {member}{ext} containing {mimetype}, moved to {mainpds}/{member}'+ '\n')
                    res = os.system(f"cp '/tmp/{pdsfile}/{member}{ext}' '{pdsfolder}/{member}' > /dev/null 2>&1") # as replaced with the XMI file
                    # loglines.append(f'{datetime.datetime.now()} - Found {member}{ext} containing {mimetype}, moved to {reponame}/{member}{ext}'+ '\n')
                    # os.system(f"cp '/tmp/{pdsfile}/{member}{ext}' '{repopath}/{member}{ext}' > /dev/null 2>&1") # as replaced with the XMI file
                    # with open(f"{pdsfolder}/{newmember}",'w') as xmipds:
                    #             xmipds.write(f"# +-------------------------------------------------------+" + "\n")
                    #             xmipds.write(f"# |{'CBT2GIT DETECTED THIS WAS AN XMI CONTAINING'.center(55)}" +  "|\n")
                    #             xmipds.write(f"# |{'AN UNSUPPORTED MIME-TYPE'.center(55)}" + "|\n")
                    #             xmipds.write(f"# |{mimetype.center(55)}" + "|\n") 
                    #             xmipds.write(f"# |{'XMI data STORED AS'.center(55)}" + "|\n")
                    #             place = f'{reponame}/{member}.xmi'
                    #             xmipds.write(f"# |{place.center(55)}" + "|\n") 
                    #             xmipds.write(f"# +-------------------------------------------------------+" + "\n")


            # Also get the @FILEnnn into README
            files  = glob.glob(repopath + f"/*{mainpds}/@FIL*")
            # sanity check. There should be only one match
            if len(files) != 1:
                cccc = 'echo "No @FILE in PDS?"'
                loglines.append(f'{datetime.datetime.now()} - No @FILExxx or @FILxxxx detected, creating README.md without extra info'+ '\n')
            else:
                cccc = f'cat {files[0]}'

            # Now this is ugly as MD... so do some magix with it
            nl = "\n"
            os.system(f'echo "# {reponame}{nl}Converted to GitHub via [cbt2git](https://github.com/wizardofzos/cbt2git)" > {repopath}/README.md')
            os.system(f'echo "This is still a work in progress. GitHub repos will be deleted and created during this period..." >> {repopath}/README.md')
            os.system(f'echo "~~~~~~~~~~~~~~~~{nl}" >> {repopath}/README.md')
            os.system(f'{cccc} >> {repopath}/README.md')
            os.system(f'echo "~~~~~~~~~~~~~~~~{nl}" >> {repopath}/README.md')

            # all parsed, write zigi files to repo (replaces existing...and that's what we want)
            os.system(f'mkdir {repopath}/.zigi')
            with open(f'{repopath}/.zigi/dsn', 'w') as dsnfile:
                dsnfile.writelines(dotzigidsn)
            for p in dotzigispf:
                with open(f'{repopath}/.zigi/{p}', 'w') as ispffile:
                    ispffile.writelines(dotzigispf[p])

            # Do the thing with the pdf's docx et-al

            # Append to logfile if we have one, otherwise create it
            with open(f'{repopath}/cbt2git.log', 'a+') as dalog:
                dalog.writelines(loglines)
            
            if new_repo:
                # do_inital_add_commit_if_first :)
                # cbt data via cbt.loc[cbt.cbtnum==cbtnum]['comment'].values[0]
                os.system(f'cd {repopath} && git init --quiet')
                os.system(f'cd {repopath} && git branch -M main --quiet')
                attribfile(repopath)
                msg = f"{z.split('/')[1].split('.')[0]} : Initial commit"
                os.system(f'cd {repopath} && git add .')
                os.system(f'cd {repopath} && git commit -m "{msg}" --quiet')
            else:
                # we're newer, so update the things..
                os.system(f'cd {repopath} && git add . ')
                os.system(f'cd {repopath} && git commit -m "Updates from cbttape.org ({datetime.datetime.now().strftime("%Y-%m-%d")})" --quiet')
            
            create_repo = False
            
            if not noremote:
                try:
                    repo = github.get_repo(f'{GITHUB_USER}/{reponame}')
                except:
                    create_repo = True
                if create_repo:
                    # create the repo at the github site, check for breakage...
                    try:
                        me.create_repo(
                            reponame,
                            private=False,
                            description=cbt.loc[cbt.cbtnum==cbtnum]['comment'].values[0]
                        )
                    except:
                        print("** SOMETHING BAD WHEN CREATING REPO (RATE LIMITS?)")
                        logfile = f'cbt2git-log-{datetime.datetime.now().strftime("%Y-%j-%H-%M-%S")}'
                        with open(logfile, 'w') as biglog:
                            biglog.writelines(fulllog)
                        # then sleep for long, so I get to keep this running unattended!
                        time.sleep(1800)
                    # sleep a bit....... github seems to lag on creation?
                    time.sleep(10) # just to be sure :)
                    repourl = f'git@github.com:cbttape/{reponame}.git' 
                    repourl = me.get_repo(reponame).ssh_url                   
                    os.system(f'cd {repopath} && git remote add origin {repourl}')
                    os.system(f'cd {repopath} && git push -u origin main --quiet')

                # repo was there, we can justpush our updates
                os.system(f'cd {repopath} && git push origin main --quiet')
                rate_used, rate_init = github.rate_limiting
                # print(f"Rate critical? ({rate_used}/{rate_init})")
                gracetime = (github.rate_limiting_resettime-math.floor(time.time())) / 1000
                if gracetime > 0:
                    # print(f'Sleep for trice the grace time...{gracetime*3} secs')
                    time.sleep(gracetime*3)
                # and wait another minute (we're not in a rush :) )
                if i % 10 == 0 and only == 0:
                    # every 10, and not if we running just one...
                    # print("Sleep another minute every 10 repos...")
                    time.sleep(60)
    # add log from this conersion to main full log
    fulllog += loglines
    # cleanup /tmp stuff
    os.system(f'rm -rf /tmp/{pdsfile}')
    os.system(f'rm -rf /tmp/{xmifile}')

# sort on datetime (as we have extra messages in it from fulllog.append warnings that don't show in repo)
fulllog = sorted(fulllog)
logfile = f'cbt2git-log-{datetime.datetime.now().strftime("%Y-%j-%H-%M-%S")}'
with open(logfile, 'w') as biglog:
    biglog.writelines(fulllog)

# nice closing status-progress-bar-thunny
done = 40 * "✅" 
pct = 100
z=''
print(f'{done} {z} ({pct}%)', flush=True)
