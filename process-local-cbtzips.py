#!/usr/bin/env python

import pandas as pd
import os
import time
import datetime

import math

import argparse 

import filecmp
import shutil

import github as pygithub

import cbtrepo


parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, description="""Create, or update a GitHub profile with data from CBTTape.org.

    1. Step One: Extract all CBTTapes and create GitHub Repo if needed.
       Process all files that were downloaded from cbttape.org. All the zipfiles stored in the path you specify with --stage. are processed. They are copied to the --cbtrepos folder if
they're new. If they're the same as  what's already present there, nothing happens :) Otherwise, they're new a repote repo
at GITHUB_NAME_OR_ORG from config.yaml will be created.
Zip files are expaned to their containing PDS/SEQ. This PDS will be scanned, nested XMIT files will be expanded into the repo too.
Things that can't be rebuilt from flat files (RECFM=U, binary sequential) are stored as NAME.XMI in the root of the repo.""")

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
                    help=f"Process the zip file even if it's the same as the last time. Regenerates the repo.")

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

fulllog = []


if not noremote:
    try:
        github, me = cbtrepo.connect()
        GITHUB_USER = me.login
    except (cbtrepo.CbtError, pygithub.GithubException) as e:
        print(f"Bad token... bad: {e}")
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
        print(f"{me.get_repos().totalCount} Repos ...")
        while me.get_repos().totalCount > 1:
            # Let's leave the discuss repo but remove all the rest...
            for repo in me.get_repos():
                if repo.name[:3] == "CBT":
                    rate_used, rate_init = github.rate_limiting
                    gracetime = (github.rate_limiting_resettime-math.floor(time.time())) / 1000
                    print(f"Deleting {repo.name:8} (gracetime={gracetime}, ratelimits={rate_used}/{rate_init})", end=' ', flush=True)
                    r = me.get_repo(repo.name)
                    #time.sleep(gracetime/24) 
                    try:
                        r.delete()
                    except:
                        print('Error????')
                    print(f" ", end='          \r', flush=True)

print('')


if not os.path.isdir(repos):
    os.mkdir(repos)

if not os.path.isdir(cbtfiles):
    os.mkdir(cbtfiles)


cbt = pd.read_pickle(pickle)
print(f'Loaded our dataframe, {len(cbt)} CBT-files ready to be processed')

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
        if args.force or not os.path.exists(dst) or not filecmp.cmp(src, dst):
            shutil.copyfile(src, dst)
            # and add to our list of things to do :)
            toprocess.append(dst)
    else:
        print(f"Sorry, {src} not found. This really shouldn't happen.")

print(f"Need to process {len(toprocess)} CBT zips")
# Let's sort them :)
toprocess = sorted(toprocess)


def writefulllog():
    logfile = f'cbt2git-log-{datetime.datetime.now().strftime("%Y-%j-%H-%M-%S")}'
    with open(logfile, 'w') as biglog:
        biglog.writelines(sorted(fulllog))


for i,z in enumerate(toprocess):
    reponame = os.path.basename(z).split('.')[0]
    cbtnum = reponame[3:]
    pct = math.floor((i/len(toprocess))*100) 
    done = math.floor((pct/100)*40)
    todo = 40 - done
    done = done * "✅" 
    todo = todo * "🟩"
    print(f'{done}{todo} {z} ({pct}%)', end='\r', flush=True)

    try:
        remote = None if noremote else cbtrepo.get_remote(me, reponame)
        repopath, changed, loglines = cbtrepo.update_repo(z, repos, remote)
    except cbtrepo.CbtError as e:
        fulllog.append(f"{datetime.datetime.now()} - ** ALERT CBT{cbtnum} ** {e}" + "\n")
        continue

    # add log from this conersion to main full log
    fulllog += loglines

    if not noremote:
        try:
            cbtrepo.push(me, repopath, reponame, remote, cbt.loc[cbt.cbtnum==cbtnum]['comment'].values[0])
        except cbtrepo.CbtError as e:
            fulllog.append(f"{datetime.datetime.now()} - ** ALERT CBT{cbtnum} ** {e}" + "\n")
            continue
        except pygithub.GithubException:
            print("** SOMETHING BAD WHEN CREATING REPO (RATE LIMITS?)")
            writefulllog()
            # then sleep for long, so I get to keep this running unattended!
            time.sleep(600)
            continue

        rate_used, rate_init = github.rate_limiting
        gracetime = (github.rate_limiting_resettime-math.floor(time.time())) / 1000
        print(f"Rate critical? ({rate_used}/{rate_init}), gracetime={gracetime}")
        if gracetime > 0:
            print(f'Sleep for twice the grace time...{gracetime*2} secs')
            time.sleep(gracetime*2)
        
        if i % 10 == 0 and only == 0:
            # every 10, and not if we running just one...
            # print("Sleep another 30secs every 10 repos...")
            print("sleep for 30...")
            time.sleep(30)

# sort on datetime (as we have extra messages in it from fulllog.append warnings that don't show in repo)
writefulllog()

# nice closing status-progress-bar-thunny
done = 40 * "✅" 
pct = 100
z=''
print(f'{done} {z} ({pct}%)', flush=True)
