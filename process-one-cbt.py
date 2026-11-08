#!/usr/bin/env python
"""Just the one CBT file: gets it from cbttape.org and creates or updates its repo. Local and on GitHub.

Usage:
    ./process-one-cbt.py 314
    ./process-one-cbt.py 314 --noremote
"""
import argparse
import os
import shutil
import sys

from github import GithubException

import cbtrepo

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, description="""Download one CBT file from cbttape.org and create or update its zigi managed repo.
If there's no local repo yet but there is one on GitHub, that one gets cloned first. No need to run any of the other scripts for this.""")

parser.add_argument("cbt", type=int, help="Number of the CBT file. 314 for CBT314.zip")

parser.add_argument("--stage", type=str,
                    default=f'{os.getcwd()}/stage',
                    help=f"""Full path to stage-folder. The zip file gets downloaded to here.
Defaults to {os.getcwd()}/stage""")

parser.add_argument("--cbtfiles", type=str,
                    default='.cbtfiles',
                    help="""Full path to the cbtfiles.
Defaults to .cbtfiles""")

parser.add_argument("--repos", type=str,
                    default='.cbtrepos',
                    help="""Full path to local repos folder.
Defaults to .cbtrepos""")

parser.add_argument("--noremote",
                    action="store_true",
                    help="Do everything, except remote GitHub actions.")

args = parser.parse_args()

reponame = f'CBT{args.cbt:03d}'

try:
    me = remote = None
    if not args.noremote:
        github, me = cbtrepo.connect()
        remote = cbtrepo.get_remote(me, reponame)

    print(f'Getting {reponame} from {cbtrepo.FTPSERVER}')
    zipfile, comment = cbtrepo.download_cbt(args.cbt, args.stage)
    print(f'{reponame}: {comment}')

    # So the process-everything run knows we've had this one
    os.makedirs(args.cbtfiles, exist_ok=True)
    shutil.copyfile(zipfile, os.path.join(args.cbtfiles, os.path.basename(zipfile)))

    repopath, changed, loglines = cbtrepo.update_repo(zipfile, args.repos, remote)
    for line in loglines:
        if 'ALERT' in line:
            print(line, end='')
    print(f'{repopath}: ' + ('committed the changes' if changed else 'nothing new'))

    if not args.noremote:
        remote = cbtrepo.push(me, repopath, reponame, remote, comment)
        print(f'Pushed to {remote.html_url}')
except (cbtrepo.CbtError, GithubException) as e:
    print(f'** {reponame} ** {e}', file=sys.stderr)
    sys.exit(8)
