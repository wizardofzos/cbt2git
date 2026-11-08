# cbt2git (Work In Progress!)

cbt2git is a set of Python tools/scripts to migrate CBT (Consolidated Software Library) tapes from cbttape.org to GitHub repositories, making them easier to access and use for a wider audience. 

## About CBTTAPE.ORG

CBTTAPE.ORG is a website that hosts a large collection of mainframe software, documentation, and related materials in the form of IBM mainframe CBT tapes. These tapes were originally created in the 1970s and 1980s as a way for IBM mainframe users to share and distribute software, and the collection has since grown to include thousands of tapes spanning multiple decades.

The CBT tapes contain a wealth of valuable information and resources, including operating systems, programming languages, utilities, and tools, many of which are no longer available from their original sources. The tapes also include documentation, tutorials, and examples that can help developers and researchers learn more about mainframe computing.

However, accessing and using the CBT tapes can be challenging, as they were originally designed to be used with IBM mainframes and require specialized tools and knowledge to extract and use the contents.

## Benefits of cbt2git

This is where cbt2git comes in - it provides a set of Python scripts that can automate the process of migrating CBT tapes from cbttape.org to GitHub repositories, making them easier to access and use for a wider audience.

By making the CBT tapes available on GitHub, they become more accessible to a wider audience, including researchers, developers, and students who may not have access to IBM mainframes or the knowledge required to use the tapes. This can help preserve a valuable part of computing history and provide resources for future generations to learn from and build upon.

Additionally, by creating "zigi managed" repositories on GitHub, the cbt2git scripts can help ensure that the contents of the repositories stay up to date and are properly maintained for future use.

## About ZIGI

ZIGI is an ISPF interface to Git that allows mainframe developers to work with Git repositories directly from the mainframe. By creating "zigi managed" repositories on GitHub using cbt2git, the contents of the repositories can be easily managed and updated by mainframe developers using ZIGI.

## Installing

You need git, Python 3.11 and libmagic (`brew install libmagic` on a Mac, `apt install libmagic1` on Debian or Ubuntu).

    python3.11 -m venv virtualenv
    source virtualenv/bin/activate
    pip install -r requirements.txt

To work with GitHub, copy `config-example.yml` to `config.yml` and fill in a classic token and your user or organization name. The token needs delete repo rights if you want to use `--clean`. `config.yml` is in `.gitignore`, keep it that way. Everything works without GitHub if you add `--noremote`.

## Usage

### One CBT file

Downloads the file from cbttape.org, creates or updates its repo and pushes it to GitHub. You don't need to run any of the other scripts first.

    ./process-one-cbt.py 314
    ./process-one-cbt.py 314 --noremote

If there's no local repo yet but there is one on GitHub, that one gets cloned first, so its history carries on.

| Argument | What it does | Default |
| --- | --- | --- |
| `cbt` | Number of the CBT file, `43` for `CBT043` (required) | |
| `--noremote` | Leave GitHub alone | off |
| `--stage` | Where the zip file is downloaded to | `./stage` |
| `--cbtfiles` | Where the copy of the processed zip file goes | `.cbtfiles` |
| `--repos` | Folder with the local repos | `.cbtrepos` |

### All CBT files

    ./get-cbtzips-locally.py
    ./process-local-cbtzips.py

`get-cbtzips-locally.py` keeps a local copy of all the zip files. A full download is about 630 MB.

| Argument | What it does | Default |
| --- | --- | --- |
| `--updates` | Only the files cbttape.org flags as updated | off |
| `--force` | Download everything, don't compare file sizes | off |
| `--threads` | Simultaneous FTP downloads, don't go over 15 | `15` |
| `--stage` | Where the zip files go | `./stage` |
| `--pickle` | Where the parsed table of contents goes | `.cbt.pkl` |

`process-local-cbtzips.py` makes the repos. Only zip files that are new or changed get processed.

| Argument | What it does | Default |
| --- | --- | --- |
| `--only` | Just this CBT number, from what's already in the stage folder | all |
| `--force` | Process the zip files even if they didn't change | off |
| `--noremote` | Leave GitHub alone | off |
| `--clean` | **Deletes all local repos and all `CBT*` repos in your GitHub organization**, then starts over. With `--noremote` it only deletes the local ones | off |
| `--stage`, `--cbtfiles`, `--repos` | As above | |
| `--pickle` | The table of contents from `get-cbtzips-locally.py` | `.cbt.pkl` |

Each run writes a `cbt2git-log-*` file. Look for `ALERT` in there to see what didn't work out.

### Analytics

`cbt-analyzer.py` lists what's in the CBT files (datasets, members, mimetypes) in `cbt.xlsx`. Use `--only` for a single file, or `--noparse` to rebuild the sheet from the previous scan.

## What's in a repo

The repos can be cloned and managed with ZIGI. Every dataset is in the root of the repo and listed in `.zigi/dsn`, with the DCB it had when it was transmitted.

| In the repo | What it is |
| --- | --- |
| `FILEnnn/` | The PDS from the CBT file |
| `NAME/` | A PDS received from an XMI inside the CBT file, named after its member |
| `NAME.SUB/` | The same for XMIs nested deeper. Member `ASM` in `XMITIP` becomes `XMITIP.ASM` |
| `NAME` (a file) | A text sequential dataset received from an XMI |
| `NAME.XMI` | An XMI that can't be rebuilt from flat files (RECFM=U or binary sequential). ZIGI puts it on z/OS as a dataset, then you RECEIVE it |
| `NAME.xmi` | The original of an XMI directly in the CBT file that was received. ZIGI ignores these |
| `docs/` | Documents such as PDF and Word files. ZIGI ignores these |
| `zips/` | Unzipped zip members. ZIGI ignores these |
| `.zigi/` | `dsn` with the datasets, and a file per PDS with the ISPF statistics |
| `.gitattributes` | ZIGI's defaults, plus a line for every binary file or PDS |
| `README.md` | The description of the CBT file, from its `@FILEnnn` member |
| `cbt2git.log` | What happened during the conversions |

Where a member was moved out of its PDS, a short member stays behind saying where it went and which dataset it originally was.

Each run regenerates the whole repo from the CBT file. Changes made by hand get overwritten, and a run that finds nothing new makes no commit.

## Known limits

- CBT files that aren't an XMI can't be converted and get no repo. Right now that's 16 of the 1077: 001-005, 007, 018, 062-064, 110, 157, 230, 808, 932 and 933.
- A few nested XMIs can't be read by xmi-reader. They stay in the repo as `NAME.XMI` with an `ALERT` in the log.
- ZIGI treats a line starting with `#` in `.zigi/dsn` as a comment, so a dataset that would start with `#` starts with `@` instead.

## Conclusion

Overall, cbt2git provides a valuable tool for preserving and sharing a part of computing history that might otherwise be lost or inaccessible. With its automated scripts and integration with GitHub and ZIGI, cbt2git makes it easier for developers and researchers to access and use these important resources.
