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

## How to Use

### Installation

```bash
git clone https://github.com/wizardofzos/cbt2git.git
cd cbt2git
pip install -e . 
```

### Configuration

Before running publishing commands, copy the sample configuration and add your GitHub credentials:

```bash
cp config-example.yml config.yml
```

Edit `config.yml` with your GitHub token and target organization or personal username:

```yaml
github:
  token: "YOUR_GITHUB_TOKEN_HERE"
  name: "YOUR_ORGANIZATION_OR_USERNAME_HERE"
```

### Commands

* **Sync tape archives locally:**
  ```bash
  cbt-get --threads 15
  ```

* **Inspect and analyze datasets (generates `cbt.xlsx`):**
  ```bash
  cbt-analyze
  ```

* **Process and extract repositories (Dry run / local-only):**
  ```bash
  cbt-process --noremote
  ```

* **Publish repositories to GitHub:**
  ```bash
  cbt-process
  ```

* **Generate markdown directory and searchable HTML catalog:**
  ```bash
  cbt-catalog
  ```

## About ZIGI

ZIGI is an ISPF interface to Git that allows mainframe developers to work with Git repositories directly from the mainframe. By creating "zigi managed" repositories on GitHub using cbt2git, the contents of the repositories can be easily managed and updated by mainframe developers using ZIGI.

## Conclusion

Overall, cbt2git provides a valuable tool for preserving and sharing a part of computing history that might otherwise be lost or inaccessible. With its automated scripts and integration with GitHub and ZIGI, cbt2git makes it easier for developers and researchers to access and use these important resources.

