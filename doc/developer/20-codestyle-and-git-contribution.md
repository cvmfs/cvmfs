# Contributing via GitHub and Codestyle

First of all: Thank you for wanting to contribute to cvmfs.
In this section you will find out how you can contribute the most efficient way by e.g. learning how we name our branches and structure GitHub issues. 
In addition, you will find some useful git commands and information about the coding style we use. 

## Contributing via GitHub

### Setting up a fork

The best way of contributing to CernVM-FS via GitHub is to fork the entire [CernVM-FS](https://github.com/cvmfs/cvmfs) project and create a separate branch for each issue.

The main developing branch of CernVM-FS is `devel`.
It is useful to have the forked `devel` branch to track the upstream `cvmfs/devel`.

```
  git remote add upstream git@github.com:cvmfs/cvmfs.git
  git fetch upstream
  git checkout devel
  git branch -u upstream/devel devel
```


### Branch naming policy

The name of the branch should be self-explanatory with a prefix (listed below) that describes the general nature of the work done within the branch, e.g. `fix-openssl3-workaround`.
It might be useful to add the issue number as a postfix to the branch name, e.g. `fix-catalogXattr#2900`.

|Type|Prefix|
|--|--|
|Fix|`fix-`|
|Feature|`feature-`|
|JIRA Ticket| `CVM-`|
|GitHub-Issue| `gh-<Number>`|

### Issues and pull requests

For each pull requests (PR) there should be an issue assigned to it.
Linking between PR and issue can be done by adding in the comment `#<issueNumber>`.
For cross-project linking, e.g. between `cvmfs/cvmfs` and `cvmfs/doc-cvmfs` one can use the full URL of the issue.

> **_NOTE_**&nbsp;
>  Please **do NOT squash commits** if it obscures the git history and tracking of changes made due to, e.g. suggestions made during review.


### Useful git commands

- Graphical representation of `git log`. Helpful to see weird divergences in git commits.

```
  git log --graph --oneline --abbrev-commit --decorate --relative-date --all
  git log --graph --oneline --abbrev-commit --decorate --relative-date <branch1> <branch2>
```

- In case of messy git commit history, consider making a new branch based on up-to-date `devel` and cherry-pick the specific commits from the broken branch

```
  git log  #to get commit hash for the specific commits
  git cherry-pick <commit hash>
```

- Before creating a new PR, integrate the latest changes from `devel`. The git history will be modified in such a way that your changes will be on top/the latest.

```bash
  git rebase upstream/devel

  # you need to force push as the history gets rearranged with git rebase
  git push -f 
```

## Codestyle

In general all information about the codestyle can be found in [cvmfs/CONTRIBUTING.md](https://github.com/cvmfs/cvmfs/blob/devel/CONTRIBUTING.md).

Important code style rules to know are:

- Tab indent: 2
- Max characters per line: 80

In case of a necessary line break, the following style should be followed
```c++
  // preferred
  foo()-> 
    bar()

  // not preferred
  foo()
    ->bar()
```

### Code style checker for C++
- Currently needs `python2`
- Go in main repo `/cvmfs`
- Call `python cpplint.py <myCppFileToCheck>`
  - e.g. `python2 cpplint.py cvmfs/cvmfs.cc`
- It is convenient to set this up as a pre-commit hook. This will only allow you to commit changes that pass the linter and avoid excessive code style commits. To enable this, create the file `.git/hooks/pre-commit` in your cvmfs repository with the following content:

```bash
#!/bin/sh
#
# A hook script to verify what is about to be committed.
# Called by "git commit" with no arguments.  The hook should
# exit with non-zero status after issuing an appropriate message if
# it wants to stop the commit.
#

if git rev-parse --verify HEAD >/dev/null 2>&1
then
  against=HEAD
else
  # Initial commit: diff against an empty tree object
  against=$(git hash-object -t tree /dev/null)
fi

cpplint="python cpplint.py"
sum=0

# for cpp
for file in $(git diff-index --name-status $against -- | grep -E '\.[ch](c)?$' | awk '{print $2}'); do
    $cpplint $file
    sum=$(expr ${sum} + $?)
done

if [ ${sum} -eq 0 ]; then
    exit 0
else
    exit 1
fi
```
