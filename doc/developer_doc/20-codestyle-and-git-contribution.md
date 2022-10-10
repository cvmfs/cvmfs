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
  git push -u upstream/devel devel
```


### Branch naming policy

The name of the branch should be self-exanatory with a prefix (listed below) that describes the general nature of the work done within the branch, e.g. `fix-openssl3-workaround`.
It might be useful to add the issue number as a postfix to the branch name, e.g. `fix-catalogXattr#2900`.

|Type|Prefix|
|--|--|
|Fix|`fix-`|
|Feature|`feature-`|
|JIRA Ticket| `CVM-`|


### Issues and pull requests

For each pull requests (PR) there should be an issue assigned to it.
Linking between PR and issue can be done by adding in the comment `#<issueNumber>`.
For cross-project linking, e.g. between `cvmfs/cvmfs` and `cvmfs/doc-cvmfs` one can use the full URL of the issue.

Please **do NOT squash commits** if it obscures the git history and tracking of changes made due to, e.g. suggestions made during review.


### Useful git commands

- Graphical representation of `git log`. Helpful to see weird divergents in git commits.

```
  git log --graph --oneline --abbrev-commit --decorate --relative-date --all
  git log --graph --oneline --abbrev-commit --decorate --relative-date <branch1> <branch2>
```

- In case of messy git commit history, consider making a new branch basend on up-to-date `devel` and cherry pick the specific commits from the broken branch

```
  git log  #to get commit hash for the specific commits
  git cherry-pick <commit hash>
```


## Codestyle

In general all information about the codestyle can be found in [cvmfs/CONTRIBUTING.md](https://github.com/cvmfs/cvmfs/blob/devel/CONTRIBUTING.md).

Important code style rules to know are:

- Tab indent: 2
- Max characters per line: 80

In case of a necessary linebreak, the following style should be followed
```c++
  // preferred
  foo()-> 
    bar()

  // not preferred
  foo()
    ->bar()
```

