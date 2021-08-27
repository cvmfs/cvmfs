# How to Contribute Code to CernVM-FS

We welcome code contributions!

If you are about to develop a bug fix or a new feature, it could be helpful to let us know about it beforehand, for instance on the cvmfs-devel@cern.ch mailing list or by private mail or through our [ticket tracker](https://sft.its.cern.ch/jira/browse/CVM).  This can avoid duplicated work and allows us to coordinate the developemnt.


# Source Code Workflow

We use git and github to merge code changes.  We roughly follow [Driessen's workflow](http://nvie.com/posts/a-successful-git-branching-model).  The current development head is in the devel branch, the latest stable release is in the master branch.  In order to contribute, please
  - Fork the cvmfs repository
  - Create a new branch from devel, named either feature-..., or fix-... or CVM-... (referring to JIRA)
  - Make changes, commit, and push the branch to your github space
  - and send a pull request to us on github

If possible, commits should be done in such a way that the code remains compilable.  That can help to find problems with `git bisect`.


# Code Conventions

Starting point is [Google C++ style guide](https://google.github.io/styleguide/cppguide.html).

We have the following exceptions/adjustments:

  - We do use `alloca()` and variable-length arrays for small allocations in order to decrease memory fragmentation.
  - We use Doxygen comments. In order to not confuse the style guide checker, we use the `/**` form only (e.g. not `///<` but `/**< */`).  If you need structure in the code comments, use [Markdown syntax](http://daringfireball.net/projects/markdown).
  - We do not include the full copyright in each and every file. It's sufficient to add a comment saying that a particular file is part of CernVM-FS.
  - We currently do not include files with project-relative paths.  The vast majority of source files are in ./cvmfs (although that might change).
  - `using namespace std;` in a .cc file is allowed. In order to keep the style checker quiet, we use `using namespace std;  // NOLINT`.
  - We do a line break before the opening curly bracket ({), if the bracket is at the end of a multi-line statement (e.g. a multi-line if or a multi-line function definition).
  - We use C++03 (not C++11) in order to stay compatible to old system compilers.

The code repository contains a style checker which can be invoked like that:

    python cpplint.py cvmfs/uuid.cc


# Testing

If possible, please add tests to any new code.  Preferably add tests as unit tests to the test/unittests folder, using the [Google Test](https://code.google.com/p/googletest/) unit test framework.  For "cross-cutting" changes that touch a few lines in many files in the code base, please add an integration test in the src/test folder.  To do so, create a new subdirectory and start from one of the "main" shell scripts from another test.  Client-side tests have numbers less than 500, server-side tests have numbers greater than 500.


# Documentation

The sources of our [technical documentation](https://cvmfs.readthedocs.io) is at [https://github.com/cvmfs/doc-cvmfs](https://github.com/cvmfs/doc-cvmfs).  We are happy to merge contributions to this document, in particular if they describe user-visible changes in the software!
