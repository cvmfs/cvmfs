# Contributing to cvmfs-csi

First off, thanks for taking the time to contribute!

See the [Table of Contents](#table-of-contents) for different ways to help and details about how this project handles them. Please make sure to read the relevant section before making your contribution. It will make it a lot easier for us maintainers and smooth out the experience for all involved.

## Table of Contents

- [Contributing to cvmfs-csi](#contributing-to-cvmfs-csi)
   * [I Have a Question](#i-have-a-question)
   * [I Want To Contribute](#i-want-to-contribute)
      + [Reporting Bugs](#reporting-bugs)
         - [Before Submitting a Bug Report](#before-submitting-a-bug-report)
         - [How Do I Submit a Good Bug Report?](#how-do-i-submit-a-good-bug-report)
      + [Suggesting Enhancements](#suggesting-enhancements)
         - [Before Submitting an Enhancement](#before-submitting-an-enhancement)
         - [How Do I Submit a Good Enhancement Suggestion?](#how-do-i-submit-a-good-enhancement-suggestion)
      + [Your First Code Contribution](#your-first-code-contribution)
   * [For project maintainers](#for-project-maintainers)
      + [Releasing a new version](#releasing-a-new-version)

## I Have a Question

> If you want to ask a question, we assume that you have read the available [Documentation](https://github.com/cvmfs/cvmfs-csi/tree/master/docs).

Before you ask a question, it is best to search for existing [Issues](https://github.com/cvmfs/cvmfs-csi/issues) that might help you. In case you have found a suitable issue and still need clarification, you can write your question in this issue. It is also advisable to search the internet for answers first.

If you then still feel the need to ask a question and need clarification, we recommend the following:

- Open an [Issue](https://github.com/cvmfs/cvmfs-csi/issues/new).
- Provide as much context as you can about what you're running into.
- Provide information about your environment, depending on what seems relevant.

## I Want To Contribute

> ### Legal Notice
> When contributing to this project, you must agree that you have authored 100% of the content, that you have the necessary rights to the content and that the content you contribute may be provided under the project license.

### Reporting Bugs

#### Before Submitting a Bug Report

A good bug report shouldn't leave others needing to chase you up for more information. Therefore, we ask you to investigate carefully, collect information and describe the issue in detail in your report. Please complete the following steps in advance to help us fix any potential bug as fast as possible.

- Make sure that you are using the latest version.
- Determine if your bug is really a bug and not an error on your side e.g. using incompatible environment components/versions (Make sure that you have read the [documentation](https://github.com/cvmfs/cvmfs-csi/tree/master/docs). If you are looking for support, you might want to check [this section](#i-have-a-question)).
- To see if other users have experienced (and potentially already solved) the same issue you are having, check if there is not already a bug report existing for your bug or error in the [bug tracker](https://github.com/cvmfs/cvmfs-csi/issues?q=label%3Abug).
- Also make sure to search the internet (including Stack Overflow) to see if users outside of the GitHub community have discussed the issue.
- Collect information about the bug:
  - Stack trace (Traceback)
  - Environment info (Kubernetes and container runtime versions, cloud provider, etc.) depending on what seems relevant
  - Possibly your input and the output
  - Can you reliably reproduce the issue? And can you also reproduce it with older versions?

#### How Do I Submit a Good Bug Report?

We use GitHub issues to track bugs and errors. If you run into an issue with the project:

- Open an [Issue](https://github.com/cvmfs/cvmfs-csi/issues/new). (Since we can't be sure at this point whether it is a bug or not, we ask you not to talk about a bug yet and not to label the issue.)
- Explain the behavior you would expect and the actual behavior.
- Please provide as much context as possible and describe the *reproduction steps* that someone else can follow to recreate the issue on their own. This may include your code (for good bug reports you should isolate the problem and create a reduced test case).

Once it's filed:

- The project team will label the issue accordingly.
- A team member will try to reproduce the issue with your provided steps. If there are no reproduction steps or no obvious way to reproduce the issue, the team will ask you for those steps and mark the issue as `needs-repro`. Bugs with the `needs-repro` tag will not be addressed until they are reproduced.
- If the team is able to reproduce the issue, it will be marked `needs-fix`, as well as possibly other tags (such as `critical`), and the issue will be left to be [implemented by someone](#your-first-code-contribution).

<!-- You might want to create an issue template for bugs and errors that can be used as a guide and that defines the structure of the information to be included. If you do so, reference it here in the description. -->


### Suggesting Enhancements

This section guides you through submitting an enhancement suggestion for cvmfs-csi, **including completely new features and minor improvements to existing functionality**. Following these guidelines will help maintainers and the community to understand your suggestion and find related suggestions.

#### Before Submitting an Enhancement

- Make sure that you are using the latest version.
- Read the [documentation](https://github.com/cvmfs/cvmfs-csi/tree/master/docs) carefully and find out if the functionality is already covered, maybe by an individual configuration.
- Perform a [search](https://github.com/cvmfs/cvmfs-csi/issues) to see if the enhancement has already been suggested. If it has, add a comment to the existing issue instead of opening a new one.
- Find out whether your idea fits with the scope and aims of the project. It's up to you to make a strong case to convince the project's developers of the merits of this feature. Keep in mind that we want features that will be useful to the majority of our users and not just a small subset. If you're just targeting a minority of users, consider writing an add-on/plugin library.

#### How Do I Submit a Good Enhancement Suggestion?

Enhancement suggestions are tracked as [GitHub issues](https://github.com/cvmfs/cvmfs-csi/issues).

- Use a **clear and descriptive title** for the issue to identify the suggestion.
- Provide a **step-by-step description of the suggested enhancement** in as many details as possible.
- **Describe the current behavior** and **explain which behavior you expected to see instead** and why. At this point you can also tell which alternatives do not work for you.
- **Explain why this enhancement would be useful** to most cvmfs-csi users. You may also want to point out the other projects that solved it better and which could serve as inspiration.

### Your First Code Contribution

If the contribution you are planning to work on is non-trivial, please open an [Issues](https://github.com/cvmfs/cvmfs-csi/issues).

- Setup your development environment and make sure you can [build cvmfs-csi from source code](/docs/building-from-source.md).
- While it is possible to run the CSI driver standalone and locally, it's best to [deploy it in a real Kubernetes cluster](/docs/deploying.md).
- Test your contribution thoroughly before sending a pull request.

This repository is hosted on GitHub, and so follows the regular [GitHub workflow](https://git-scm.com/book/en/v2/GitHub-Contributing-to-a-Project).

## For project maintainers

### Releasing a new version

* Major and minor releases must have their own branchs named `release-<Major>.<Minor>`
* Only important bugfixes may be backported. Use [`git cherry-pick`](https://git-scm.com/docs/git-cherry-pick) to cherrypick the relevant commit(s) into the release branch. Make sure to link to the original PR (and Issue if one exists) in the commit message.
* To publish a release, create a [Git tag](https://git-scm.com/book/en/v2/Git-Basics-Tagging) in the format of `v<Major>.<Minor>.<Patch>` (e.g. `v1.2.3`, see [Semantic versioning](https://semver.org/)). By pushing the tag, a CI pipeline is triggered: a new container image and chart will be built and pushed to CERN's container artifact registry. Note that tags that are pushed are immutable and may not be deleted or overwritten. Before tagging a release, consider first tagging a release candidate -- in that case use format `v<Major>.<Minor>.<Patch>-rc.<Release candidate number>` (e.g. `v1.2.3-rc.0`).
  * The built image is tagged with Git's tag.
  * The chart is tagged with the `version` value specified in [Chart.yaml](/deployments/helm/cvmfs-csi). Don't forget to update this value too before making a release.
* When done, [publish the release](https://github.com/cvmfs/cvmfs-csi/releases/new).

## Attribution
This guide is based on the **contributing-gen**. [Make your own](https://github.com/bttger/contributing-gen)!
