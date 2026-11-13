
The key words "MAY", "MUST", "MUST NOT", and "SHOULD" in this document are to be interpreted as described
in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

## Motivation

- We see an increased amount of contributions which are entirely generated, or at least assisted, by AI tools.
- This policy aims to appeal to the contributors to understand the impact of usage of AI tools to the codebase,
as well as to the wider project dynamics. This policy therefore establishes a framework on how we expect contributors
to use AI tools responsibly.
  - Impact to the codebase: Contributors **MUST** be able to demonstrate they understand the impact and justification
for their changes within the codebase. While AI tools are helpful in complimenting a contributor's knowledge, they
**MUST NOT** be used as a substitute for thoughtful contributions that improve the project. These types of changes
require each change, line by line, to be understood.
  - Impact to the community: Our developers community relies on a delicate web of trust between the contributors and
reviewers. Reviewers invest their own time not only to ensure that the quality standards of the project are met, but
also to educate contributors to in turn become more effective contributors or reviewers themselves. If reviewers feel
that their effort is merely being used to "improve" the output of an AI tool, but not to have a long-lasting impact of
improved returns, this web of trust crumbles. (Comprehension debt)
  - Impact to the community: Wrongly used AI tools have the tendency to create lower quality, potentially faulty, code
which creates an additional burden to the reviewers, as the code is harder to review. Thus an imbalance is created of
faster generated, AI assisted, pull requests which require disproportionally more effort for review, which we already
lack.

## Policy

1. You **MAY** use AI tools for contributing to CVMFS, as long as you follow the principles described below.
2. You **MUST** disclose the usage of AI tools. This includes any pull request to the codebase, as well as any type
of communication in the project, such as documentation, GitHub issues, answering support requests, etc.
3. You **MUST** take full responsibility for your contributions. We expect all contributions, whether entirely human
authored or assisted by AI tools, to meet the project's quality standards. This means, you **MUST** understand and
be able to explain the details of all changes proposed in your pull request. Your primary aim of using AI tools
**MUST** be to create better quality code, not to create code faster.
4. You SHOULD NOT use AI tools in personal communications
with other developers and maintainers, for example when
responding to review comments.
