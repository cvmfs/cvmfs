# CernVM Security and Vulnerability Disclosure

This page describes the CernVM security policy and vulnerability
disclosure information.

## Report a Vulnerability

We are extremely grateful for security researchers and users that report
vulnerabilities in CernVM. All reports are thoroughly investigated by
the CernVM development team.

There are two options to report a security vulnerability in CernVM:

 - Directly on GitHub, at https://github.com/cvmfs/cvmfs/security
 - Via email, to cernvm-security AT cern DOT ch

Please include in your report a description of the vulnerability, along
with the details expected for regular bug reports, such as the affected
CernVM version, relevant configuration directives, sample log files, etc.

### When Should I Report a Vulnerability?

 - You think you discovered a potential security vulnerability in CernVM
   or in one of the projects that CernVM depends on

### When Should I NOT Report a Vulnerability?

 - You need help configuring CernVM authentication or security plugins
 - You need help applying security related updates
 - Your issue is not related to security

## Security Policy

### Security Vulnerability Response

Each report shall be acknowledged and analyzed within 3 working days.
This does not mean that a fix will be available within 3 days, but that
a confirmation of receipt and an assessment of whether or not CernVM is
affected by the vulnerability will be provided within this time frame.

Any vulnerability information shared with the development team stays
within the collaboration and will not be disseminated to other projects
unless it is necessary to get the issue fixed.

The reporter of a vulnerability will be kept up to date on progress as
the security issue moves from triage, to identified fix, to release
planning.

### Public Disclosure Timing

A public disclosure date is negotiated by the bug submitter and the
CernVM development team. We prefer to fully disclose the bug as soon as
possible once a mitigation is available. It is reasonable to delay
disclosure when the bug or the fix are not yet fully understood, the
solution is not well-tested, or for coordination to get the issue fixed.
The time frame for disclosure is from immediate (especially if it's
already publicly known) to a few weeks. For a vulnerability with a
straightforward mitigation, we expect report date to disclosure date to
be on the order of 1 week. The CernVM collaboration reserves the right
to set a disclosure date based on all the factors as described above.

## Security Announcements

Security advisories will be published on the official repository on
GitHub, at https://github.com/cvmfs/cvmfs/security. Announcements
related to CernVM security will also be sent to the same recipients
used for release announcements, including cvmfs-announce AT cern DOT ch
If you would like to receive security announcements but do not want
to subscribe to our users' mailing list, please use the contact page
above to ask to be included in the list of recipients for security
announcements. Instructions for subscribing to the users' mailing
list can also be found on the contact page.
