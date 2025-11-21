# Contributing Guidelines
Thank you for considering contributing to ParquetSharp! We welcome contributions from the community, whether it's reporting bugs, suggesting features, or submitting code changes.
However, while we welcome contributions, our main focus is ensuring the project continues to meet our requirements. We are more likely to accept bug fixes and documentation updates but may be conservative about new features.
We will review issues and pull requests on a best-effort basis.

## Submission Guidelines

### Submitting an Issue
Please report bugs and feature requests by opening an [Issue](https://github.com/G-Research/ParquetSharp/issues).

If your issue appears to be a bug, and you're certain it hasn't been reported, open a new issue. Help us to maximize the effort we can spend fixing issues and adding new features, by not reporting duplicate issues.

Providing the following information will increase the chances of your issue being dealt with quickly:

* **Overview of the Issue** - if an error is being thrown a stack trace helps
* **Motivation for or Use Case** - explain why this is a bug for you
* **Version(s)** - is it a regression?
* **Operating System** - is this a problem with all platforms or only specific ones?
* **Reproduce the Error** - provide an example or an unambiguous set of steps
* **Related Issues** - has a similar issue been reported before?
* **Suggest a Fix** - if you can't fix the bug yourself, perhaps you can point to what might be causing the problem (line of code or commit)

If you get help, help others.

For security-related issues, **do not** open a public issue. Instead, refer to our [security policy](https://github.com/G-Research/ParquetSharp/blob/master/SECURITY.md).

### Submitting a Pull Request

To increase the likelihood of acceptance for your PR:
 - Keep changes small and focused
 - Include tests for your modifications
 - Follow project coding standards
 - Provide clear explanations for design choices
  
Before you submit your pull request consider the following guidelines:

* Search GitHub for an open or closed Pull Request that relates to your submission. You don't want to duplicate effort.
* Make your changes in a new git branch:
  
```bash
git checkout -b my-fix-branch develop
```

* Follow our Coding Rules.
* Run the test suite using the development environment described below.
* Commit your changes using a descriptive commit message.

```bash
git commit -a
```

* In GitHub, send a pull request to `master`.

If we suggest changes, then:

* Make the required updates.
* Re-run the test suite to ensure tests are still passing.
* Commit your changes to your branch (e.g. `my-fix-branch`).
* Push the changes to your GitHub repository (this will update your Pull Request).

If the PR gets too outdated we may ask you to rebase and force push to update the PR:
```bash
git rebase target_branch -i
git push origin my-fix-branch -f
```

That's it! Thank you for your contribution!

### Coding Rules

To ensure consistency throughout the source code, keep these rules in mind as you are working:

* All features or bug fixes **must** be tested by one or more unit tests (if possible and applicable).
* All public API methods **must** be documented with XML documentation.