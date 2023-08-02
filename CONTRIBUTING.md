# Contributing to Network Wrangler

## Roles

## How to Contribute

### Setup

1. Make sure you have a [GitHub](https://github.com/) account.  
2. Make sure you have [git](https://git-scm.com/downloads), a terminal (e.g. Mac Terminal, CygWin, etc.), and a text editor installed on your local machine.  Optionally, you will likely find it easier to use [GitHub Desktop](https://desktop.github.com/), an IDE instead of a simple text editor like [VSCode](https://code.visualstudio.com/), [Eclipse](https://www.eclipse.org/), [Sublime Text](https://www.sublimetext.com/), etc.  
3. [Fork the repository](https://github.com/wsp-sag/network_wrangler/fork) into your own GitHub account and [clone it locally](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository).  
4. Install your `network_wrangler` clone in development mode: `pip install . -e`
5. Install documentation requirements: `pip install -r requirements.docs.txt`
6. Install development requirements: `pip install -r requirements.tests.txt`
7. \[Optional\] [Install act](https://github.com/nektos/act) to run github actions locally.  

### Development Workflow

1. Create [an issue](https://github.com/wsp-sag/network_wrangler/issues) for any features/bugs that you are working on.
2. [Create a branch](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-and-deleting-branches-within-your-repository) to work on a new issue (or checkout an existing one where the issue is being worked on).  
3. Develop comprehensive tests in the `/tests` folder.
4. Modify code including inline documentation such that it passes *all*  tests (not just your new ones)
5. Lint code using `pre-commit run --all-files`
6. Fill out information in the [pull request template](https://github.com/wsp-sag/network_wrangler/blob/master/.github/pull_request_template.md)
7. Submit all pull requests to the `develop` branch.
8. Core developer will review your pull request and suggest changes.
9. After requested changes are complete, core developer will sign off on pull-request merge.

!tip: Keep pull requests small and focused. One issue is best.

!tip: Don't forget to update any associated #documentation as well!

## Documentation

Documentation is produced by mkdocs:

- `mkdocs build`: builds documentation
- `mkdocs serve`: builds and serves documentation to review locally in browswer

Documentation is built and deployed using the [`mike`](https://github.com/jimporter/mike) package and Github Actions configured in `.github/workflows/` for each "ref" (i.e. branch) in the network_wrangler repository.

## Testing and Continuous Integration

Tests and test data reside in the `/tests` directory:

- `pytest`: runs all tests

Continuous Integration is managed by Github Actions in `.github/workflows`.  
All tests other than those with the decorator `@pytest.mark.skipci` will be run.

## Project Governance

The project is currently governed by representatives of its two major organizational contributors:

- Metropolitan Council (MN)
- Metropolitan Transportation Commission (California)

## Code of Conduct

Contributors to the Network Wrangler Project are expected to read and follow the [CODE_OF_CONDUCT](CODE_OF_CONDUCT.md) for the project.
