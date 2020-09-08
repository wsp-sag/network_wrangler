# Contributing

## Development Workflow

1. Create [an issue](https://github.com/wsp-sag/network_wrangler/issues) for any features/bugs that you are working on.
2. Develop comprehensive tests.
3. Modify code including inline documentation such that it passes *all*  tests (not just your new ones)
4. Lint code to PEP8 using a tool like `black`
5. Fill out information in the [pull request template](https://github.com/wsp-sag/network_wrangler/blob/master/.github/.github/pull_request_template.md)
6. Submit pull requests to the `develop` branch.
7. Core developer will review your pull request and suggest changes.
8. After requested changes are complete, core developer will sign off on pull-request merge.

## Documentation

Documentation is produced by Sphinx and can be run by executing the following from the `/docs` folder:

```bash
make html
```

## Roadmap

- [Issue List](https://github.com/wsp-sag/network_wrangler/issues)  
- [To Do List](todo)  

## Testing

Tests and test data reside in the `/tests` directory. To run:

```bash
pytest
```

## Continuous Integration

Continuous integration is set up in [Travis CI](https://travis-ci.org/wsp-sag/network_wrangler).
