## How to be a contributor to this project

### Are you new and looking to dive in?
* Check our [issues](https://github.com/kagkarlsson/db-scheduler/issues) to see if there is something you can dive in to.

### Are you submitting a pull request?

* Opening up an issue before you PR, will help with traceability and allow for a conversation before you fix an issue/implement
  a new feature to make ensure the maintainers understand not just how you fixed/implemented it but what you are trying
  to fix/implement.
* Try to fix one thing per pull request! Many people work on this code, so the more focused your changes are, the less
  of a headache other people will have when they merge their work in.
* Ensure your Pull Request passes tests either locally or via Github Actions (once triggered by a maintainer it will run automatically on your PR)

### Tools needed to do development and execute tests
* Java 8 (since it's the lowest supported java version to date, although we do support newer versions and execute tests again those as well)
* Docker

### Running tests
Simply running `mvn clean test` will run most of the tests. There are a layer of compatibilty-tests that also should be run
but take longer time as they spin up numerous docker containers. Run using `mvn -Pcompatibility clean test`.
This is the command that CI will execute so make sure to run it locally and verify all is good on your end first.

### Formatting
An `.editorconfig` is setup to take care of consistent formatting make sure your editor of choice is respecting it.
