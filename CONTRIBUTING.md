## Contribution Guidelines

### AI Contribution Policy

Contributions made with the assistance of AI tools are welcome, but contributors must use them responsibly and disclose that use clearly.

* If AI tools were used, **disclose** in the PR-description what tool was used and the extent to which the work was AI-assisted.

* The contributor must **fully understand** all the changes made and all the code supplied.

* The supplied code must always be **reviewed critically** prior to creating the PR. This includes tests.

### Are you new and looking to dive in?

* Check our [issues](https://github.com/kagkarlsson/db-scheduler/issues) to see if there is something you can dive in to.
  * Specifically check for issues labeled `help wanted`, `pri1` and `pri2`

### Are you submitting a pull request?

* Opening up an issue before you PR, will help with traceability and allow for a conversation before you fix an issue/implement
  a new feature to make ensure the maintainers understand not just how you fixed/implemented it but what you are trying
  to fix/implement.
* Try to fix one thing per pull request! Many people work on this code, so the more focused your changes are, the less
  of a headache other people will have when they merge their work in.
* Ensure your Pull Request passes tests either locally or via Github Actions (once triggered by a maintainer it will run automatically on your PR)

### Tools needed to do development and execute tests

* Java 17 (since it's the lowest supported java version to date, although we do support newer versions and execute tests again those as well)
* Docker

### Running tests

Simply running `mvn clean test` will run most of the tests. There are a layer of compatibilty-tests that also should be run
but take longer time as they spin up numerous docker containers. Run using `mvn -Ptests-for-ci clean test`. (NB: Some of these
might fail on ARM arch)

### Formatting

An `.editorconfig` is setup to take care of consistent formatting make sure your editor of choice is respecting it.

Standard Java code-style is applied using `mvn spotless:apply`
