# AI Contribution Policy

Contributions made with the assistance of AI tools are welcome, but contributors must use them responsibly and disclose that use clearly.

## In general

* **Don't paste unreviewed AI-generated text into issues, discussions, or PR comments**;
  use your own words. AI may help you arrive at an answer,
  but the message itself should reflect your analysis and understanding.

* Contributors are responsible for ensuring that all submitted content complies with the project's license
  and contribution standards, regardless of how it was produced.

## Pull requests

* If AI tools were used, **disclose** this in the PR description. Include what tools and
  **models** were used (e.g. "Assisted by claude-opus-5-5 (Claude Code).") and optionally what they were used for.

* The contributor must **fully understand** all submitted changes and be able to explain and maintain them.

* All AI-assisted code must always be **reviewed critically** prior to creating the PR.
  This includes tests and generated boilerplate. Added code comes with a maintenance cost. AI tools
  often create unnecessary abstraction, duplication, excessive test coverage, or too much boilerplate.

## Agent-authored pull requests

When an AI agent wrote most of the change:

* Before opening the PR, have the change **self-reviewed** in a fresh context (not the session
  that wrote it) by the most capable model available, and state the review model in the PR
  description. The [`pre-pr`](.claude/skills/pre-pr/SKILL.md) skill describes the steps.
* The review should prioritize correctness and readability. Don't add code to handle unlikely
  edge cases just because a review tool flagged them.
* A human must read the whole diff before opening the PR. No unattended or bulk PRs.
* Open an issue first for anything non-trivial, as for any other contribution.

