---
name: consolidate-deps
description: Use when consolidating open dependabot PRs, or bumping dependency, Maven plugin, or GitHub Actions versions in db-scheduler ("bump versions", "merge the dependabot PRs", "dependency updates", "low-hanging version upgrades").
---

# Consolidate dependency bumps

Turn open dependabot PRs plus other available minor/patch updates into a few
draft PRs:

- **Combined PR:** every test-scoped dependency, build plugin, example-module
  dependency and GitHub Action, all in one PR.
- **Runtime PRs:** dependencies that ship to users go in separate PRs, one per
  family: Spring Boot, jackson (2.x + 3.x), gson, micrometer, cron-utils,
  jspecify, slf4j.

Precedent: #962 (combined) and #963 (Spring Boot).

## 1. Collect

```bash
git checkout master && git pull --ff-only     # local master is often behind
gh pr list --author app/dependabot --state open --json number,title,headRefName
gh pr diff <n> | grep -E '^[-+] |^\+\+\+'     # exact old -> new for each PR
```

Then scan for Maven updates dependabot hasn't filed yet. Run the scans one after
the other (~2 min). Scan once without majors and once with them, so a newer minor
isn't hidden behind a major:

```bash
MVN=$(command -v mvn || ls -d ~/.m2/wrapper/dists/apache-maven-*/*/bin/mvn | tail -1)
V=org.codehaus.mojo:versions-maven-plugin:2.18.0
for MAJ in false true; do
  $MVN -B $V:display-dependency-updates $V:display-plugin-updates \
    -DprocessDependencyManagement=false -DallowMajorUpdates=$MAJ \
    -Dmaven.version.ignore='(?i).*(alpha|beta|milestone|rc|cr|preview|snapshot|-m\d+|\.M\d+|-ea|-jdk5).*' \
    2>&1 | grep -E -- '->|Building' | sed 's/\[INFO\] *//' | awk '!s[$0]++'
done
```

The scan reports **effective** versions. Find where each version is actually
declared (a root `pom.xml` property, `db-scheduler/pom.xml`, or an inline
`<version>`) and edit it there. `display-property-updates` misses the root
properties that feed BOM imports, so don't rely on it.

GitHub Actions versions come only from dependabot PRs; the scan doesn't cover
them.

## 2. Classify each candidate

Runtime means: declared in a published module (`db-scheduler`, `db-scheduler-spring-*`)
with no `<scope>`, or with scope `compile`/`runtime`, including `<optional>true`.
BOM imports count by what they manage (jackson-bom → jackson).

| Candidate | Action |
|---|---|
| Runtime dependency, minor/patch | Runtime PR for its family |
| Test-scoped dependency, plugin, examples-only dependency, GitHub Action (minor/patch) | Combined PR |
| Any major, including one dependabot proposed | Don't apply. List under "Not included" |
| Dependabot proposes a major, and the scan shows a newer minor in the current major (mariadb 4.0.0 vs 3.5.11) | Apply the minor; list the major |
| Dependency with no `<version>` in starters/examples (Spring Boot BOM: micrometer, junit, hsqldb, logback) | Skip. It follows Spring Boot |
| `slf4j.version` 1.7.x, `logback.version` 1.2.x in core | Skip. Pinned on purpose |
| `com.github.kagkarlsson:db-scheduler` self-reference | Skip |
| Scan suggests a different suffix (`.jre8` → `.jre11`, `-jdk5`) | Keep the repo's suffix; bump the number only |

## 3. Apply and verify

One branch per PR, each off fresh master: `deps/consolidate-bumps-YYYY-MM-DD`,
`deps/<family>-YYYY-MM-DD`. Edit with Edit or sed, then run:

```bash
$MVN -B spotless:apply license:format
$MVN -B install -DskipTests -DskipChecks       # puts every module in ~/.m2 so -pl works without -am
for M in <modules>; do                          # one module at a time; memory is tight
  $MVN -B verify -pl $M 2>&1 | grep -E 'Tests run:.*Fail|BUILD|\[ERROR\]' | grep -v ' -- in ' | tail -5
done
```

Modules:
- Combined PR: `db-scheduler`, `examples/features`, `examples/spring-boot-example`,
  plus every starter module listed below.
- Spring Boot PR: the starter modules and `examples/spring-boot-example`.
- Other runtime PRs: `db-scheduler` and the starter modules.

Starter modules, under `db-scheduler-spring-boot-starter-parent/`:
`db-scheduler-spring-common`, `db-scheduler-spring-boot3-starter`, `db-scheduler-spring-boot4-starter`.

The Boot 4 starter logs `Expected one execution to be updated, but updated 0`.
That is expected noise from its per-test truncate. Judge by `Tests run` and
`BUILD SUCCESS`.

If a bump breaks the build, drop it and list it under "Not included" with the
error. Don't fix source code in a deps PR.

`git diff` touches only `pom.xml` files and `.github/workflows/*`. If spotless
changed source files after a formatter bump, put that in its own commit:
`style: apply <plugin> <version> formatting`.

## 4. Push and wait for CI

Pushing a branch runs `build` (full `-Ptests-for-ci` with the Docker compatibility
tests that can't run locally) and `spring-boot-compatibility`. Each takes about 5
minutes. Push every branch, then wait for both workflows to finish before
previewing any PR:

```bash
git push -u origin "$B:$B"
gh run list --branch "$B" --event push --json databaseId,workflowName,status,conclusion
gh run watch <databaseId> --exit-status      # run in background; one per workflow
gh run view <databaseId> --log-failed | tail -50   # on failure
```

If a run fails because of one bump, drop that bump, push a new commit (never
force-push), and list it under "Not included" with the failing test.

## 5. PR

Use the `open-pr` skill: preview, wait for approval, open as `--draft`. The
branches are already pushed, so `open-pr` only runs `gh pr create`. Preview all
the PRs together so they can be approved in one go.

Titles must pass the semantic-PR lint:
- Combined: `build(deps): consolidate dependency bumps`
- Runtime: `build(deps): bump <family> to <version>`

Body shape. Each paragraph is a single line; don't hard-wrap. Use only these
parts, in this order, and drop any part that would be empty:

1. One sentence: what it bumps or replaces, and `Supersedes #a, #b`.
2. Bullets of `artifact from -> to`, grouped by kind (test drivers/tooling,
   plugins, actions).
3. `Not included: <artifact> <to> (<major | build failure>).`
4. `Verified with mvn verify on <modules> locally and a green CI run on the branch.`
5. `---` then `This PR was prepared with Claude Code.`

Example (runtime PR):

```
Bumps the Spring Boot 4 starter from 4.0.9 to 4.1.2 (minor release).

- spring-boot-4.version 4.0.9 -> 4.1.2

Verified with `mvn verify` on spring-common, both starters and the spring-boot example locally and a green CI run on the branch.

---
This PR was prepared with Claude Code.
```

Write "Supersedes" only for a dependabot PR whose every update is covered across
the PRs you open. If something from it was left out (a major), write
"partially covers #n". Never close dependabot PRs yourself; dependabot closes
them once master has the versions.
