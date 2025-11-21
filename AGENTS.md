# Instructions for AI Assistants

Start off by reading [README.md](README.md) to understand the purpose of this
project and its usage.

## Development standards

Code should be:

 - Be formatted according to `uv run ruff format flatpak_indexer tests tools`
 - Have no warnings with `uv run ruff check flatpak_indexer tests tools`
 - Have no warnings or errors with `uv run pyright flatpak_indexer tests tools`
 - Have 100% test coverage

 Running `uv run ./tools/test.sh` will check these all at once, and should
 be done before considering a change complete.

## Test Driven Development

When possible, changes should be done via test-driven-development.
That means: first write a test that tests the expected behavior,
then develop the implementation of the change.

Note that additional test cases will be needed to reach 100% test coverage,
but these should be added *after* the functional tests are passing.

## Commits and commit messages

When committing changes, logically separate changes should be committed in
separate commits - for example, a commit should not contain both a new
feature and a bug fix for a bug discovered while implementing the new feature.

For a small change, the body of the commit messages should start off with
a sentence describing what was wrong - what was missing or what didn't work right
with the current code. Then there should be one or more sentences describing
what was done at a high level.

For a large change, the body of the commit messages should start off with
a *paragraph* describing what was wrong - what was missing or what didn't work right
with the current code. Then there should be one or more *paragraphs* describing
what was done at a high level.
