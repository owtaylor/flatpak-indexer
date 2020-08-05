#!/bin/bash

set -e

if ! git diff-index --cached --quiet HEAD ; then
    echo "Can't update test data with staged changes"
    exit 1
fi

old_branch=$(git symbolic-ref HEAD)
cleanup() {
    unset GIT_WORK_TREE
    git symbolic-ref HEAD $old_branch
    git reset
}

trap cleanup EXIT

export GIT_WORK_TREE=test-data

if git rev-parse --verify --quiet refs/heads/test-data-cache > /dev/null ; then
    git symbolic-ref HEAD refs/heads/test-data-cache
    git reset
elif git rev-parse --verify --quiet refs/remotes/origin/test-data-cache > /dev/null ; then
    git branch --track test-data-cache origin/test-data-cache
    git symbolic-ref HEAD refs/heads/test-data-cache
    git reset
else
    git checkout --orphan test-data-cache
fi

git add -A
git commit -m "Update test data"


