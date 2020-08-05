#!/bin/bash

set -e

from_cache=false
fetch=true
update=true

while [ "$#" '>' 0 ] ; do
    case "$1" in
        --from-cache)
            from_cache=true
            ;;
        --only-fetch-cache)
            from_cache=true
            update=false
            ;;
        --no-fetch-cache)
            from_cache=true
            fetch=false
            ;;
    esac

    shift
done

[ -d test-data ] || from_cache=true

if $from_cache && $fetch ; then
    from_rev=$(git rev-parse --quiet --verify refs/heads/test-data-cache || true)
    if [ "$from_rev" = "" ] ; then
        git fetch --depth=1 origin test-data-cache:refs/remotes/origin/test-data-cache
        from_rev=$(git rev-parse --quiet --verify refs/remotes/origin/test-data-cache || true)
    fi
    if [ "$from_rev" = "" ] ; then
        echo "No test-data-cache or origin/test-data-cache branch"
        exit 1
    fi

    if ! git diff-index --cached --quiet HEAD ; then
        echo "Can't checkout test data with staged changes"
        exit 1
    fi

    [ -d .test-data ] || mkdir .test-data


    # Temporarily switch to a detached HEAD
    git_dir=$(git rev-parse --git-dir)
    cp $git_dir/HEAD $git_dir/HEAD.save
    echo $from_rev > $git_dir/HEAD.new && mv $git_dir/HEAD.new $git_dir/HEAD
    git --work-tree=.test-data reset --hard
    # Now put the HEAD ref and index back into place
    mv $git_dir/HEAD.save $git_dir/HEAD
    git reset
fi

success=false
cleanup() {
    if ! $success ; then
        rm -rf test-data.new
    fi
}

trap cleanup EXIT

if $update ; then
    if $from_cache ; then
        tools/create-test-data.py -b .test-data -o test-data.new
    else
        tools/create-test-data.py -b test-data -o test-data.new
    fi

    if rm -rf test-data ; then
        mv test-data.new test-data
    else
        rsync -a test-data.new/ test-data && rm -rf test-data.new
    fi
fi


success=true
