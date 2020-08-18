#!/usr/bin/env bash

# TODO(bwplotka): Take it from outside as param?
# Regexp take from https://semver.org/
# If we want to limit those we can sort, and have only head -n X of them etc
RELEASE_FILTER_RE="release-(0|[1-9]\d*)\.(0|[1-9]\d*)(\.[0-9]|)$"
WEBSITE_DIR="website"
ORIGINAL_CONTENT_DIR="docs"
OUTPUT_CONTENT_DIR="${WEBSITE_DIR}/docs-pre-processed"
FILES="${WEBSITE_DIR}/docs-pre-processed/*"

# git clone https://github.com/thanos-io/thanos.git

git remote add upstream https://github.com/thanos-io/thanos.git
git remote add origin https://github.com/thanos-io/thanos.git
git remote -v
git fetch origin
# TODO: Remove head -n 3 when ready for prod.
# TODO: Here add logic what releases to filter (regexp) based on some parameter.
RELEASE_BRANCHES=$(git branch --all | grep -P "remotes/origin/${RELEASE_FILTER_RE}"| egrep --invert-match '(:?HEAD|master)$' | sort -nr)
echo ">> chosen $(echo ${RELEASE_BRANCHES}) releases to deploy docs from"

rm -rf ${OUTPUT_CONTENT_DIR}
mkdir -p "${OUTPUT_CONTENT_DIR}/tip"

# Copy original content from current state first.
cp -r ${ORIGINAL_CONTENT_DIR}/* "${OUTPUT_CONTENT_DIR}/tip"
bash scripts/website/contentpreprocess.sh "${OUTPUT_CONTENT_DIR}/tip" ${RELEASE_BRANCHES}

# TODO: In future, fix older release that does not have font matter.
for branchRef in ${RELEASE_BRANCHES}; do
    branchName=${branchRef##*/}
    branch=${branchName/release-/v}
    echo ">> cloning docs for versioning ${branch}"
    mkdir -p "${OUTPUT_CONTENT_DIR}/${branch}"
    git archive --format=tar "refs/${branchRef}" | tar -C${OUTPUT_CONTENT_DIR}/${branch} -x "docs/" --strip-components=1
    bash scripts/website/contentpreprocess.sh "${OUTPUT_CONTENT_DIR}/${branch}"
done

# Find and remove _index.md from all firectory inorder to render docs content
for f in $FILES
do
  # take action on each file. $f store current file name
  find $FILES -name "_index.md" -delete
done

# TODO: Open problems to solve:
# * We can first ensure that public contains the layout we want, then we can adjust html accordingly.