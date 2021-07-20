#!/usr/bin/env bash

OUTPUT_CONTENT_DIR=$1
TOP_WEIGHT=$2
COMMIT_SHA=$(git rev-parse HEAD)

echo ">> preprocessing content of dir ${OUTPUT_CONTENT_DIR}"

# Add headers to special CODE_OF_CONDUCT.md, CONTRIBUTING.md and CHANGELOG.md files.
echo "$(
  cat <<EOF
---
title: Code of Conduct
type: docs
menu: contributing
---
EOF
)" >${OUTPUT_CONTENT_DIR}/CODE_OF_CONDUCT.md
cat CODE_OF_CONDUCT.md >>${OUTPUT_CONTENT_DIR}/CODE_OF_CONDUCT.md

echo "$(
  cat <<EOF
---
title: Contributing
type: docs
menu: contributing
---
EOF
)" >${OUTPUT_CONTENT_DIR}/CONTRIBUTING.md
cat CONTRIBUTING.md >>${OUTPUT_CONTENT_DIR}/CONTRIBUTING.md

echo "$(
  cat <<EOF
---
title: Changelog
type: docs
menu: thanos
---
EOF
)" >${OUTPUT_CONTENT_DIR}/CHANGELOG.md
cat CHANGELOG.md >>${OUTPUT_CONTENT_DIR}/CHANGELOG.md

echo "$(
  cat <<EOF
---
title: Maintainers
type: docs
menu: thanos
---
EOF
)" >${OUTPUT_CONTENT_DIR}/MAINTAINERS.md
cat MAINTAINERS.md >>${OUTPUT_CONTENT_DIR}/MAINTAINERS.md

echo "$(
  cat <<EOF
---
title: Security
type: docs
menu: thanos
---
EOF
)" >${OUTPUT_CONTENT_DIR}/SECURITY.md
cat SECURITY.md >>${OUTPUT_CONTENT_DIR}/SECURITY.md

# Move root things to "thanos" and "contributing" dir for easy organizing.
mkdir -p ${OUTPUT_CONTENT_DIR}/thanos/
mv ${OUTPUT_CONTENT_DIR}/*.md ${OUTPUT_CONTENT_DIR}/thanos/
mv ${OUTPUT_CONTENT_DIR}/thanos/CONTRIBUTING.md ${OUTPUT_CONTENT_DIR}/contributing/CONTRIBUTING.md
mv ${OUTPUT_CONTENT_DIR}/thanos/CODE_OF_CONDUCT.md ${OUTPUT_CONTENT_DIR}/contributing/CODE_OF_CONDUCT.md
# This is needed only for v0.13-0.16 versions.
mv ${OUTPUT_CONTENT_DIR}/thanos/community.md ${OUTPUT_CONTENT_DIR}/contributing/community.md

# Create an _index.md in Thanos dir.
echo "$(
  cat <<EOF
---
title: "Thanos General Documents:"
---
EOF
)" >${OUTPUT_CONTENT_DIR}/thanos/_index.md

# Create an _index.md in this dir to enable sorting capabilities and make this version appear top in version picker
echo "$(
  cat <<EOF
---
weight: ${TOP_WEIGHT}
---
EOF
)" >${OUTPUT_CONTENT_DIR}/_index.md

# Add edit footer to all markdown files assumed as content.
ALL_DOC_CONTENT_FILES=$(echo "${OUTPUT_CONTENT_DIR}/**/*.md")
for file in ${ALL_DOC_CONTENT_FILES}; do
  relFile=${file##${OUTPUT_CONTENT_DIR}/thanos/}
  echo "$(
    cat <<EOF

---

Found a typo, inconsistency or missing information in our docs?
Help us to improve [Thanos](https://thanos.io) documentation by proposing a fix [on GitHub here](https://github.com/thanos-io/thanos/edit/main/docs/${relFile}) :heart:

EOF
  )" >>${file}

done

# All the absolute links are replaced with a direct link to the file on github, including the current commit SHA.
perl -pi -e 's/]\(\//]\(https:\/\/github.com\/thanos-io\/thanos\/tree\/'${COMMIT_SHA}'\//g' ${ALL_DOC_CONTENT_FILES}

# All the relative links needs to have ../  This is because Hugo is missing: https://github.com/gohugoio/hugo/pull/3934
perl -pi -e 's/]\((?!http)/]\(..\//g' ${ALL_DOC_CONTENT_FILES}
# All the relative links in src= needs to have ../ as well.
perl -pi -e 's/src=\"(?!http)/src=\"..\//g' ${ALL_DOC_CONTENT_FILES}
