#!/usr/bin/env bash

OUTPUT_CONTENT_DIR=$1
TOP_WEIGHT=$2
COMMIT_SHA=$(git rev-parse HEAD)

echo ">> preprocessing content of dir ${OUTPUT_CONTENT_DIR}"

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
  relFile=${file##${OUTPUT_CONTENT_DIR}/}
  echo "$(
    cat <<EOF

---

Found a typo, inconsistency or missing information in our docs?
Help us to improve [Thanos](https://thanos.io) documentation by proposing a fix [on GitHub here](https://github.com/thanos-io/thanos/edit/main/docs/${relFile}) :heart:

EOF
  )" >>${file}

done
