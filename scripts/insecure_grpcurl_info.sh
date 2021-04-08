#!/usr/bin/env bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

HELP='
insecure_grpcurl_info.sh allows you to use call StoreAPI.Series gRPC method and receive streamed series in JSON format.

Usage:
  # Start some example Thanos component that exposes gRPC or use existing one. To start example one run: `thanos query &`
  bash scripts/insecure_grpcurl_info.sh localhost:10901 '"'"'[{"type": 0, "name": "__name__", "value":"go_goroutines"}]'"'"' 0 10
'

INFO_API_HOSTPORT=$1
if [ -z "${INFO_API_HOSTPORT}" ]; then
  echo '$1 is missing (INFO_API_HOSTPORT). Expected host:port string for the target StoreAPI to grpcurl against, e.g. localhost:10901'
  echo "${HELP}"
  exit 1
fi

go install github.com/fullstorydev/grpcurl/cmd/grpcurl

INFO_REQUEST='{}'

GOGOPROTO_ROOT="$(GO111MODULE=on go list -f '{{ .Dir }}' -m github.com/gogo/protobuf)"

cd $DIR/../pkg/ || exit
grpcurl \
  -import-path="${GOGOPROTO_ROOT}" \
  -import-path=. \
  -proto=info/infopb/rpc.proto \
  -plaintext \
  -d="${INFO_REQUEST}" "${INFO_API_HOSTPORT}" thanos.Info/Info
