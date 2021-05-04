#!/usr/bin/env bash
#
# Starts three Prometheus servers scraping themselves and sidecars for each.
# Two query nodes are started and all are clustered together.

trap 'kill 0' SIGTERM

MINIO_ENABLED=${MINIO_ENABLED:-""}
MINIO_EXECUTABLE=${MINIO_EXECUTABLE:-"minio"}
MC_EXECUTABLE=${MC_EXECUTABLE:-"mc"}
PROMETHEUS_EXECUTABLE=${PROMETHEUS_EXECUTABLE:-"prometheus"}
THANOS_EXECUTABLE=${THANOS_EXECUTABLE:-"thanos"}
S3_ENDPOINT=""

if [ ! $(command -v "$PROMETHEUS_EXECUTABLE") ]; then
  echo "Cannot find or execute Prometheus binary $PROMETHEUS_EXECUTABLE, you can override it by setting the PROMETHEUS_EXECUTABLE env variable"
  exit 1
fi

if [ ! $(command -v "$THANOS_EXECUTABLE") ]; then
  echo "Cannot find or execute Thanos binary $THANOS_EXECUTABLE, you can override it by setting the THANOS_EXECUTABLE env variable"
  exit 1
fi

# Start local object storage, if desired.
# NOTE: If you would like to use an actual S3-compatible API with this setup
#       set the S3_* environment variables set in the Minio example.
if [ -n "${MINIO_ENABLED}" ]; then
  if [ ! $(command -v "$MINIO_EXECUTABLE") ]; then
    echo "Cannot find or execute Minio binary $MINIO_EXECUTABLE, you can override it by setting the MINIO_EXECUTABLE env variable"
    exit 1
  fi

  if [ ! $(command -v "$MC_EXECUTABLE") ]; then
    echo "Cannot find or execute Minio client binary $MC_EXECUTABLE, you can override it by setting the MC_EXECUTABLE env variable"
    exit 1
  fi

  export MINIO_ACCESS_KEY="THANOS"
  export MINIO_SECRET_KEY="ITSTHANOSTIME"
  export MINIO_ENDPOINT="127.0.0.1:9000"
  export MINIO_BUCKET="thanos"
  export S3_ACCESS_KEY=${MINIO_ACCESS_KEY}
  export S3_SECRET_KEY=${MINIO_SECRET_KEY}
  export S3_BUCKET=${MINIO_BUCKET}
  export S3_ENDPOINT=${MINIO_ENDPOINT}
  export S3_INSECURE="true"
  export S3_V2_SIGNATURE="true"
  rm -rf data/minio
  mkdir -p data/minio

  ${MINIO_EXECUTABLE} server ./data/minio \
    --address ${MINIO_ENDPOINT} &
  sleep 3
  # create the bucket
  ${MC_EXECUTABLE} config host add tmp http://${MINIO_ENDPOINT} ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}
  ${MC_EXECUTABLE} mb tmp/${MINIO_BUCKET}
  ${MC_EXECUTABLE} config host rm tmp

  cat <<EOF >data/bucket.yml
type: S3
config:
  bucket: $S3_BUCKET
  endpoint: $S3_ENDPOINT
  insecure: $S3_INSECURE
  signature_version2: $S3_V2_SIGNATURE
  access_key: $S3_ACCESS_KEY
  secret_key: $S3_SECRET_KEY
EOF
fi

# Setup alert / rules config file.
cat >data/rules.yml <<-EOF
	groups:
	  - name: example
	    rules:
	    - record: job:go_threads:sum
	      expr: sum(go_threads) by (job)
EOF

STORES=""

# Start three Prometheus servers monitoring themselves.
for i in $(seq 0 2); do
  rm -rf data/prom"${i}"
  mkdir -p data/prom"${i}"/

  cat >data/prom"${i}"/prometheus.yml <<-EOF
		global:
		  external_labels:
		    prometheus: prom-${i}
		rule_files:
		  - 'rules.yml'
		scrape_configs:
		- job_name: prometheus
		  scrape_interval: 5s
		  static_configs:
		  - targets:
		    - "localhost:909${i}"
		    - "localhost:5909${i}"
		    - "localhost:5909${i}"
		    - "localhost:5909${i}"
		- job_name: thanos-sidecar
		  scrape_interval: 5s
		  static_configs:
		  - targets:
		    - "localhost:109${i}2"
		- job_name: thanos-store
		  scrape_interval: 5s
		  static_configs:
		  - targets:
		    - "localhost:10906"
		- job_name: thanos-receive
		  scrape_interval: 5s
		  static_configs:
		  - targets:
		    - "localhost:10909"
		    - "localhost:11909"
		    - "localhost:12909"
		- job_name: thanos-query
		  scrape_interval: 5s
		  static_configs:
		  - targets:
		    - "localhost:10904"
		    - "localhost:10914"
	EOF

  cp data/rules.yml data/prom${i}/rules.yml

  ${PROMETHEUS_EXECUTABLE} \
    --config.file data/prom"${i}"/prometheus.yml \
    --storage.tsdb.path data/prom"${i}" \
    --log.level warn \
    --web.enable-lifecycle \
    --storage.tsdb.min-block-duration=2h \
    --storage.tsdb.max-block-duration=2h \
    --web.listen-address 0.0.0.0:909"${i}" &

  sleep 0.25
done

sleep 0.5

OBJSTORECFG=""
if [ -n "${MINIO_ENABLED}" ]; then
  OBJSTORECFG="--objstore.config-file      data/bucket.yml"
fi

# Start one sidecar for each Prometheus server.
for i in $(seq 0 2); do
  ${THANOS_EXECUTABLE} sidecar \
    --debug.name sidecar-"${i}" \
    --log.level debug \
    --grpc-address 0.0.0.0:109"${i}"1 \
    --grpc-grace-period 1s \
    --http-address 0.0.0.0:109"${i}"2 \
    --http-grace-period 1s \
    --prometheus.url http://localhost:909"${i}" \
    --tsdb.path data/prom"${i}" \
    ${OBJSTORECFG} &

  STORES="${STORES} --store 127.0.0.1:109${i}1"

  sleep 0.25
done

sleep 0.5

if [ -n "${GCS_BUCKET}" -o -n "${S3_ENDPOINT}" ]; then
  ${THANOS_EXECUTABLE} store \
    --debug.name store \
    --log.level debug \
    --grpc-address 0.0.0.0:10905 \
    --grpc-grace-period 1s \
    --http-address 0.0.0.0:10906 \
    --http-grace-period 1s \
    --data-dir data/store \
    ${OBJSTORECFG} &

  STORES="${STORES} --store 127.0.0.1:10905"
fi

sleep 0.5

if [ -n "${REMOTE_WRITE_ENABLED}" ]; then

  for i in $(seq 0 1 2); do
    ${THANOS_EXECUTABLE} receive \
      --debug.name receive${i} \
      --log.level debug \
      --tsdb.path "./data/remote-write-receive-${i}-data" \
      --grpc-address 0.0.0.0:1${i}907 \
      --grpc-grace-period 1s \
      --http-address 0.0.0.0:1${i}909 \
      --http-grace-period 1s \
      --receive.replication-factor 1 \
      --tsdb.min-block-duration 5m \
      --tsdb.max-block-duration 5m \
      --label "receive_replica=\"${i}\"" \
      --label 'receive="true"' \
      --receive.local-endpoint 127.0.0.1:1${i}907 \
      --receive.hashrings '[{"endpoints":["127.0.0.1:10907","127.0.0.1:11907","127.0.0.1:12907"]}]' \
      --remote-write.address 0.0.0.0:1${i}908 \
      ${OBJSTORECFG} &

    STORES="${STORES} --store 127.0.0.1:1${i}907"
  done

  for i in $(seq 0 1 2); do
    mkdir -p "data/local-prometheus-${i}-data/"
    cat <<EOF >data/local-prometheus-${i}-data/prometheus.yml
global:
  external_labels:
    prometheus: prom${i}
    replica: 1
# When the Thanos remote-write-receive component is started,
# this is an example configuration of a Prometheus server that
# would scrape a local node-exporter and replicate its data to
# the remote write endpoint.
scrape_configs:
  - job_name: test
    scrape_interval: 1s
    static_configs:
    - targets:
        - fake
remote_write:
- url: http://localhost:1${i}908/api/v1/receive
EOF
    ${PROMETHEUS_EXECUTABLE} \
      --web.listen-address ":5909${i}" \
      --config.file data/local-prometheus-${i}-data/prometheus.yml \
      --storage.tsdb.path "data/local-prometheus-${i}-data/" &
  done
fi

sleep 0.5

QUERIER_JAEGER_CONFIG=$(
  cat <<-EOF
		type: JAEGER
		config:
		  service_name: thanos-query
		  sampler_type: ratelimiting
		  sampler_param: 2
	EOF
)

REMOTE_WRITE_FLAGS=""
if [ -n "${STATELESS_RULER_ENABLED}" ]; then
  cat >/data/rule-remote-write.yaml <<-EOF
  name: "thanos-receivers"
  remote_write:
    url: "http://127.0.0.1:10908/api/v1/receive"
    name: "receive-0"
EOF

  REMOTE_WRITE_FLAGS="--remote-write --remote-write.config-file data/rule-remote-write.yaml
  "
fi

# Start Thanos Ruler.
${THANOS_EXECUTABLE} rule \
  --data-dir data/ \
  --eval-interval "30s" \
  --rule-file "data/rules.yml" \
  --alert.query-url "http://0.0.0.0:9090" \
  --query "http://0.0.0.0:10904" \
  --query "http://0.0.0.0:10914" \
  --http-address="0.0.0.0:19999" \
  --grpc-address="0.0.0.0:19998" \
  --label 'rule="true"' \
  "${REMOTE_WRITE_FLAGS}" \
  ${OBJSTORECFG} &

STORES="${STORES} --store 127.0.0.1:19998"

# Start two query nodes.
for i in $(seq 0 1); do
  ${THANOS_EXECUTABLE} query \
    --debug.name query-"${i}" \
    --log.level debug \
    --grpc-address 0.0.0.0:109"${i}"3 \
    --grpc-grace-period 1s \
    --http-address 0.0.0.0:109"${i}"4 \
    --http-grace-period 1s \
    --query.replica-label prometheus \
    --tracing.config="${QUERIER_JAEGER_CONFIG}" \
    --query.replica-label receive_replica \
    ${STORES} &
done

sleep 0.5

if [ -n "${GCS_BUCKET}" -o -n "${S3_ENDPOINT}" ]; then
  ${THANOS_EXECUTABLE} tools bucket web \
    --debug.name bucket-web \
    --log.level debug \
    --http-address 0.0.0.0:10933 \
    --http-grace-period 1s \
    ${OBJSTORECFG} &
fi

sleep 0.5

echo "all started; waiting for signal"

wait
