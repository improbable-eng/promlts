# Object Storage & Data Format

Thanos uses object storage as primary storage for metrics and metadata related to them. In this document you can learn how to configure your object storage and what is the data layout and format for primary Thanos components that are "block" aware, like: `sidecar` `compact`, `receive` and `store gateway`.

## Configuring Access to Object Storage

Thanos supports any object stores that can be implemented against Thanos [objstore.Bucket interface](../pkg/objstore/objstore.go).

All clients can be configured using `--objstore.config-file` to reference to the configuration file or `--objstore.config` to put yaml config directly.

### How to use our special `config` flags?

**You can either pass YAML file defined below in `--objstore.config-file` or pass the YAML content directly using `--objstore.config`** We recommend the latter as it gives an explicit static view of configuration for each component. It also saves you the fuss of creating and managing additional file.

Don't be afraid of multiline flags!

In Kubernetes it is as easy as (on Thanos sidecar example):

```yaml
      - args:
        - sidecar
        - |
          --objstore.config=type: GCS
          config:
            bucket: <bucket>
        - --prometheus.url=http://localhost:9090
        - |
          --tracing.config=type: STACKDRIVER
          config:
            service_name: ""
            project_id: <project>
            sample_factor: 16
        - --tsdb.path=/prometheus-data
```

### Supported Clients

Current object storage client implementations:

| Provider                                                                               | Maturity           | Aimed For             | Auto-tested on CI | Maintainers             |
|----------------------------------------------------------------------------------------|--------------------|-----------------------|-------------------|-------------------------|
| [Google Cloud Storage](#gcs)                                                           | Stable             | Production Usage      | yes               | @bwplotka               |
| [AWS/S3](#s3) (and all S3-compatible storages e.g disk-based [Minio](https://min.io/)) | Stable             | Production Usage      | yes               | @bwplotka               |
| [Azure Storage Account](#azure)                                                        | Stable             | Production Usage      | no                | @vglafirov              |
| [OpenStack Swift](#openstack-swift)                                                    | Beta (working PoC) | Production Usage      | yes               | @FUSAKLA                |
| [Tencent COS](#tencent-cos)                                                            | Beta               | Production Usage      | no                | @jojohappy              |
| [AliYun OSS](#aliyun-oss)                                                              | Beta               | Production Usage      | no                | @shaulboozhiao,@wujinhu |
| [Local Filesystem](#filesystem)                                                        | Stable             | Testing and Demo only | yes               | @bwplotka               |

**Missing support to some object storage?** Check out [how to add your client section](#how-to-add-a-new-client-to-thanos)

NOTE: Currently Thanos requires strong consistency (write-read) for object store implementation for singleton Compaction purposes.

#### S3

Thanos uses the [minio client](https://github.com/minio/minio-go) library to upload Prometheus data into AWS S3.

You can configure an S3 bucket as an object store with YAML, either by passing the configuration directly to the `--objstore.config` parameter, or (preferably) by passing the path to a configuration file to the `--objstore.config-file` option.

NOTE: Minio client was mainly for AWS S3, but it can be configured against other S3-compatible object storages e.g Ceph

```yaml mdox-exec="go run scripts/cfggen/main.go --name=s3.Config"
type: S3
config:
  bucket: ""
  endpoint: ""
  region: ""
  access_key: ""
  insecure: false
  signature_version2: false
  secret_key: ""
  put_user_metadata: {}
  http_config:
    idle_conn_timeout: 1m30s
    response_header_timeout: 2m
    insecure_skip_verify: false
    tls_handshake_timeout: 10s
    expect_continue_timeout: 1s
    max_idle_conns: 100
    max_idle_conns_per_host: 100
    max_conns_per_host: 0
  trace:
    enable: false
  list_objects_version: ""
  part_size: 67108864
  sse_config:
    type: ""
    kms_key_id: ""
    kms_encryption_context: {}
    encryption_key: ""
```

At a minimum, you will need to provide a value for the `bucket`, `endpoint`, `access_key`, and `secret_key` keys. The rest of the keys are optional.

The AWS region to endpoint mapping can be found in this [link](https://docs.aws.amazon.com/general/latest/gr/s3.html).

Make sure you use a correct signature version. Currently AWS requires signature v4, so it needs `signature_version2: false`. If you don't specify it, you will get an `Access Denied` error. On the other hand, several S3 compatible APIs use `signature_version2: true`.

You can configure the timeout settings for the HTTP client by setting the `http_config.idle_conn_timeout` and `http_config.response_header_timeout` keys. As a rule of thumb, if you are seeing errors like `timeout awaiting response headers` in your logs, you may want to increase the value of `http_config.response_header_timeout`.

Please refer to the documentation of [the Transport type](https://golang.org/pkg/net/http/#Transport) in the `net/http` package for detailed information on what each option does.

`part_size` is specified in bytes and refers to the minimum file size used for multipart uploads, as some custom S3 implementations may have different requirements. A value of `0` means to use a default 128 MiB size.

Set `list_objects_version: "v1"` for S3 compatible APIs that don't support ListObjectsV2 (e.g. some versions of Ceph). Default value (`""`) is equivalent to `"v2"`.

For debug and testing purposes you can set

* `insecure: true` to switch to plain insecure HTTP instead of HTTPS

* `http_config.insecure_skip_verify: true` to disable TLS certificate verification (if your S3 based storage is using a self-signed certificate, for example)

* `trace.enable: true` to enable the minio client's verbose logging. Each request and response will be logged into the debug logger, so debug level logging must be enabled for this functionality.

##### S3 Server-Side Encryption

SSE can be configued using the `sse_config`. [SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html), [SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html), and [SSE-C](https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html) are supported.

* If type is set to `SSE-S3` you do not need to configure other options.

* If type is set to `SSE-KMS` you must set `kms_key_id`. The `kms_encryption_context` is optional, as [AWS provides a default encryption context](https://docs.aws.amazon.com/kms/latest/developerguide/services-s3.html#s3-encryption-context).

* If type is set to `SSE-C` you must provide a path to the encryption key using `encryption_key`.

If the SSE Config block is set but the `type` is not one of `SSE-S3`, `SSE-KMS`, or `SSE-C`, an error is raised.

You will also need to apply the following AWS IAM policy for the user to access the KMS key:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "KMSAccess",
            "Effect": "Allow",
            "Action": [
                "kms:GenerateDataKey",
                "kms:Encrypt",
                "kms:Decrypt"
            ],
            "Resource": "arn:aws:kms:<region>:<account>:key/<KMS key id>"
        }
    ]
}
```

##### Credentials

By default Thanos will try to retrieve credentials from the following sources:

1. From config file if BOTH `access_key` and `secret_key` are present.
2. From the standard AWS environment variable - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
3. From `~/.aws/credentials`
4. IAM credentials retrieved from an instance profile.

NOTE: Getting access key from config file and secret key from other method (and vice versa) is not supported.

##### AWS Policies

Example working AWS IAM policy for user:

* For deployment (policy for Thanos services):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Statement",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::<bucket>/*",
                "arn:aws:s3:::<bucket>"
            ]
        }
    ]
}
```

(No bucket policy)

To test the policy, set env vars for S3 access for *empty, not used* bucket as well as:

```
THANOS_TEST_OBJSTORE_SKIP=GCS,AZURE,SWIFT,COS,ALIYUNOSS
THANOS_ALLOW_EXISTING_BUCKET_USE=true
```

And run: `GOCACHE=off go test -v -run TestObjStore_AcceptanceTest_e2e ./pkg/...`

* For testing (policy to run e2e tests):

We need access to CreateBucket and DeleteBucket and access to all buckets:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Statement",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:PutObject",
                "s3:CreateBucket",
                "s3:DeleteBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<bucket>/*",
                "arn:aws:s3:::<bucket>"
            ]
        }
    ]
}
```

With this policy you should be able to run set `THANOS_TEST_OBJSTORE_SKIP=GCS,AZURE,SWIFT,COS,ALIYUNOSS` and unset `S3_BUCKET` and run all tests using `make test`.

Details about AWS policies: https://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html

#### GCS

To configure Google Cloud Storage bucket as an object store you need to set `bucket` with GCS bucket name and configure Google Application credentials.

For example:

```yaml mdox-exec="go run scripts/cfggen/main.go --name=gcs.Config"
type: GCS
config:
  bucket: ""
  service_account: ""
```

##### Using GOOGLE_APPLICATION_CREDENTIALS

Application credentials are configured via JSON file and only the bucket needs to be specified, the client looks for:

1. A JSON file whose path is specified by the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
2. A JSON file in a location known to the gcloud command-line tool. On Windows, this is `%APPDATA%/gcloud/application_default_credentials.json`. On other systems, `$HOME/.config/gcloud/application_default_credentials.json`.
3. On Google App Engine it uses the `appengine.AccessToken` function.
4. On Google Compute Engine and Google App Engine Managed VMs, it fetches credentials from the metadata server. (In this final case any provided scopes are ignored.)

You can read more on how to get application credential json file in [https://cloud.google.com/docs/authentication/production](https://cloud.google.com/docs/authentication/production)

##### Using inline a Service Account

Another possibility is to inline the ServiceAccount into the Thanos configuration and only maintain one file. This feature was added, so that the Prometheus Operator only needs to take care of one secret file.

```yaml
type: GCS
config:
  bucket: "thanos"
  service_account: |-
    {
      "type": "service_account",
      "project_id": "project",
      "private_key_id": "abcdefghijklmnopqrstuvwxyz12345678906666",
      "private_key": "-----BEGIN PRIVATE KEY-----\...\n-----END PRIVATE KEY-----\n",
      "client_email": "project@thanos.iam.gserviceaccount.com",
      "client_id": "123456789012345678901",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/thanos%40gitpods.iam.gserviceaccount.com"
    }
```

##### GCS Policies

**Note:** GCS Policies should be applied at the project level, not at the bucket level

For deployment:

`Storage Object Creator` and `Storage Object Viewer`

For testing:

`Storage Object Admin` for ability to create and delete temporary buckets.

To test the policy is working as expected, exec into the sidecar container, eg:

```sh
kubectl exec -it -n <namespace> <prometheus with sidecar pod name> -c <sidecar container name> -- /bin/sh
```

Then test that you can at least list objects in the bucket, eg:

```sh
thanos tools bucket ls --objstore.config="${OBJSTORE_CONFIG}"
```

#### Azure

To use Azure Storage as Thanos object store, you need to precreate storage account from Azure portal or using Azure CLI. Follow the instructions from Azure Storage Documentation: [https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account](https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=portal)

To configure Azure Storage account as an object store you need to provide a path to Azure storage config file in flag `--objstore.config-file`.

Config file format is the following:

```yaml mdox-exec="go run scripts/cfggen/main.go --name=azure.Config"
type: AZURE
config:
  storage_account: ""
  storage_account_key: ""
  container: ""
  endpoint: ""
  max_retries: 0
  msi_resource: ""
  pipeline_config:
    max_tries: 0
    try_timeout: 0s
    retry_delay: 0s
    max_retry_delay: 0s
  reader_config:
    max_retry_requests: 0
  http_config:
    idle_conn_timeout: 0s
    response_header_timeout: 0s
    insecure_skip_verify: false
    tls_handshake_timeout: 0s
    expect_continue_timeout: 0s
    max_idle_conns: 0
    max_idle_conns_per_host: 0
    max_conns_per_host: 0
    disable_compression: false
```

If `msi_resource` is used, authentication is done via ServicePrincipalToken. The value for Azure should be `https://<storage-account-name>.blob.core.windows.net`. The generic `max_retries` will be used as value for the `pipeline_config`'s `max_tries` and `reader_config`'s `max_retry_requests`. For more control, `max_retries` could be ignored (0) and one could set specific retry values.

#### OpenStack Swift

Thanos uses [ncw/swift](https://github.com/ncw/swift) client to upload Prometheus data into [OpenStack Swift](https://docs.openstack.org/swift/latest/).

Below is an example configuration file for thanos to use OpenStack swift container as an object store. Note that if the `name` of a user, project or tenant is used one must also specify its domain by ID or name. Various examples for OpenStack authentication can be found in the [official documentation](https://developer.openstack.org/api-ref/identity/v3/index.html?expanded=password-authentication-with-scoped-authorization-detail#password-authentication-with-unscoped-authorization).

By default, OpenStack Swift has a limit for maximum file size of 5 GiB. Thanos index files are often larger than that. To resolve this issue, Thanos uses [Static Large Objects (SLO)](https://docs.openstack.org/swift/latest/overview_large_objects.html) which are uploaded as segments. These are by default put into the `segments` directory of the same container. The default limit for using SLO is 1 GiB which is also the maximum size of the segment. If you don't want to use the same container for the segments (best practise is to use `<container_name>_segments` to avoid polluting listing of the container objects) you can use the `large_file_segments_container_name` option to override the default and put the segments to other container. *In rare cases you can switch to [Dynamic Large Objects (DLO)](https://docs.openstack.org/swift/latest/overview_large_objects.html) by setting the `use_dynamic_large_objects` to true, but use it with caution since it even more relies on eventual consistency.*

```yaml mdox-exec="go run scripts/cfggen/main.go --name=swift.Config"
type: SWIFT
config:
  auth_version: 0
  auth_url: ""
  username: ""
  user_domain_name: ""
  user_domain_id: ""
  user_id: ""
  password: ""
  domain_id: ""
  domain_name: ""
  project_id: ""
  project_name: ""
  project_domain_id: ""
  project_domain_name: ""
  region_name: ""
  container_name: ""
  large_object_chunk_size: 1073741824
  large_object_segments_container_name: ""
  retries: 3
  connect_timeout: 10s
  timeout: 5m
  use_dynamic_large_objects: false
```

#### Tencent COS

To use Tencent COS as storage store, you should apply a Tencent Account to create an object storage bucket at first. Note that detailed from Tencent Cloud Documents: [https://cloud.tencent.com/document/product/436](https://cloud.tencent.com/document/product/436)

To configure Tencent Account to use COS as storage store you need to set these parameters in yaml format stored in a file:

```yaml mdox-exec="go run scripts/cfggen/main.go --name=cos.Config"
type: COS
config:
  bucket: ""
  region: ""
  app_id: ""
  secret_key: ""
  secret_id: ""
```

Set the flags `--objstore.config-file` to reference to the configuration file.

#### AliYun OSS

In order to use AliYun OSS object storage, you should first create a bucket with proper Storage Class , ACLs and get the access key on the AliYun cloud. Go to [https://www.alibabacloud.com/product/oss](https://www.alibabacloud.com/product/oss) for more detail.

To use AliYun OSS object storage, please specify following yaml configuration file in `objstore.config*` flag.

```yaml mdox-exec="go run scripts/cfggen/main.go --name=oss.Config"
type: ALIYUNOSS
config:
  endpoint: ""
  bucket: ""
  access_key_id: ""
  access_key_secret: ""
```

Use --objstore.config-file to reference to this configuration file.

#### Filesystem

This storage type is used when user wants to store and access the bucket in the local filesystem. We treat filesystem the same way we would treat object storage, so all optimization for remote bucket applies even though, we might have the files locally.

NOTE: This storage type is experimental and might be inefficient. It is NOT advised to use it as the main storage for metrics in production environment. Particularly there is no planned support for distributed filesystems like NFS. This is mainly useful for testing and demos.

```yaml mdox-exec="go run scripts/cfggen/main.go --name=filesystem.Config"
type: FILESYSTEM
config:
  directory: ""
```

### How to add a new client to Thanos?

Following checklist allows adding new Go code client to supported providers:

1. Create new directory under `pkg/objstore/<provider>`
2. Implement [objstore.Bucket interface](../pkg/objstore/objstore.go)
3. Add `NewTestBucket` constructor for testing purposes, that creates and deletes temporary bucket.
4. Use created `NewTestBucket` in [ForeachStore method](../pkg/objstore/objtesting/foreach.go) to ensure we can run tests against new provider. (In PR)
5. RUN the [TestObjStoreAcceptanceTest](../pkg/objstore/objtesting/acceptance_e2e_test.go) against your provider to ensure it fits. Fix any found error until test passes. (In PR)
6. Add client implementation to the factory in [factory](../pkg/objstore/client/factory.go) code. (Using as small amount of flags as possible in every command)
7. Add client struct config to [bucketcfggen](../scripts/cfggen/main.go) to allow config auto generation.

At that point, anyone can use your provider by spec.

## Data in Object Storage

Thanos supports writing and reading data in native Prometheus `TSDB blocks` in [TSDB format](https://github.com/prometheus/prometheus/tree/master/tsdb/docs/format). This is the format used by [Prometheus](https://prometheus.io) TSDB database for persisting data on the local disk. With the efficient index and [chunk](design.md#chunk) binary formats, it also fits well to be used directly from object storage using range GET API.

Following sections explain this format in details with the additional files and entries that Thanos system supports.

### TSDB Block

Official docs for Prometheus TSDB format can be found [here](https://github.com/prometheus/prometheus/tree/master/tsdb/docs/format), but this section lists the most important elements here.

TSDB Block means particularly a set of Blobs (files) in a single directory (or `prefix` if we talk in Object Storage terms) named with [ULID](https://github.com/ulid/spec) e.g `01ARZ3NDEKTSV4RRFFQ69G5FAV`.

**Those files contain series (labels with compressed samples) for particular time duration (e.g 2h) from particular `Source` (e.g Prometheus or Thanos Receive)**

In Thanos system, all files are **strictly immutable**. (NOTE: In Prometheus too, but with some caveats like tombstones). This means that any modification like `rewrite` `deletion` or `compaction` has to be done by creating a new block and removing (with delay!) old one.

> NOTE: Any other not-known file present in this directory is ignored when reading the data. However, those can be removed when the block is being deleted from object storage/disk.

Example block file structure (on the local filesystem) can look like this:

```
01DN3SK96XDAEKRB1AN30AAW6E:
total 2209344
drwxr-xr-x 2 bwplotka bwplotka       4096 Dec 10  2019 chunks
-rw-r--r-- 1 bwplotka bwplotka 1962383742 Dec 10  2019 index
-rw-r--r-- 1 bwplotka bwplotka       6761 Dec 10  2019 meta.json
-rw-r--r-- 1 bwplotka bwplotka        111 Dec 10  2019 delete-mark.json      # <-- Optional marker.
-rw-r--r-- 1 bwplotka bwplotka        124 Dec 10  2019 no-compact-mark.json  # <-- Optional marker.

01DN3SK96XDAEKRB1AN30AAW6E/chunks:
total 8202452
-rw-r--r-- 1 bwplotka bwplotka 536870490 Dec 10  2019 000001
-rw-r--r-- 1 bwplotka bwplotka 536869843 Dec 10  2019 000002
-rw-r--r-- 1 bwplotka bwplotka 536869848 Dec 10  2019 000003
-rw-r--r-- 1 bwplotka bwplotka 536868209 Dec 10  2019 000004
-rw-r--r-- 1 bwplotka bwplotka 536869517 Dec 10  2019 000005
-rw-r--r-- 1 bwplotka bwplotka 536870654 Dec 10  2019 000006
-rw-r--r-- 1 bwplotka bwplotka 536855168 Dec 10  2019 000007
-rw-r--r-- 1 bwplotka bwplotka 536859441 Dec 10  2019 000008
-rw-r--r-- 1 bwplotka bwplotka 536862863 Dec 10  2019 000009
-rw-r--r-- 1 bwplotka bwplotka 536868432 Dec 10  2019 000010
-rw-r--r-- 1 bwplotka bwplotka 536861395 Dec 10  2019 000011
-rw-r--r-- 1 bwplotka bwplotka 536870859 Dec 10  2019 000012
-rw-r--r-- 1 bwplotka bwplotka 536854971 Dec 10  2019 000013
-rw-r--r-- 1 bwplotka bwplotka 536846973 Dec 10  2019 000014
-rw-r--r-- 1 bwplotka bwplotka 536866732 Dec 10  2019 000015
-rw-r--r-- 1 bwplotka bwplotka 346266827 Dec 10  2019 000016
```

Let's look at each file one by one.

#### Metadata file (meta.json)

> NOTE: Currently supported meta.json version: v1 Currently supported meta.json Thanos section version: v1

This file is an important entry that described the block and its data.

This file allows you to find for example:

* The block ID (`ulid`)
* Duration of the block (`minTime` and `maxTime`)
* Important statistics (`stats.numSeries`)
* How many times block was re-compacted (`compaction.level`)
* What initial smaller blocks IDs are part of this block (`compaction.sources`)
* What smaller (including intermittent) blocks IDs are part of this block (`compaction.parents`)
* Thanos Section (only visible for blocks generated by Thanos components like `sidecar`, `receive` or `compact`):
  * External Labels for block (identifying producers) (`thanos.labels`)
  * Downsampling resolution if downsampling was done on this block (`thanos.downsample.resolution`). `0` means no downsampling.
  * What component created block (`thanos.source`)
  * Files and its sizes that are part of this block (`thanos.files`)

> NOTE: In theory, you can modify this data manually. However, components like Compactor and Store Gateway currently infinitely cache that meta.json, (sometimes on disk if configured), so manual cache removal and restart might be needed.

Example meta.json file:

```json
{
	"ulid": "01DN3SK96XDAEKRB1AN30AAW6E",
	"minTime": 1567641600000,
	"maxTime": 1568851200000,
	"stats": {
		"numSamples": 5397517846,
		"numSeries": 8377876,
		"numChunks": 67874256
	},
	"compaction": {
		"level": 4,
		"sources": [
			"01DKZNX70TQQ0R025G66ZF1V5P",
			"01DKZWS55317K7JGVMCSBR68Z2",  // Trimmed items for readability.
			"01DN3GH4A71RD6NYQ2VZPBQTFH"
		],
		"parents": [
			{
				"ulid": "01DM4WK3F9ZGW19W16MZJJFF6T",
				"minTime": 1567641600000,
				"maxTime": 1567814400000
			},
			{
				"ulid": "01DMA1BXHK3G2KDKAPMBTVATRT",
				"minTime": 1567814400000,
				"maxTime": 1567987200000
			},
			{
				"ulid": "01DMF65TY6JSTCDVTPZ094B5D6",
				"minTime": 1567987200000,
				"maxTime": 1568160000000
			},
			{
				"ulid": "01DMMB0SK28FKC55RNK7ZZWS1A",
				"minTime": 1568160000000,
				"maxTime": 1568332800000
			},
			{
				"ulid": "01DMSFSXNE8Y76G5KCQ2BABYFA",
				"minTime": 1568332800000,
				"maxTime": 1568505600000
			},
			{
				"ulid": "01DMYMM5SW0FPJSHQQQM05FBN9",
				"minTime": 1568505600000,
				"maxTime": 1568678400000
			},
			{
				"ulid": "01DN3SDE1M9W1JG7JFSM5QFP2Y",
				"minTime": 1568678400000,
				"maxTime": 1568851200000
			}
		]
	},
	"version": 1,
	"thanos": {
		"labels": {
            "cluster": "eu1",
			"monitor": "prometheus",
            "tenant": "team-a",
			"replica": "1"
		},
		"downsample": {
			"resolution": 0
		},
		"source": "compactor",
       	"files": [
       			{
       				"rel_path": "index",
       				"size_bytes": 1313
       			}, // Trimmed items for readability.
       	],
        "version": 1
	}
}
```

Format in Go code can be found [here](https://github.com/thanos-io/thanos/blob/main/pkg/block/metadata/meta.go).

##### External Labels

External labels are extremely important block metadata. They are stored in `meta.json` in `thanos.labels` section and allows to identify the producer and owner of those blocks. This information will be used further by different Thanos components:

* Those labels will be visible when data is queried. You can aggregate across those in PromQL etc.
* [Querier](components/query.md) to filter out store APIs to touch during query requests.
* Many object storage readers like [compactor](components/compact.md) and [store gateway](components/store.md) which groups the blocks by external labels. This grouping allows horizontal scalability like sharding or concurrency.
* Some of those labels can be chosen as **replication** labels. Querier and Compactor will then deduplicate such blocks identified by same HA groups.
* Some of those labels can be chosen as **tenancy** labels. This allows read, write and storage isolation mechanism.

The `meta.json` and `thanos.labels` labels are filled during block upload/creation. For example:

* Each produced TSDB block by Prometheus is labelled with Prometheus [external labels](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#configuration-file) by `sidecar` before upload to object storage.
* Each produced TSDB block by `compact` is labelled with whatever source blocks had. The exception is the deduplication process that removes the chosen replica flag(s).
* Each produced TSDB block by `receive` is labelled with labels given labels in repeated [receive](components/receive.md) `--labels` flag.

The recommended information that should be given in those labels:

Example Prometheus useful external labels:

* Replication information e.g `replica="0"`
* Cluster, environment, zone, so target origin e.g `cluster="eu-1-production"` or `cluster="1",env="production",region="us-west1"`
* Tenancy information e.g `tenant="organizationABC"`

> NOTE: Be careful with receive external flags. Remote Write clients can stream any labels. If some label will duplicate with the external label of receive, it will be masked with what receiver has specified. This is why it's recommended to have `receive_` prefix to all receive labels. (e.g to not confuse with Prometheus replicas)

Example Receive useful external labels:

* Replication information e.g `receive_replica="0"` (to not confuse with Prometheus `replica` often stated).
* Cluster, environment, zone, so target origin e.g `receive_cluster="eu-west1-production-1"` or `receive_cluster="1",receive_env="production",receive_region="us-west1"`
* Tenancy information e.g `tenant="organizationABC"`

#### Index Format (index)

> NOTE: Currently supported index file versions: v1 and v2

> This file stores the index created to allow efficient lookup for series and its samples.

**All entries are sorted lexicographically unless stated otherwise.**

From high level it allows to find:

* Label names
* Label values for label name
* All series labels
* Given (or all) series' chunk reference. This can be used to find [chunk](design.md#chunk) with samples in the [chunk files](#chunks-file-format)

The following describes the format of the `index` file found in each block directory. It is terminated by a table of contents which serves as an entry point into the index.

```
┌────────────────────────────┬─────────────────────┐
│ magic(0xBAAAD700) <4b>     │ version(1) <1 byte> │
├────────────────────────────┴─────────────────────┤
│ ┌──────────────────────────────────────────────┐ │
│ │                 Symbol Table                 │ │
│ ├──────────────────────────────────────────────┤ │
│ │                    Series                    │ │
│ ├──────────────────────────────────────────────┤ │
│ │                 Label Index 1                │ │
│ ├──────────────────────────────────────────────┤ │
│ │                      ...                     │ │
│ ├──────────────────────────────────────────────┤ │
│ │                 Label Index N                │ │
│ ├──────────────────────────────────────────────┤ │
│ │                   Postings 1                 │ │
│ ├──────────────────────────────────────────────┤ │
│ │                      ...                     │ │
│ ├──────────────────────────────────────────────┤ │
│ │                   Postings N                 │ │
│ ├──────────────────────────────────────────────┤ │
│ │               Label Offset Table             │ │
│ ├──────────────────────────────────────────────┤ │
│ │             Postings Offset Table            │ │
│ ├──────────────────────────────────────────────┤ │
│ │                      TOC                     │ │
│ └──────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────┘
```

When the index is written, an arbitrary number of padding bytes may be added between the lined out main sections above. When sequentially scanning through the file, any zero bytes after a section's specified length must be skipped.

Most of the sections described below start with a `len` field. It always specifies the number of bytes just before the trailing CRC32 checksum. The checksum is always calculated over those `len` bytes.

##### Symbol Table

The symbol table holds a sorted list of deduplicated strings that occurred in label pairs of the stored series. They can be referenced from subsequent sections and significantly reduce the total index size.

The section contains a sequence of the string entries, each prefixed with the string's length in raw bytes. All strings are utf-8 encoded. Strings are referenced by sequential indexing. The strings are sorted in lexicographically ascending order.

```
┌────────────────────┬─────────────────────┐
│ len <4b>           │ #symbols <4b>       │
├────────────────────┴─────────────────────┤
│ ┌──────────────────────┬───────────────┐ │
│ │ len(str_1) <uvarint> │ str_1 <bytes> │ │
│ ├──────────────────────┴───────────────┤ │
│ │                . . .                 │ │
│ ├──────────────────────┬───────────────┤ │
│ │ len(str_n) <uvarint> │ str_n <bytes> │ │
│ └──────────────────────┴───────────────┘ │
├──────────────────────────────────────────┤
│ CRC32 <4b>                               │
└──────────────────────────────────────────┘
```

##### Series

The section contains a sequence of series that hold the label set of the series as well as its [chunks](design.md#chunk) within the block. The series are sorted lexicographically by their label sets. Each series section is aligned to 16 bytes. The ID for a series is the `offset/16`. This serves as the series' ID in all subsequent references. Thereby, a sorted list of series IDs implies a lexicographically sorted list of series label sets.

```
┌───────────────────────────────────────┐
│ ┌───────────────────────────────────┐ │
│ │   series_1                        │ │
│ ├───────────────────────────────────┤ │
│ │                 . . .             │ │
│ ├───────────────────────────────────┤ │
│ │   series_n                        │ │
│ └───────────────────────────────────┘ │
└───────────────────────────────────────┘
```

Every series entry first holds its number of labels, followed by tuples of symbol table references that contain the label name and value. The label pairs are lexicographically sorted. After the labels, the number of indexed [chunks](design.md#chunk) is encoded, followed by a sequence of metadata entries containing the chunks minimum (`mint`) and maximum (`maxt`) timestamp and a reference to its position in the chunk file. The `mint` is the time of the first sample and `maxt` is the time of the last sample in the chunk. Holding the time range data in the index allows dropping chunks irrelevant to queried time ranges without accessing them directly.

`mint` of the first [chunk](design.md#chunk) is stored, it's `maxt` is stored as a delta and the `mint` and `maxt` are encoded as deltas to the previous time for subsequent chunks. Similarly, the reference of the first chunk is stored and the next ref is stored as a delta to the previous one.

```
┌──────────────────────────────────────────────────────────────────────────┐
│ len <uvarint>                                                            │
├──────────────────────────────────────────────────────────────────────────┤
│ ┌──────────────────────────────────────────────────────────────────────┐ │
│ │                     labels count <uvarint64>                         │ │
│ ├──────────────────────────────────────────────────────────────────────┤ │
│ │              ┌────────────────────────────────────────────┐          │ │
│ │              │ ref(l_i.name) <uvarint32>                  │          │ │
│ │              ├────────────────────────────────────────────┤          │ │
│ │              │ ref(l_i.value) <uvarint32>                 │          │ │
│ │              └────────────────────────────────────────────┘          │ │
│ │                             ...                                      │ │
│ ├──────────────────────────────────────────────────────────────────────┤ │
│ │                     chunks count <uvarint64>                         │ │
│ ├──────────────────────────────────────────────────────────────────────┤ │
│ │              ┌────────────────────────────────────────────┐          │ │
│ │              │ c_0.mint <varint64>                        │          │ │
│ │              ├────────────────────────────────────────────┤          │ │
│ │              │ c_0.maxt - c_0.mint <uvarint64>            │          │ │
│ │              ├────────────────────────────────────────────┤          │ │
│ │              │ ref(c_0.data) <uvarint64>                  │          │ │
│ │              └────────────────────────────────────────────┘          │ │
│ │              ┌────────────────────────────────────────────┐          │ │
│ │              │ c_i.mint - c_i-1.maxt <uvarint64>          │          │ │
│ │              ├────────────────────────────────────────────┤          │ │
│ │              │ c_i.maxt - c_i.mint <uvarint64>            │          │ │
│ │              ├────────────────────────────────────────────┤          │ │
│ │              │ ref(c_i.data) - ref(c_i-1.data) <varint64> │          │ │
│ │              └────────────────────────────────────────────┘          │ │
│ │                             ...                                      │ │
│ └──────────────────────────────────────────────────────────────────────┘ │
├──────────────────────────────────────────────────────────────────────────┤
│ CRC32 <4b>                                                               │
└──────────────────────────────────────────────────────────────────────────┘
```

##### Label Index

A label index section indexes the existing (combined) values for one or more label names. The `#names` field determines the number of indexed label names, followed by the total number of entries in the `#entries` field. The body holds #entries / #names tuples of symbol table references, each tuple being of #names length. The value tuples are sorted in lexicographically increasing order. This is no longer used.

```
┌───────────────┬────────────────┬────────────────┐
│ len <4b>      │ #names <4b>    │ #entries <4b>  │
├───────────────┴────────────────┴────────────────┤
│ ┌─────────────────────────────────────────────┐ │
│ │ ref(value_0) <4b>                           │ │
│ ├─────────────────────────────────────────────┤ │
│ │ ...                                         │ │
│ ├─────────────────────────────────────────────┤ │
│ │ ref(value_n) <4b>                           │ │
│ └─────────────────────────────────────────────┘ │
│                      . . .                      │
├─────────────────────────────────────────────────┤
│ CRC32 <4b>                                      │
└─────────────────────────────────────────────────┘
```

For instance, a single label name with 4 different values will be encoded as:

```
┌────┬───┬───┬──────────────┬──────────────┬──────────────┬──────────────┬───────┐
│ 24 │ 1 │ 4 │ ref(value_0) | ref(value_1) | ref(value_2) | ref(value_3) | CRC32 |
└────┴───┴───┴──────────────┴──────────────┴──────────────┴──────────────┴───────┘
```

The sequence of label index sections is finalized by a [label offset table](#label-offset-table) containing label offset entries that points to the beginning of each label index section for a given label name.

##### Postings

Postings sections store monotonically increasing lists of series references that contain a given label pair associated with the list.

```
┌────────────────────┬────────────────────┐
│ len <4b>           │ #entries <4b>      │
├────────────────────┴────────────────────┤
│ ┌─────────────────────────────────────┐ │
│ │ ref(series_1) <4b>                  │ │
│ ├─────────────────────────────────────┤ │
│ │ ...                                 │ │
│ ├─────────────────────────────────────┤ │
│ │ ref(series_n) <4b>                  │ │
│ └─────────────────────────────────────┘ │
├─────────────────────────────────────────┤
│ CRC32 <4b>                              │
└─────────────────────────────────────────┘
```

The sequence of postings sections is finalized by a [postings offset table](#postings-offset-table) containing postings offset entries that points to the beginning of each postings section for a given label pair.

##### Label Offset Table

A label offset table stores a sequence of label offset entries. Every label offset entry holds the label name and the offset to its values in the label index section. They are used to track label index sections. This is no longer used.

```
┌─────────────────────┬──────────────────────┐
│ len <4b>            │ #entries <4b>        │
├─────────────────────┴──────────────────────┤
│ ┌────────────────────────────────────────┐ │
│ │  n = 1 <1b>                            │ │
│ ├──────────────────────┬─────────────────┤ │
│ │ len(name) <uvarint>  │ name <bytes>    │ │
│ ├──────────────────────┴─────────────────┤ │
│ │  offset <uvarint64>                    │ │
│ └────────────────────────────────────────┘ │
│                    . . .                   │
├────────────────────────────────────────────┤
│  CRC32 <4b>                                │
└────────────────────────────────────────────┘
```

##### Postings Offset Table

A postings offset table stores a sequence of postings offset entries, sorted by label name and value. Every postings offset entry holds the label name/value pair and the offset to its series list in the postings section. They are used to track postings sections. They are partially read into memory when an index file is loaded.

```
┌─────────────────────┬──────────────────────┐
│ len <4b>            │ #entries <4b>        │
├─────────────────────┴──────────────────────┤
│ ┌────────────────────────────────────────┐ │
│ │  n = 2 <1b>                            │ │
│ ├──────────────────────┬─────────────────┤ │
│ │ len(name) <uvarint>  │ name <bytes>    │ │
│ ├──────────────────────┼─────────────────┤ │
│ │ len(value) <uvarint> │ value <bytes>   │ │
│ ├──────────────────────┴─────────────────┤ │
│ │  offset <uvarint64>                    │ │
│ └────────────────────────────────────────┘ │
│                    . . .                   │
├────────────────────────────────────────────┤
│  CRC32 <4b>                                │
└────────────────────────────────────────────┘
```

##### TOC

The table of contents serves as an entry point to the entire index and points to various sections in the file. If a reference is zero, it indicates the respective section does not exist and empty results should be returned upon lookup.

```
┌─────────────────────────────────────────┐
│ ref(symbols) <8b>                       │
├─────────────────────────────────────────┤
│ ref(series) <8b>                        │
├─────────────────────────────────────────┤
│ ref(label indices start) <8b>           │
├─────────────────────────────────────────┤
│ ref(label offset table) <8b>            │
├─────────────────────────────────────────┤
│ ref(postings start) <8b>                │
├─────────────────────────────────────────┤
│ ref(postings offset table) <8b>         │
├─────────────────────────────────────────┤
│ CRC32 <4b>                              │
└─────────────────────────────────────────┘
```

#### Chunks File Format

> NOTE: Currently supported index file versions: v1.

> NOTE: Don't confuse with `chunks format` (XOR encoded, Gorilla compressed set of samples). Overall chunk files are containing multiple series chunks (:

The following describes the format of a chunks file, which is created in the `chunks/` directory of a block. The maximum size per segment file is 512MiB.

[Chunks](design.md#chunk) in the files are referenced from the index by uint64 composed of in-file offset (lower 4 bytes) and segment sequence number (upper 4 bytes).

```
┌──────────────────────────────┐
│  magic(0x85BD40DD) <4 byte>  │
├──────────────────────────────┤
│    version(1) <1 byte>       │
├──────────────────────────────┤
│    padding(0) <3 byte>       │
├──────────────────────────────┤
│ ┌──────────────────────────┐ │
│ │         Chunk 1          │ │
│ ├──────────────────────────┤ │
│ │          ...             │ │
│ ├──────────────────────────┤ │
│ │         Chunk N          │ │
│ └──────────────────────────┘ │
└──────────────────────────────┘
```

##### Chunk

```
┌───────────────┬───────────────────┬──────────────┬────────────────┐
│ len <uvarint> │ encoding <1 byte> │ data <bytes> │ CRC32 <4 byte> │
└───────────────┴───────────────────┴──────────────┴────────────────┘
```

#### Tombstones

Thanos ignores any tombstones files. They are also deleted by sidecar on upload.
