include .bingo/Variables.mk
FILES_TO_FMT      ?= $(shell find . -path ./vendor -prune -o -name '*.go' -print)
MD_FILES_TO_FORMAT = $(shell find docs -name "*.md") $(shell ls *.md)

DOCKER_IMAGE_REPO ?= quay.io/thanos/thanos
DOCKER_IMAGE_TAG  ?= $(subst /,-,$(shell git rev-parse --abbrev-ref HEAD))-$(shell date +%Y-%m-%d)-$(shell git rev-parse --short HEAD)
DOCKER_CI_TAG     ?= test

BASE_DOCKER_SHA=''
arch = $(shell uname -m)
# Run `DOCKER_CLI_EXPERIMENTAL=enabled docker manifest inspect quay.io/prometheus/busybox:latest` to get SHA or
# just visit https://quay.io/repository/prometheus/busybox?tag=latest&tab=tags.
# TODO(bwplotka): Pinning is important but somehow quay kills the old images, so make sure to update regularly.
# Update at 2021.6.07
ifeq ($(arch), x86_64)
    # amd64
    BASE_DOCKER_SHA="de4af55df1f648a334e16437c550a2907e0aed4f0b0edf454b0b215a9349bdbb"
else ifeq ($(arch), armv8)
    # arm64
    BASE_DOCKER_SHA="5591971699f6cf8abf6776495385e9d62751111a8cba56bf4946cf1d0de425ed"
else
    echo >&2 "only support amd64 or arm64 arch" && exit 1
endif

# Ensure everything works even if GOPATH is not set, which is often the case.
# The `go env GOPATH` will work for all cases for Go 1.8+.
GOPATH            ?= $(shell go env GOPATH)
TMP_GOPATH        ?= /tmp/thanos-go
GOBIN             ?= $(firstword $(subst :, ,${GOPATH}))/bin
export GOBIN

# Promu is using this exact variable name, do not rename.
PREFIX  ?= $(GOBIN)

GO111MODULE       ?= on
export GO111MODULE
GOPROXY           ?= https://proxy.golang.org
export GOPROXY

GOTEST_OPTS ?= -failfast -timeout 10m -v
BIN_DIR ?= $(shell pwd)/tmp/bin
OS ?= $(shell uname -s | tr '[A-Z]' '[a-z]')
ARCH ?= $(shell uname -m)

# Tools.
PROTOC            ?= $(GOBIN)/protoc-$(PROTOC_VERSION)
PROTOC_VERSION    ?= 3.4.0
GIT               ?= $(shell which git)

# Support gsed on OSX (installed via brew), falling back to sed. On Linux
# systems gsed won't be installed, so will use sed as expected.
SED ?= $(shell which gsed 2>/dev/null || which sed)

THANOS_MIXIN            ?= mixin
JSONNET_VENDOR_DIR      ?= mixin/vendor

WEB_DIR           ?= website
WEBSITE_BASE_URL  ?= https://thanos.io
MDOX_VALIDATE_CONFIG ?= .mdox.validate.yaml
# for website pre process
export MDOX
PUBLIC_DIR        ?= $(WEB_DIR)/public
ME                ?= $(shell whoami)

REACT_APP_PATH = pkg/ui/react-app
REACT_APP_SOURCE_FILES = $(shell find $(REACT_APP_PATH)/public/ $(REACT_APP_PATH)/src/ $(REACT_APP_PATH)/tsconfig.json)
REACT_APP_OUTPUT_DIR = pkg/ui/static/react
REACT_APP_NODE_MODULES_PATH = $(REACT_APP_PATH)/node_modules

define require_clean_work_tree
	@git update-index -q --ignore-submodules --refresh

	@if ! git diff-files --quiet --ignore-submodules --; then \
		echo >&2 "cannot $1: you have unstaged changes."; \
		git diff-files --name-status -r --ignore-submodules -- >&2; \
		echo >&2 "Please commit or stash them."; \
		exit 1; \
	fi

	@if ! git diff-index --cached --quiet HEAD --ignore-submodules --; then \
		echo >&2 "cannot $1: your index contains uncommitted changes."; \
		git diff-index --cached --name-status -r --ignore-submodules HEAD -- >&2; \
		echo >&2 "Please commit or stash them."; \
		exit 1; \
	fi

endef

help: ## Displays help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-z0-9A-Z_-]+:.*?##/ { printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: all
all: format build

$(REACT_APP_NODE_MODULES_PATH): $(REACT_APP_PATH)/package.json $(REACT_APP_PATH)/yarn.lock
	   cd $(REACT_APP_PATH) && yarn --frozen-lockfile

$(REACT_APP_OUTPUT_DIR): $(REACT_APP_NODE_MODULES_PATH) $(REACT_APP_SOURCE_FILES)
	   @echo ">> building React app"
	   @scripts/build-react-app.sh

.PHONY: assets
assets: # Repacks all static assets into go file for easier deploy.
assets: $(GO_BINDATA) $(REACT_APP_OUTPUT_DIR)
	@echo ">> deleting asset file"
	@rm pkg/ui/bindata.go || true
	@echo ">> writing assets"
	@$(GO_BINDATA) $(bindata_flags) -pkg ui -o pkg/ui/bindata.go -ignore '(.*\.map|bootstrap\.js|bootstrap-theme\.css|bootstrap\.css)'  pkg/ui/templates/... pkg/ui/static/...
	@$(MAKE) format

.PHONY: react-app-lint
react-app-lint: $(REACT_APP_NODE_MODULES_PATH)
	   @echo ">> running React app linting"
	   cd $(REACT_APP_PATH) && yarn lint:ci

.PHONY: react-app-lint-fix
react-app-lint-fix:
	@echo ">> running React app linting and fixing errors where possible"
	cd $(REACT_APP_PATH) && yarn lint

.PHONY: react-app-test
react-app-test: | $(REACT_APP_NODE_MODULES_PATH) react-app-lint
	@echo ">> running React app tests"
	cd $(REACT_APP_PATH) && export CI=true && yarn test --no-watch

.PHONY: react-app-start
react-app-start: $(REACT_APP_NODE_MODULES_PATH)
	@echo ">> running React app"
	cd $(REACT_APP_PATH) && yarn start

.PHONY: build
build: ## Builds Thanos binary using `promu`.
build: check-git deps $(PROMU)
	@echo ">> building Thanos binary in $(PREFIX)"
	@$(PROMU) build --prefix $(PREFIX)

.PHONY: crossbuild
crossbuild: ## Builds all binaries for all platforms.
crossbuild: | $(PROMU)
	@echo ">> crossbuilding all binaries"
	$(PROMU) crossbuild -v

.PHONY: deps
deps: ## Ensures fresh go.mod and go.sum.
	@go mod tidy
	@go mod verify

.PHONY: docker
docker: ## Builds 'thanos' docker with no tag.
ifeq ($(OS)_$(ARCH), linux_x86_64)
docker: build
	@echo ">> copying Thanos from $(PREFIX) to ./thanos_tmp_for_docker"
	@cp $(PREFIX)/thanos ./thanos_tmp_for_docker
	@echo ">> building docker image 'thanos'"
	@docker build -t "thanos" --build-arg BASE_DOCKER_SHA=$(BASE_DOCKER_SHA) .
	@rm ./thanos_tmp_for_docker
else
docker: docker-multi-stage
endif

.PHONY: docker-multi-stage
docker-multi-stage: ## Builds 'thanos' docker image using multi-stage.
docker-multi-stage:
	@echo ">> building docker image 'thanos' with Dockerfile.multi-stage"
	@docker build -f Dockerfile.multi-stage -t "thanos" --build-arg BASE_DOCKER_SHA=$(BASE_DOCKER_SHA) .

.PHONY: docker-push
docker-push: ## Pushes 'thanos' docker image build to "$(DOCKER_IMAGE_REPO):$(DOCKER_IMAGE_TAG)".
docker-push:
	@echo ">> pushing image"
	@docker tag "thanos" "$(DOCKER_IMAGE_REPO):$(DOCKER_IMAGE_TAG)"
	@docker push "$(DOCKER_IMAGE_REPO):$(DOCKER_IMAGE_TAG)"

.PHONY: docs
docs: ## Regenerates flags in docs for all thanos commands localise links, ensure GitHub format.
docs: $(MDOX) build
	@echo ">> generating docs"
	PATH=${PATH}:$(GOBIN) $(MDOX) fmt -l --links.localize.address-regex="https://thanos.io/.*" --links.validate.config-file=$(MDOX_VALIDATE_CONFIG) $(MD_FILES_TO_FORMAT)

.PHONY: check-docs
check-docs: ## checks docs against discrepancy with flags, links, white noise.
check-docs: $(MDOX) build
	@echo ">> checking formatting and local/remote links"
	PATH=${PATH}:$(GOBIN) $(MDOX) fmt --check -l --links.localize.address-regex="https://thanos.io/.*" --links.validate.config-file=$(MDOX_VALIDATE_CONFIG) $(MD_FILES_TO_FORMAT)
	$(call require_clean_work_tree,'run make docs and commit changes')

.PHONY: shell-format
shell-format: $(SHFMT)
	@echo ">> formatting shell scripts"
	@$(SHFMT) -i 2 -ci -w -s $(shell find . -type f -name "*.sh" -not -path "*vendor*" -not -path "tmp/*")

.PHONY: format
format: ## Formats code including imports and cleans up white noise.
format: go-format shell-format
	@SED_BIN="$(SED)" scripts/cleanup-white-noise.sh $(FILES_TO_FMT)

.PHONY: go-format
go-format: ## Formats Go code including imports.
go-format: $(GOIMPORTS)
	@echo ">> formatting go code"
	@gofmt -s -w $(FILES_TO_FMT)
	@$(GOIMPORTS) -w $(FILES_TO_FMT)

.PHONY: proto
proto: ## Generates Go files from Thanos proto files.
proto: check-git $(GOIMPORTS) $(PROTOC) $(PROTOC_GEN_GOGOFAST)
	@GOIMPORTS_BIN="$(GOIMPORTS)" PROTOC_BIN="$(PROTOC)" PROTOC_GEN_GOGOFAST_BIN="$(PROTOC_GEN_GOGOFAST)" scripts/genproto.sh

.PHONY: tarballs-release
tarballs-release: ## Build tarballs.
tarballs-release: $(PROMU)
	@echo ">> Publishing tarballs"
	$(PROMU) crossbuild -v tarballs
	$(PROMU) checksum -v .tarballs
	$(PROMU) release -v .tarballs

.PHONY: test
test: ## Runs all Thanos Go unit tests against each supported version of Prometheus. This excludes tests in ./test/e2e.
test: export GOCACHE= $(TMP_GOPATH)/gocache
test: export THANOS_TEST_MINIO_PATH= $(MINIO)
test: export THANOS_TEST_PROMETHEUS_PATHS= $(PROMETHEUS_ARRAY)
test: export THANOS_TEST_ALERTMANAGER_PATH= $(ALERTMANAGER)
test: check-git install-deps
	@echo ">> install thanos GOOPTS=${GOOPTS}"
	@echo ">> running unit tests (without /test/e2e). Do export THANOS_TEST_OBJSTORE_SKIP=GCS,S3,AZURE,SWIFT,COS,ALIYUNOSS if you want to skip e2e tests against all real store buckets. Current value: ${THANOS_TEST_OBJSTORE_SKIP}"
	@go test $(shell go list ./... | grep -v /vendor/ | grep -v /test/e2e);

.PHONY: test-local
test-local: ## Runs test excluding tests for ALL  object storage integrations.
test-local: export THANOS_TEST_OBJSTORE_SKIP=GCS,S3,AZURE,SWIFT,COS,ALIYUNOSS
test-local:
	$(MAKE) test

.PHONY: test-e2e
test-e2e: ## Runs all Thanos e2e docker-based e2e tests from test/e2e. Required access to docker daemon.
test-e2e: docker
	@echo ">> cleaning docker environment."
	@docker system prune -f --volumes
	@echo ">> cleaning e2e test garbage."
	@rm -rf ./test/e2e/e2e_integration_test*
	@echo ">> running /test/e2e tests."
	# NOTE(bwplotka):
	# * If you see errors on CI (timeouts), but not locally, try to add -parallel 1 to limit to single CPU to reproduce small 1CPU machine.
	@go test $(GOTEST_OPTS) ./test/e2e/...

.PHONY: test-e2e-local
test-e2e-local: ## Runs all thanos e2e tests locally.
test-e2e-local: export THANOS_TEST_OBJSTORE_SKIP=GCS,S3,AZURE,SWIFT,COS,ALIYUNOSS
test-e2e-local:
	$(MAKE) test-e2e

.PHONY: quickstart
quickstart: ## Installs and runs a quickstart example of thanos.
quickstart: build install-deps
quickstart:
	scripts/quickstart.sh

.PHONY: install-deps
install-deps: ## Installs dependencies for integration tests. It installs supported versions of Prometheus and alertmanager to test against in integration tests.
install-deps: $(ALERTMANAGER) $(MINIO) $(PROMETHEUS_ARRAY)
	@echo ">>GOBIN=$(GOBIN)"

.PHONY: check-git
check-git:
ifneq ($(GIT),)
	@test -x $(GIT) || (echo >&2 "No git executable binary found at $(GIT)."; exit 1)
else
	@echo >&2 "No git binary found."; exit 1
endif

.PHONY: web-pre-process
web-pre-process: $(MDOX)
	@echo ">> running documentation website pre processing"
	scripts/website/websitepreprocess.sh

.PHONY: web
web: ## Builds our website.
web: web-pre-process $(HUGO)
	@echo ">> building documentation website"
	@rm -rf "$(WEB_DIR)/public"
	@cd $(WEB_DIR) && HUGO_ENV=production $(HUGO) --config hugo.yaml --minify -v -b $(WEBSITE_BASE_URL)

.PHONY: web-serve
web-serve: ## Builds and serves Thanos website on localhost.
web-serve: web-pre-process $(HUGO)
	@echo ">> serving documentation website"
	@cd $(WEB_DIR) && $(HUGO) --config hugo.yaml -v server

.PHONY:lint
lint: ## Runs various static analysis against our code.
lint: go-lint react-app-lint shell-lint
	@echo ">> detecting white noise"
	@find . -type f \( -name "*.md" -o -name "*.go" \) | SED_BIN="$(SED)" xargs scripts/cleanup-white-noise.sh
	$(call require_clean_work_tree,'detected white noise, run make lint and commit changes')

# PROTIP:
# Add
#      --cpu-profile-path string   Path to CPU profile output file
#      --mem-profile-path string   Path to memory profile output file
# to debug big allocations during linting.
.PHONY: go-lint
go-lint: check-git deps $(GOLANGCI_LINT) $(FAILLINT)
	$(call require_clean_work_tree,'detected not clean work tree before running lint, previous job changed something?')
	@echo ">> verifying modules being imported"
	@# TODO(bwplotka): Add, Printf, DefaultRegisterer, NewGaugeFunc and MustRegister once exception are accepted. Add fmt.{Errorf}=github.com/pkg/errors.{Errorf} once https://github.com/fatih/faillint/issues/10 is addressed.
	@$(FAILLINT) -paths "errors=github.com/pkg/errors,\
github.com/prometheus/tsdb=github.com/prometheus/prometheus/tsdb,\
github.com/prometheus/prometheus/pkg/testutils=github.com/thanos-io/thanos/pkg/testutil,\
github.com/prometheus/client_golang/prometheus.{DefaultGatherer,DefBuckets,NewUntypedFunc,UntypedFunc},\
github.com/prometheus/client_golang/prometheus.{NewCounter,NewCounterVec,NewCounterVec,NewGauge,NewGaugeVec,NewGaugeFunc,\
NewHistorgram,NewHistogramVec,NewSummary,NewSummaryVec}=github.com/prometheus/client_golang/prometheus/promauto.{NewCounter,\
NewCounterVec,NewCounterVec,NewGauge,NewGaugeVec,NewGaugeFunc,NewHistorgram,NewHistogramVec,NewSummary,NewSummaryVec},\
sync/atomic=go.uber.org/atomic" ./...
	@$(FAILLINT) -paths "fmt.{Print,Println,Sprint}" -ignore-tests ./...
	@echo ">> linting all of the Go files GOGC=${GOGC}"
	@$(GOLANGCI_LINT) run
	@echo ">> ensuring Copyright headers"
	@go run ./scripts/copyright
	@echo ">> ensuring generated proto files are up to date"
	@$(MAKE) proto
	$(call require_clean_work_tree,'detected files without copyright, run make lint and commit changes')

.PHONY: shell-lint
shell-lint: ## Runs static analysis against our shell scripts.
shell-lint: $(SHELLCHECK)
	@echo ">> linting all of the shell script files"
	@$(SHELLCHECK) --severity=error -o all -s bash $(shell find . -type f -name "*.sh" -not -path "*vendor*" -not -path "tmp/*" -not -path "*node_modules*")

.PHONY: examples
examples: jsonnet-vendor jsonnet-format $(EMBEDMD) ${THANOS_MIXIN}/README.md examples/alerts/alerts.md examples/alerts/alerts.yaml examples/alerts/rules.yaml examples/dashboards examples/tmp mixin/runbook.md
	$(EMBEDMD) -w examples/alerts/alerts.md
	$(EMBEDMD) -w ${THANOS_MIXIN}/README.md

.PHONY: examples/tmp
examples/tmp:
	-rm -rf examples/tmp/
	-mkdir -p examples/tmp/
	$(JSONNET) -J ${JSONNET_VENDOR_DIR} -m examples/tmp/ ${THANOS_MIXIN}/separated_alerts.jsonnet | xargs -I{} sh -c 'cat {} | $(GOJSONTOYAML) > {}.yaml; rm -f {}' -- {}

.PHONY: examples/dashboards # to keep examples/dashboards/dashboards.md.
examples/dashboards: $(JSONNET) ${THANOS_MIXIN}/mixin.libsonnet ${THANOS_MIXIN}/config.libsonnet ${THANOS_MIXIN}/dashboards/*
	-rm -rf examples/dashboards/*.json
	$(JSONNET) -J ${JSONNET_VENDOR_DIR} -m examples/dashboards ${THANOS_MIXIN}/dashboards.jsonnet

examples/alerts/alerts.yaml: $(JSONNET) $(GOJSONTOYAML) ${THANOS_MIXIN}/mixin.libsonnet ${THANOS_MIXIN}/config.libsonnet ${THANOS_MIXIN}/alerts/*
	$(JSONNET) ${THANOS_MIXIN}/alerts.jsonnet | $(GOJSONTOYAML) > $@

examples/alerts/rules.yaml: $(JSONNET) $(GOJSONTOYAML) ${THANOS_MIXIN}/mixin.libsonnet ${THANOS_MIXIN}/config.libsonnet ${THANOS_MIXIN}/rules/*
	$(JSONNET) ${THANOS_MIXIN}/rules.jsonnet | $(GOJSONTOYAML) > $@

.PHONY: mixin/runbook.md
mixin/runbook.md: $(PROMDOC) examples/alerts/alerts.yaml
	$(PROMDOC) generate  examples/alerts/alerts.yaml -i mixin -o $@

.PHONY: jsonnet-vendor
jsonnet-vendor: $(JB) $(THANOS_MIXIN)/jsonnetfile.json $(THANOS_MIXIN)/jsonnetfile.lock.json
	rm -rf ${JSONNET_VENDOR_DIR}
	cd ${THANOS_MIXIN} && $(JB) install

JSONNETFMT_CMD := $(JSONNETFMT) -n 2 --max-blank-lines 2 --string-style s --comment-style s

.PHONY: jsonnet-format
jsonnet-format: $(JSONNETFMT)
	find . -name 'vendor' -prune -o -name '*.libsonnet' -print -o -name '*.jsonnet' -print | \
		xargs -n 1 -- $(JSONNETFMT_CMD) -i

.PHONY: jsonnet-lint
jsonnet-lint: $(JSONNET_LINT) ${JSONNET_VENDOR_DIR}
	find . -name 'vendor' -prune -o -name '*.libsonnet' -print -o -name '*.jsonnet' -print | \
		xargs -n 1 -- $(JSONNET_LINT) -J ${JSONNET_VENDOR_DIR}

.PHONY: example-rules-lint
example-rules-lint: $(PROMTOOL) examples/alerts/alerts.yaml examples/alerts/rules.yaml
	$(PROMTOOL) check rules examples/alerts/alerts.yaml examples/alerts/rules.yaml
	$(PROMTOOL) test rules examples/alerts/tests.yaml

.PHONY: check-examples
check-examples: examples example-rules-lint
	$(call require_clean_work_tree,'all generated files should be committed, run make check-examples and commit changes.')

.PHONY: examples-clean
examples-clean:
	rm -f examples/alerts/alerts.yaml
	rm -f examples/alerts/rules.yaml
	rm -f examples/dashboards/*.json
	rm -f examples/tmp/*.yaml

# non-phony targets
$(BIN_DIR):
	mkdir -p $(BIN_DIR)

SHELLCHECK ?= $(BIN_DIR)/shellcheck
$(SHELLCHECK): $(BIN_DIR)
	@echo "Downloading Shellcheck"
	curl -sNL "https://github.com/koalaman/shellcheck/releases/download/stable/shellcheck-stable.$(OS).$(ARCH).tar.xz" | tar --strip-components=1 -xJf - -C $(BIN_DIR)

$(PROTOC):
	@mkdir -p $(TMP_GOPATH)
	@echo ">> fetching protoc@${PROTOC_VERSION}"
	@PROTOC_VERSION="$(PROTOC_VERSION)" TMP_GOPATH="$(TMP_GOPATH)" scripts/installprotoc.sh
	@echo ">> installing protoc@${PROTOC_VERSION}"
	@mv -- "$(TMP_GOPATH)/bin/protoc" "$(GOBIN)/protoc-$(PROTOC_VERSION)"
	@echo ">> produced $(GOBIN)/protoc-$(PROTOC_VERSION)"

