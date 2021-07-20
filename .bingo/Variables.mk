# Auto generated binary variables helper managed by https://github.com/bwplotka/bingo v0.4.3. DO NOT EDIT.
# All tools are designed to be build inside $GOBIN.
BINGO_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
GOPATH ?= $(shell go env GOPATH)
GOBIN  ?= $(firstword $(subst :, ,${GOPATH}))/bin
GO     ?= $(shell which go)

# Below generated variables ensure that every time a tool under each variable is invoked, the correct version
# will be used; reinstalling only if needed.
# For example for alertmanager variable:
#
# In your main Makefile (for non array binaries):
#
#include .bingo/Variables.mk # Assuming -dir was set to .bingo .
#
#command: $(ALERTMANAGER)
#	@echo "Running alertmanager"
#	@$(ALERTMANAGER) <flags/args..>
#
ALERTMANAGER := $(GOBIN)/alertmanager-v0.20.0
$(ALERTMANAGER): $(BINGO_DIR)/alertmanager.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/alertmanager-v0.20.0"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=alertmanager.mod -o=$(GOBIN)/alertmanager-v0.20.0 "github.com/prometheus/alertmanager/cmd/alertmanager"

BINGO := $(GOBIN)/bingo-v0.4.3
$(BINGO): $(BINGO_DIR)/bingo.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/bingo-v0.4.3"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=bingo.mod -o=$(GOBIN)/bingo-v0.4.3 "github.com/bwplotka/bingo"

FAILLINT := $(GOBIN)/faillint-v1.5.0
$(FAILLINT): $(BINGO_DIR)/faillint.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/faillint-v1.5.0"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=faillint.mod -o=$(GOBIN)/faillint-v1.5.0 "github.com/fatih/faillint"

GO_BINDATA := $(GOBIN)/go-bindata-v3.1.1+incompatible
$(GO_BINDATA): $(BINGO_DIR)/go-bindata.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/go-bindata-v3.1.1+incompatible"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=go-bindata.mod -o=$(GOBIN)/go-bindata-v3.1.1+incompatible "github.com/go-bindata/go-bindata/go-bindata"

GOIMPORTS := $(GOBIN)/goimports-v0.0.0-20200526224456-8b020aee10d2
$(GOIMPORTS): $(BINGO_DIR)/goimports.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/goimports-v0.0.0-20200526224456-8b020aee10d2"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=goimports.mod -o=$(GOBIN)/goimports-v0.0.0-20200526224456-8b020aee10d2 "golang.org/x/tools/cmd/goimports"

GOJSONTOYAML := $(GOBIN)/gojsontoyaml-v0.0.0-20191212081931-bf2969bbd742
$(GOJSONTOYAML): $(BINGO_DIR)/gojsontoyaml.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/gojsontoyaml-v0.0.0-20191212081931-bf2969bbd742"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=gojsontoyaml.mod -o=$(GOBIN)/gojsontoyaml-v0.0.0-20191212081931-bf2969bbd742 "github.com/brancz/gojsontoyaml"

GOLANGCI_LINT := $(GOBIN)/golangci-lint-v1.39.1-0.20210330125642-6844f6abf817
$(GOLANGCI_LINT): $(BINGO_DIR)/golangci-lint.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/golangci-lint-v1.39.1-0.20210330125642-6844f6abf817"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=golangci-lint.mod -o=$(GOBIN)/golangci-lint-v1.39.1-0.20210330125642-6844f6abf817 "github.com/golangci/golangci-lint/cmd/golangci-lint"

HUGO := $(GOBIN)/hugo-v0.80.0
$(HUGO): $(BINGO_DIR)/hugo.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/hugo-v0.80.0"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=hugo.mod -o=$(GOBIN)/hugo-v0.80.0 "github.com/gohugoio/hugo"

JB := $(GOBIN)/jb-v0.4.0
$(JB): $(BINGO_DIR)/jb.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jb-v0.4.0"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=jb.mod -o=$(GOBIN)/jb-v0.4.0 "github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb"

JSONNET_LINT := $(GOBIN)/jsonnet-lint-v0.17.1-0.20210113194615-cd59751527e0
$(JSONNET_LINT): $(BINGO_DIR)/jsonnet-lint.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jsonnet-lint-v0.17.1-0.20210113194615-cd59751527e0"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=jsonnet-lint.mod -o=$(GOBIN)/jsonnet-lint-v0.17.1-0.20210113194615-cd59751527e0 "github.com/google/go-jsonnet/cmd/jsonnet-lint"

JSONNET := $(GOBIN)/jsonnet-v0.17.0
$(JSONNET): $(BINGO_DIR)/jsonnet.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jsonnet-v0.17.0"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=jsonnet.mod -o=$(GOBIN)/jsonnet-v0.17.0 "github.com/google/go-jsonnet/cmd/jsonnet"

JSONNETFMT := $(GOBIN)/jsonnetfmt-v0.17.0
$(JSONNETFMT): $(BINGO_DIR)/jsonnetfmt.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jsonnetfmt-v0.17.0"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=jsonnetfmt.mod -o=$(GOBIN)/jsonnetfmt-v0.17.0 "github.com/google/go-jsonnet/cmd/jsonnetfmt"

MDOX := $(GOBIN)/mdox-v0.2.2-0.20210617120014-ffae630a4141
$(MDOX): $(BINGO_DIR)/mdox.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/mdox-v0.2.2-0.20210617120014-ffae630a4141"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=mdox.mod -o=$(GOBIN)/mdox-v0.2.2-0.20210617120014-ffae630a4141 "github.com/bwplotka/mdox"

MINIO := $(GOBIN)/minio-v0.0.0-20200527010300-cccf2de129da
$(MINIO): $(BINGO_DIR)/minio.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/minio-v0.0.0-20200527010300-cccf2de129da"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=minio.mod -o=$(GOBIN)/minio-v0.0.0-20200527010300-cccf2de129da "github.com/minio/minio"

PROMDOC := $(GOBIN)/promdoc-v0.7.0
$(PROMDOC): $(BINGO_DIR)/promdoc.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/promdoc-v0.7.0"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=promdoc.mod -o=$(GOBIN)/promdoc-v0.7.0 "github.com/plexsystems/promdoc"

PROMETHEUS_ARRAY := $(GOBIN)/prometheus-v2.4.3+incompatible $(GOBIN)/prometheus-v1.8.2-0.20200724121523-657ba532e42f $(GOBIN)/prometheus-v1.8.2-0.20210331101223-3cafc58827d1
$(PROMETHEUS_ARRAY): $(BINGO_DIR)/prometheus.mod $(BINGO_DIR)/prometheus.1.mod $(BINGO_DIR)/prometheus.2.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/prometheus-v2.4.3+incompatible"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=prometheus.mod -o=$(GOBIN)/prometheus-v2.4.3+incompatible "github.com/prometheus/prometheus/cmd/prometheus"
	@echo "(re)installing $(GOBIN)/prometheus-v1.8.2-0.20200724121523-657ba532e42f"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=prometheus.1.mod -o=$(GOBIN)/prometheus-v1.8.2-0.20200724121523-657ba532e42f "github.com/prometheus/prometheus/cmd/prometheus"
	@echo "(re)installing $(GOBIN)/prometheus-v1.8.2-0.20210331101223-3cafc58827d1"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=prometheus.2.mod -o=$(GOBIN)/prometheus-v1.8.2-0.20210331101223-3cafc58827d1 "github.com/prometheus/prometheus/cmd/prometheus"

PROMTOOL := $(GOBIN)/promtool-v1.8.2-0.20200522113006-f4dd45609a05
$(PROMTOOL): $(BINGO_DIR)/promtool.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/promtool-v1.8.2-0.20200522113006-f4dd45609a05"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=promtool.mod -o=$(GOBIN)/promtool-v1.8.2-0.20200522113006-f4dd45609a05 "github.com/prometheus/prometheus/cmd/promtool"

PROMU := $(GOBIN)/promu-v0.5.0
$(PROMU): $(BINGO_DIR)/promu.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/promu-v0.5.0"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=promu.mod -o=$(GOBIN)/promu-v0.5.0 "github.com/prometheus/promu"

PROTOC_GEN_GOGOFAST := $(GOBIN)/protoc-gen-gogofast-v1.3.2
$(PROTOC_GEN_GOGOFAST): $(BINGO_DIR)/protoc-gen-gogofast.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/protoc-gen-gogofast-v1.3.2"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=protoc-gen-gogofast.mod -o=$(GOBIN)/protoc-gen-gogofast-v1.3.2 "github.com/gogo/protobuf/protoc-gen-gogofast"

SHFMT := $(GOBIN)/shfmt-v3.1.2
$(SHFMT): $(BINGO_DIR)/shfmt.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/shfmt-v3.1.2"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=shfmt.mod -o=$(GOBIN)/shfmt-v3.1.2 "mvdan.cc/sh/v3/cmd/shfmt"

