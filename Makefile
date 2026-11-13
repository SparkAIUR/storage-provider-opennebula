SELF := $(patsubst %/,%,$(dir $(abspath $(firstword $(MAKEFILE_LIST)))))
PATH := $(SELF)/bin:$(PATH)

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

CHARTS_DIR := $(SELF)/_charts
DEPLOY_DIR  := $(SELF)/_deploy

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN := $(shell go env GOPATH)/bin
else
GOBIN := $(shell go env GOBIN)
endif

GOLANGCI_LINT_VERSION		?= 2.15.0
HELM_VERSION				?= 3.21.4

LOCAL_GOLANGCI_LINT := $(SELF)/bin/golangci-lint-v$(GOLANGCI_LINT_VERSION)
LOCAL_HELM := $(SELF)/bin/helm-v$(HELM_VERSION)
GOLANGCI_LINT ?= $(LOCAL_GOLANGCI_LINT)
HELM ?= $(LOCAL_HELM)
HELM_OS := $(shell go env GOHOSTOS)
HELM_ARCH := $(shell go env GOHOSTARCH)

CLOSEST_TAG ?= $(shell git -C $(SELF) describe --tags --abbrev=0 2>/dev/null || echo v0.0.0)
BUILD_VERSION ?= $(shell git -C $(SELF) describe --tags --always --dirty 2>/dev/null || echo dev)
GIT_COMMIT ?= $(shell git -C $(SELF) rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GO_LDFLAGS := -X github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/driver.driverVersion=$(BUILD_VERSION) -X github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/driver.driverCommit=$(GIT_COMMIT) -X github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/driver.driverBuildDate=$(BUILD_DATE)

# Local registry and tag used for building/pushing image targets
LOCAL_TAG ?= latest
LOCAL_REGISTRY ?= localhost:5005
# Registry to use for building/pushing image targets
REMOTE_REGISTRY ?= ghcr.io/sparkaiur
DOCKERHUB_REGISTRY ?= docker.io/nudevco

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Binaries to build
BUILD_BINS := opennebula-csi
# List of container image names to build and push
IMAGE_NAMES := opennebula-csi

-include .env
export LOCAL_REGISTRY LOCAL_TAG ONE_XMLRPC ONE_AUTH DEBUG_PORT WORKLOAD_CLUSTER_NAME WORKLOAD_CLUSTER_KUBECONFIG WORKER_NODES

include Makefile.dev.mk

.PHONY: all clean

all: build

clean: tilt-clean
	rm --preserve-root -rf '$(SELF)/bin/'
	rm --preserve-root -rf '$(DEPLOY_DIR)'
	rm --preserve-root -rf '$(CHARTS_DIR)'

# Development

.PHONY: lint lint-fix fmt vet test

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run

lint-fix: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --fix

fmt:
	go fmt ./...

vet:
	go vet ./...

test:
	go test ./... -v -count=1

# Build

.PHONY: build docker-build docker-push docker-release

build: fmt vet $(addprefix build-,$(BUILD_BINS))

build-%:
	go build -ldflags "$(GO_LDFLAGS)" -o $(SELF)/bin/$* ./cmd/$*/main.go

docker-build: $(addprefix docker-build-,$(IMAGE_NAMES))

docker-build-%:
	$(CONTAINER_TOOL) build \
		--build-arg VERSION=$(BUILD_VERSION) \
		--build-arg COMMIT=$(GIT_COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t $(LOCAL_REGISTRY)/$*:$(LOCAL_TAG) .

docker-push: $(addprefix docker-push-,$(IMAGE_NAMES))

docker-push-%:
	$(CONTAINER_TOOL) push $(LOCAL_REGISTRY)/$*:$(LOCAL_TAG)

# _PLATFORMS defines the target platforms for the manager image be built to provide support to multiple architectures.
# To use this option you need to:
# - be able to use docker buildx (https://docs.docker.com/reference/cli/docker/buildx/)
# - have enabled BuildKit (https://docs.docker.com/build/buildkit/)
# - be able to push the image to your registry
_PLATFORMS ?= linux/amd64,linux/arm64

docker-release: $(addprefix docker-release-,$(IMAGE_NAMES))

docker-release-%:
	-$(CONTAINER_TOOL) buildx create --name $*-builder
	$(CONTAINER_TOOL) buildx use $*-builder
	$(CONTAINER_TOOL) buildx build --push --platform=$(_PLATFORMS) \
		--build-arg VERSION=$(CLOSEST_TAG) \
		--build-arg COMMIT=$(GIT_COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t $(REMOTE_REGISTRY)/$*:$(CLOSEST_TAG) \
		-t $(REMOTE_REGISTRY)/$*:latest \
		-t $(DOCKERHUB_REGISTRY)/$*:$(CLOSEST_TAG) \
		-t $(DOCKERHUB_REGISTRY)/$*:latest \
		-f Dockerfile .
	-$(CONTAINER_TOOL) buildx rm $*-builder

# Helm

.PHONY: helm package helm-deploy helm-undeploy manifests manifests-dev

helm-package: $(HELM)
	install -m u=rwx,go=rx -d $(CHARTS_DIR)/$(CLOSEST_TAG)/opennebula-csi
	$(HELM) package helm/opennebula-csi/  \
	-d $(CHARTS_DIR)/$(CLOSEST_TAG)/opennebula-csi \
	--version $(subst v,,$(CLOSEST_TAG)) \
	--app-version $(CLOSEST_TAG)

helm-deploy: $(HELM) # Deploy OpenNebula CSI plugin using Helm to the cluster specified in ~/.kube/config.
	$(HELM) upgrade --install opennebula-csi helm/opennebula-csi
		--set image.repository=$(REMOTE_REGISTRY)/opennebula-csi \
		--set image.tag=$(CLOSEST_TAG) \
		--set image.pullPolicy="IfNotPresent" \
		--set oneApiEndpoint=$(ONE_XMLRPC) \
		--set credentials.inlineAuth=$(ONE_AUTH)

helm-undeploy: $(HELM) # Undeploy OpenNebula CSI plugin from the cluster specified in ~/.kube/config.
	$(HELM) uninstall opennebula-csi

manifests: $(HELM)
	$(HELM) template opennebula-csi helm/opennebula-csi \
		--set image.repository=$(REMOTE_REGISTRY)/opennebula-csi \
		--set image.tag=$(CLOSEST_TAG) \
		--set image.pullPolicy="IfNotPresent" \
		--set oneApiEndpoint=$(ONE_XMLRPC) \
		--set credentials.inlineAuth=$(ONE_AUTH) \
		| install -m u=rw,go=r -D /dev/fd/0 $(DEPLOY_DIR)/release/opennebula-csi.yaml

manifests-dev: $(HELM)
	$(HELM) template opennebula-csi helm/opennebula-csi \
		--set image.repository=$(LOCAL_REGISTRY)/opennebula-csi \
		--set image.tag=$(LOCAL_TAG) \
		--set image.pullPolicy="Always" \
		--set oneApiEndpoint=$(ONE_XMLRPC) \
		--set credentials.inlineAuth=$(ONE_AUTH) \
		| install -m u=rw,go=r -D /dev/fd/0 $(DEPLOY_DIR)/dev/opennebula-csi.yaml

# Dependencies

.PHONY: golangci-lint helm

golangci-lint: $(GOLANGCI_LINT)
$(LOCAL_GOLANGCI_LINT):
	@mkdir -p "$(@D)"
	@tool_dir=$$(mktemp -d "$(@D)/.golangci-lint.XXXXXX"); \
	trap 'rm -rf "$$tool_dir"' EXIT; \
	GOBIN="$$tool_dir" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v$(GOLANGCI_LINT_VERSION); \
	mv "$$tool_dir/golangci-lint" "$@"

helm: $(HELM)
$(LOCAL_HELM):
	@mkdir -p "$(@D)"
	@tool_dir=$$(mktemp -d "$(@D)/.helm.XXXXXX"); \
	trap 'rm -rf "$$tool_dir"' EXIT; \
	curl -fsSL https://get.helm.sh/helm-v$(HELM_VERSION)-$(HELM_OS)-$(HELM_ARCH).tar.gz \
		| tar -xzO -f- $(HELM_OS)-$(HELM_ARCH)/helm > "$$tool_dir/helm"; \
	chmod 755 "$$tool_dir/helm"; \
	mv "$$tool_dir/helm" "$@"
