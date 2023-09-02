IMAGE_REPO ?= ghcr.io/galleybytes
IMAGE_NAME ?= fswatch
VERSION ?= $(shell  git describe --tags --dirty)
ifeq ($(VERSION),)
VERSION := 0.0.0
endif
IMG ?= ${IMAGE_REPO}/${IMAGE_NAME}:${VERSION}

RELEASE_PROJECT = true

build:
	docker build . -t ${IMG}

reload-to-kind: build
	kind load docker-image ${IMG}

release: build
	docker push ${IMG}

.PHONY: build reload-to-kind release
