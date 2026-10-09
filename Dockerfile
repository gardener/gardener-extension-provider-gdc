# syntax=docker/dockerfile:1.28@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
# SPDX-FileCopyrightText: 2026 Google LLC
#
# SPDX-License-Identifier: Apache-2.0

#############      builder       #############
FROM --platform=$BUILDPLATFORM golang:1.27.2@sha256:5bc7f572bbaa98885a3a1fd9c0aa76b59e3e14e8628bfc316bbfd0c701e4818c AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /build

# Copy go mod and sum files
COPY go.mod go.sum ./
# Download all dependencies. Cached via BuildKit cache mount independent of layer cache.
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH make release

############# base
FROM gcr.io/distroless/static-debian13:nonroot AS base
WORKDIR /
USER nonroot:nonroot

#############      gardener-extension-provider-gdch     #############
FROM base AS gardener-extension-provider-gdch

COPY --from=builder /build/bin/gardener-extension-provider-gdch /extension-provider

ENTRYPOINT ["/extension-provider"]

#############      gardener-extension-admission-gdch    #############
FROM base AS gardener-extension-admission-gdch

COPY --from=builder /build/bin/gardener-extension-admission-gdch /extension-admission

ENTRYPOINT ["/extension-admission"]

#############      gdch-sa-auth-plugin                  #############
FROM base AS gdch-sa-auth-plugin

COPY --from=builder /build/bin/gdch-sa-auth-plugin /gdch-sa-auth-plugin

ENTRYPOINT ["/gdch-sa-auth-plugin"]
