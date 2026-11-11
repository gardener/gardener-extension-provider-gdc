# syntax=docker/dockerfile:1.29@sha256:572cebbc2b15e6f84c077742952674889cb63cd5e9b80abdffcbe587b6f19183
# SPDX-FileCopyrightText: 2026 Google LLC
#
# SPDX-License-Identifier: Apache-2.0

#############      builder       #############
FROM --platform=$BUILDPLATFORM golang:1.27.3@sha256:e519d3b0a5b285e5aa53ab9eef02bc6bbb9cb04608389b743a136a9ce75f0d1e AS builder
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
