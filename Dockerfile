# syntax=docker/dockerfile:1.26@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
# SPDX-FileCopyrightText: 2026 Google LLC
#
# SPDX-License-Identifier: Apache-2.0

#############      builder       #############
FROM --platform=$BUILDPLATFORM golang:1.26.5@sha256:7caba5286b4c3613a337b709c573047d8ae62ee76106647313b61e72b99f20af AS builder
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
