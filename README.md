# gardener-extension-provider-gdc

## Introduction

This repository contains the Gardener extension provider for Google Distributed Cloud (GDC).
It enables Gardener to provision and manage Kubernetes clusters natively on GDC infrastructure.

## Components

This repository contains the following components, located in `cmd/` and `gdc-sa-auth-plugin/`:

*   **Extension Provider (`gardener-extension-provider-gdc`)**: The GDC Extension Provider implements the Gardener extension API to manage GDC-specific resources for Shoot clusters (e.g., infrastructure, control plane, workers).
*   **Extension Admission (`gardener-extension-admission-gdc`)**: Admission webhook controller validating and mutating GDC Shoot and CloudProfile configurations.
*   **Service Account Auth Plugin (`gdc-sa-auth-plugin`)**: Executable credential plugin for Kubernetes client authentication with GDC service accounts.

## Development & Build Workflow

This repository uses a standard Go toolchain and `Makefile` matching upstream Gardener build standards.

### Prerequisites
- Go 1.25 or higher
- Docker (for building container images)

### Common Make Targets

| Target | Description |
| :--- | :--- |
| `make format` | Formats all Go source files with `goimports` |
| `make check` | Runs code linters (`golangci-lint`, `go vet`) |
| `make unittests` | Runs unit test suite across all packages |
| `make build-local` | Builds binaries locally in current environment |
| `make release` | Builds cross-compiled release binaries |
| `make docker-images` | Builds multi-stage Docker images |
| `make clean` | Cleans built binaries and test tools cache |

### Managing Dependencies

- **Add a new dependency**:
  ```bash
  go get <package-name>
  go mod tidy
  ```
- **Verify and download dependencies**:
  ```bash
  go mod download
  go mod verify
  ```
- **Format and check code before submitting**:
  ```bash
  make format
  make check
  make unittests
  ```

## Contributing

Contributions are welcome! Please ensure that your changes pass all linters and tests before submitting a Pull Request:

```bash
make format
make check
make test
```

## License

`gardener-extension-provider-gdc` is licensed under the Apache 2.0 license.
