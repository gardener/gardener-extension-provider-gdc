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
| `make test-integration` | Runs presubmit integration tests against GDC |
| `make build-local` | Builds binaries locally in current environment |
| `make release` | Builds cross-compiled release binaries |
| `make docker-images` | Builds multi-stage Docker images |
| `make clean` | Cleans built binaries and test tools cache |

### Presubmit Integration Tests

The `Presubmit Integration Test` GitHub Actions workflow (`.github/workflows/integration-test.yaml`) runs automatically on same-repository Pull Requests targeting `main` authored by repository maintainers (`OWNERS_ALIASES` / `CODEOWNERS`).

For automated bot PRs (such as Renovate), forked PRs, or manual re-runs, an authorized maintainer can trigger the presubmit integration test using any of the following methods:

1. **PR Comment (Recommended):**
   Leave a comment on the Pull Request to run the full suite:
   ```text
   /test-integration
   ```
   Or specify a comma-separated list of controllers/webhooks to run a subset:
   ```text
   /test-integration ControlplaneController,WorkerController
   ```
   Supported options: `BackupController`, `BastionController`, `ControlplaneController`, `DNSRecordController`, `InfraController`, `WorkerController`, `ExtensionProviderWebhook`.
2. **GitHub Actions UI (`workflow_dispatch`):**
   Navigate to **Actions → Presubmit Integration Test → Run workflow**, keep **Use workflow from: `Branch: main`**, enter the target Pull Request number in **`pr_number`** (and optional **`controllers`**), and click **Run workflow**.
3. **GitHub CLI (`gh`):**
   ```bash
   gh workflow run integration-test.yaml \
     --repo gardener/gardener-extension-provider-gdc \
     --ref main \
     -f pr_number=<PR_NUMBER> \
     -f controllers=<OPTIONAL_CONTROLLERS>
   ```

When triggered manually on a PR, the workflow merges `origin/main` into the PR branch under test and posts the `Extension Provider Integration Test (GDC Staging)` check-run result directly onto the PR's head commit.

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
