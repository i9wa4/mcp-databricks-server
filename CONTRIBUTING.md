# CONTRIBUTING

## 1. Development Setup

### 1.1. Prerequisites

- [mise](https://mise.jdx.dev/getting-started.html)

### 1.2. Setup

```bash
# Install pre-commit hooks
mise exec -- pre-commit install
```

## 2. Testing

```bash
mise run test
```

## 3. Linting and Formatting

Automatically run via pre-commit. To run manually:

```bash
mise exec -- pre-commit run --all-files
```
