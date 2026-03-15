# CONTRIBUTING

## 1. Development Setup

### 1.1. Prerequisites

- [uv](https://docs.astral.sh/uv/) for dependency management
- [Nix](https://nixos.org/) (recommended, for pre-commit hooks)

### 1.2. With Nix (recommended)

```bash
nix develop
```

### 1.3. Without Nix

```bash
uv sync
uv run pytest
```

## 2. Testing

```bash
uv run pytest
```

## 3. Linting and Formatting

Automatically run via pre-commit (installed by `nix develop`).
CI enforces all checks via `nix flake check`.
