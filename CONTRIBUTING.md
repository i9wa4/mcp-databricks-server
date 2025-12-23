# Contributing

## Development Setup

### Prerequisites

- Python 3.10+
- [mise](https://mise.jdx.dev/getting-started.html)

### Setup

```bash
# Install tools
mise install

# Sync dependencies
mise run sync
```

## Testing

```bash
mise run test
```

## Linting and Formatting

```bash
uv run ruff check src tests
uv run ruff format src tests
uv run mypy src
```

## Pre-commit

```bash
# Install hooks
pre-commit install

# Run manually
pre-commit run --all-files
```
