@develop:
  maturin develop

@install: && develop
  python -m pip install -r requirements-dev.txt

@lint:
  echo cargo check
  just --justfile {{justfile()}} check
  echo cargo clippy
  just --justfile {{justfile()}} clippy
  echo cargo fmt
  just --justfile {{justfile()}} fmt
  echo mypy
  just --justfile {{justfile()}} mypy
  echo ruff linting
  just --justfile {{justfile()}} ruff
  echo ruff formatting
  just --justfile {{justfile()}} ruff-format

@check:
  cargo check

@clippy:
  cargo clippy --all-targets

@fmt:
  cargo fmt --all -- --check

@mypy:
  mypy .

@ruff:
  ruff check . --fix

@ruff-format:
  ruff format python/mary_elizabeth_utils tests

@test:
  pytest
