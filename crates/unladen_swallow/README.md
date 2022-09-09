## Developing

The Python extension is written using [PyO3](https://pyo3.rs/v0.16.2/)
together with [maturin](https://github.com/PyO3/maturin).

### One-time setup

```
# Run the command from the extension directory, not the git root
# cd crates/unladen_swallow

python -m venv .env
source .env/bin/activate
pip install -r requirements/dev.txt
```

### Once-per-new-terminal setup

Make sure that you're activating the venv in every new terminal where you want to develop.

```
source .env/bin/activate
```

### Building the extension

Build in debug mode.

```
maturin develop
```

Build in release mode.

```
maturin develop --release
```

Rebuild the extension in release mode 2 seconds after the last file change.
This is an optional step.

```
cargo watch --shell 'maturin develop --release' --delay 2
```

### Testing

Running the tests

```
pytest tests
```

### Formatting and linting

```
# Runs code formatter https://pypi.org/project/black/
black tests

# Sort imports using https://pypi.org/project/isort/
isort tests

# Verify with https://pypi.org/project/flake8/
flake8 tests

# Very types using http://mypy-lang.org
mypy .
```
