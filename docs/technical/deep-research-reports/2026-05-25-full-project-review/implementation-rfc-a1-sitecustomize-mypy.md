# Implementation RFC A1 - `sitecustomize.py` Mypy Fix

Date: 2026-05-25

Status: awaiting approval before code edits

## Purpose

Close backlog item A1 from `fix-plan-backlog.md`: fix the remaining Mypy failure in `sitecustomize.py` without changing runtime behavior.

## Current Behavior

`sitecustomize.py` is automatically loaded from the repo root by Python. On Windows, it avoids a slow/blocking WMI path by replacing:

- `platform.machine`
- `platform.processor`

with cheap functions backed by processor-related environment variables.

Current implementation:

```python
if os.name == "nt":
    _machine = os.environ.get("PROCESSOR_ARCHITEW6432") or os.environ.get(
        "PROCESSOR_ARCHITECTURE"
    )
    _processor = os.environ.get("PROCESSOR_IDENTIFIER")

    if _machine:
        platform.machine = lambda: _machine  # type: ignore[assignment]
    if _processor:
        platform.processor = lambda: _processor  # type: ignore[assignment]
```

Current verification failure:

```text
sitecustomize.py:20: error: Incompatible return value type (got "str | None", expected "str")  [return-value]
sitecustomize.py:22: error: Incompatible return value type (got "str | None", expected "str")  [return-value]
```

The issue was type narrowing: after `if _machine`, Mypy saw the lambda closure as returning `str | None`.

## Dependencies and Public Surface

Public surface:

- Python startup import behavior.
- `platform.machine() -> str`.
- `platform.processor() -> str`.

Dependencies:

- `os.environ`.
- `os.name`.
- Python 3.11 typing behavior.

Side effects:

- On Windows only, assigns `platform.machine` and/or `platform.processor`.
- No file, network, database, or market side effects.

## Proposed Change

Use named helper factories or default-bound lambda parameters so the closure captures a concrete `str`, not the outer nullable variable.

Preferred minimal implementation:

```python
if _machine:
    machine = _machine
    platform.machine = lambda: machine  # type: ignore[assignment]
if _processor:
    processor = _processor
    platform.processor = lambda: processor  # type: ignore[assignment]
```

Alternative if Mypy still dislikes assignment:

```python
def _constant_platform_value(value: str) -> Callable[[], str]:
    return lambda: value
```

Then:

```python
platform.machine = _constant_platform_value(_machine)
```

The helper approach is clearer if assignment typing also needs the callable shape.

## TDD / Verification Plan

RED:

- Existing failing command is the current RED: `uv run mypy .`.
- If strict runtime-test-first is required, add one focused test before the production change:
  - `tests/test_sitecustomize.py`
  - monkeypatch Windows-like env variables
  - reload `sitecustomize`
  - assert `platform.machine()` and `platform.processor()` return the configured strings
  - restore original platform functions after the test

GREEN:

- Apply the smallest typing-only production fix.
- Run:

```powershell
uv run ruff check .
uv run mypy .
```

REFRACTOR:

- Only if needed, replace duplicated closure code with a tiny helper.
- Do not alter startup behavior outside the Mypy fix.

VERIFY:

- After fast verification is green, proceed to backlog A3 full verification.

## Exact Files

Likely:

- `sitecustomize.py`

Optional if runtime-test-first is chosen:

- `tests/test_sitecustomize.py`

## Risk

Low.

The only meaningful risk is changing startup monkeypatch behavior. The proposed fix should preserve the same values and Windows-only guard.

## Approval Request

Approve one of these options:

1. **Mypy RED only**: treat the existing failing `uv run mypy .` as the RED gate and patch only `sitecustomize.py`.
2. **Runtime test first**: add `tests/test_sitecustomize.py` first, then patch `sitecustomize.py`.

Recommended: option 1, because the failure is purely static typing and the code path is a startup guard with no previous runtime regression.
