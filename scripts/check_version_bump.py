#!/usr/bin/env python3
"""Enforce semantic version bumps when code changes are staged.

This is designed for use as a pre-commit hook and in CI.

Rules:
- If staged changes touch `src/xlr8/**` (excluding tests/docs), require `pyproject.toml`
  version to be bumped vs `origin/main`.
- If staged changes touch `rust/xlr8_rust/**`, require `rust/xlr8_rust/pyproject.toml`
  version to be bumped vs `origin/main`.
- If both packages change, require both to be bumped.

Notes:
- Intended to be conservative; adjust the path filters if you want to include
    docs/tests.
"""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path


def run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\n{result.stdout}\n{result.stderr}".strip()
        )
    return result.stdout


def parse_version(version: str) -> tuple[int, int, int]:
    version_str = version.lstrip("v")
    major, minor, patch = version_str.split(".")
    return (int(major), int(minor), int(patch))


def is_version_greater_than(version1: str, version2: str) -> bool:
    return parse_version(version1) > parse_version(version2)


def load_version_from_toml(path: Path) -> str:
    data = tomllib.loads(path.read_bytes().decode("utf-8"))
    return str(data["project"]["version"])


def load_version_from_git(ref: str, path: str) -> str | None:
    try:
        raw = run(["git", "show", f"{ref}:{path}"])
        data = tomllib.loads(raw)
        return str(data["project"]["version"])
    except Exception:
        return None


def staged_files() -> list[str]:
    out = run(["git", "diff", "--cached", "--name-only"])
    return [line.strip() for line in out.splitlines() if line.strip()]


def touches_xlr8_python(files: list[str]) -> bool:
    for file in files:
        if file.startswith("src/xlr8/"):
            # ignore docs-ish changes inside package if you ever add them
            if file.endswith(".md"):
                continue
            return True
    return False


def touches_xlr8_rust(files: list[str]) -> bool:
    return any(f.startswith("rust/xlr8_rust/") for f in files)


def main() -> int:
    # Skip on initial commit / empty repo situations.
    try:
        subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, check=True)
    except subprocess.CalledProcessError:
        return 0

    files = staged_files()
    if not files:
        print("No staged changes; skipping version check")
        return 0

    python_changed = touches_xlr8_python(files)
    rust_changed = touches_xlr8_rust(files)

    if not (python_changed or rust_changed):
        print("No xlr8 package changes; skipping version check")
        return 0

    errors: list[str] = []

    if python_changed:
        current = load_version_from_toml(Path("pyproject.toml"))
        previous = load_version_from_git("origin/main", "pyproject.toml")
        if previous is None:
            print(
                f"Python package version check: current={current}, "
                "main=unknown (skipping)"
            )
        else:
            print(f"Python package version check: current={current}, main={previous}")
            if not is_version_greater_than(current, previous):
                errors.append(
                    "Python package version bump required: update pyproject.toml "
                    f"so version is > {previous} (current {current})."
                )

    if rust_changed:
        current = load_version_from_toml(Path("rust/xlr8_rust/pyproject.toml"))
        previous = load_version_from_git("origin/main", "rust/xlr8_rust/pyproject.toml")
        if previous is None:
            print(
                f"Rust package version check: current={current}, "
                "main=unknown (skipping)"
            )
        else:
            print(f"Rust package version check: current={current}, main={previous}")
            if not is_version_greater_than(current, previous):
                errors.append(
                    "Rust package version bump required: update "
                    "rust/xlr8_rust/pyproject.toml "
                    f"so version is > {previous} (current {current})."
                )

    if errors:
        print("\nERROR: version bump required\n", file=sys.stderr)
        for err in errors:
            print(f"- {err}", file=sys.stderr)
        return 1

    print("Version bump check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
