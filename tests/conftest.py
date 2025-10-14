"""conftest module"""

import os

import pytest


def get_env_or_skip(key: str, message: str | None = None) -> str:
    """Get environment variable or skip test."""
    value = os.environ.get(key, "")
    if message is None:
        message = f"Needed environment '{key}' is not set."
    if value == "":
        pytest.skip(message)
    return value
