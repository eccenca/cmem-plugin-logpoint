"""Test search log task"""

from dataclasses import dataclass

import pytest
from cmem_plugin_base.testing import TestExecutionContext

from cmem_plugin_logpoint.search_logs_task import RetrieveLogs
from tests.conftest import get_env_or_skip


@dataclass
class SearchEnvironment:
    """Search Environment Fixture"""

    base_url: str
    account: str
    secret_key: str
    plugin: RetrieveLogs


@pytest.fixture
def search_environment() -> SearchEnvironment:
    """Provide SearchEnvironment"""
    base_url = get_env_or_skip("LOGPOINT_BASE_URL")
    account = get_env_or_skip("LOGPOINT_ACCOUNT")
    secret_key = get_env_or_skip("LOGPOINT_SECRET_KEY")
    plugin = RetrieveLogs(
        base_url=base_url,
        account=account,
        secret_key=secret_key,
        query="",
        time_range="Last 1 hour",
        limit=1,
        repos="",
        paths_list="",
    )
    return SearchEnvironment(
        base_url=base_url,
        account=account,
        secret_key=secret_key,
        plugin=plugin,
    )


def test_search(search_environment: SearchEnvironment) -> None:
    """Test start search"""
    plugin = search_environment.plugin
    query = '"device_name" = "EKSAuditDevice"'
    search_id = plugin.search_start(repos=[], limit=10, time_range="Last 1 hour", query=query)
    plugin.search_retrieve_logs(search_id, TestExecutionContext())


def test_list_repos(search_environment: SearchEnvironment) -> None:
    """Test start search"""


def test_preview_output_paths(search_environment: SearchEnvironment) -> None:
    """Test preview action to show output paths"""
    plugin = search_environment.plugin
    plugin.query = "| chart count() by device_ip"
    plugin.time_range = "Last 15 minutes"
    plugin.limit = 10
    assert "count()" in plugin.preview_output_paths()


def test_plugin_with_output_specified() -> None:
    """Test plugin with a specific output specified"""
    base_url = get_env_or_skip("LOGPOINT_BASE_URL")
    account = get_env_or_skip("LOGPOINT_ACCOUNT")
    secret_key = get_env_or_skip("LOGPOINT_SECRET_KEY")
    plugin = RetrieveLogs(
        base_url=base_url,
        account=account,
        secret_key=secret_key,
        query="| chart count() by device_ip",
        time_range="Last 15 minutes",
        limit=10,
        repos="",
        paths_list="_type_str,count()",
    )
    result = plugin.execute(inputs=[], context=TestExecutionContext())

    assert len(result.schema.paths) == len(plugin.paths_list.split(","))
    assert len(list(result.entities)) > 0
