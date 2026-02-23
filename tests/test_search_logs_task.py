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
        limit=100,
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
    plugin.search_retrieve_logs(search_id)


def test_plugin_execution_big_limit(search_environment: SearchEnvironment) -> None:
    """Test big limit search"""
    plugin = search_environment.plugin
    plugin.query = "norm_id = *"
    plugin.limit = 4000
    plugin.time_range = "Last 24 hours"
    result = plugin.execute(inputs=[], context=TestExecutionContext())
    assert len(list(result.entities)) != 0


def test_plugin_no_output_specified(search_environment: SearchEnvironment) -> None:
    """Test plugin with no specified output paths"""
    plugin = search_environment.plugin
    plugin.query = '"device_name" = "EKSAuditDevice"'
    result = plugin.execute(inputs=[], context=TestExecutionContext())
    assert len(list(result.entities)) != 0


def test_plugin_with_output_specified(search_environment: SearchEnvironment) -> None:
    """Test plugin with a specific output specified"""
    plugin = search_environment.plugin
    plugin.query = "| chart count() by device_ip"
    plugin.time_range = "Last 15 minutes"
    plugin.limit = 10
    plugin.paths_list = ["_type_str", "count()"]
    result = plugin.execute(inputs=[], context=TestExecutionContext())

    assert len(result.schema.paths) == len(plugin.paths_list)
    assert len(list(result.entities)) > 0


def test_plugin_with_output_and_warning(search_environment: SearchEnvironment) -> None:
    """Test plugin with a specific output and warning"""
    plugin = search_environment.plugin
    plugin.query = "| chart count() by device_ip"
    plugin.time_range = "Last 15 minutes"
    plugin.limit = 10
    plugin.paths_list = ["_type_str", "count()", "non-existent"]
    result = plugin.execute(inputs=[], context=TestExecutionContext())

    assert len(result.schema.paths) == len(plugin.paths_list)
    assert len(list(result.entities)) > 0


def test_broken_query(search_environment: SearchEnvironment) -> None:
    """Test broken query"""
    plugin = search_environment.plugin
    plugin.repos = ["this does not work"]
    with pytest.raises(KeyError, match=r"No search_id was found due to the query being incorrect."):
        plugin.execute(inputs=[], context=TestExecutionContext())


def test_preview_output_paths(search_environment: SearchEnvironment) -> None:
    """Test preview action to show output paths"""
    plugin = search_environment.plugin
    plugin.query = "| chart count() by device_ip"
    plugin.time_range = "Last 15 minutes"
    plugin.limit = 10
    assert "count()" in plugin.preview_output_paths()


def test_preview_repos(search_environment: SearchEnvironment) -> None:
    """Test start search"""
    plugin = search_environment.plugin
    preview = plugin.preview_repositories()
    assert "127.0.0.1:5504/windows" in preview


def test_plugin_with_specified_repos(search_environment: SearchEnvironment) -> None:
    """Test plugin with specific repos"""
    plugin = search_environment.plugin
    plugin.query = "| chart count() by device_ip"
    plugin.repos = [
        "127.0.0.1:5504/windows",
        "127.0.0.1:5504/sap",
        "127.0.0.1:5504/cloudwatch",
        "127.0.0.1:5504/Proxy_WAF",
        "127.0.0.1:5504/apache",
        "127.0.0.1:5504/AgentX",
        "127.0.0.1:5504/vmware",
        "127.0.0.1:5504/cloud",
    ]
    result = plugin.execute(inputs=[], context=TestExecutionContext())
    assert len(list(result.entities)) > 0


def test_negative_limit_init() -> None:
    """Test negative limit"""
    with pytest.raises(ValueError, match=r"Limit must be positive."):
        RetrieveLogs(
            base_url="https://example.com",
            account="account",
            secret_key="***",  # noqa: S106
            time_range="Last 1 hour",
            limit=-1,
            repos="",
            paths_list="",
            query="",
        )
