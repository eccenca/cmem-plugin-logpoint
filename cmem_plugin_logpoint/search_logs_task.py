"""Search Logs"""

import json
from collections.abc import Sequence

import requests
from cmem_plugin_base.dataintegration.context import ExecutionContext
from cmem_plugin_base.dataintegration.description import Icon, Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import (
    Entities,
)
from cmem_plugin_base.dataintegration.parameter.password import Password, PasswordParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import (
    FixedNumberOfInputs,
    UnknownSchemaPort,
)
from cmem_plugin_base.dataintegration.types import IntParameterType
from cmem_plugin_base.dataintegration.utils.entity_builder import build_entities_from_data


@Plugin(
    label="Search for Logs",
    description="Search and retrieve logs from a Logpoint service.",
    documentation="""
...
""",
    icon=Icon(package=__package__, file_name="logpoint.svg"),
    parameters=[
        PluginParameter(
            name="base_url",
            label="Service URL",
            default_value="https://demo.logpoint.com/",
        ),
        PluginParameter(
            name="account",
            label="Username",
            default_value="partner",
        ),
        PluginParameter(
            name="secret_key",
            label="Secret Key",
            param_type=PasswordParameterType(),
        ),
        PluginParameter(
            name="query",
            label="Query",
        ),
        PluginParameter(
            name="time_range",
            label="Time Range",
            default_value="Last 1 hour",
        ),
        PluginParameter(
            name="limit",
            label="Limit",
            param_type=IntParameterType(),
            default_value=1000,
        ),
    ],
)
class RetrieveLogs(WorkflowPlugin):
    """Search and retrieve logs from a Logpoint service"""

    def __init__(  # noqa: PLR0913
        self,
        base_url: str,
        account: str,
        secret_key: Password | str,
        query: str,
        time_range: str,
        limit: int,
    ) -> None:
        self.base_url = base_url.removesuffix("/")
        self.account = account
        self.secret_key = secret_key if isinstance(secret_key, str) else secret_key.decrypt()
        self.query = query
        self.time_range = time_range
        if limit < 1:
            raise ValueError("Limit must be positive.")
        self.limit = limit
        self.input_ports = FixedNumberOfInputs(ports=[])
        self.output_port = UnknownSchemaPort()

    def execute(
        self,
        inputs: Sequence[Entities],  # noqa: ARG002
        context: ExecutionContext,  # noqa: ARG002
    ) -> Entities:
        """Run the workflow operator."""
        search_id = self.search_start(
            query=self.query, time_range=self.time_range, limit=self.limit, repos=[]
        )
        results = self.search_retrieve_logs(search_id)
        return build_entities_from_data(results)

    def search_retrieve_logs(self, search_id: str) -> list[dict]:
        """Get search results for a search id"""
        url = self.base_url + "/getsearchlogs"
        request_data = {
            "search_id": search_id,
            "waiter_id": "cmem-plugin-logpoint",
            "seen_version": None,
        }
        data = {
            "username": self.account,
            "secret_key": self.secret_key,
            "requestData": json.dumps(request_data),
        }
        response = requests.post(url=url, data=data, timeout=100)
        rows: list[dict] = response.json()["rows"]
        return rows

    def search_start(self, repos: list[str], limit: int, time_range: str, query: str) -> str:
        """Start a search and get a search ID"""
        url = self.base_url + "/getsearchlogs"
        request_data = {
            "repos": repos,
            "limit": limit,
            "time_range": time_range,
            "query": query,
        }
        data = {
            "username": self.account,
            "secret_key": self.secret_key,
            "requestData": json.dumps(request_data),
        }
        response = requests.post(url=url, data=data, timeout=100)
        search_id: str = response.json()["search_id"]
        return search_id
