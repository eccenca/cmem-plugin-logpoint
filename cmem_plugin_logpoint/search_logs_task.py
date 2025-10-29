"""Search Logs"""

import json
import uuid
from collections.abc import Sequence
from typing import Any

import requests
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import Icon, Plugin, PluginAction, PluginParameter
from cmem_plugin_base.dataintegration.entity import (
    Entities,
    Entity,
    EntityPath,
    EntitySchema,
)
from cmem_plugin_base.dataintegration.parameter.password import Password, PasswordParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import (
    FixedNumberOfInputs,
    FixedSchemaPort,
    UnknownSchemaPort,
)
from cmem_plugin_base.dataintegration.types import IntParameterType, StringParameterType
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
        PluginParameter(
            name="paths_list",
            label="List of output paths",
            description="Comma seperated list of output paths. If any values are set here,"
            "the output port will adjust according to those."
            "Do not leave whitespaces between paths. Use the 'Preview output paths' action "
            "to see possible values.",
            param_type=StringParameterType(),
            default_value="",
        ),
    ],
    actions=[
        PluginAction(
            name="preview_output_paths",
            label="Preview output paths",
            description="This action lists the potential paths for the query. "
            "These can be used to specify the needed output schema paths.",
        ),
        PluginAction(
            name="preview_repositories",
            label="Preview repositories",
            description="This action lists the potential repositories for the query.",
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
        paths_list: str,
    ) -> None:
        self.base_url = base_url.removesuffix("/")
        self.account = account
        self.secret_key = secret_key if isinstance(secret_key, str) else secret_key.decrypt()
        self.query = query
        self.time_range = time_range
        if limit < 1:
            raise ValueError("Limit must be positive.")
        self.limit = limit
        self.paths_list = paths_list
        self.input_ports = FixedNumberOfInputs(ports=[])
        self.output_port = (
            FixedSchemaPort(schema=self.generate_schema())
            if self.paths_list
            else UnknownSchemaPort()
        )

    def execute(
        self,
        inputs: Sequence[Entities],  # noqa: ARG002
        context: ExecutionContext,
    ) -> Entities:
        """Run the workflow operator."""
        search_id = self.search_start(
            query=self.query, time_range=self.time_range, limit=self.limit, repos=[]
        )
        results = self.search_retrieve_logs(search_id, context)

        if not self.paths_list:
            return build_entities_from_data(results)

        schema = self.generate_schema()
        entities = []
        for result in results:
            entity_uri = str(uuid.uuid4())
            values = []
            for path in schema.paths:
                try:
                    values.append([str(result[path.path])])
                except KeyError as e:
                    raise KeyError("The output path list contains invalid paths.") from e
            entities.append(Entity(uri=entity_uri, values=values))
        return Entities(entities=iter(entities), schema=schema)

    def search_retrieve_logs(self, search_id: str, context: ExecutionContext | None) -> list[dict]:
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

        final = False
        full_response: list[dict[str, Any]] = []
        report_amount_of_pages = 0
        while not final:
            report_amount_of_pages += 1
            response = requests.post(url=url, data=data, timeout=100)
            response.raise_for_status()

            response_data = response.json()
            rows = response_data["rows"]
            full_response.extend(rows)
            final = response_data.get("final", True)

        if context:
            context.report.update(
                ExecutionReport(
                    entity_count=report_amount_of_pages,
                    operation_desc=f"page{'' if report_amount_of_pages == 1 else 's'} used.",
                )
            )

        return full_response

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

    def list_repositories(self, context: ExecutionContext) -> None:
        """List all repositories available in the logpoint service.

        Currently, this is not working as a JSON Web Token (JWT) is needed for this.
        It returns 200 but is not successful in retrieving the data.
        """
        url = self.base_url + "/Repo/get_all_searchable_logpoint"
        data = {
            "username": self.account,
            "secret_key": self.secret_key,
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {context.user.token()}",
        }
        requests.post(url=url, data=data, headers=headers, timeout=100)

    def preview_output_paths(self) -> str:
        """Preview output paths"""
        preview_string = ""
        search_id = self.search_start(
            query=self.query, time_range=self.time_range, limit=1, repos=[]
        )
        results = self.search_retrieve_logs(search_id, None)
        result = results[0]
        for r in result:
            preview_string += f"- {r}\n"
        return preview_string

    def preview_repositories(self) -> str:
        """Preview repositories"""
        return "Not yet implemented"

    def generate_schema(self) -> EntitySchema:
        """Generate the specified output schema."""
        return EntitySchema(
            type_uri="test",
            paths=[EntityPath(split_path) for split_path in self.paths_list.split(",")],
        )
