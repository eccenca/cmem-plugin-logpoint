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


def write_execution_report(
    warning_occurred: bool, context: ExecutionContext, entities: list[Any], schema: EntitySchema
) -> None:
    """Write the execution report depending on weather a KeyError occurred or not."""
    if context:
        if warning_occurred:
            context.report.update(
                ExecutionReport(
                    entity_count=len(entities),
                    operation_desc="entities created.",
                    sample_entities=Entities(iter(entities[:5]), schema=schema),
                    warnings=["KeyError"],
                    summary=[
                        (
                            "KeyError",
                            "At least some of the paths specified are empty in the result.",
                        )
                    ],
                )
            )
        else:
            context.report.update(
                ExecutionReport(
                    entity_count=len(entities),
                    operation_desc="entities created.",
                    sample_entities=Entities(iter(entities[:5]), schema=schema),
                )
            )


@Plugin(
    label="Search for Logs",
    description="Search and retrieve logs from a Logpoint SIEM system with flexible schema output.",
    documentation="""
This plugin integrates with Logpoint systems
to search and retrieve log data based on custom queries.

## Key Features

- **Flexible Querying**: Execute custom Logpoint queries with configurable time ranges
- **Repository Filtering**: Limit searches to specific log repositories
- **Dynamic Schema**: Define output schema paths to structure the data as needed
- **Preview Actions**: Inspect available output paths and repositories before execution

## Configuration

### Authentication
Configure the connection to your Logpoint service using:
- **Service URL**: The base URL of your Logpoint instance
- **Username**: Service account username with appropriate permissions
- **Secret Key**: API secret key for authentication

### Query Parameters
- **Query**: Logpoint search query syntax (see Logpoint documentation TODO ADD LINK)
- **Time Range**: Relative time range (e.g., "Last 1 hour", "Last 24 hours")
- **Limit**: Maximum number of log entries to retrieve
- **Repositories**: Optional comma-separated list of specific repositories to search

### Output Schema
- **List of output paths**: Define specific fields to extract from logs
- Use the "Preview output paths" action to discover possible, available fields
- Leave empty to return all fields with automatic schema detection
- Format: comma-separated paths
- **Note:** It may occur that not all possible output paths are listed here. Logpoint follows
standardized field naming conventions documented at:
https://docs.logpoint.com/docs/logpoint-taxonomy-guideline/en/latest/Field%20naming%20convention.html
A warning will be given if at least one of the created entities does not have a value at the given
path.

## Usage Example

### Basic Log Search
- Query: `norm_id=*`
- Time Range: `Last 1 hour`
- Limit: `1000`
- Repositories: windows, linux
- Output paths: (empty for all fields)

Common fields include:
- `source_address`: Source IP address
- `destination_address`: Destination IP address
- `user`: Username associated with the event
- `log_ts`: The timestamp of a log.
- `device_id`: The ID of a device.

## Actions

Use the plugin actions to explore and configure your searches:
- **Preview output paths**: Run a test query to see available field paths
- **Preview repositories**: List all accessible log repositories in your Logpoint instance
""",
    icon=Icon(package=__package__, file_name="logpoint.svg"),
    parameters=[
        PluginParameter(
            name="base_url",
            label="Service URL",
            description="Base URL of the Logpoint service.",
            default_value="https://demo.logpoint.com/",
        ),
        PluginParameter(
            name="account",
            label="Username",
            default_value="partner",
            description="Username for authenticating with the Logpoint service. "
            "This account must have appropriate permissions to search logs and "
            "access repositories.",
        ),
        PluginParameter(
            name="secret_key",
            label="Secret Key",
            param_type=PasswordParameterType(),
            description="API secret key for authentication. This is securely encrypted and used "
            "to authenticate requests to the Logpoint service.",
        ),
        PluginParameter(
            name="query",
            label="Query",
            description="Logpoint search query using Logpoint query syntax. "
            "Example: 'norm_id=*' or '| chart count() by device_ip'. ",
        ),
        PluginParameter(
            name="time_range",
            label="Time Range",
            default_value="Last 1 hour",
            description="Relative time range for the search. "
            "Common values: 'Last 1 hour', 'Last 24 hours', 'Last 7 days', 'Last 30 days'. ",
        ),
        PluginParameter(
            name="limit",
            label="Limit",
            param_type=IntParameterType(),
            default_value=1000,
            description="Maximum number of log entries to retrieve. Must be a positive integer. ",
        ),
        PluginParameter(
            name="repos",
            label="Repositories",
            description="Comma-separated list of repository names to search. "
            "Example: 'windows,linux,firewall'. Leave empty to search all accessible repositories. "
            "Use the 'Preview repositories' action to see available options.",
            default_value="",
        ),
        PluginParameter(
            name="paths_list",
            label="List of output paths",
            description="Comma-separated list of field paths to include in output. "
            "Example: 'source_address, destination_address, user, log_ts'. "
            "If specified, creates a fixed output schema with only these fields. "
            "Leave empty for automatic schema detection with all available fields. "
            "Use 'Preview output paths' action to discover available field names. "
            "See Logpoint field naming conventions at: "
            "https://docs.logpoint.com/docs/logpoint-taxonomy-guideline/en/latest/Field%20naming%20convention.html",
            param_type=StringParameterType(),
            default_value="",
        ),
    ],
    actions=[
        PluginAction(
            name="preview_output_paths",
            label="Preview output paths",
            description="Executes the configured query with a limit of 1 to retrieve and display "
            "available field paths in the result set. Use this to discover which fields "
            "you can specify in the 'List of output paths' parameter for schema customization.",
        ),
        PluginAction(
            name="preview_repositories",
            label="Preview repositories",
            description="Retrieves and displays all log repositories accessible with the "
            "configured credentials. Use this to identify repository names for the "
            "'Repositories' parameter to filter searches to specific log sources.",
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
        repos: str,
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
        self.repos = [repo.strip() for repo in repos.split(",")]
        self.paths_list = [path_list.strip() for path_list in paths_list.split(",")]
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
            query=self.query, time_range=self.time_range, limit=self.limit, repos=self.repos
        )
        results = self.search_retrieve_logs(search_id)

        if len(self.paths_list) == 1 and self.paths_list[0] == "":
            build_entities = build_entities_from_data(results)
            if context:
                context.report.update(
                    ExecutionReport(
                        entity_count=len(results),
                        operation_desc="entities created.",
                    )
                )
            return build_entities

        schema = self.generate_schema()
        entities = []
        warning_occurred = False

        for result in results:
            entity_uri = str(uuid.uuid4())
            values = []
            for path in schema.paths:
                try:
                    values.append([str(result[path.path])])
                    entities.append(Entity(uri=entity_uri, values=values))
                except KeyError:
                    values.append([""])
                    warning_occurred = True
                    entities.append(Entity(uri=entity_uri, values=values))

        write_execution_report(warning_occurred, context, entities, schema)

        return Entities(entities=iter(entities), schema=schema)

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
        response.raise_for_status()
        response_data = response.json()

        while response_data["final"] is False:
            response = requests.post(url=url, data=data, timeout=100)
            response.raise_for_status()
            response_data = response.json()

        rows: list[dict[str, Any]] = response_data["rows"]

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
        try:
            search_id: str = response.json()["search_id"]
        except KeyError as e:
            raise KeyError("No search_id was found due to the query being incorrect.") from e
        return search_id

    def preview_output_paths(self) -> str:
        """Preview output paths"""
        preview_string = ""
        search_id = self.search_start(
            query=self.query, time_range=self.time_range, limit=1, repos=self.repos
        )
        results = self.search_retrieve_logs(search_id)
        result = results[0]
        for r in result:
            preview_string += f"- {r}\n"
        return preview_string

    def preview_repositories(self) -> str:
        """Preview repositories"""
        url = self.base_url + "/getalloweddata"

        data = {"username": self.account, "secret_key": self.secret_key, "type": "logpoint_repos"}

        response = requests.post(url=url, data=data, timeout=100)
        response_data = response.json()
        allowed_repos = response_data["allowed_repos"]
        preview_string = ""
        for repo in allowed_repos:
            preview_string += f"- {(repo['repo'])}\n"

        return preview_string

    def generate_schema(self) -> EntitySchema:
        """Generate the specified output schema."""
        return EntitySchema(
            type_uri="test",
            paths=[EntityPath(split_path) for split_path in self.paths_list],
        )
