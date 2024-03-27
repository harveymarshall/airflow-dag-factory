import inspect
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict

import yaml
from airflow import DAG, Dataset
from airflow.models.param import Param
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8_models

from factory.utils import dag_builder_utils
from modules.helpers.env_config_helper import env_config
from modules.utils.jinja_utils import USER_DEFINED_FILTERS, USER_DEFINED_MACROS

SUPPORTED_OPERATOR_TYPES = {
    "BashOperator": BashOperator,
    "GKEStartPodOperator": GKEStartPodOperator,
}
"""These are the operator types we can currently build.

Keys are the string we expect to see under the 'operator' field in a rendered
DAG config task section, and the values are the specific Airflow operators we
build when they're requested.
"""

DAG_BUILD_ARGUMENTS = [p.name for p in inspect.signature(DAG).parameters.values()]
"""These are the arguments airflow expects us to pass into a DAG definition.

We use these to filter the kwargs we're given, and only pass the relevant ones
into the function call. Doing this dynamically allows us to pass new features
straight into the DAG build function from a template, so we don't have to keep
reworking this file just to add new features.
"""


def generate_dag(yaml_file: Path, parsing_dag_id: str) -> DAG:
    """
    Builds a single DAG from a yaml config file.

    Args:
        yaml_file: the path to the file we want to generate a DAG from
        parsing_dag_id: the name of the dag we're currently supposed to be
            parsing, if we're on a worker node and not a DAG builder.

    Returns:
        A single DAG
    """

    # short-circuit if we're parsing DAGs on a worker node:
    if parsing_dag_id and parsing_dag_id != yaml_file.stem:
        return

    with yaml_file.open() as file:
        dag_config = yaml.safe_load(file)

    allowed_envs = dag_config.pop("allowed_envs", [])
    if allowed_envs and env_config.environment not in allowed_envs:
        return

    # Setting default args for a DAG etc.
    local_timezone = dag_config.pop("timezone")
    start_date = dag_config.get("start_date")

    dag_config["start_date"] = dag_builder_utils.get_start_date(
        start_date, local_timezone
    )

    base_params = dag_config.pop("params", {})

    # Creating instance of a DAG
    dag_args = {k: v for k, v in dag_config.items() if k in DAG_BUILD_ARGUMENTS}
    jinja_filters = {}
    jinja_filters.update(dag_builder_utils.DF_USER_DEFINED_FILTERS)
    jinja_filters.update(USER_DEFINED_FILTERS)
    jinja_macros = {}
    jinja_macros.update(dag_builder_utils.DF_USER_DEFINED_MACROS)
    jinja_macros.update(USER_DEFINED_MACROS)

    dag_args["user_defined_filters"] = jinja_filters
    dag_args["user_defined_macros"] = jinja_macros

    dag_args["doc_md"] = dag_config.get("documentation")

    # add dataset-driven scheduling if needed:
    if isinstance(dag_args["schedule"], dict):
        if datasets := dag_args["schedule"].get("datasets"):
            dag_args["schedule"] = [Dataset(label) for label in datasets]

    dag_params = {}
    for param in base_params:
        dag_params[param["id"]] = Param(
            param.get("default_value", ""),
            type=(
                param["type"]
                if param.get("required", False)
                else ["null", param["type"]]
            ),
            description=param["description"],
        )

    dag_params["manual_interval_start_date"] = Param(
        "",
        type=["null", "string"],
        description="Please provide a datetime for the start of this dag run in the required format: `%Y-%m-%dT%H:%M:%S+00:00`",
    )
    dag_params["manual_interval_end_date"] = Param(
        "",
        type=["null", "string"],
        description="Please provide a datetime for the start of this dag run in the required format: `%Y-%m-%dT%H:%M:%S+00:00`",
    )

    with DAG(**dag_args, params=dag_params) as dag:
        # get a unique set of all the task groups named in our config:
        task_group_names = set(
            [task_group["id"] for task_group in dag_config.get("task_groups", [])]
            + [
                task_config["group"]
                for task_config in dag_config.get("tasks", [])
                if task_config.get("group")
            ]
        )

        # then make a task group for each:
        task_groups = {
            task_group: TaskGroup(group_id=task_group)
            for task_group in task_group_names
        }

        def _provide_default_value(kwargs: Dict, key: str, value: Any) -> Dict:
            """Replaces specific values in a dictionary if the current value
            for the given key == "default".

            If the existing value doesn't exist, or isn't set to "default", we
            return the dictionary unchanged.

            Args:
                kwargs: dictionary
                key: key to check for a default value
                value: the value to replace the "default" entry with
            """
            if kwargs.get(key) == "default":
                kwargs[key] = value
            return kwargs

        # Define a operators dict and begin assigning tasks to the above DAG instance
        operators = {}
        for task_config in dag_config.get("tasks", []):
            operator_type = task_config.get("operator")
            if operator_type not in SUPPORTED_OPERATOR_TYPES:
                raise ValueError(f"Unsupported operator type '{operator_type}'")

            operator_kwargs = task_config.get("operator_kwargs", {})

            if execution_timeout := task_config.get("execution_timeout", ""):
                for k, v in execution_timeout.items():
                    if k == "hours":
                        operator_kwargs["execution_timeout"] = timedelta(hours=v)
                    elif k == "minutes":
                        operator_kwargs["execution_timeout"] = timedelta(minutes=v)
                    else:
                        operator_kwargs["execution_timeout"] = timedelta(seconds=v)
            # Set outlet datasets, if any have been provided:
            if outlets := task_config.get("outlets"):
                operator_kwargs["outlets"] = [Dataset(outlet) for outlet in outlets]

            # Set trigger rule if we've been given one:
            if trigger_rule := task_config.pop("trigger_rule", "").upper():
                if hasattr(TriggerRule, trigger_rule):
                    operator_kwargs["trigger_rule"] = getattr(TriggerRule, trigger_rule)

            # Set some specifics, depending on the type of operator we're dealing with:
            if operator_type == "BashOperator":
                task_type = BashOperator

            elif operator_type == "GKEStartPodOperator":
                task_type = GKEStartPodOperator

                operator_kwargs["project_id"] = env_config.project_id
                operator_kwargs["location"] = env_config.region

                if container_resources := task_config.pop("container_resources", ""):
                    limits = {}
                    for key, value in container_resources:
                        limits[key] = value
                    operator_kwargs[
                        "container_resources"
                    ] = k8_models.V1ResourceRequirements(limits=limits)
                # replace default values where they've been asked for:
                for key, value in {
                    "cluster_name": env_config.cluster_name,
                    "namespace": env_config.cluster_namespace,
                    "service_account_name": env_config.cluster_workload_sa,
                }.items():
                    _provide_default_value(operator_kwargs, key, value)

            # finally, create the task using the arguments from above:
            operators[task_config["id"]] = task_type(
                dag=dag,
                task_id=task_config["id"],
                task_group=task_groups.get(task_config.get("group")),
                params=task_config.get("params", ""),
                **operator_kwargs,
            )

        # Set dependencies/labels between tasks/task groups:
        for config in dag_config.get("tasks", []) + dag_config.get("task_groups", []):
            for dependency in config.get("dependencies", []):
                # figure out whether we're pointing to tasks, or task groups:
                upstream = operators.get(dependency["id"]) or task_groups.get(
                    dependency["id"]
                )

                downstream = operators.get(config["id"]) or task_groups.get(
                    config["id"]
                )

                # if we can't find the task/group anywhere, shout about it:
                if not upstream or not downstream:
                    raise ValueError("Dependency issue: %s", dependency)

                # set dependencies, using labels where requested:
                if dependency.get("label"):
                    upstream >> Label(dependency["label"]) >> downstream
                else:
                    upstream >> downstream

        return dag
