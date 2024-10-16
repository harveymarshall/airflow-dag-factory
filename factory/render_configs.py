#! /usr/bin/env python

import json
import logging
import os
import tempfile
from pathlib import Path

import yaml
from actions_toolkit import core
from constructors import CUSTOM_CONSTRUCTORS
from jinja2 import Environment, FileSystemLoader, Template

ROOT = Path(__file__).parent.parent
"""This is the root directory of this repository.

We store this so we can manage files without using hard-coded paths, and not
worry about where this module is called from.
"""


def main():
    """
    Function takes in all the initial config files and writes out fully rendered DAG YAML configs
    using the jinja2 templates.

    Args:
        dir: directory where all the dag configs exist
    """

    for k, v in CUSTOM_CONSTRUCTORS.items():
        yaml.FullLoader.add_constructor(k, v)

    # Load templates file from templates folder
    env = Environment(
        loader=FileSystemLoader(["./templates", "../*"]),
        trim_blocks=True,
        lstrip_blocks=True,
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("base.j2")

    errors_encountered = False

    for yaml_file in (ROOT / "dag_configs").glob("**/*.y*ml"):
        values = None

        if "#!multi" in yaml_file.read_text():
            errors_encountered = render_multi_dag(
                yaml_file, template, errors_encountered
            )
            continue  # all iterables sorted; move onto the next yaml file

        try:
            with yaml_file.open() as r:
                values = yaml.full_load(r)
        except Exception as e:
            logging.exception("Error opening yaml '%s': %s", yaml_file, e)
            errors_encountered = True
            continue

        if not values:
            logging.exception("Config values is empty...")
            continue

            # don't render configs if they've been disabled:
        if values.get("_do_not_render"):
            logging.info("Skipping config as '_do_not_render' is set: %s", yaml_file)
            continue

        errors_encountered = render_single_dag(
            values, yaml_file, template, errors_encountered
        )


def replace_values(template: str, values: dict, prefix: str) -> str:
    """Replaces placeholder values from a template string.

    If null values are given, we insert an empty string in place of the placeholder.

    Args:
        template: the string to replace values within
        values: key/value dictionary of replacement items
        prefix: the identifier used for placeholder replacement, e.g. if a
                template contains ``${each.id}``, the prefix is ``each``.

    Returns:
        The template string with all placeholder values replaced.
    """
    if len(values) == 0:
        return template

    for key, value in values.items():
        if not value:
            replacement = ""
        elif isinstance(value, (dict, list)):
            replacement = json.dumps(value, sort_keys=False)
        elif isinstance(value, int):
            replacement = str(value)
        else:
            replacement = value
        template = template.replace(f"${{{prefix}.{key}}}", replacement)

    return template


def render_multi_dag(
    yaml_file: Path, template: Template, errors_encountered: bool
) -> bool:
    """Renders multiple DAGs from a single file.

    Since our constructors might do stuff like try and find files on the
    filesystem, we can't just load the full multi-DAG definition up front
    (as we might have stuff like ``${each.value}`` in there). This function
    handles that by splitting the raw file test on an identifier, then loading
    just the iterative half so we can do string replacement on the templated
    half later.

    After that, this is effectively a wrapper for ``render_single_dag()``.
    """
    parts = yaml_file.read_text().split("#!multi")
    # iterables come after the #!multi separator, so grab them on their own:
    iterables = yaml.full_load(parts[1])

    for config in iterables["_for_each"]:
        # reset template:
        conf = parts[0]
        # then overwrite values in the template:
        conf = replace_values(conf, config, "each")
        # now load and render the templated instance:
        with tempfile.NamedTemporaryFile(
            dir=yaml_file.parent, delete=False
        ) as temp_file:
            temp_file.write(bytes(conf, encoding="utf-8"))

        with open(temp_file.name, "r") as file:
            single_config = yaml.full_load(file)

        os.remove(temp_file.name)

        errors_encountered = render_single_dag(
            single_config,
            yaml_file.parent / single_config["dag_id"],
            template,
            errors_encountered,
        )
    return errors_encountered


def render_single_dag(
    values: dict, yaml_file: Path, template: Template, errors_encountered: bool
) -> bool:
    """Renders a single DAG from a yaml file."""
    doc_name = yaml_file.stem + ".md"
    docs_path = yaml_file.parent / doc_name
    if os.path.exists(docs_path):
        with open(docs_path, "r") as f:
            docs = f.read()

            values["documentation"] = docs

    # grab the directory structure between 'dag_configs' and this file so we
    # can use it for setting owners/tags:
    domain_folder_path_parts = yaml_file.parent.relative_to(ROOT / "dag_configs").parts
    # we set the top-level domain folder name as the owner of the DAG:
    values["owner"] = domain_folder_path_parts[0]

    # tidy the format of any tags provided in the config so we're consistent:
    domain_tags = [dir.replace("_", " ").lower() for dir in domain_folder_path_parts]
    config_tags = [tag.replace("_", " ").lower() for tag in values.get("tags", [])]
    # then take a sorted final set to avoid any duplication:
    values["tags"] = sorted(set(domain_tags + config_tags))

    if "${value_set." in yaml.dump(values, sort_keys=False):
        new_task_list = []
        for task_spec in values["tasks"]:
            if not task_spec.get("workflow_description"):
                # pass normal tasks straight through:
                new_task_list.append(task_spec)
            else:
                # parse workflows out into multiple new tasks:
                for value_dict in task_spec["value_set"]:
                    for task_template in task_spec["task_set"]:
                        # dump task spec to string to simplify value replacement:
                        new_task = yaml.dump(task_template, sort_keys=False)
                        new_task = replace_values(new_task, value_dict, "value_set")
                        new_task = yaml.full_load(new_task)
                        new_task_list.append(new_task)
        # now replace the task list with our newly populated one:
        values["tasks"] = new_task_list

    try:
        rendered = template.render(values)
        loaded = yaml.safe_load(rendered)
        if loaded.get("tasks"):
            if isinstance(loaded.get("tasks"), dict):
                logging.error("Tasks is of type dict not list.")
                errors_encountered = True
        if len(loaded.get("tasks", [])) == 0:
            logging.error("No tasks for rendered config")
            errors_encountered = True
        # Write out fully rendered YAML to new location with full file naming convention
        new_path = (
            ROOT / "factory" / "rendered_configs" / "/".join(domain_folder_path_parts)
        )
        if not new_path.exists() and not new_path.is_dir():
            new_path.mkdir(parents=True, exist_ok=True)
        with (new_path / f"{values['dag_id']}.yaml").open(mode="w") as f:
            f.write(rendered)

    except Exception as e:
        logging.error("Error rendering dag config for '%s': %s", yaml_file, e)
        errors_encountered = True

    finally:
        if errors_encountered:
            core.set_failed("Error rendering dag config")

    return errors_encountered


if __name__ == "__main__":
    main()
