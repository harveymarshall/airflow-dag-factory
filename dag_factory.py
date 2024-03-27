"""Builds Airflow Dags from config files."""

from pathlib import Path

from airflow.utils.dag_parsing_context import get_parsing_context

from factory.utils.dag_builder import generate_dag


def run():
    """
    Iterates over rendered DAG configs, turning them into proper DAGs.
    """
    dag_configs = (Path(__file__).parent / "rendered_configs").glob("**/*.yaml")
    if not dag_configs:
        raise ValueError("nothing to build")

    print("Attempting to build DAGs from config files")
    current_dag_id = get_parsing_context().dag_id
    for file in dag_configs:
        dag = generate_dag(file, current_dag_id)
        if dag:
            globals()[file] = dag
            print(f"Successfully built DAG '{dag.dag_id}' from {file}")


run()
