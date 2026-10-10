import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

DBT_ROOT_PATH = Path(__file__).parent / "dbt_analytics"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")

profile_config = ProfileConfig(
    profile_name="dbt_analytics",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        profile_args={"database": "dbt_db", "schema": "dbt_schema"},
    ),
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(DBT_ROOT_PATH),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{AIRFLOW_HOME}/dbt_venv/bin/dbt"
    ),
    schedule="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_dag",
    tags=["dbt", "snowflake"],
    default_args={"retries": 2},
)