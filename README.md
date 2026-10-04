# dbt + Snowflake + Airflow Practice Project

This is a hands-on practice project created to learn and understand how **dbt**, **Snowflake**, and **Apache Airflow** work together in a modern data stack.

---

## 🎯 What This Project Covers

1. **dbt (Data Build Tool)**:
   - Understanding data transformations using SQL and Jinja.
   - Organizing models into layers: **staging** (views) and **marts** (fact/dimension tables).
   - Reusable logic using **macros** (e.g., `discounted_amount`).
   - Data quality assurance with **generic and singular tests**.
   - Managing dependencies with `dbt_utils`.

2. **Snowflake**:
   - Using Snowflake as the cloud data platform.
   - Configuring roles, warehouses, databases (`dbt_db`), and schemas (`dbt_schema`).
   - Materializing models as views vs. tables in Snowflake.

3. **Apache Airflow (via Astronomer Cosmos)**:
   - Orchestrating dbt models as native Airflow task DAGs using [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/).
   - Parsing dbt models dynamically into Airflow tasks with dependency graphs.
   - Executing dbt commands inside an isolated virtual environment (`dbt_venv`) in the Airflow runtime.

---

## 📁 Project Structure

```text
.
├── Dockerfile                  # Astro runtime image with dbt-snowflake in a virtualenv
├── requirements.txt            # Python dependencies (astronomer-cosmos, snowflake provider)
├── airflow_settings.yaml       # Local Airflow connections/variables (git-ignored)
├── dags/
│   ├── dbt_dag.py              # Airflow DAG orchestrating dbt models via Cosmos
│   ├── .airflowignore          # Ignores dbt internal files from Airflow DAG parser
│   └── dbt_learn/              # The dbt project
│       ├── dbt_project.yml     # dbt project configuration & model materializations
│       ├── packages.yml        # dbt package dependencies (dbt_utils)
│       ├── macros/             # Custom dbt macros (pricing.sql)
│       ├── models/
│       │   ├── staging/        # Staging models (stg_tpch_orders, stg_tpch_line_items)
│       │   └── marts/          # Fact & intermediate models (fct_orders, int_order_items)
│       └── tests/              # Custom singular & generic tests
└── tests/                      # Airflow DAG integrity tests
```

---

## 🚀 Running Locally with Astro CLI

### 1. Prerequisites
- Docker Desktop installed and running.
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed.

### 2. Configure Snowflake Connection
Create an Airflow connection named `snowflake_conn` either in the Airflow UI (**Admin -> Connections**) or in `airflow_settings.yaml`:
- **Conn Id**: `snowflake_conn`
- **Conn Type**: `Snowflake`
- **Account**: `<your_snowflake_account_identifier>`
- **Login / Password**: Your Snowflake credentials
- **Database / Schema**: `dbt_db` / `dbt_schema`
- **Warehouse**: `dbt_wh`

### 3. Start Airflow
```bash
astro dev start
```

Access the Airflow UI at `http://localhost:8080` (default credentials: `admin` / `admin`).
