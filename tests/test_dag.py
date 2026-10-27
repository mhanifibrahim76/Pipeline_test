"""Airflow DAG tests: parsing behaviour, configuration, and parse-time safety."""

import sys

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

from tests.conftest import PROJECT_ROOT  # noqa: E402

DAGS_FOLDER = PROJECT_ROOT / "dags"
DEFAULT_DAG_ID = "mart_pocc_fleet_realization"
EXPECTED_TASK_IDS = {"validate_connections", "run_etl"}
DAG_ENV_VARS = (
    "DAG_ID",
    "DAG_SCHEDULE",
    "DAG_CATCHUP",
    "DAG_RETRIES",
    "DAG_RETRY_DELAY_MINUTES",
    "DAG_EXECUTION_TIMEOUT_MINUTES",
    "DAG_MAX_ACTIVE_RUNS",
    "DAG_IS_PAUSED",
    "DAG_TIMEZONE",
    "ETL_START_TIME",
    "ETL_INTERVAL_MINUTES",
)


def _purge_cached_modules():
    """Drop cached src and dags modules so the DAG file re-executes per test."""
    for name in [name for name in sys.modules if name.startswith("src")]:
        del sys.modules[name]
    for name, module in list(sys.modules.items()):
        module_file = getattr(module, "__file__", None)
        if module_file and str(DAGS_FOLDER) in module_file:
            del sys.modules[name]


def _load_dagbag():
    return DagBag(dag_folder=str(DAGS_FOLDER))


@pytest.fixture
def dag_bag(monkeypatch):
    """Build a fresh DagBag with all DAG settings cleared for deterministic defaults."""
    for name in DAG_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    _purge_cached_modules()
    bag = _load_dagbag()
    assert bag.import_errors == {}
    return bag


def test_dag_imports_without_errors(dag_bag):
    assert len(dag_bag.dags) == 1


def test_dag_id_is_stable(dag_bag):
    assert DEFAULT_DAG_ID in dag_bag.dags


def test_dag_id_is_env_driven(monkeypatch):
    monkeypatch.setenv("DAG_ID", "custom_dag_id")
    _purge_cached_modules()
    assert "custom_dag_id" in _load_dagbag().dags


def test_schedule_defaults_to_three_minutes(dag_bag):
    dag = dag_bag.dags[DEFAULT_DAG_ID]
    assert dag.timetable.expression == "*/3 * * * *"


def test_schedule_uses_interval_from_env(monkeypatch):
    monkeypatch.setenv("ETL_INTERVAL_MINUTES", "15")
    _purge_cached_modules()
    dag = _load_dagbag().dags[DEFAULT_DAG_ID]
    assert dag.timetable.expression == "*/15 * * * *"


def test_schedule_env_var_overrides_interval(monkeypatch):
    monkeypatch.setenv("ETL_INTERVAL_MINUTES", "15")
    monkeypatch.setenv("DAG_SCHEDULE", "0 5 * * *")
    _purge_cached_modules()
    dag = _load_dagbag().dags[DEFAULT_DAG_ID]
    assert dag.timetable.expression == "0 5 * * *"


def test_catchup_disabled_by_default(dag_bag):
    assert dag_bag.dags[DEFAULT_DAG_ID].catchup is False


def test_catchup_env_driven(monkeypatch):
    monkeypatch.setenv("DAG_CATCHUP", "TRUE")
    _purge_cached_modules()
    assert _load_dagbag().dags[DEFAULT_DAG_ID].catchup is True


def test_max_active_runs_is_one_by_default(dag_bag):
    assert dag_bag.dags[DEFAULT_DAG_ID].max_active_runs == 1


def test_expected_task_ids_exist(dag_bag):
    dag = dag_bag.dags[DEFAULT_DAG_ID]
    assert {task.task_id for task in dag.tasks} == EXPECTED_TASK_IDS


def test_run_etl_depends_on_validate_connections(dag_bag):
    dag = dag_bag.dags[DEFAULT_DAG_ID]
    assert dag.get_task("run_etl").upstream_task_ids == {"validate_connections"}


def test_timezone_is_wib(dag_bag):
    assert dag_bag.dags[DEFAULT_DAG_ID].timezone.name == "Asia/Jakarta"


def test_start_date_is_env_driven(monkeypatch):
    monkeypatch.setenv("ETL_START_TIME", "2026-01-02T03:04:00+07:00")
    _purge_cached_modules()
    dag = _load_dagbag().dags[DEFAULT_DAG_ID]
    assert dag.start_date is not None
    assert dag.start_date.year == 2026
    assert dag.start_date.month == 1


def test_retries_and_timeout_are_env_driven(dag_bag):
    run_etl = dag_bag.dags[DEFAULT_DAG_ID].get_task("run_etl")
    assert run_etl.retries == 1
    assert run_etl.retry_delay.total_seconds() == 60
    assert run_etl.execution_timeout.total_seconds() == 600


def test_invalid_interval_fails_parse(monkeypatch):
    monkeypatch.setenv("ETL_INTERVAL_MINUTES", "0")
    _purge_cached_modules()
    assert _load_dagbag().import_errors


def test_invalid_boolean_fails_parse(monkeypatch):
    monkeypatch.setenv("DAG_CATCHUP", "banana")
    _purge_cached_modules()
    assert _load_dagbag().import_errors


def test_no_database_access_during_parsing(monkeypatch, dag_bag):
    """Re-import the DAG with engine creation and env loading sabotaged."""
    import dotenv
    import sqlalchemy

    def _fail(*args, **kwargs):
        raise AssertionError("must not run during DAG parsing")

    monkeypatch.setattr(sqlalchemy, "create_engine", _fail)
    monkeypatch.setattr(dotenv, "load_dotenv", _fail)
    _purge_cached_modules()
    bag = _load_dagbag()
    assert bag.import_errors == {}
    assert DEFAULT_DAG_ID in bag.dags
