from importlib.metadata import version

import schedr


def test_version_is_the_installed_package_version() -> None:
    assert schedr.__version__ == version("schedr")


def test_every_exported_name_exists() -> None:
    for name in schedr.__all__:
        assert hasattr(schedr, name), name


def test_the_public_api_is_exported() -> None:
    expected = {
        "Dag",
        "task",
        "TaskDefaults",
        "CommandOperator",
        "EmptyOperator",
        "Cron",
        "Every",
        "DAILY",
        "dag_ctx",
        "chain",
        "Output",
        "KiB",
        "MiB",
    }
    assert expected <= set(schedr.__all__)
