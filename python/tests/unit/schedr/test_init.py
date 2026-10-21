from importlib.metadata import version

import schedr


def test_version_is_the_installed_package_version() -> None:
    assert schedr.__version__ == version("schedr")
