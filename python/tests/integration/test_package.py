import subprocess
import sys

import pytest


@pytest.mark.integration
def test_package_importable_in_subprocess() -> None:
    result = subprocess.run(
        [sys.executable, "-c", "import schedr; print(schedr.greet('world'))"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "Hello, world!"
