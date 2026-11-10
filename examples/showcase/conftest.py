"""The showcase's DAG files are imported by its test from their own root, as on a worker."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "dags"))
