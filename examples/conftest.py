"""The example DAG files are imported by their tests as top-level modules."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
