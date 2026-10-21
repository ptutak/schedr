"""``python -m schedr.runner``: see :mod:`schedr.runner.main`."""

import os
import sys

from schedr.runner.main import main

raise SystemExit(main(sys.argv[1:], os.environ))
