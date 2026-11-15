"""``python -m dewey.worker --config worker.json`` is ``dewey --config worker.json run``."""

import sys

from dewey.cli import main

if __name__ == "__main__":
    raise SystemExit(main([*sys.argv[1:], "run"]))
