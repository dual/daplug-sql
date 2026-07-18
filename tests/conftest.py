import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Silence daplug_core.logger output during tests; never stub the module itself,
# a stub hides real import breaks from the suite.
os.environ.setdefault('RUN_MODE', 'unittest')
