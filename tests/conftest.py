import pathlib
import sys
import types

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if 'daplug_core.logger' not in sys.modules:
    module = types.ModuleType('daplug_core.logger')
    class _Logger:
        def log(self, *args, **kwargs):
            pass
    module.logger = _Logger()
    sys.modules['daplug_core.logger'] = module
