"""TofuPilot integration with OpenHTF.

`upload()` is an OpenHTF output callback: register it with
`test.add_output_callbacks(upload())` and the run is sent to TofuPilot when
the test finishes.

`record_plugs(*phases)` wraps the test's phases so the run also records the
instrument behind each plug (manufacturer, model, serial number, firmware
from the plug's `_asdict()`) and the plugs each phase received.

`TofuPilot` is a deprecated context manager kept for existing bench scripts;
it wraps that same callback and warns on use.

For a new test procedure, use the TofuPilot Framework rather than OpenHTF:
install the TofuPilot CLI (https://www.tofupilot.com/docs/cli) and run
`tofupilot init`.
"""

from ..error_tracking import ApiV1Error
from .upload import upload
from .tofupilot import TofuPilot
from .plugs import record_plugs

# ApiV1Error is re-exported because `upload` raises it when a run fails to
# reach the server. Catching that needed an import from a private-looking
# module before.
__all__ = ["upload", "record_plugs", "TofuPilot", "ApiV1Error"]
