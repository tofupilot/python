"""TofuPilot integration with OpenHTF.

`upload()` is an OpenHTF output callback: register it with
`test.add_output_callbacks(upload())` and the run is sent to TofuPilot when
the test finishes.

`TofuPilot` is a deprecated context manager kept for existing bench scripts;
it wraps that same callback and warns on use.
"""

from ..error_tracking import ApiV1Error
from .upload import upload
from .tofupilot import TofuPilot

# ApiV1Error is re-exported because `upload` raises it when a run fails to
# reach the server. Catching that needed an import from a private-looking
# module before.
__all__ = ["upload", "TofuPilot", "ApiV1Error"]
