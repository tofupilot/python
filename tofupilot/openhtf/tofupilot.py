"""Deprecated compatibility wrapper around the `upload` output callback.

Keeps `with TofuPilot(test):` working for existing bench scripts: it
registers the `upload` callback and preserves the old quiet Ctrl-C exit,
nothing more. The TofuPilot CLI (`tofupilot run`) provides the live operator
UI.
"""

import warnings
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:  # pragma: no cover - types only, never imported at runtime
    from openhtf import Test

from .upload import upload


class TofuPilot:
    """
    Deprecated context manager that uploads the OpenHTF test to TofuPilot on
    completion. Prefer registering the callback directly:

    ```python
    from tofupilot.openhtf import upload

    test.add_output_callbacks(upload())
    test.execute(lambda: "SN15")
    ```

    For a live operator UI, run the unmodified test file with the CLI:
    `tofupilot run`.

    Behavioral notes for scripts migrating off this wrapper:
    - `stream` is accepted for compatibility and ignored.
    - The wrapper suppresses `KeyboardInterrupt` on exit (quiet Ctrl-C), as
      it always did. The plain callback does not.
    """

    def __init__(
        self,
        test: "Test",
        stream: Optional[bool] = True,
        api_key: Optional[str] = None,
        url: Optional[str] = None,
        verify: Optional[str] = None,
    ):
        warnings.warn(
            "tofupilot.openhtf.TofuPilot is deprecated and will be removed in "
            "a future release. It now only wraps the `upload` output "
            "callback; the live operator UI is provided by the TofuPilot CLI.\n"
            "  Uploading only:   test.add_output_callbacks(upload())\n"
            "  Live operator UI: run the test with `tofupilot run`\n"
            "See https://tofupilot.com/docs/frameworks/openhtf",
            FutureWarning,
            stacklevel=2,
        )
        self.test = test
        # Public attributes the original wrapper exposed; kept so legacy
        # scripts that introspect the object (`if tp.stream:`, logging
        # `tp.url`) keep running.
        self.stream = stream
        self.api_key = api_key
        self.url = url
        # The callback owns the client wiring — one code path to thread
        # credentials/TLS parameters through, not two.
        self._upload = upload(api_key=api_key, url=url, verify=verify)
        self.client = self._upload.client
        self._logger = self.client._logger

    def __enter__(self):
        self.test.add_output_callbacks(self._upload)
        # Quiet the client's console logging during the test, exactly like the
        # original wrapper; the upload callback resumes it while it runs.
        if hasattr(self._logger, "pause"):
            self._logger.pause()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self._logger, "resume"):
            self._logger.resume()
        # Deregister the callback so re-entering — or a fresh wrapper on the
        # same Test object, the classic `for sn in queue: with TofuPilot(test)`
        # station loop — does not stack callbacks and upload each run once per
        # accumulated wrapper. OpenHTF has no public remove API, so reach into
        # the options list and degrade gracefully if its internals change.
        try:
            self.test._test_options.output_callbacks.remove(self._upload)
        except (AttributeError, ValueError):  # pragma: no cover - defensive
            # Swallowing silently would quietly reintroduce the double-upload
            # bug this remove prevents — say so if OpenHTF's internals moved.
            self._logger.warning(
                "Could not deregister the TofuPilot upload callback; reusing "
                "this Test object in another with-block may upload runs twice."
            )
        # Historical contract: Ctrl-C during a test exits without a traceback.
        return exc_type is KeyboardInterrupt
