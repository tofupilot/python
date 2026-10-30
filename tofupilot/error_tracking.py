import os
import functools
import traceback
from typing import Optional, Dict, Any, Callable, TypeVar
import posthog
from .telemetry_config import is_telemetry_enabled

if is_telemetry_enabled():
    posthog.api_key = "phc_teUdXacHioniALiYDBE3MwJrMJscNx5NTHt6obkLGRHV"
    # The dashboard's /ingest proxy: a PostHog region move needs no SDK release.
    posthog.host = "https://www.tofupilot.app/ingest"
    posthog.debug = False
else:
    posthog.api_key = "I need to be a truthy value, even if posthog is disabled"
    posthog.disabled = True


class ApiVersionWarning(Exception):
    """Not intended to be raised, instead used for posthog"""

    pass


class ApiV1Error(Exception):
    pass
