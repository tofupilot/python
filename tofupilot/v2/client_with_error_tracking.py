"""TofuPilot SDK with enhanced error tracking and logging capabilities."""

import mimetypes
import os
from pathlib import Path
from typing import Optional, Union

from pydantic_core import ValidationError

import httpx as _httpx

from .sdk import TofuPilot
from .errors.tofupiloterror import TofuPilotError
from ._hooks.types import BeforeRequestContext, BeforeRequestHook
from ._version import __version__


def _enhance_error_message(e: TofuPilotError) -> None:
    """Enhance a TofuPilotError with validation issue details before re-raising."""
    if hasattr(e, "data") and hasattr(e.data, "issues") and e.data.issues:
        details = "; ".join(issue.message for issue in e.data.issues)
        object.__setattr__(e, "message", f"{e.message}: {details}")


class TofuPilotValidationError(Exception):
    """Clear validation error for TofuPilot SDK."""
    pass


def _format_validation_error(e: ValidationError) -> str:
    """Format all pydantic validation errors into a clear message."""
    lines = []
    for error in e.errors():
        loc = " → ".join(str(segment) for segment in error.get('loc', ()))
        msg = error.get('msg', '')
        input_value = error.get('input')
        line = f"  {loc}: {msg}"
        if input_value is not None:
            line += f" (got {input_value!r})"
        lines.append(line)
    return "Invalid input:\n" + "\n".join(lines)


class _ResourceWithBetterErrors:
    """Wraps any SDK resource to enhance TofuPilotError messages with validation details."""

    def __init__(self, resource):
        self._resource = resource

    def __getattr__(self, name):
        attr = getattr(self._resource, name)
        if not callable(attr):
            return attr

        def wrapper(*args, **kwargs):
            try:
                return attr(*args, **kwargs)
            except TofuPilotError as e:
                _enhance_error_message(e)
                raise

        return wrapper


def _upload_to_presigned_url(upload_url: str, file: Path) -> None:
    """Upload file bytes to a pre-signed URL."""
    import httpx

    content_type = mimetypes.guess_type(str(file))[0] or "application/octet-stream"
    with open(file, "rb") as f:
        resp = httpx.put(upload_url, content=f.read(), headers={"Content-Type": content_type})
    if resp.status_code != 200:
        raise RuntimeError(f"File upload failed with status {resp.status_code}")


def _download_from_url(url: str, dest: Path) -> Path:
    """Download a file from a URL to a local path."""
    import httpx

    resp = httpx.get(url)
    if resp.status_code != 200:
        raise RuntimeError(f"Download failed with status {resp.status_code}")
    dest.write_bytes(resp.content)
    return dest


class _RunAttachments:
    """Sub-resource: client.runs.attachments.upload() / .download()"""

    def __init__(self, resource):
        self._resource = resource

    def upload(self, id: str, file: Union[str, Path]) -> str:
        """Upload a file and attach it to a run.

        Args:
            id: Run ID.
            file: Path to the file to upload.

        Returns:
            The attachment ID.
        """
        file = Path(file)
        if not file.exists():
            raise FileNotFoundError(f"File not found: {file}")

        result = self._resource.create_attachment(id=id, name=file.name)
        _upload_to_presigned_url(result.upload_url, file)
        return result.id

    def download(self, attachment, dest: Union[str, Path, None] = None) -> Path:
        """Download an attachment to a local file.

        Args:
            attachment: An attachment object with download_url and name.
            dest: Destination path. Defaults to the attachment name in the current directory.

        Returns:
            The path to the downloaded file.
        """
        url = attachment.download_url
        if not url:
            raise ValueError(f"Attachment '{attachment.name}' has no download URL")

        dest = Path(dest) if dest else Path(attachment.name)
        return _download_from_url(url, dest)


class _UnitAttachments:
    """Sub-resource: client.units.attachments.upload() / .download() / .delete()"""

    def __init__(self, resource):
        self._resource = resource

    def upload(self, serial_number: str, file: Union[str, Path]) -> str:
        """Upload a file and attach it to a unit.

        Args:
            serial_number: Unit serial number.
            file: Path to the file to upload.

        Returns:
            The attachment ID.
        """
        file = Path(file)
        if not file.exists():
            raise FileNotFoundError(f"File not found: {file}")

        result = self._resource.create_attachment(serial_number=serial_number, name=file.name)
        _upload_to_presigned_url(result.upload_url, file)
        return result.id

    def download(self, attachment, dest: Union[str, Path, None] = None) -> Path:
        """Download an attachment to a local file.

        Args:
            attachment: An attachment object with download_url and name.
            dest: Destination path. Defaults to the attachment name in the current directory.

        Returns:
            The path to the downloaded file.
        """
        url = attachment.download_url
        if not url:
            raise ValueError(f"Attachment '{attachment.name}' has no download URL")

        dest = Path(dest) if dest else Path(attachment.name)
        return _download_from_url(url, dest)

    def delete(self, serial_number: str, ids: list) -> object:
        """Delete attachments from a unit.

        Args:
            serial_number: Unit serial number.
            ids: List of attachment IDs to delete.

        Returns:
            Response with deleted IDs.
        """
        try:
            return self._resource.delete_attachment(serial_number=serial_number, ids=ids)
        except TofuPilotError as e:
            _enhance_error_message(e)
            raise


class _RunsWithBetterErrors(_ResourceWithBetterErrors):
    """Extends resource wrapper with ValidationError handling and attachments sub-resource."""

    def __init__(self, resource):
        super().__init__(resource)
        self.attachments = _RunAttachments(resource)

    def create(self, **kwargs):
        try:
            return self._resource.create(**kwargs)
        except TofuPilotError as e:
            _enhance_error_message(e)
            raise
        except ValidationError as e:
            raise TofuPilotValidationError(_format_validation_error(e)) from None


class _UnitsWithAttachments(_ResourceWithBetterErrors):
    """Extends units resource with attachments sub-resource."""

    def __init__(self, resource):
        super().__init__(resource)
        self.attachments = _UnitAttachments(resource)


class _ClientInfoHook(BeforeRequestHook):
    """Injects x-client-type and x-client-version headers into every request."""

    def before_request(
        self, hook_ctx: BeforeRequestContext, request: _httpx.Request
    ):
        request.headers["x-client-type"] = "python"
        request.headers["x-client-version"] = __version__
        return request


class TofuPilotWithErrorTracking(TofuPilot):
    """
    Enhanced TofuPilot client with automatic error tracking and improved logging.

    This wrapper extends the base TofuPilot SDK with:
    - Automatic error tracking and categorization
    - Enhanced logging for debugging
    - Better error context and suggestions
    - Transparent API - all original methods work exactly the same
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        server_url: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        retry_config=None,
        debug: bool = False,
        **kwargs
    ):
        """
        Initialize TofuPilot client with error tracking.

        Args:
            api_key: API key for authentication
            server_url: Override default server URL
            timeout_ms: Request timeout in milliseconds
            retry_config: Retry configuration
            debug: Enable debug logging
            **kwargs: Additional arguments passed to base SDK
        """

        if api_key is None:
            api_key = os.environ.get("TOFUPILOT_API_KEY", None)

        # Initialize base SDK
        super().__init__(
            api_key=api_key,
            server_url=server_url,
            timeout_ms=timeout_ms,
            retry_config=retry_config,
            **kwargs
        )

        # Register client info hook for API activity tracking
        self.sdk_configuration._hooks.register_before_request_hook(_ClientInfoHook())

    def __getattr__(self, name: str):
        attr = super().__getattr__(name)
        if name == 'runs':
            attr = _RunsWithBetterErrors(attr)
        elif name == 'units':
            attr = _UnitsWithAttachments(attr)
        else:
            attr = _ResourceWithBetterErrors(attr)
        setattr(self, name, attr)
        return attr
