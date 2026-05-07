"""TofuPilot pytest plugin for test integration."""

from .plugin import TestPilotPlugin

# Alias for backward compatibility
TofuPilotPlugIn = TestPilotPlugin

__all__ = ["TofuPilotPlugIn", "TestPilotPlugin"]
