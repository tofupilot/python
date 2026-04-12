# TofuPilot Python Client

[![PyPI version](https://badge.fury.io/py/tofupilot.svg)](https://badge.fury.io/py/tofupilot)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/upview/8e092a304bae7d91aa154e19bdf694a4/raw/python-client-tests.json)](https://github.com/tofupilot/python)

The official Python client for [TofuPilot](https://tofupilot.com). Integrate your hardware test runs into one app with just a few lines of Python.

## Installation

```bash
pip install tofupilot
```

## Quick Start

```python
import os
from tofupilot.v2 import TofuPilot

with TofuPilot(api_key=os.getenv("TOFUPILOT_API_KEY")) as client:
    run = client.runs.create(
        procedure_id="your-procedure-id",
        serial_number="SN001",
        part_number="PN001",
        outcome="PASS",
    )
    print(f"Run created: {run.id}")
```

## Authentication

Set your API key as an environment variable:

```bash
export TOFUPILOT_API_KEY="your-api-key"
```

Or pass it directly:

```python
client = TofuPilot(api_key="your-api-key")
```

To point to a different server (e.g. self-hosted):

```python
client = TofuPilot(api_key="your-api-key", server_url="https://your-instance.com/api")
```

## Available Resources

| Resource | Operations |
|----------|-----------|
| `client.runs` | list, create, get, delete, update |
| `client.runs.attachments` | upload, download |
| `client.units` | list, create, get, delete, update, add_child, remove_child |
| `client.units.attachments` | upload, download, delete |
| `client.parts` | list, create, get, delete, update |
| `client.parts.revisions` | create, get, delete, update |
| `client.procedures` | list, create, get, delete, update |
| `client.procedures.versions` | create, get, delete |
| `client.batches` | list, create, get, delete, update |
| `client.stations` | list, create, get, get_current, remove, update |
| `client.user` | list |

## Usage Examples

### Create a run with measurements

```python
from datetime import datetime, timedelta, timezone

run = client.runs.create(
    procedure_id=procedure_id,
    serial_number="SN-001",
    part_number="PCB-V1",
    outcome="PASS",
    started_at=datetime.now(timezone.utc) - timedelta(minutes=5),
    ended_at=datetime.now(timezone.utc),
    phases=[{
        "name": "Voltage Test",
        "outcome": "PASS",
        "started_at": datetime.now(timezone.utc) - timedelta(minutes=5),
        "ended_at": datetime.now(timezone.utc),
        "measurements": [{
            "name": "Output Voltage",
            "outcome": "PASS",
            "measured_value": 3.3,
            "units": "V",
            "validators": [
                {"operator": ">=", "expected_value": 3.0},
                {"operator": "<=", "expected_value": 3.6},
            ],
        }],
    }],
)
```

### List and filter runs

```python
result = client.runs.list(
    part_numbers=["PCB-V1"],
    outcomes=["PASS"],
    limit=10,
)

for run in result.data:
    print(f"{run.id} — {run.unit.serial_number}")
```

### Manage units and sub-units

```python
# Create part and revision
client.parts.create(number="PCB-V1", name="Main Board")
client.parts.revisions.create(part_number="PCB-V1", number="REV-A")

# Create units
client.units.create(serial_number="PARENT-001", part_number="PCB-V1", revision_number="REV-A")
client.units.create(serial_number="CHILD-001", part_number="PCB-V1", revision_number="REV-A")

# Link parent-child
client.units.add_child(serial_number="PARENT-001", child_serial_number="CHILD-001")
```

### Attach files to runs and units

```python
# Upload a file to a run
client.runs.attachments.upload(id=run_id, file="report.pdf")

# Upload a file to a unit
client.units.attachments.upload(serial_number="SN-0001", file="calibration.pdf")

# Download an attachment
run = client.runs.get(id=run_id)
client.runs.attachments.download(run.attachments[0], dest="local-copy.pdf")

# Delete a unit attachment
client.units.attachments.delete(serial_number="SN-0001", ids=[attachment_id])
```

## Error Handling

```python
from tofupilot.v2.models.errors import ErrorNOTFOUND, ErrorBADREQUEST

try:
    client.runs.get(id="nonexistent-id")
except ErrorNOTFOUND as e:
    print(f"Not found: {e.message}")
except ErrorBADREQUEST as e:
    print(f"Bad request: {e.message}")
```

| Exception | Status Code |
|-----------|------------|
| `ErrorBADREQUEST` | 400 |
| `ErrorUNAUTHORIZED` | 401 |
| `ErrorFORBIDDEN` | 403 |
| `ErrorNOTFOUND` | 404 |
| `ErrorCONFLICT` | 409 |
| `ErrorUNPROCESSABLECONTENT` | 422 |
| `ErrorINTERNALSERVERERROR` | 500 |

## Running Tests

```bash
cd clients/python-speakeasy
cp tests/.env.local.example tests/.env.local  # Set your API key and URL
python -m pytest tests/v2/
```

## Documentation

- [Getting Started](https://tofupilot.com/docs/dashboard)
- [API Reference](https://tofupilot.com/docs/dashboard/api/v2)
- [Changelog](https://tofupilot.com/changelog)

## License

MIT
