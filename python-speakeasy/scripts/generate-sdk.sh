#!/bin/bash
set -e

echo "🔄 Starting SDK generation..."

# Step 1: Backup critical v2 files
echo "📦 Backing up critical files..."

# Backup pyproject.toml
if [ -f "tofupilot/v2/pyproject.toml" ]; then
    cp tofupilot/v2/pyproject.toml /tmp/v2-pyproject.toml.backup
    PYPROJECT_BACKUP_EXISTS=true
else
    echo "⚠️  No existing v2/pyproject.toml found"
    PYPROJECT_BACKUP_EXISTS=false
fi

# Backup error tracking files
if [ -f "tofupilot/v2/client_with_error_tracking.py" ]; then
    cp tofupilot/v2/client_with_error_tracking.py /tmp/client_with_error_tracking.backup
    CLIENT_BACKUP_EXISTS=true
else
    CLIENT_BACKUP_EXISTS=false
fi

# Backup custom __init__.py
if [ -f "tofupilot/v2/__init__.py" ]; then
    cp tofupilot/v2/__init__.py /tmp/v2-init.backup
    INIT_BACKUP_EXISTS=true
else
    INIT_BACKUP_EXISTS=false
fi

# Backup README.md (Speakeasy overwrites it with auto-generated content)
cp README.md /tmp/root-readme.backup

# Backup root pyproject.toml (Speakeasy overwrites it with name="tofupilot.v2")
cp pyproject.toml /tmp/root-pyproject.toml.backup

# Step 2: Remove old v2 directory and create temp directory
echo "🗑️  Removing old v2 directory..."
rm -rf tofupilot/v2
mkdir -p .tmp
cp -r python-speakeasy/.speakeasy .tmp/.speakeasy

# Step 3: Fetch and fix OpenAPI spec
echo "📥 Fetching OpenAPI spec from localhost..."
curl -s http://localhost:3000/api/v2/openapi.json > .tmp/openapi-raw.json

echo "🔧 Fixing OpenAPI spec for Speakeasy compatibility..."
python3 << 'PYEOF'
import json

with open('.tmp/openapi-raw.json', 'r') as f:
    spec = json.load(f)

# Fix 1: Convert type: "null" to nullable pattern (OpenAPI 3.0 compatibility for Speakeasy)
def fix_null_types(obj):
    if isinstance(obj, dict):
        for key in ['oneOf', 'anyOf']:
            if key in obj and isinstance(obj[key], list):
                has_null = False
                non_null_items = []
                for item in obj[key]:
                    if isinstance(item, dict) and item.get('type') == 'null':
                        has_null = True
                    else:
                        non_null_items.append(item)

                if has_null and len(non_null_items) == 1:
                    single_type = non_null_items[0]
                    del obj[key]
                    obj.update(single_type)
                    obj['nullable'] = True
                elif has_null:
                    obj[key] = non_null_items
                    obj['nullable'] = True

        for v in obj.values():
            fix_null_types(v)
    elif isinstance(obj, list):
        for v in obj:
            fix_null_types(v)

fix_null_types(spec)
print("  Fixed null type patterns for Speakeasy 3.0 compat")

# Fix 3: Remove examples with problematic "type" values that conflict with JSON Schema
def remove_problematic_examples(obj):
    if isinstance(obj, dict):
        if 'example' in obj:
            ex = obj['example']
            def has_problematic_type(val):
                if isinstance(val, dict):
                    if 'type' in val and val['type'] not in ['array', 'boolean', 'integer', 'number', 'object', 'string', None]:
                        return True
                    for v in val.values():
                        if has_problematic_type(v):
                            return True
                elif isinstance(val, list):
                    for v in val:
                        if has_problematic_type(v):
                            return True
                return False

            if has_problematic_type(ex):
                del obj['example']

        for v in list(obj.values()):
            remove_problematic_examples(v)
    elif isinstance(obj, list):
        for v in obj:
            remove_problematic_examples(v)

remove_problematic_examples(spec)
print("  Removed problematic examples")

# Fix 4: Add x-speakeasy-name-override for attachment endpoints to avoid name collisions
for path_key, methods in spec.get('paths', {}).items():
    for method_key, op in methods.items():
        if not isinstance(op, dict):
            continue
        op_id = op.get('operationId', '')
        if op_id == 'run-createAttachment':
            op['x-speakeasy-name-override'] = 'create_attachment'
            print("  Added name override for run-createAttachment")
        elif op_id == 'unit-createAttachment':
            op['x-speakeasy-name-override'] = 'create_attachment'
            print("  Added name override for unit-createAttachment")
        elif op_id == 'unit-deleteAttachment':
            op['x-speakeasy-name-override'] = 'delete_attachment'
            print("  Added name override for unit-deleteAttachment")

print("  Security scheme kept as http/bearer")

with open('.tmp/openapi-fixed.json', 'w') as f:
    json.dump(spec, f, indent=2)

print("✅ OpenAPI spec fixed for Speakeasy")
PYEOF

# Step 4: Generate SDK using the fixed spec
echo "🚀 Generating SDK with Speakeasy..."
cd .tmp
speakeasy generate sdk -s openapi-fixed.json -l python -o .
cd ..

# Step 5: Move generated SDK to final location
echo "📁 Moving generated SDK..."
mv .tmp/src/tofupilot/v2 tofupilot/v2

# Step 6: Clean up temp directory
rm -rf .tmp

# Step 7: Restore all backed up files
echo "📦 Restoring backed up files..."

# Restore pyproject.toml
if [ "$PYPROJECT_BACKUP_EXISTS" = true ]; then
    echo "📦 Restoring v2/pyproject.toml..."
    cp /tmp/v2-pyproject.toml.backup tofupilot/v2/pyproject.toml
    rm /tmp/v2-pyproject.toml.backup
else
    echo "⚠️  Creating default v2/pyproject.toml..."
    cat > tofupilot/v2/pyproject.toml << 'EOF'
[project]
name = "tofupilot-v2"
version = "0.7.3"
description = "TofuPilot v2 Speakeasy-generated SDK client"
authors = [
    { name = "TofuPilot Team", email = "hello@tofupilot.com" },
    { name = "Speakeasy" }
]
readme = "../../README.md"
license = { text = "MIT" }
requires-python = ">=3.9.2"

dependencies = [
    "httpcore>=1.0.9",
    "httpx>=0.28.1",
    "pydantic>=2.11.2",
    "posthog>=3.0.0",
]

[build-system]
requires = ["setuptools>=50.0.0", "wheel"]
build-backend = "setuptools.build_meta"

[tool.setuptools]
packages = ["tofupilot.v2"]
include-package-data = true

[tool.setuptools.package-data]
"*" = ["py.typed"]
EOF
fi

# Restore error tracking files
if [ "$CLIENT_BACKUP_EXISTS" = true ]; then
    echo "📦 Restoring client_with_error_tracking.py..."
    cp /tmp/client_with_error_tracking.backup tofupilot/v2/client_with_error_tracking.py
    rm /tmp/client_with_error_tracking.backup
fi

# Restore custom __init__.py or create one
if [ "$INIT_BACKUP_EXISTS" = true ]; then
    echo "📦 Restoring custom __init__.py..."
    cp /tmp/v2-init.backup tofupilot/v2/__init__.py
    rm /tmp/v2-init.backup
else
    echo "🔧 Creating error tracking enabled __init__.py..."
    cat > tofupilot/v2/__init__.py << 'EOF'
"""Code generated by Speakeasy (https://speakeasy.com). DO NOT EDIT."""

from ._version import (
    __title__,
    __version__,
    __openapi_doc_version__,
    __gen_version__,
    __user_agent__,
)
# Import the error tracking enhanced client by default
from .client_with_error_tracking import TofuPilotWithErrorTracking as TofuPilot
# Still export the base SDK for those who want it
from .sdk import TofuPilot as TofuPilotBase
from .sdkconfiguration import *


VERSION: str = __version__
OPENAPI_DOC_VERSION = __openapi_doc_version__
SPEAKEASY_GENERATOR_VERSION = __gen_version__
USER_AGENT = __user_agent__

# Export both versions
__all__ = [
    "TofuPilot",  # Default with error tracking
    "TofuPilotBase",  # Base without error tracking
    # Version info
    "VERSION",
    "OPENAPI_DOC_VERSION",
    "SPEAKEASY_GENERATOR_VERSION",
    "USER_AGENT",
]
EOF
fi

# Restore README.md (prevents Speakeasy from replacing our custom README)
echo "📦 Restoring README.md..."
cp /tmp/root-readme.backup README.md
rm /tmp/root-readme.backup

# Restore root pyproject.toml (prevents Speakeasy from changing package name)
echo "📦 Restoring root pyproject.toml..."
cp /tmp/root-pyproject.toml.backup pyproject.toml
rm /tmp/root-pyproject.toml.backup

echo "✅ SDK generation complete with error tracking preserved!"
