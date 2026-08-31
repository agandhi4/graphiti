"""
Pytest configuration for MCP server tests.
This file prevents pytest from loading the parent project's conftest.py
"""

import sys
from pathlib import Path

import pytest

# Add src directory to Python path for imports
src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

from config.schema import GraphitiConfig  # noqa: E402


@pytest.fixture(autouse=True)
def isolated_queue_spool(tmp_path, monkeypatch):
    """Point the queue's durable spool at a temp dir.

    QueueService persists episodes under GRAPHITI_QUEUE_DIR (default
    ~/.graphiti/queue) and replays them on initialize(); without this,
    tests would write into — and consume from — the real spool.
    """
    monkeypatch.setenv('GRAPHITI_QUEUE_DIR', str(tmp_path / 'queue'))


@pytest.fixture
def config():
    """Provide a default GraphitiConfig for tests."""
    return GraphitiConfig()
