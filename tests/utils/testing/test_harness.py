import asyncio

import pytest


@pytest.mark.asyncio
async def test_event_loop_fixture(event_loop):
    """Confirm event loop fixture works and is a uvloop"""
    assert 'uvloop' in repr(event_loop), 'Event loop fixture should be a uvloop'
    await asyncio.sleep(0, loop=event_loop)
