import asyncio

import pytest


@pytest.mark.asyncio
async def test_event_loop_fixture(event_loop):
    """An async test!"""
    assert 'uvloop' in repr(event_loop), 'Test event loop should be a uvloop'
    await asyncio.sleep(0, loop=event_loop)
