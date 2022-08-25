#!/usr/bin/env python3
import asyncio
import pytest
import pytest_asyncio

import bin


@pytest_asyncio.fixture
async def example_00():
    return {
    }


@pytest.mark.asyncio
async def test_cleanup():
    print('ok')
    return 0
