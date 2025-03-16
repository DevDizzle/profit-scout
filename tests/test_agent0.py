import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from httpx import AsyncClient
from app.main import app

@pytest.mark.asyncio
async def test_validate_stock():
    async with AsyncClient(app=app, base_url="http://test", follow_redirects=True) as client:
        response = await client.get("/agent0/validate_stock/AMZN")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_validate_invalid_stock():
    async with AsyncClient(app=app, base_url="http://test", follow_redirects=True) as client:
        response = await client.get("/agent0/validate_stock/INVALID")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_stock_suggestions():
    async with AsyncClient(app=app, base_url="http://test", follow_redirects=True) as client:
        response = await client.get("/agent0/stock_suggestions/AI stocks")
    assert response.status_code == 200
