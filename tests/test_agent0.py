import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app

@pytest.mark.asyncio
async def test_validate_stock():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test", follow_redirects=True) as client:
        response = await client.get("/agent0/validate_stock/AMZN")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_validate_invalid_stock():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test", follow_redirects=True) as client:
        response = await client.get("/agent0/validate_stock/INVALID")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_stock_suggestions():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test", follow_redirects=True) as client:
        response = await client.get("/agent0/stock_suggestions/AI stocks")
    assert response.status_code == 200
