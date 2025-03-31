import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app

@pytest.mark.asyncio
async def test_validate_stock():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test", follow_redirects=True) as client:
        # Updated endpoint from /agent0/validate_stock to /greeter/validate_stock
        response = await client.get("/greeter/validate_stock/AMZN")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_validate_invalid_stock():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test", follow_redirects=True) as client:
        # Updated endpoint from /agent0/validate_stock to /greeter/validate_stock
        response = await client.get("/greeter/validate_stock/INVALID")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_stock_suggestions():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test", follow_redirects=True) as client:
        # Updated endpoint from /agent0/stock_suggestions to /greeter/stock_suggestions
        response = await client.get("/greeter/stock_suggestions/AI stocks")
    assert response.status_code == 200
