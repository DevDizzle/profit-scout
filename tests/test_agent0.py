import pytest
from httpx import AsyncClient
from app.main import app

@pytest.mark.asyncio
async def test_validate_stock():
    """Test stock validation API"""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/agent0/validate_stock/AAPL")
    assert response.status_code == 200
    assert "ticker" in response.json()
    assert "company_name" in response.json()

@pytest.mark.asyncio
async def test_validate_invalid_stock():
    """Test invalid stock validation"""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/agent0/validate_stock/INVALID")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_stock_suggestions():
    """Test stock suggestion API"""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/agent0/stock_suggestions/tech stocks")
    assert response.status_code == 200
    assert "suggestions" in response.json()
