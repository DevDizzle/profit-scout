import pytest
from httpx import AsyncClient
from app.main import app

@pytest.mark.asyncio
async def test_analyze_stock():
    """Test stock analysis API"""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/agent1/analyze_stock/AAPL")
    assert response.status_code == 200
    assert "financial_ratios" in response.json()
    assert "analysis" in response.json()

@pytest.mark.asyncio
async def test_analyze_invalid_stock():
    """Test analyzing a stock that doesn't exist"""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/agent1/analyze_stock/INVALID")
    assert response.status_code == 404
