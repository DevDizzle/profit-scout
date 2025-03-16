import sys
import os

# Explicitly add project root to sys.path to resolve 'app' module import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from httpx import AsyncClient
from app.main import app  # Now this import explicitly works

@pytest.mark.asyncio
async def test_validate_stock():
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/agent0/validate_stock/AMZN")
    assert response.status_code == 200
    assert response.json() == {"ticker": "AMZN", "company_name": "Amazon"}

@pytest.mark.asyncio
async def test_validate_invalid_stock():
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/agent0/validate_stock/INVALID")
    assert response.status_code == 404
    assert response.json()["detail"] == "Stock not found in S&P 500"

@pytest.mark.asyncio
async def test_stock_suggestions():
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/agent0/stock_suggestions/AI")
    assert response.status_code == 200
    suggestions = response.json().get("suggestions")
    assert suggestions is not None
    print("\n✅ Stock Suggestions explicitly from Gemini:", suggestions)
