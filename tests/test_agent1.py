import sys
import os

# Explicitly add project root to sys.path for correct imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_analyze_amzn_stock():
    """Test Agent 1 analyzing stock AMZN."""
    async with AsyncClient() as client:
        response = await client.get("http://localhost:8000/agent1/analyze_stock/AMZN")
    assert response.status_code == 200
    data = response.json()
    assert "financial_ratios" in data
    assert "analysis" in data
    print("LLM Analysis for AMZN:", data["analysis"])
