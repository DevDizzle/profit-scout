from locust import HttpUser, task, between

class APIPerformanceTest(HttpUser):
    wait_time = between(1, 3)

    @task
    def test_validate_stock(self):
        self.client.get("/agent0/validate_stock/AAPL")

    @task
    def test_analyze_stock(self):
        self.client.get("/agent1/analyze_stock/MSFT")

    @task
    def test_stock_suggestions(self):
        self.client.get("/agent0/stock_suggestions/tech stocks")
