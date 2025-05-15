# agent_tools.py
#
# A collection of tool wrappers that the Gemini agent can invoke:
#   - web search
#   - BigQuery SQL execution
#   - Python code execution
#   - Cloud Storage file fetching

from typing import Any, Dict, List
import subprocess, sys, traceback
from io import StringIO
from google.cloud import bigquery, storage
import requests

# Initialize Google clients
bq_client = bigquery.Client()
storage_client = storage.Client()

class AgentTools:
    """
    Tool registry for the LLM agent.
    Methods can be called by name from prompts.
    """

    @staticmethod
    def search_web(query: str) -> str:
        """
        Perform a simple web search and return aggregated results.
        (Swap in your own search‑API code here.)
        """
        try:
            resp = requests.get(
                "https://api.example.com/search",
                params={"q": query},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            snippets = [item.get("snippet") for item in data.get("items", [])[:3]]
            return "\n---\n".join(snippets)
        except Exception as e:
            return f"[search_web error] {e}"

    @staticmethod
    def run_bigquery(sql: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query against BigQuery and return rows as dicts.
        """
        try:
            query_job = bq_client.query(sql)
            results = query_job.result()
            return [dict(row) for row in results]
        except Exception as e:
            return [{"error": str(e)}]

    @staticmethod
    def execute_python(code: str) -> str:
        """
        Safely execute Python code and capture stdout or errors.
        WARNING: sandbox appropriately in production.
        """
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        try:
            exec_globals = {}
            exec(code, exec_globals)
            output = sys.stdout.getvalue()
        except Exception:
            output = traceback.format_exc()
        finally:
            sys.stdout = old_stdout
        return output

    @staticmethod
    def fetch_file(bucket_name: str, blob_path: str) -> bytes:
        """
        Download a file from GCS and return its bytes.
        """
        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            return blob.download_as_bytes()
        except Exception as e:
            return f"[fetch_file error] {e}".encode("utf-8")
