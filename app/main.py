import os
import yaml
import logging
from fastapi import FastAPI
from dotenv import load_dotenv
from app.api import agent0, agent1

# Load environment variables
load_dotenv()

# Load Configuration from YAML
CONFIG_PATH = "config/config.yaml"

def load_config():
    """Load YAML config file"""
    with open(CONFIG_PATH, "r") as file:
        return yaml.safe_load(file)

config = load_config()

# Set up Logging
logging.basicConfig(
    level=config["logging"]["level"],
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(config["logging"]["log_file"]), logging.StreamHandler()]
)

# Initialize FastAPI App
app = FastAPI(title=config["app"]["name"], version=config["app"]["version"])

# Include Agent0 & Agent1 Routes (prefixes managed explicitly in routers)
app.include_router(agent0.router, tags=["Stock Selection"])
app.include_router(agent1.router, tags=["Financial Analysis"])

@app.get("/")
async def root():
    return {"message": f"Welcome to {config['app']['name']}!"}

print("✅ ProfitScout API is running!")
