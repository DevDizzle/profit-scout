import os
import yaml
import logging
from fastapi import FastAPI
from dotenv import load_dotenv
from app.api import greeter, quantative, qualitative, synthesizer

# Load environment variables
load_dotenv()

# Load configuration from YAML
CONFIG_PATH = "config/config.yaml"

def load_config():
    """Load YAML config file"""
    with open(CONFIG_PATH, "r") as file:
        return yaml.safe_load(file)

config = load_config()

# Set up logging
logging.basicConfig(
    level=config["logging"]["level"],
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(config["logging"]["log_file"]),
        logging.StreamHandler()
    ]
)

# Initialize FastAPI app
app = FastAPI(title=config["app"]["name"], version=config["app"]["version"])

# Include routes from the updated agent modules
app.include_router(greeter.router, tags=["Stock Selection"])
app.include_router(quantative.router, tags=["Quantitative Analysis"])
app.include_router(qualitative.router, tags=["Qualitative Analysis"])
app.include_router(synthesizer.router, tags=["Synthesis"])

@app.get("/")
async def root():
    return {"message": f"Welcome to {config['app']['name']}!"}

print("✅ ProfitScout API is running!")
