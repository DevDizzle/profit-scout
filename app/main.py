import os
import yaml
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
# Remove 'qualitative' from imports as its router is no longer included
from app.api import greeter, quantative, synthesizer # <--- Removed qualitative

# Load environment variables
load_dotenv()

# Load configuration from YAML
CONFIG_PATH = "config/config.yaml" # Ensure this path is correct relative to where main.py runs

def load_config():
    """Load YAML config file"""
    # Add basic error handling for file loading
    try:
        with open(CONFIG_PATH, "r") as file:
            config_data = yaml.safe_load(file)
            if not config_data:
                raise ValueError("Config file is empty or invalid.")
            return config_data
    except FileNotFoundError:
        logging.error(f"❌ Configuration file not found at: {CONFIG_PATH}")
        raise SystemExit(f"Configuration file not found: {CONFIG_PATH}")
    except yaml.YAMLError as e:
        logging.error(f"❌ Error parsing configuration file {CONFIG_PATH}: {e}")
        raise SystemExit(f"Error parsing configuration file: {e}")
    except Exception as e:
        logging.error(f"❌ An unexpected error occurred loading configuration: {e}")
        raise SystemExit(f"Failed to load configuration: {e}")


config = load_config()

# Validate essential config sections (example)
if not config.get("logging") or not config.get("app"):
     raise SystemExit("❌ Invalid configuration: Missing 'logging' or 'app' section.")

# Set up logging
log_config = config["logging"]
log_handlers = []
if log_config.get("log_file"):
     log_handlers.append(logging.FileHandler(log_config["log_file"]))
# Always add stream handler unless explicitly disabled (optional)
if log_config.get("use_stream_handler", True):
     log_handlers.append(logging.StreamHandler())

if not log_handlers:
     print("Warning: No logging handlers configured (log_file not set and use_stream_handler is false).")
     # Add a basic handler to avoid 'No handlers could be found' error
     log_handlers.append(logging.StreamHandler())


logging.basicConfig(
    level=log_config.get("level", "INFO").upper(), # Default to INFO if not set
    format=log_config.get("format", "%(asctime)s - %(levelname)s - %(module)s - %(message)s"), # Improved default format
    handlers=log_handlers
)

# Initialize FastAPI app
app_config = config["app"]
app = FastAPI(
    title=app_config.get("name", "DefaultAppName"), # Use .get for safety
    version=app_config.get("version", "0.1.0")
)

# Add CORSMiddleware to allow requests from the frontend
# Consider making allowed_origins configurable via environment variable or config file for production
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",") # Example: Read from env var
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins, # Use configured origins
    allow_credentials=True,
    allow_methods=["*"], # Or specific methods: ["GET", "POST"]
    allow_headers=["*"], # Or specific headers
)

# --- Include routes from the updated agent modules ---
# Note: Assuming 'quantative' filename/module name typo is intentional in your project structure
app.include_router(greeter.router, tags=["Stock Selection & Orchestration"]) # Updated tag
app.include_router(quantative.router, tags=["Quantitative Analysis"])
# app.include_router(qualitative.router, tags=["Qualitative Analysis"]) # <--- REMOVED THIS LINE
app.include_router(synthesizer.router, tags=["Analysis Synthesis"]) # Updated tag
# --- End Route Inclusion ---

@app.get("/")
async def root():
    # Use app name from config
    return {"message": f"Welcome to {app_config.get('name', 'the API')}!"}

# Optional: Add startup/shutdown events if needed
# @app.on_event("startup")
# async def startup_event():
#     logging.info("Application startup.")
#
# @app.on_event("shutdown")
# async def shutdown_event():
#     logging.info("Application shutdown.")

# Log that the API is starting (use logger instead of print)
logging.info(f"✅ {app_config.get('name', 'API')} is starting up!")
# Note: The print statement below might execute before uvicorn fully starts the server.
# The logging message above, or uvicorn's own startup message, is more reliable.
# print("✅ ProfitScout API is running!") # Keep if desired, but logging is better practice
