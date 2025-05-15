import os
import json
import yaml
import logging
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# your existing routers
from app.api import greeter, quantative, synthesizer

# new auth & session imports
from app.services.firebase_auth import verify_token
from app.services.session_store import load_session, save_message

# -----------------------------------------------------------------------------
# Load environment variables
# -----------------------------------------------------------------------------
load_dotenv()

# -----------------------------------------------------------------------------
# Load configuration from YAML
# -----------------------------------------------------------------------------
CONFIG_PATH = os.getenv("CONFIG_PATH", "config/config.yaml")

def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            cfg = yaml.safe_load(f)
            if not cfg:
                raise ValueError("Config file is empty or invalid.")
            return cfg
    except FileNotFoundError:
        logging.error(f"❌ Config not found at {CONFIG_PATH}")
        raise SystemExit(f"Config not found: {CONFIG_PATH}")
    except yaml.YAMLError as e:
        logging.error(f"❌ Error parsing {CONFIG_PATH}: {e}")
        raise SystemExit(f"Error parsing config: {e}")
    except Exception as e:
        logging.error(f"❌ Unexpected error loading config: {e}")
        raise SystemExit(f"Failed to load config: {e}")

config = load_config()

# -----------------------------------------------------------------------------
# Set up logging
# -----------------------------------------------------------------------------
log_cfg = config.get("logging", {})
handlers = []
if log_cfg.get("log_file"):
    handlers.append(logging.FileHandler(log_cfg["log_file"]))
if log_cfg.get("use_stream_handler", True):
    handlers.append(logging.StreamHandler())
if not handlers:
    handlers.append(logging.StreamHandler())

logging.basicConfig(
    level=log_cfg.get("level", "INFO").upper(),
    format=log_cfg.get(
        "format",
        "%(asctime)s - %(levelname)s - %(module)s - %(message)s"
    ),
    handlers=handlers
)

# -----------------------------------------------------------------------------
# Initialize FastAPI
# -----------------------------------------------------------------------------
app_cfg = config.get("app", {})
app = FastAPI(
    title=app_cfg.get("name", "AgentAPI"),
    version=app_cfg.get("version", "0.1.0")
)

# -----------------------------------------------------------------------------
# CORS
# -----------------------------------------------------------------------------
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Protected routes with Firebase Auth
# -----------------------------------------------------------------------------
app.include_router(
    greeter.router,
    tags=["Stock Selection & Orchestration"],
    dependencies=[Depends(verify_token)]
)
app.include_router(
    quantative.router,
    tags=["Quantitative Analysis"],
    dependencies=[Depends(verify_token)]
)
app.include_router(
    synthesizer.router,
    tags=["Analysis Synthesis"],
    dependencies=[Depends(verify_token)]
)

# -----------------------------------------------------------------------------
# Root
# -----------------------------------------------------------------------------
@app.get("/")
async def root():
    return {"message": f"Welcome to {app_cfg.get('name', 'the API')}!"}

# -----------------------------------------------------------------------------
# Example of adding a chat endpoint override here if you prefer
# -----------------------------------------------------------------------------
# @app.post("/chat")
# async def chat(request: Request, token_data=Depends(verify_token)):
#     user_id = token_data["uid"]
#     body = await request.json()
#     user_msg = body.get("message", "")
#
#     # 1) load session history
#     session = load_session(user_id)
#
#     # 2) build your prompt (system + session["messages"] + user_msg + tool defs)
#     # 3) call your LLM orchestrator, dispatch AgentTools
#     # 4) save messages:
#     save_message(user_id, {"role": "user", "content": user_msg})
#     #    … after agent reply …
#     save_message(user_id, {"role": "assistant", "content": assistant_msg})
#
#     # return StreamingResponse or JSON
#     return {"reply": assistant_msg}

# -----------------------------------------------------------------------------
# Startup log
# -----------------------------------------------------------------------------
logging.info(f"✅ {app_cfg.get('name', 'API')} is starting up!")
