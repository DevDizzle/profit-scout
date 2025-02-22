import os
import re
import json
import logging
import time
from collections import deque

import discord
from dotenv import load_dotenv
import google.generativeai as genai
from google.cloud import bigquery

# Define base directory for consistent file paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
STOCK_JSON_PATH = os.path.join(DATA_DIR, "stock.json")
GEMINI_LOG_PATH = os.path.join(LOGS_DIR, "gemini_responses.log")

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables from .env file
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

if not GEMINI_API_KEY or not DISCORD_BOT_TOKEN:
    logging.error("❌ Missing API keys! Ensure GEMINI_API_KEY and DISCORD_BOT_TOKEN are set in .env.")
    raise ValueError("❌ ERROR: Missing API keys!")

# Configure Gemini 1.5 Pro
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-1.5-pro")

# Initialize BigQuery client
bq_client = bigquery.Client()
BQ_TABLE = "aialchemy.financial_data.sp500_metadata"

# Ensure required directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# Discord Bot Client
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# Store previous user request for context tracking
previous_context = ""

# SECURITY CONSTANTS
MAX_MESSAGE_LENGTH = 1000       # Maximum allowed characters per message
RATE_LIMIT_WINDOW = 10          # Time window in seconds for rate limiting
RATE_LIMIT_COUNT = 5            # Maximum messages allowed within the time window

# Dictionary to track user message timestamps for rate limiting
user_message_times = {}

async def validate_stock(stock_query):
    """Check if a stock ticker or company name is valid using exact matching in BigQuery."""
    logging.debug(f"🔍 Validating stock: {stock_query}")

    query = f"""
    SELECT ticker, company_name FROM `{BQ_TABLE}`
    WHERE LOWER(ticker) = @stock_query OR LOWER(company_name) = @stock_query
    LIMIT 1
    """
    params = [bigquery.ScalarQueryParameter("stock_query", "STRING", stock_query.lower())]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    
    try:
        query_job = bq_client.query(query, job_config=job_config)
        results = list(query_job)
    except Exception as e:
        logging.error(f"❌ BigQuery error: {e}")
        return None, None

    if results:
        logging.info(f"✅ Valid stock found: {results[0]['company_name']} ({results[0]['ticker']})")
        return results[0]['ticker'], results[0]['company_name']
    
    logging.warning(f"❌ Invalid stock: {stock_query} not found in S&P 500 list")
    return None, None

async def guide_stock_selection(user_message="", previous_context=""):
    """Use Gemini to suggest stocks immediately instead of asking endless clarifications."""
    logging.info("🧠 Generating stock selection guidance...")

    guidance_prompt = f"""
    You are an AI financial assistant that helps users **select S&P 500 stocks** based on their interests.
    - **IMMEDIATELY suggest stocks**, do NOT ask for more clarifications unless absolutely necessary.
    - **Ensure every response contains at least 3 unique S&P 500 stocks** relevant to the user's query.
    - If the user's request is vague, assume a **general list of relevant stocks** instead of asking more questions.
    - If the user has previously asked about a sector (e.g., AI infrastructure, data centers, energy), ensure **the new response refines the results rather than repeating**.
    - Provide **distinct stock recommendations for each sector**, avoiding repeated mentions across topics.
    - Example Sectors:
      - **AI Infrastructure** → NVDA (GPUs for AI), AVGO (Networking for AI), MSFT (Azure AI cloud)
      - **Data Centers** → DLR (Data Center REIT), EQIX (Equinix), CSCO (Cisco networking for DCs)
      - **Energy for AI** → NEE (Renewable grid infrastructure), DUK (Utility energy provider), XEL (Grid AI integration)
    - Previous topic: "{previous_context}"
    - Current user query: "{user_message}"
    """

    try:
        logging.debug(f"📡 Sending prompt to Gemini: {guidance_prompt}")
        response = model.generate_content(
            guidance_prompt,
            generation_config={"temperature": 0.5, "max_output_tokens": 150}
        )

        gemini_response = response.text.strip() if response.text else "⚠️ No response received from Gemini"

        logging.debug(f"📡 Gemini response received: {gemini_response}")

        with open(GEMINI_LOG_PATH, "a+", encoding="utf-8") as log_file:
            log_file.write(
                f"\nUser Query: {user_message}\nPrevious Context: {previous_context}\nGemini Response: {gemini_response}\n---\n"
            )

        return gemini_response
    except Exception as e:
        logging.error(f"❌ Gemini API error: {e}")
        return "⚠️ An error occurred while generating stock suggestions."

@client.event
async def on_ready():
    logging.info(f'✅ Bot is running as {client.user}')

@client.event
async def on_message(message):
    global previous_context  # Store last topic for better contextual recommendations

    # Ignore messages from the bot itself
    if message.author == client.user:
        return

    user_message = message.content.strip()
    logging.debug(f"📩 Received message: {user_message} from {message.author}")

    # SECURITY: Rate limiting to prevent abuse
    current_time = time.time()
    user_id = message.author.id
    if user_id not in user_message_times:
        user_message_times[user_id] = deque()
    user_message_times[user_id].append(current_time)
    # Remove timestamps older than the defined window
    while user_message_times[user_id] and current_time - user_message_times[user_id][0] > RATE_LIMIT_WINDOW:
        user_message_times[user_id].popleft()
    if len(user_message_times[user_id]) > RATE_LIMIT_COUNT:
        logging.warning(f"Rate limit exceeded for user {user_id}")
        await message.channel.send("❌ You are sending messages too quickly. Please slow down.")
        return

    # SECURITY: Limit input length to prevent overloading the system
    if len(user_message) > MAX_MESSAGE_LENGTH:
        await message.channel.send("❌ Your message is too long. Please shorten your query.")
        return

    # 🔹 Handle standard greetings and onboarding
    greetings = ["hello", "hi", "hey", "what's up", "how are you"]
    if any(greet in user_message.lower() for greet in greetings):
        await message.channel.send(
            "👋 **Welcome to ProfitScout!**\n\n"
            "I specialize in analyzing **S&P 500** stocks and helping you find investment opportunities.\n"
            "💡 You can ask me for stock recommendations based on sectors (e.g., *AI infrastructure, energy, semiconductors, data centers*).\n"
            "📊 When you're ready, type **`!analyze [Stock Ticker]`** to get detailed insights on a specific company.\n"
            "🔎 I can suggest stocks but currently do **not** analyze crypto or non-S&P 500 stocks.\n\n"
            "How can I help you today?"
        )
        return

    # 🔹 Handle stock selection & sector-based queries with context tracking
    if "stock" in user_message.lower() or "recommend" in user_message.lower() or "suggest" in user_message.lower():
        suggestion = await guide_stock_selection(user_message, previous_context)
        previous_context = user_message  # Store user’s last request for better responses
        await message.channel.send(f"💡 {suggestion}")
        return

    # 🔹 Handle ticker validation for stock analysis
    if user_message.lower().startswith("!analyze"):
        stock_query = user_message.replace("!analyze", "").strip()
        ticker, company_name = await validate_stock(stock_query)

        if ticker:
            stock_data = {"ticker": ticker, "company_name": company_name}
            with open(STOCK_JSON_PATH, "w", encoding="utf-8") as json_file:
                json.dump(stock_data, json_file)

            logging.info(f"📦 Stock data saved: {json.dumps(stock_data)}")
            await message.channel.send(f"✅ **Stock recognized: {company_name} ({ticker})**. Passing to Agent 1...")
        else:
            await message.channel.send("❌ That stock does not appear to be in the **S&P 500**. Please try another.")
        return

    # 🔹 Default to Gemini for general stock-related questions
    response = await guide_stock_selection(user_message, previous_context)
    previous_context = user_message
    await message.channel.send(f"💬 {response}")

client.run(DISCORD_BOT_TOKEN)
