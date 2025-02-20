import os
import re
import discord
import google.generativeai as genai
from google.cloud import bigquery

# Load API keys
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

# Configure Gemini 1.5 Pro
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-1.5-pro")

# Initialize BigQuery client
bq_client = bigquery.Client()

# Discord Bot Client
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# BigQuery Table Info
BQ_TABLE = "aialchemy.financial_data.sp500_metadata"

async def validate_stock(stock_query):
    """Check if a stock ticker or company name is valid using BigQuery."""
    query = f"""
    SELECT ticker, company_name FROM `{BQ_TABLE}`
    WHERE LOWER(ticker) = '{stock_query.lower()}' OR LOWER(company_name) LIKE '%{stock_query.lower()}%'
    LIMIT 1
    """
    query_job = bq_client.query(query)
    results = list(query_job)

    if results:
        return results[0]['ticker'], results[0]['company_name']
    return None, None

async def guide_stock_selection():
    """Use Gemini to guide users in selecting a stock."""
    guidance_prompt = """
    You are a financial assistant helping a user select a stock for analysis.
    If the user has no preference, suggest well-known S&P 500 stocks from different sectors.
    Ask clarifying questions to refine their choice.
    """
    response = model.generate_content(guidance_prompt, generation_config={"temperature": 0.5, "max_output_tokens": 100})
    return response.text.strip()

@client.event
async def on_ready():
    print(f'✅ Bot is running as {client.user}')

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith("!analyze"):
        user_input = message.content.split(" ", 1)

        if len(user_input) > 1:
            stock_query = user_input[1]
            ticker, company_name = await validate_stock(stock_query)

            if ticker:
                await message.channel.send(f"✅ Stock recognized: **{company_name} ({ticker})**. Passing to Agents 1 & 2...")
                await process_stock(ticker, message)
            else:
                await message.channel.send("❌ Invalid stock ticker or company name. Try again.")
        else:
            suggestion = await guide_stock_selection()
            await message.channel.send(suggestion)

async def process_stock(ticker, message):
    """Passes the stock to Agents 1 & 2 for analysis."""
    formatted_query = f"""
    User selected {ticker}.
    Retrieve financials and 10-Q insights for {ticker}.
    Analyze:
    - Revenue Growth
    - Free Cash Flow
    - Operating Margins
    - Key Management Discussion insights
    """
    await message.channel.send(f"📊 Analyzing {ticker}...")

    # Forward to Agents 1 & 2
    # Here, we should trigger Agent 1 (Financial Metrics) and Agent 2 (Text Summarization)
    # Example: send to an async task queue or call directly.

client.run(DISCORD_BOT_TOKEN)
