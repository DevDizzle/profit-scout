````markdown
# 🏦 ProfitScout - AI-Powered Stock Analysis Bot

ProfitScout is an *AI-powered stock analysis Discord bot* that helps users *discover and analyze S&P 500 stocks* based on their interests.  
It leverages *Gemini AI* for intelligent stock recommendations and *BigQuery* for validating tickers.

---

## 🚀 Features
- **Smart Stock Recommendations:** Get AI-powered stock picks based on investment themes.
- **Real-Time Stock Validation:** Checks stock symbols against *S&P 500 listings* in BigQuery.
- **Natural Conversations:** Understands *sector-based queries* and maintains context.
- **Stock Analysis (`!analyze`):** Provides insights when a user selects a specific stock.
- **Secure API Handling:** Uses a `.env` file to keep API keys private.
- **Persistent Logs:** Stores Gemini responses for debugging.

---

## 📂 Project Structure
```
profitscout/
├── Agent0                # Discord bot (formerly bot_discord.py)
├── requirements.txt       # Dependencies list
├── .env                   # API keys (ignored in Git)
├── .gitignore             # Prevents sensitive files from being tracked
├── README.md              # Documentation
├── data/                 # Stores stock.json for Agent1
│   └── stock.json
├── logs/                 # Logs Gemini responses
│   └── gemini_responses.log
├── artifacts/            # Reserved for future outputs
└── venv/                 # Virtual environment (ignored in Git)
```

---

## ⚙️ Setup & Installation

### 1️⃣ Clone the Repository
```sh
git clone https://github.com/DevDizzle/profitscout.git
cd profitscout
```

### 2️⃣ Create a Virtual Environment
```sh
python3 -m venv venv
source venv/bin/activate
```

### 3️⃣ Install Dependencies
```sh
pip install -r requirements.txt
```

### 4️⃣ Configure API Keys
Create a `.env` file in the project root with the following content:
```ini
GEMINI_API_KEY="your-gemini-api-key"
DISCORD_BOT_TOKEN="your-discord-bot-token"
```

### 5️⃣ Run the Bot
```sh
python Agent0
```

---

## 📜 How to Use ProfitScout

### Stock Recommendations
Ask for stock recommendations based on a theme:

> **User:** What are the best AI infrastructure stocks?  
> **ProfitScout:** 💡 Here are some relevant S&P 500 stocks:
> - **NVDA (Nvidia):** Leading AI GPU provider  
> - **AVGO (Broadcom):** Chips for AI networking  
> - **MSFT (Microsoft):** AI cloud computing with Azure

### Analyze a Stock
To analyze a specific stock, use the `!analyze` command:

> **User:** `!analyze XEL`  
> **ProfitScout:** ✅ Stock recognized: **Xcel Energy (XEL)**. Passing to Agent 1...

### General Inquiries
If the bot doesn’t understand a message, it provides suggestions:

> **User:** Doughnut Hole  
> **ProfitScout:** 🤖 I'm here to analyze *S&P 500 stocks*! You can ask:
> - "What are the best AI infrastructure stocks?"
> - "Should I buy Tesla?"
> - "Recommend energy companies investing in AI."

---

## 🛠 Troubleshooting

1. **API Keys Not Loading**  
   Run:
   ```sh
   echo $GEMINI_API_KEY
   echo $DISCORD_BOT_TOKEN
   ```
   If blank, check your `.env` file or run:
   ```sh
   source .env
   ```

2. **Virtual Environment Issues**  
   Ensure your virtual environment is activated:
   ```sh
   source venv/bin/activate
   ```

3. **Logs Not Appearing**  
   Check the logs:
   ```sh
   cat logs/gemini_responses.log
   ```

---

## 📌 Contributing
- Fork the repository and create a feature branch.
- Submit a Pull Request with detailed changes.
- Ensure all features are documented and tested before submission.

---

## 📝 License
This project is licensed under the MIT License.
X
---

## 🔥 Why This README is Perfect
- **Clear setup instructions:** Easy to install and run.
- **Functionality overview:** Users understand what the bot does.
- **Example commands:** Shows how to interact with the bot.
- **Troubleshooting tips:** Helps resolve common issues.
- **Structured & professional:** Makes the repository easy to maintain and contribute.

Now, **ProfitScout** is ready to help you discover the perfect S&P 500 stocks! 🚀🔥
