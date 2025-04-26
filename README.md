ProfitScout - AI-Powered Stock Analysis Bot
ProfitScout is an AI-powered stock analysis Discord web app that helps users discover and analyze S&P 500 stocks based on their interests.
It leverages Gemini AI, Google Cloud, and FastAPI to provide intelligent stock recommendations and actionable financial analyses.

🚀 Features
S&P 500 Stock Discovery: Enter a ticker (e.g., AAPL) or company name to retrieve AI-powered stock suggestions.

Quantitative Analysis: Automatically analyzes structured financial metrics (revenue growth, FCF yield, debt-to-equity, etc.) from Yahoo Finance.

Qualitative Analysis: Integrates qualitative insights based on SEC 10-K filings (under active enhancement).

Synthesis Engine: Combines financial metrics + qualitative findings to generate a final Buy, Hold, or Sell recommendation.

Real-Time Feedback: Backend uses Server-Sent Events (SSE) to stream updates to the frontend during analysis.

🛠️ Tech Stack
Backend:

Python 3.11

FastAPI (Web Server + API Routing)

Uvicorn (ASGI Server)

Google Gemini (AI-powered stock suggestion, financial analysis, and synthesis)

Google Cloud Storage (data storage for financial documents)

Pandas (data processing)

Async HTTPX (internal service communication)

Pydantic (data validation)

Frontend:

React + TypeScript

TailwindCSS (UI Styling)

Server-Sent Events (Real-time streaming from backend)

Cloud Infrastructure:

Google Cloud Compute Engine (VM hosting backend and frontend)

Google Cloud Storage (file storage for Yahoo Finance and SEC filings)

📚 Repository Structure
bash
Copy
Edit
profit-scout/
│
├── app/
│   ├── api/
│   │   ├── greeter.py         # Stock validation, suggestions, chat orchestration
│   │   ├── quantative.py       # Quantitative financial analysis
│   │   ├── synthesizer.py      # Synthesis of financial and qualitative analysis
│   ├── services/
│   │   ├── gemini_service.py   # Interactions with Gemini AI (suggest, analyze, synthesize)
│   ├── main.py                 # FastAPI server setup and routing
│
├── frontend/
│   └── src/components/
│       └── ChatUI.tsx          # Frontend chat interface for stock analysis
│
├── requirements.txt            # Python dependencies
├── config/
│   └── config.yaml             # App configuration
⚡ Quick Start
Backend (FastAPI Server)
bash
Copy
Edit
# SSH into your server
cd profit-scout
source pipeline-env/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0
Frontend (React App)
bash
Copy
Edit
cd profit-scout/frontend
npm install
npm run dev -- --host
The frontend will then connect to the backend for real-time stock analysis!

⚠️ Known Limitations
No Persistent Memory Yet: The app currently does not maintain chat memory across sessions. Each new input is treated independently.

Optimized for S&P 500: ProfitScout works best when users enter official S&P 500 tickers (in ALL CAPS, like MSFT, AAPL, etc.). Support for broader stock coverage is planned for future updates.

SEC Filings Summaries: Integration with SEC 10-K filings is functional but still being improved for richer qualitative insights.

We are actively working on fixing these limitations and planning a v2 release!

🏗️ Project Origin
ProfitScout was developed as part of an academic project focused on real-world AI application development, combining:

Natural Language Processing (NLP)

Financial Data Engineering

Cloud Deployment

Real-Time Application Architectures

👏 Acknowledgements
Gemini AI (Google)

FastAPI Team

Wikipedia S&P 500 dataset contributors

Open Source Libraries (Pandas, AsyncHTTPX, React, TailwindCSS)

🎯 Future Plans
Add chat memory to retain user preferences and historical queries

Expand beyond S&P 500 stocks (Russell 1000 support next)

Improve SEC 10-K parsing and multi-modal embeddings for deeper qualitative analysis

Deploy a production-ready containerized version (Docker + Cloud Run)

Would you also like me to quickly generate:

A shorter "About this project" for LinkedIn posting?

A banner image prompt you could use if you want a nice project thumbnail?

Would take me 2 minutes if you want! 🎯
(Also, feel free to ask for different style: more casual, professional, or detailed!)






You said:
Ok update this 

🏗️ Project Origin
ProfitScout was developed as part of an academic project focused on real-world AI application development, combining:

Natural Language Processing (NLP)

Financial Data Engineering

Cloud Deployment

Real-Time Application Architectures (this is not real-time) This was to be a project about using enAI in the SDLC building agentic applications


ChatGPT said:
Got it — here's the updated and corrected version for the 🏗️ Project Origin section:

🏗️ Project Origin
ProfitScout was developed as part of an academic project focused on using Generative AI throughout the Software Development Lifecycle (SDLC) to build agentic applications.
The project integrated:

Natural Language Processing (NLP)

Financial Data Engineering

Cloud-Native Application Development

Agentic Orchestration Using AI Models

Rather than emphasizing real-time architecture, the focus was on embedding AI into every major phase of the application lifecycle — from design and analysis to backend processing and user interaction.
