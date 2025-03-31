import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronUp } from "lucide-react"; // Assuming an icon library like lucide-react

interface Message {
  id: string;
  text: string;
  type: "user" | "bot";
  timestamp: string;
  expandable?: boolean;
  expanded?: boolean;
  summary?: string;
}

function generateUniqueId() {
  return Date.now().toString() + "-" + Math.random().toString(36).substr(2, 9);
}

// Format quantitative data into a table
const formatQuantitativeMetrics = (ticker: string, data: any) => {
  const { revenue_growth, latest_revenue, free_cash_flow, debt_to_equity, market_cap, price_trend_ratio } = data;
  return `
### Quantitative Metrics for ${ticker}
| Metric                | Value              |
|-----------------------|--------------------|
| **Revenue Growth**    | ${(revenue_growth * 100).toFixed(2)}% |
| **Latest Revenue**    | $${(latest_revenue / 1e9).toFixed(2)}B |
| **Free Cash Flow**    | $${(free_cash_flow / 1e9).toFixed(2)}B |
| **Debt-to-Equity**   | ${debt_to_equity.toFixed(3)} |
| **Market Cap**        | $${(market_cap / 1e12).toFixed(2)}T |
| **Price Trend**       | ${price_trend_ratio > 1 ? "Positive" : "Negative"} (${price_trend_ratio.toFixed(2)}) |
[Expand for full metrics]`;
};

// Simplify qualitative analysis
const formatQualitativeAnalysis = (ticker: string, text: string) => {
  const lines = text.split("*").filter(Boolean).map(line => line.trim());
  const summary = lines.slice(0, 3).map(line => `- ${line}`).join("\n");
  return {
    summary: `### Qualitative Analysis for ${ticker}\n${summary}\n[Expand for details]`,
    full: `### Qualitative Analysis for ${ticker}\n${lines.map(line => `- ${line}`).join("\n")}`,
  };
};

// Format final analysis
const formatFinalAnalysis = (ticker: string, text: string) => {
  const lines = text.split("*").filter(Boolean).map(line => line.trim());
  const summary = lines.slice(0, 3).map(line => `- ${line}`).join("\n");
  return {
    summary: `### 📊 Final Analysis for ${ticker}\n${summary}\n[Expand for full analysis]`,
    full: `### 📊 Final Analysis for ${ticker}\n${lines.map(line => `- ${line}`).join("\n")}`,
  };
};

export default function ChatUI() {
  const [query, setQuery] = useState("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [isProcessing, setIsProcessing] = useState(false);

  const backendUrl = import.meta.env.VITE_BACKEND_URL;

  const fetchSynthesis = async (ticker: string) => {
    setIsProcessing(true);

    setMessages((prev) => [
      ...prev,
      {
        id: generateUniqueId(),
        text: `ProfitScout is analyzing ${ticker}...`,
        type: "bot",
        timestamp: new Date().toLocaleString(),
      },
    ]);

    try {
      const quantitativeRes = await fetch(`${backendUrl}/quantative/analyze_stock/${ticker}`);
      if (!quantitativeRes.ok) throw new Error("Quantitative analysis failed");
      const quantitativeData = await quantitativeRes.json();
      setMessages((prev) => [
        ...prev,
        {
          id: generateUniqueId(),
          text: formatQuantitativeMetrics(ticker, quantitativeData.quantitative_analysis),
          type: "bot",
          timestamp: new Date().toLocaleString(),
          expandable: true,
          expanded: false,
          summary: formatQuantitativeMetrics(ticker, quantitativeData.quantitative_analysis),
        },
      ]);

      const qualitativeRes = await fetch(`${backendUrl}/qualitative/analyze_sec/${ticker}`);
      if (!qualitativeRes.ok) throw new Error("Qualitative analysis failed");
      const qualitativeData = await qualitativeRes.json();
      const qualitativeFormatted = formatQualitativeAnalysis(ticker, qualitativeData.qualitative_analysis);
      setMessages((prev) => [
        ...prev,
        {
          id: generateUniqueId(),
          text: qualitativeFormatted.full,
          type: "bot",
          timestamp: new Date().toLocaleString(),
          expandable: true,
          expanded: false,
          summary: qualitativeFormatted.summary,
        },
      ]);

      const synthesisRes = await fetch(`${backendUrl}/synthesizer/synthesize`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ticker,
          yahoo_analysis: quantitativeData.quantitative_analysis,
          sec_analysis: qualitativeData.qualitative_analysis,
        }),
      });
      if (!synthesisRes.ok) throw new Error("Synthesis failed");
      const synthesisData = await synthesisRes.json();
      const finalFormatted = formatFinalAnalysis(ticker, synthesisData.synthesis);
      setMessages((prev) => [
        ...prev,
        {
          id: generateUniqueId(),
          text: finalFormatted.full,
          type: "bot",
          timestamp: new Date().toLocaleString(),
          expandable: true,
          expanded: false,
          summary: finalFormatted.summary,
        },
      ]);
    } catch (error) {
      setMessages((prev) => [
        ...prev,
        {
          id: generateUniqueId(),
          text: `Error: ${error.message}. Please try again.`,
          type: "bot",
          timestamp: new Date().toLocaleString(),
        },
      ]);
    }
    setIsProcessing(false);
  };

  const handleSend = () => {
    if (!query.trim()) return;
    setMessages((prev) => [
      ...prev,
      {
        id: generateUniqueId(),
        text: query,
        type: "user",
        timestamp: new Date().toLocaleString(),
      },
    ]);
    fetchSynthesis(query);
    setQuery("");
  };

  const toggleExpand = (id: string) => {
    setMessages((prev) =>
      prev.map((msg) =>
        msg.id === id && msg.expandable
          ? { ...msg, expanded: !msg.expanded }
          : msg
      )
    );
  };

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100 p-4">
      <div className="w-full max-w-2xl shadow-lg rounded-xl bg-white p-4 space-y-4">
        <div className="h-96 overflow-y-auto border rounded-lg p-3 bg-gray-50">
          {messages.map((msg) => (
            <div
              key={msg.id}
              className={`p-2 ${msg.type === "user" ? "text-right" : "text-left"}`}
            >
              <span className="block text-sm text-gray-500">{msg.timestamp}</span>
              <div
                className={`inline-block p-2 rounded-lg ${
                  msg.type === "user" ? "bg-blue-500 text-white" : "bg-gray-200 text-gray-900"
                }`}
              >
                <pre className="whitespace-pre-wrap">
                  {msg.expandable && !msg.expanded ? msg.summary : msg.text}
                </pre>
                {msg.expandable && (
                  <button
                    onClick={() => toggleExpand(msg.id)}
                    className="text-sm text-blue-500 mt-1 flex items-center"
                  >
                    {msg.expanded ? (
                      <>
                        Collapse <ChevronUp className="ml-1" size={16} />
                      </>
                    ) : (
                      <>
                        Expand <ChevronDown className="ml-1" size={16} />
                      </>
                    )}
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
        <div className="flex space-x-2">
          <Input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Enter stock ticker (e.g., AMZN)"
            disabled={isProcessing}
          />
          <Button onClick={handleSend} disabled={isProcessing}>
            {isProcessing ? "Analyzing..." : "Send"}
          </Button>
        </div>
      </div>
    </div>
  );
}
